import asyncio
import json
import os
import shutil
import sqlite3
from collections import defaultdict
from pathlib import Path

from googleapiclient.http import MediaFileUpload

from app.config import Settings
from app.config_gdrive import folder_id_by_name, SettingsGdrive, calculate_md5, sanitize_filename
from app.routes.auth import load_drive_service, load_drive_service_token
from app.routes.gdrive_from_lokal import save_structured_hashes
from app.utils.logger_config import setup_logger
from app.utils.move_utils import move_single_image
from app.utils.progress import list_all_files, save_simple_hashes, update_progress_text, update_progress
from app.utils.progress_detail import update_detail_status, stop_detail_progress, calc_detail_progress, \
    start_detail_progress, update_detail_progress
from utils.find_duplicate_md5_filnames_local import rename_file

TASK_TYPE = 'gemini'

logger = setup_logger(__name__)


async def get_uploaded_tasks(task_type: str = TASK_TYPE) -> list[tuple[str, str]] | None:
    """Holt alle Tasks mit Status 'uploaded' aus der DB"""
    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            cursor = conn.execute("""
                                  SELECT file_md5, file_name
                                  FROM external_tasks
                                  WHERE status = 'uploaded'
                                    AND task_type = ?
                                  """, (task_type,))
            return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"[{task_type}] Fehler beim Abrufen der uploaded Tasks: {e}")
        return None


async def clear_gemini_tasks(task_type: str = TASK_TYPE):
    """Löscht alle Einträge aus der external_tasks Tabelle"""
    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            logger.info(f"[{task_type}] Lösche alle Einträge aus external_tasks...")
            conn.execute("DELETE FROM external_tasks WHERE task_type = ?", (task_type,))
            logger.info(f"[{task_type}] Alle Einträge gelöscht")
            return True
    except sqlite3.Error as e:
        logger.error(f"[{task_type}] Fehler beim Löschen der Tasks: {e}")
        return False


async def get_task_status(file_md5: str, task_type: str = TASK_TYPE) -> str | None:
    """Prüft den Status einer Aufgabe in der DB"""
    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            cursor = conn.execute(
                """
                SELECT status
                FROM external_tasks
                WHERE file_md5 = ?
                  AND task_type = ?
                """,
                (file_md5, task_type)
            )
            result = cursor.fetchone()
            return result[0] if result else None
    except sqlite3.Error as e:
        logger.error(f"[{task_type}] Fehler beim Abrufen des Task-Status: {e}")
        return None


async def update_task_status(file_md5: str, status: str, task_type: str = TASK_TYPE, drive_file_id: str = None) -> bool:
    """Aktualisiert den Status einer Aufgabe"""
    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            if drive_file_id:
                conn.execute(
                    """
                    UPDATE external_tasks
                    SET status        = ?,
                        drive_file_id = ?,
                        updated_at    = CURRENT_TIMESTAMP
                    WHERE file_md5 = ?
                      AND task_type = ?
                    """,
                    (status, drive_file_id, file_md5, task_type)
                )
            else:
                conn.execute(
                    """
                    UPDATE external_tasks
                    SET status     = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE file_md5 = ?
                      AND task_type = ?
                    """,
                    (status, file_md5, task_type)
                )
            return True
    except sqlite3.Error as e:
        logger.error(f"[{task_type}] Fehler beim Aktualisieren des Task-Status: {e}")
        return False


async def insert_task(file_md5: str, task_type: str, file_name: str, status: str = 'pending') -> bool:
    """Fügt einen neuen Task in die Datenbank ein"""
    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            conn.execute(
                """
                INSERT INTO external_tasks (file_md5, task_type, file_name, status)
                VALUES (?, ?, ?, ?) ON CONFLICT(file_md5) DO
                UPDATE SET
                    task_type = excluded.task_type,
                    file_name = excluded.file_name,
                    status = excluded.status,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (file_md5, task_type, file_name, status)
            )
            return True
    except sqlite3.Error as e:
        logger.error(f"[{task_type}] Fehler beim Einfügen des Tasks: {e}")
        return False


async def move_file_to_textfiles(service, file_id: str, file_name: str, task_type: str) -> bool:
    """Verschiebt eine Datei in den textfiles-Ordner"""
    try:
        textfiles_folder_id = folder_id_by_name(Settings.TEXTFILES_FOLDERNAME)

        # Datei in textfiles-Ordner verschieben
        service.files().update(
            fileId=file_id,
            addParents=textfiles_folder_id,
            removeParents='root',  # oder der aktuelle Parent
            fields='id, parents'
        ).execute()

        logger.info(f"[{task_type}] Datei in textfiles verschoben: {file_name}")
        return True
    except Exception as e:
        logger.error(f"[{task_type}] Fehler beim Verschieben der Datei {file_name}: {e}")
        return False


async def delete_drive_file(service, file_id: str, file_name: str, task_type: str) -> bool:
    """Löscht eine Datei aus Google Drive"""
    try:
        service.files().delete(fileId=file_id).execute()
        logger.info(f"[{task_type}] GDrive: Datei gelöscht: {file_name}")
        return True
    except Exception as e:
        logger.error(f"[{task_type}] GDrive:: Fehler beim Löschen der Datei {file_name}: {e}")
        return False


async def delete_rendered_html_files(md5: str) -> None:
    """Löscht alle gerenderten HTML-Dateien für eine bestimmte MD5"""
    try:
        html_dir = Path(Settings.RENDERED_HTML_DIR)
        if not html_dir.exists():
            return

        # Suche nach allen HTML-Dateien die mit der MD5 beginnen
        pattern = f"{md5}*.html"
        for html_file in html_dir.glob(pattern):
            if html_file.exists():
                html_file.unlink()
                logger.info(f"Gelöschte HTML-Datei: {html_file}")

    except Exception as e:
        logger.error(f"[delete_rendered_html_files] Fehler beim Löschen der HTML-Dateien für MD5 {md5}: {e}")


async def process_completed_task(service, file_name: str, save_files_dict: dict, task_type: str) -> bool:
    """Verarbeitet einen abgeschlossenen Task"""
    try:
        # 1. Text-Datei herunterladen
        text_file_id = save_files_dict.get(f"{file_name}.txt")
        if not text_file_id or not await download_file(service, Path(Settings.TEXT_FILE_CACHE_DIR), text_file_id,
                                                       f"{file_name}.txt"):
            return False

        # 2. Text-Datei in textfiles-Ordner verschieben
        if not await move_file_to_textfiles(service, text_file_id, f"{file_name}.txt", task_type):
            return False

        # Berechne MD5 der heruntergeladenen Text-Datei
        text_path = Path(Settings.TEXT_FILE_CACHE_DIR)
        text_file_full_path = text_path / f"{file_name}.txt"
        text_md5 = calculate_md5(text_file_full_path)
        await update_hash_files(text_path, f"{file_name}.txt", text_md5, text_file_id)

        # 3. Bild-Datei aus Google Drive löschen
        image_file_id = save_files_dict.get(file_name)
        if not image_file_id or not await delete_drive_file(service, image_file_id, file_name, task_type):
            return False

        logger.info(f"[{task_type}] Alle Aktionen erfolgreich für: {file_name}")
        return True
    except Exception as e:
        logger.error(f"[{task_type}] Fehler bei der Verarbeitung von {file_name}: {e}")
        return False


async def check_uploading_tasks(service, image_text_pairs, task_type: str = TASK_TYPE) -> None:
    """Überprüft und verarbeitet alle Tasks mit Status 'uploaded'"""
    logger.info(f"[{task_type}] Prüfe 'uploaded' Tasks...")

    if not image_text_pairs:
        logger.info("Keine Bild/Text Paare im Save-Ordner gefunden")
        return

    try:
        # Hole alle uploaded Tasks
        with sqlite3.connect(Settings.DB_PATH) as conn:
            cursor = conn.execute("""
                                  SELECT file_md5, file_name
                                  FROM external_tasks
                                  WHERE status = 'uploaded'
                                    AND task_type = ?
                                  """, (task_type,))
            uploading_tasks = cursor.fetchall()

        if not uploading_tasks:
            logger.info(f"[{task_type}] Keine 'uploading' Tasks gefunden")
            return

        # Hole alle Dateien aus dem Save-Ordner
        save_folder_id = folder_id_by_name("save")
        save_files = await list_all_files(save_folder_id, service)
        save_files_dict = {f['name']: f['id'] for f in save_files}

        local_gemini_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / task_type

        for file_md5, file_name in uploading_tasks:
            try:
                image_in_save = file_name in save_files_dict
                text_in_save = f"{file_name}.txt" in save_files_dict

                # Finde lokale Datei über MD5
                local_file = None
                if local_gemini_path.exists():
                    for file in local_gemini_path.iterdir():
                        if file.is_file() and file.suffix.lower() in Settings.IMAGE_EXTENSIONS:
                            current_md5 = calculate_md5(file)
                            if current_md5 == file_md5:
                                local_file = file
                                break

                if image_in_save and text_in_save:
                    # Beide Dateien existieren bereits
                    logger.info(f"[{task_type}] Dateien im Save: {file_name}")

                    if await process_completed_task(service, file_name, save_files_dict, task_type):
                        await update_task_status(file_md5, 'completed', task_type)
                    else:
                        await update_task_status(file_md5, 'error', task_type)

                elif local_file is None:
                    # Lokale Datei nicht gefunden
                    logger.warning(f"[{task_type}] Lokale Datei nicht gefunden für MD5: {file_md5}")
                    # update_task_status(file_md5, 'missing', task_type)

            except Exception as e:
                logger.error(f"[{task_type}] Fehler bei Task {file_name}: {e}")
                await update_task_status(file_md5, 'error', task_type)

    except Exception as e:
        logger.error(f"[{task_type}] Fehler bei der Überprüfung der uploading Tasks: {e}")


async def check_file_in_folder(
        service,
        cached_folder_files,
        file_md5,
        file_name) -> dict:
    result = {}  # Initialize result dictionary
    try:
        categories = list(Settings.kategorien())  # Konvertiere zu Liste um sie zu modifizieren
        categories.append({"key": "temp", "label": "Temporär"})  # Füge temp hinzu
        for cat in categories:
            folder_name_cat = cat["key"]
            folder_id_cat = folder_id_by_name(folder_name_cat)
            result[folder_id_cat] = []  # Initialize list for each category

            # Get files from cache or fetch if not cached
            if folder_id_cat not in cached_folder_files:
                page_token = None
                files = []

                while True:
                    response = service.files().list(
                        q=f"'{folder_id_cat}' in parents and trashed=false",
                        fields="nextPageToken, files(id, name, md5Checksum, parents)",
                        pageSize=Settings.PAGESIZE,
                        pageToken=page_token
                    ).execute()

                    files.extend(response.get('files', []))
                    await update_detail_status(f"Gelesen {folder_name_cat} : {len(files)}")

                    page_token = response.get('nextPageToken')
                    if not page_token:
                        cached_folder_files[folder_id_cat] = files  # Cache the files
                        break

            # Use files from cache
            cached_files = cached_folder_files[folder_id_cat]
            for file in cached_files:
                if file.get('md5Checksum') == file_md5 or file.get('name') == file_name:
                    result[folder_id_cat].append(file['id'])

    except Exception as e:
        logger.error(f"Fehler bei der Dateisuche: {e}")

    return result

async def manage_gemini_process(service: None, task_type: str = TASK_TYPE):
    """Überwacht den Gemini-Ordner und verarbeitet Bilddateien"""
    logger.info(f"[{task_type}] Starte Überwachungsprozess")

    if not service:
        service = load_drive_service()

    # Konfiguration
    local_gemini_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / task_type
    gemini_folder_id = folder_id_by_name(task_type)

    # Initialisiere DB-Tabelle
    with sqlite3.connect(Settings.DB_PATH) as conn:
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS external_tasks
                     (
                         file_md5
                         TEXT
                         PRIMARY
                         KEY,
                         task_type
                         TEXT
                         NOT
                         NULL,
                         status
                         TEXT
                         DEFAULT
                         'pending',
                         created_at
                         TIMESTAMP
                         DEFAULT
                         CURRENT_TIMESTAMP,
                         updated_at
                         TIMESTAMP
                         DEFAULT
                         CURRENT_TIMESTAMP,
                         file_name
                         TEXT,
                         drive_file_id
                         TEXT
                     )
                     """)

    gemini_folder_id = folder_id_by_name("gemini")

    while True:
        try:
            await clean_filenames_in_save_folder(service)

            # Hole alle Dateien aus dem Save-Ordner
            save_folder_id = folder_id_by_name("save")
            save_files = await list_all_files(save_folder_id, service)
            save_files_dict = {f['name']: f for f in save_files}

            # Finde Bild/Text Paare
            image_text_pairs = await get_pairs(save_files_dict)

            # Überprüfe uploading Tasks in jedem Durchlauf
            await check_uploading_tasks(service, image_text_pairs, task_type)

            await process_save_folder_pairs(service, image_text_pairs)

            # Hole nur erlaubte Bilddateien
            local_files = [f for f in local_gemini_path.iterdir()
                           if f.is_file() and f.suffix.lower() in Settings.IMAGE_EXTENSIONS]

            if not local_files:
                await asyncio.sleep(60)
                continue

            cached_folder_files = {}

            # Verarbeite Dateien
            moved = 0
            for file in local_files:
                # MD5 berechnen
                file_md5 = calculate_md5(file)
                try:
                    current_status = await get_task_status(file_md5, task_type)

                    # Überspringe, wenn bereits erfolgreich hochgeladen
                    if current_status:
                        logger.info(f"[{task_type}] Datei bereits verarbeitet: {file.name} (MD5: {file_md5})")
                        continue

                    found_folders_dict = await check_file_in_folder(service, cached_folder_files, file_md5, file.name)
                    found_folders = [folder for folder, files in found_folders_dict.items() if files]
                    found_file_ids = [file_id for folder_files in found_folders_dict.values() for file_id in
                                      folder_files]

                    if len(found_folders) == 1 and len(found_file_ids) == 1:
                        if found_folders[0] != gemini_folder_id:
                            service.files().update(
                                fileId=found_file_ids[0],
                                addParents=gemini_folder_id,
                                removeParents=found_folders[0],
                                fields='id, parents'
                            ).execute()
                            logger.info(
                                f"[{task_type}] Datei verschoben: {file.name} (MD5: {file_md5}) in Gemini-Ordner")
                        else:
                            logger.info(f"[{task_type}] Datei bereits vorhanden: {file.name} (MD5: {file_md5})")
                        continue
                    else:
                        logger.error(
                            f"[{task_type}] Datei in {len(found_folders)} Ordnern mit {len(found_file_ids)} Dateien: {file.name} (MD5: {file_md5})")
                        continue

                    # Füge neuen Task hinzu oder aktualisiere Status auf 'uploading'
                    await insert_task(file_md5, task_type, file.name, 'uploading')

                    # Upload zu Google Drive
                    file_metadata = {
                        'name': file.name,
                        'parents': [gemini_folder_id]
                    }
                    media = MediaFileUpload(str(file), resumable=True)
                    result = service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id'
                    ).execute()

                    # Status aktualisieren
                    await update_task_status(file_md5, 'uploaded', task_type, result['id'])
                    moved += 1
                    logger.info(f"[{task_type}] Datei hochgeladen: {file.name} (MD5: {file_md5})")

                except Exception as e:
                    logger.error(f"[{task_type}] Fehler beim Verarbeiten von {file.name}: {e}")
                    await update_task_status(file_md5, 'error', task_type)

            if moved > 0:
                logger.info(f"[{task_type}] Verarbeitung abgeschlossen: {moved} neue Bilddateien hochgeladen")

            del cached_folder_files

            await asyncio.sleep(60)

        except Exception as e:
            logger.error(f"[{task_type}] Unerwarteter Fehler im Prozess: {e}")
            await asyncio.sleep(300)


async def get_pairs(save_files_dict):
    image_text_pairs = {}
    for file_name, file_info in save_files_dict.items():
        if any(file_name.lower().endswith(ext) for ext in Settings.IMAGE_EXTENSIONS):
            base_name = file_name
            text_file = base_name + '.txt'
            if text_file in save_files_dict:
                image_text_pairs[base_name] = {
                    'image': file_info,
                    'text': save_files_dict[text_file]
                }
    return image_text_pairs


async def download_file(service, target_dir: Path, file_id: str, file_name: str) -> bool:
    """Downloads a file from Google Drive"""
    try:
        file_path = target_dir / file_name
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with file_path.open('wb') as f:
            downloader = service.files().get_media(fileId=file_id)
            f.write(downloader.execute())

        logger.info(f"Datei heruntergeladen: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Fehler beim Herunterladen von {file_name}: {e}")
        return False


async def update_hash_files(directory: Path, file_name: str, file_md5: str, file_id: str) -> None:
    """Aktualisiert beide Hash-Dateien in einem Verzeichnis"""
    try:
        # Update GDrive hash file
        gdrive_hash_path = directory / Settings.GDRIVE_HASH_FILE
        gdrive_hashes = {}
        if gdrive_hash_path.exists():
            try:
                with gdrive_hash_path.open('r', encoding='utf-8') as f:
                    gdrive_hashes = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Fehler beim Lesen der GDrive Hash-Datei: {e}")
                # Erstelle Backup der beschädigten Datei
                backup_path = gdrive_hash_path.with_suffix('.bak')
                gdrive_hash_path.rename(backup_path)
                gdrive_hashes = {}

        gdrive_hashes[file_name] = {
            'md5': file_md5,
            'id': file_id
        }
        await save_structured_hashes(gdrive_hashes, gdrive_hash_path)

        # Update Gallery hash file
        gallery_hash_path = directory / Settings.GALLERY_HASH_FILE
        gallery_hashes = {}
        if gallery_hash_path.exists():
            try:
                with gallery_hash_path.open('r', encoding='utf-8') as f:
                    gallery_hashes = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Fehler beim Lesen der Gallery Hash-Datei: {e}")
                # Erstelle Backup der beschädigten Datei
                backup_path = gallery_hash_path.with_suffix('.bak')
                gallery_hash_path.rename(backup_path)
                gallery_hashes = {}

        gallery_hashes[file_name] = file_md5
        await save_simple_hashes(gallery_hashes, gallery_hash_path)

        # Text-Cache aktualisieren wenn es eine Textdatei ist
        if file_name.lower().endswith('.txt'):
            file_name_without_ext = file_name.lower()[:-4]
            Settings.CACHE["text_cache"].pop(file_name_without_ext, None)
            await delete_rendered_html_files(file_md5)

        logger.info(f"Hash-Dateien aktualisiert für: {file_name}")
    except Exception as e:
        logger.error(f"Fehler beim Aktualisieren der Hash-Dateien für {file_name}: {e}")


async def clean_filenames_in_save_folder(service) -> bool:
    """Bereinigt Dateinamen im 'save' Ordner von Zeilenumbrüchen"""
    try:
        save_folder_id = folder_id_by_name("save")

        # Alle Dateien im save-Ordner auflisten
        files = service.files().list(
            q=f"'{save_folder_id}' in parents and trashed=false",
            fields="files(id, name)"
        ).execute().get('files', [])

        cleaned_count = 0
        for file in files:
            original_name = file['name']
            clean_name = sanitize_filename(original_name)
            if original_name != clean_name:
                try:
                    # Datei umbenennen
                    service.files().update(
                        fileId=file['id'],
                        body={'name': clean_name}
                    ).execute()
                    cleaned_count += 1
                    logger.info(f"Datei bereinigt: '{original_name}' -> '{clean_name}'")
                except Exception as e:
                    logger.error(f"Fehler beim Bereinigen von '{original_name}': {e}")

        logger.info(f"Bereinigung abgeschlossen. {cleaned_count} Dateien korrigiert.")
        return True
    except Exception as e:
        logger.error(f"Fehler bei der Dateinamenbereinigung: {e}")
        return False


async def process_save_folder_pairs(service, image_text_pairs) -> None:
    """Verarbeitet Bild/Text-Datei Paare im Save-Ordner unabhängig von external_tasks"""
    logger.info("Prüfe Save-Ordner auf Bild/Text Paare...")

    try:
        if not image_text_pairs:
            logger.info("Keine Bild/Text Paare im Save-Ordner gefunden")
            return

        # Verarbeite gefundene Paare
        for base_name, files in image_text_pairs.items():
            try:
                # Text-Datei verarbeiten
                text_file = files['text']
                text_path = Path(Settings.TEXT_FILE_CACHE_DIR)
                if await download_file(service, text_path, text_file['id'], f"{base_name}.txt"):
                    text_md5 = calculate_md5(text_path / f"{base_name}.txt")
                    await update_hash_files(text_path, f"{base_name}.txt", text_md5, text_file['id'])
                else:
                    logger.error(f"Fehler beim Herunterladen der Textdatei: {base_name}.txt")
                    continue

                # Prüfe ob Bild bereits existiert
                image_exists = False
                for category in Settings.kategorien():
                    category_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / category["key"]
                    if category_path.exists():
                        image_name = files['image']['name']
                        image_path = category_path / image_name
                        if image_path.exists():
                            logger.info(f"Bild {image_name} bereits in Kategorie {category['key']} vorhanden")
                            if category["key"] != Settings.RECHECK:
                                await move_single_image(image_name, category["key"], Settings.RECHECK)
                                image_exists = True
                            break

                # Bild herunterladen wenn nicht vorhanden
                if not image_exists:
                    image_file = files['image']
                    image_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / Settings.RECHECK
                    if await download_file(service, image_path, image_file['id'], image_file['name']):
                        image_md5 = calculate_md5(image_path / image_file['name'])
                        await update_hash_files(image_path, image_file['name'], image_md5, image_file['id'])
                    else:
                        logger.error(f"Fehler beim Herunterladen des Bildes: {image_file['name']}")
                        continue

                save_folder_id = folder_id_by_name("save")
                recheck_folder_id = folder_id_by_name(Settings.RECHECK)
                textfiles_folder_id = folder_id_by_name(Settings.TEXTFILES_FOLDERNAME)

                # Verschiebe Dateien in Google Drive
                try:
                    # Text-Datei verschieben
                    service.files().update(
                        fileId=text_file['id'],
                        addParents=textfiles_folder_id,
                        removeParents=save_folder_id,
                        fields='id, parents'
                    ).execute()
                    logger.info(f"Textdatei nach textfiles verschoben: {base_name}.txt")

                    # Bild verschieben
                    service.files().update(
                        fileId=files['image']['id'],
                        addParents=recheck_folder_id,
                        removeParents=save_folder_id,
                        fields='id, parents'
                    ).execute()
                    logger.info(f"Bild nach recheck verschoben: {files['image']['name']}")

                except Exception as e:
                    logger.error(f"Fehler beim Verschieben der Dateien für {base_name}: {e}")

            except Exception as e:
                logger.error(f"Fehler bei der Verarbeitung von {base_name}: {e}")

    except Exception as e:
        logger.error(f"Fehler bei der Überprüfung des Save-Ordners: {e}")

async def rename_double_files_to_md5_and_move_to_recheck_local(image_cache_dir):
    recheck_dir = image_cache_dir / Settings.RECHECK
    recheck_dir.mkdir(exist_ok=True)

    # Group files by complete filename (including extension)
    name_groups = defaultdict(list)
    md5_groups = defaultdict(list)

    # Scan through all category directories
    total_dirs = sum(1 for d in image_cache_dir.iterdir() if d.is_dir())
    for dir_idx, category_dir in enumerate(image_cache_dir.iterdir(), 1):
        if not category_dir.is_dir():
            continue

        await update_progress_text(f"Verarbeite Verzeichnis {category_dir.name} ({dir_idx}/{total_dirs})")

        # Count files for progress
        total_files = sum(1 for f in category_dir.glob('*')
                          if f.is_file() and f.suffix.lower() in Settings.IMAGE_EXTENSIONS)

        # Scan all image files in this category
        try:
            await start_detail_progress(f"Verarbeite Dateien {total_files}")
            for file_idx, file_path in enumerate(category_dir.glob('*'), 1):
                if file_path.is_file() and file_path.suffix.lower() in Settings.IMAGE_EXTENSIONS:
                    md5 = calculate_md5(file_path)
                    file_info = {
                        'path': file_path,
                        'name': file_path.name,
                        'md5': md5,
                        'category': category_dir.name
                    }
                    name_groups[file_path.name].append(file_info)
                    md5_groups[md5].append(file_info)

                    # Detail progress nur alle 100 Dateien oder bei der letzten Datei
                    if file_idx % 100 == 0 or file_idx == total_files:
                        progress = calc_detail_progress(file_idx, total_files)
                        await update_detail_progress(
                            f"Verarbeite Datei {file_idx}/{total_files}",
                            progress
                        )

        except Exception as e:
            await stop_detail_progress(f"❌ Fehler bei Verarbeitung: {e}")
        finally:
            await stop_detail_progress(f"✅ {total_files} Dateien verarbeitet")

        # Update overall progress after each directory
        await update_progress(
            f"Verzeichnis {category_dir.name} abgeschlossen ({len(name_groups)} Dateien gefunden)",
            int((dir_idx / total_dirs) * 100)
        )

    # Verarbeite Dateien mit identischen Namen
    for filename, files in name_groups.items():
        if len(files) > 1:
            await update_progress_text(f"\nVerarbeite Dateien mit identischem Namen: {filename}")

            # Verarbeite lokale Dateien
            for file in files:
                try:
                    # Verwende MD5 als neuen Namen mit Original-Erweiterung
                    base_name, ext = os.path.splitext(filename)
                    new_name = f"{file['md5']}{ext}"

                    # Erstelle Zielpfade
                    source_path = file['path']
                    recheck_path = recheck_dir / new_name

                    # Erst umbenennen und dann verschieben
                    if source_path.exists():
                        # Wenn die Zieldatei bereits existiert, füge eine Nummer hinzu
                        counter = 1
                        while recheck_path.exists():
                            new_name = f"{file['md5']}_{counter}{ext}"
                            recheck_path = recheck_dir / new_name
                            counter += 1

                        # Verschiebe die Datei nach recheck mit neuem Namen
                        shutil.move(str(source_path), str(recheck_path))
                        await update_progress_text(f"Verschoben nach recheck: {file['name']} -> {new_name}")

                except Exception as e:
                    await update_progress_text(f"Fehler beim Verarbeiten von {file['name']}: {e}")

    changetemp = False
    for md5, files in md5_groups.items():
        if len(files) >= 2:
            # Für mehrfache Dateien: Zeige MD5 und alle Pfade
            file_paths = [f"{f['category']}/{f['name']}" for f in files]
            logger.info(f"🔄 Mehrfach gefunden MD5 {md5}:")
            for path in file_paths:
                logger.info(f"    └── {path}")

async def rename_double_files_to_md5_and_move_to_recheck_gdrive(service):
    cached_folder_files = {}
    await check_file_in_folder(service, cached_folder_files, None, None)

    recheck_folder_id = folder_id_by_name(Settings.RECHECK)
    if not recheck_folder_id:
        await update_progress_text(f"Could not find 'recheck' folder ID")
        return

    # Create recheck directory locally if it doesn't exist
    local_recheck_dir = Path(Settings.IMAGE_FILE_CACHE_DIR) / Settings.RECHECK
    local_recheck_dir.mkdir(exist_ok=True)

    # Group files by complete filename (including extension)
    name_groups = defaultdict(list)
    md5_groups = defaultdict(list)
    for folder_id, files in cached_folder_files.items():
        for file in files:
            if file['name'].lower().endswith(tuple(Settings.IMAGE_EXTENSIONS)):
                name_groups[file['name']].append({
                    'folder_id': folder_id,
                    'file_id': file['id'],
                    'md5': file['md5Checksum'],
                    'name': file['name']
                })
                md5_groups[file['md5Checksum']].append({
                    'folder_id': folder_id,
                    'file_id': file['id'],
                    'md5': file['md5Checksum'],
                    'name': file['name']
                })

    changetemp = False
    for filename, files in md5_groups.items():
        if len(files) >= 2:
            # Zähle Dateien in temp und nicht in temp
            temp_files = []
            non_temp_files = []
            for file in files:
                if file['folder_id'] == folder_id_by_name('temp'):
                    temp_files.append(file)
                else:
                    non_temp_files.append(file)

            # Wenn genau eine Datei NICHT in temp ist und der Rest in temp
            if len(non_temp_files) == 1 and len(temp_files) >= 1:
                for temp_file in temp_files:
                    try:
                        service.files().delete(fileId=temp_file['file_id']).execute()
                        await update_progress_text(f"🗑️ Gelöscht aus temp: {temp_file['name']}")
                        changetemp = True
                    except Exception as e:
                        await update_progress_text(f"❌ Fehler beim Löschen von {temp_file['name']}: {e}")
                continue
        elif len(files) == 1:
            file = files[0]
            # Wenn die einzige Datei in temp ist, verschiebe sie nach recheck
            if file['folder_id'] == folder_id_by_name('temp'):
                try:
                    recheck_folder_id = folder_id_by_name('recheck')
                    # Verschiebe die Datei nach recheck
                    service.files().update(
                        fileId=file['file_id'],
                        addParents=recheck_folder_id,
                        removeParents=file['folder_id']
                    ).execute()
                    await update_progress_text(f"↗️ Verschoben nach recheck: {file['name']}")
                    changetemp = True
                except Exception as e:
                    await update_progress_text(f"❌ Fehler beim Verschieben von {file['name']}: {e}")
                continue

    if changetemp:
        await rename_double_files_to_md5_and_move_to_recheck_gdrive(service)

    # Process only files with identical names
    for filename, files in name_groups.items():
        if len(files) > 1:
            await update_progress_text(f"\nProcessing files with identical name: {filename}")

            # Process GDrive files
            for file in files:
                try:
                    # Use MD5 as the new name with original extension
                    base_name, ext = os.path.splitext(filename)
                    new_name = f"{file['md5']}{ext}"

                    # First rename the file
                    rename_file(service, file['file_id'], new_name)
                    await update_progress_text(f"Renamed in GDrive: {file['name']} -> {new_name}")

                    # Then move it to recheck folder
                    file_metadata = service.files().get(
                        fileId=file['file_id'],
                        fields='parents'
                    ).execute()
                    previous_parents = ','.join(file_metadata.get('parents', []))

                    # Move to recheck folder
                    service.files().update(
                        fileId=file['file_id'],
                        addParents=recheck_folder_id,
                        removeParents=previous_parents,
                        fields='id, parents'
                    ).execute()
                    await update_progress_text(f"Moved to recheck: {new_name}")

                except Exception as e:
                    await update_progress_text(f"Error processing file in GDrive {file['name']}: {e}")

def p4():
    """Konfiguration und Start des Gemini-Prozesses"""
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")
    Settings.RENDERED_HTML_DIR = "../../cache/rendered_html"

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))

    asyncio.run(clean_filenames_in_save_folder(service))
    save_folder_id = folder_id_by_name("save")
    save_files = asyncio.run(list_all_files(save_folder_id, service))
    save_files_dict = {f['name']: f for f in save_files}

    # Finde Bild/Text Paare
    image_text_pairs = asyncio.run(get_pairs(save_files_dict))
    asyncio.run(process_save_folder_pairs(service, image_text_pairs))

def p5():
    """Konfiguration und Start des Gemini-Prozesses"""
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")
    Settings.RENDERED_HTML_DIR = "../../cache/rendered_html"

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))

    asyncio.run(manage_gemini_process(service, TASK_TYPE))


def p9():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))
    #asyncio.run(rename_double_files_to_md5_and_move_to_recheck_gdrive(service))
    asyncio.run(rename_double_files_to_md5_and_move_to_recheck_local(Path(Settings.IMAGE_FILE_CACHE_DIR)))

if __name__ == "__main__":
    p9()