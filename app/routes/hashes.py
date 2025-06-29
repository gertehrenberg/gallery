import asyncio
import hashlib
import json
import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Any
from typing import Optional

from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

from app.config import Settings, score_type_map
from app.config_gdrive import sanitize_filename, folder_id_by_name, SettingsGdrive, collect_all_folders, calculate_md5
from app.database import clear_folder_status_db_by_name
from app.routes.auth import load_drive_service_token
from app.routes.gdrive_from_lokal import save_structured_hashes
from app.tools import readimages
from app.utils.db_utils import save_folder_status_to_db, load_folder_status_from_db
from app.utils.progress import init_progress_state, progress_state, update_progress, update_progress_text, \
    save_simple_hashes, hold_progress, stop_progress
from app.utils.progress_detail import update_detail_status, update_detail_progress


async def process_text_files():
    file_folder_dir = Settings.TEXT_FILE_CACHE_DIR
    await update_progress_text("🧮 Verarbeite Textdateien")

    # Initialize progress state
    await init_progress_state()
    progress_state["running"] = True

    if Settings.TEXT_FILE_CACHE_DIR == file_folder_dir:
        text_cache = Settings.CACHE["text_cache"]
        text_cache.clear()
        await update_progress_text("Cache geleert")

    # Update database entries
    await update_progress_text("🔄 Aktualisiere Datenbankeinträge")
    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            query = """
                    UPDATE image_quality_scores
                    SET image_name = SUBSTR(image_name, 1, LENGTH(image_name) - 4)
                    WHERE score_type = ?
                      AND LOWER(image_name) LIKE '%.txt'
                    """
            conn.execute(query, (score_type_map['text'],))
            conn.commit()
        await update_progress_text("✅ Datenbankeinträge aktualisiert")
    except Exception as e:
        await update_progress_text(f"❌ Fehler bei Datenbankaktualisierung: {e}")

    # Collect files
    subdir = Path(file_folder_dir)
    local_hashes: Dict[str, str] = {}
    image_files = [f for f in subdir.iterdir() if f.is_file() and f.suffix.lower() in Settings.TEXT_EXTENSIONS]

    total_files = len(image_files)
    if total_files == 0:
        await update_progress_text("ℹ️ Keine Textdateien gefunden")
        return

    await update_progress_text(f"🔄 Verarbeite {total_files} Textdateien")

    # Initialize progress for file processing
    await update_detail_progress(
        detail_status="Starte Verarbeitung",
        detail_progress=0
    )

    # Process files
    total_files = len(image_files)
    for idx, file in enumerate(image_files):
        image_name = file.name.lower()
        current_progress = int((idx / total_files) * 1000)

        await update_detail_progress(
            detail_status=f"Verarbeite {image_name} ({idx + 1}/{total_files})",
            detail_progress=current_progress
        )

        try:
            md5_local = calculate_md5(file)
            local_hashes[image_name] = md5_local

            # Search for PNG files
            from app.services.image_processing import find_png_file
            image_name_bild = image_name[:-4]
            matching_files = find_png_file(image_name_bild)

            if matching_files:
                found_file = matching_files[0]
                found_file_str = str(found_file.name)

                # Database check and processing...
                with sqlite3.connect(Settings.DB_PATH) as conn:
                    cursor = conn.execute("""
                                          SELECT score
                                          FROM image_quality_scores
                                          WHERE image_name = ?
                                            AND score_type = ?
                                          """, (found_file_str, score_type_map['text']))
                    db_result = cursor.fetchone()

                if db_result and db_result[0]:
                    await update_detail_status(
                        f"DB Eintrag gefunden: {db_result[0]} Zeichen"
                    )
                else:
                    # Process new file...
                    from app.services.image_processing import download_text_file
                    content = download_text_file(
                        folder_name=found_file.parent.name,
                        image_name=found_file.name,
                        cache_dir=Settings.TEXT_FILE_CACHE_DIR
                    )

                    if content:
                        with sqlite3.connect(Settings.DB_PATH) as conn:
                            conn.execute("""
                                INSERT OR REPLACE INTO image_quality_scores 
                                (image_name, score_type, score)
                                VALUES (?, ?, ?)
                            """, (found_file_str, score_type_map['text'], len(content)))
                            await update_detail_status(
                                f"Neuer Eintrag: {len(content)} Zeichen"
                            )

        except Exception as e:
            await update_detail_status(f"Fehler: {str(e)}")

    # Mark completion
    await update_detail_progress(
        detail_status="Verarbeitung abgeschlossen",
        detail_progress=1000
    )

    # Save hashes
    hashfile_name = Settings.GALLERY_HASH_FILE
    hash_path = subdir / hashfile_name
    await update_progress_text(f"💾 Speichere {len(local_hashes)} Hashes")
    await save_simple_hashes(local_hashes, hash_path)

    await update_progress_text(f"✅ Verarbeitung abgeschlossen: {total_files} Dateien")


def is_valid_image(filename: str) -> bool:
    """Prüft, ob eine Datei eine gültige Bilddatei ist."""
    return any(filename.lower().endswith(ext) for ext in Settings.IMAGE_EXTENSIONS)


async def delete_all_hashfiles_async(file_folder_dir: str, subfolders: bool = True) -> int:
    """Löscht alle Hash-Dateien in einem Verzeichnis und optional seinen Unterverzeichnissen."""
    await init_progress_state()
    await update_progress_text("🔄 Starte Löschvorgang der Hash-Dateien...")

    root = Path(file_folder_dir)
    all_dirs = [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]
    deleted = 0
    total_dirs = len(all_dirs)

    for idx, subdir in enumerate(all_dirs, 1):
        await update_progress(f"Verarbeite Verzeichnis {subdir.name}", int((idx / total_dirs) * 100))

        for file in subdir.glob(f"*{Settings.GDRIVE_HASH_FILE}"):
            try:
                file.unlink()
                await update_progress_text(f"🗑️ Gelöscht: {file}")
                deleted += 1
            except Exception as e:
                await update_progress_text(f"❌ Fehler beim Löschen von {file}: {e}")

    await update_progress_text(f"✅ Hash-Dateien gelöscht: {deleted}")
    await hold_progress()
    return deleted


async def update_local_hashes(folder_name):
    local_cache = {}
    folder_path = os.path.join(Settings.IMAGE_FILE_CACHE_DIR, folder_name)

    await update_progress_text(f"📁 Verarbeite Kategorie: {folder_name}")

    await readimages(folder_path, local_cache)

    clear_folder_status_db_by_name(Settings.DB_PATH, folder_name)

    for image_name, entry in local_cache.items():
        image_id = entry.get('image_id')
        if image_id:
            save_folder_status_to_db(Settings.DB_PATH, image_id, folder_name)

    hash_file = Path(folder_path) / Settings.GALLERY_HASH_FILE
    local_hashes = {name: data.get('image_id', '') for name, data in local_cache.items()}

    try:
        await save_simple_hashes(local_hashes, hash_file)
        await update_progress_text(f"✅ {len(local_hashes)} Hashes gespeichert für {folder_name}")
    except Exception as e:
        await update_progress_text(f"❌ Fehler beim Speichern der Hashes für {folder_name}: {e}")


async def update_all_local_hashes():
    """Aktualisiert die Hash-Dateien für alle Kategorien."""
    await init_progress_state()
    progress_state["running"] = True

    try:
        total_kategorien = len(Settings.kategorien)

        for idx, kategorie in enumerate(Settings.kategorien, 1):
            folder_name = kategorie["key"]
            folder_path = os.path.join(Settings.IMAGE_FILE_CACHE_DIR, folder_name)

            await update_progress(f"Kategorie {folder_name}", int((idx / total_kategorien) * 100))
            await update_local_hashes(folder_path)

        await update_progress_text("✅ Hash-Aktualisierung abgeschlossen")

    except Exception as e:
        await update_progress_text(f"❌ Fehler bei der Hash-Aktualisierung: {e}")
    finally:
        await hold_progress()


async def update_all_gdrive_hashes(service) -> None:
    """Aktualisiert die Hashes für alle Google Drive Ordner."""
    await update_progress_text("🔄 Starte Google Drive Hash-Aktualisierung...")

    total_kategorien = len(Settings.kategorien)
    for idx, kategorie in enumerate(Settings.kategorien, 1):
        folder_name = kategorie["key"]
        folder_id = folder_id_by_name(folder_name)

        if not folder_id:
            await update_progress_text(f"⚠️ Keine Folder-ID gefunden für: {folder_name}")
            continue

        await update_progress(f"Verarbeite {folder_name}", int((idx / total_kategorien) * 100))
        await update_gdrive_hashes(service, folder_name)


async def process_files_with_progress(
        files: list,
        extension,
        status_prefix: str = "") -> dict:
    """
    Verarbeitet eine Liste von Dateien mit Fortschrittsanzeige.

    Args:
        files: Liste der zu verarbeitenden Dateien
        status_prefix: Prefix für die Statusmeldung

    Returns:
        Dict mit verarbeiteten Dateien
    """
    gdrive_hashes = {}
    total_files = len(files)
    files_processed = 0

    # Initialize progress
    await update_detail_progress(
        detail_status=f"{status_prefix}Starte Verarbeitung...",
        detail_progress=0
    )

    for item in files:
        name = sanitize_filename(item.get('name', ''))
        fext = any(name.endswith(ext) for ext in extension)

        if name and fext:
            md5_checksum = item.get('md5Checksum')
            file_id = item.get('id')
            if md5_checksum and file_id:
                gdrive_hashes[name] = {
                    "md5": md5_checksum,
                    "id": file_id
                }
                files_processed += 1

                # Update progress
                progress = int((files_processed / total_files) * 1000)
                await update_detail_progress(
                    detail_status=f"{status_prefix}Verarbeite {name} ({files_processed}/{total_files})",
                    detail_progress=progress
                )

    # Final progress update
    await update_detail_progress(
        detail_status=f"{status_prefix}Verarbeitung abgeschlossen: {files_processed} Dateien",
        detail_progress=1000
    )

    return gdrive_hashes


async def update_gdrive_hashes(
        service,
        folder_name: Optional[str] = None,
        extension=Settings.IMAGE_EXTENSIONS,
        base_dir=Path(Settings.IMAGE_FILE_CACHE_DIR)):
    """Liest Dateien aus Google Drive und aktualisiert die hashes.json Dateien."""
    await update_progress_text(f"🔄 Aktualisiere GDrive Hashes{' für ' + folder_name if folder_name else ''}")
    await init_progress_state()
    progress_state["running"] = True

    try:
        folders_to_process = [base_dir / folder_name] if folder_name else [p for p in base_dir.iterdir() if p.is_dir()]
        total_folders = len(folders_to_process)

        for folder_idx, folder_path in enumerate(sorted(folders_to_process), 1):
            current_folder = folder_path.name
            folder_id = folder_id_by_name(current_folder)

            if not folder_id:
                await update_progress_text(f"⚠️ Keine Folder-ID gefunden für: {current_folder}")
                continue

            await update_progress_text(f"📂 Verarbeite {current_folder} ({folder_idx}/{total_folders})")

            gdrive_hashes: Dict[str, Dict[str, str]] = {}
            page_token = None

            w = 1
            while True:
                await update_progress(f"Lesen von {page_token}", w)
                w += 2
                try:
                    results = service.files().list(
                        q=f"'{folder_id}' in parents and trashed=false",
                        spaces='drive',
                        fields="nextPageToken, files(id, name, md5Checksum)",
                        pageSize=Settings.PAGESIZE,
                        pageToken=page_token
                    ).execute()

                    gdrive_hashes = await process_files_with_progress(
                        results.get('files', []),
                        extension,
                        status_prefix="🔄 "
                    )

                    page_token = results.get('nextPageToken')
                    if not page_token:
                        break

                except Exception as e:
                    await update_progress_text(f"❌ Fehler beim Lesen von {current_folder}: {e}")
                    break

            if not gdrive_hashes:
                gdrive_hashes = {}
            try:
                hash_file_path = folder_path / Settings.GDRIVE_HASH_FILE
                await save_structured_hashes(gdrive_hashes, hash_file_path)
                await update_progress_text(f"✅ {current_folder}: {len(gdrive_hashes)} Einträge gespeichert")
            except Exception as e:
                await update_progress_text(f"❌ Fehler beim Speichern für {current_folder}: {e}")

        await update_progress_text("✅ GDrive Hash-Aktualisierung abgeschlossen")

    except Exception as e:
        await update_progress_text(f"❌ Fehler bei Hash-Aktualisierung: {e}")
    finally:
        await hold_progress()


async def reloadcache_progress(service, folder_key: Optional[str] = None):
    """
    Reloads the cache for folders based on the folder_key parameter.

    Args:
        folder_key: Optional folder key.
                   If None, processes all categories.
                   If in CHECKBOX_CATEGORIES, processes only that folder.
                   If "textfiles", processes text files.
    """
    try:
        await init_progress_state()
        progress_state["running"] = True
        await update_progress_text(f"🔄 Starte reloadcache_progress für Ordner: {folder_key}")
        Settings.folders_loaded = 0

        if folder_key == Settings.TEXTFILES_FOLDERNAME:
            await update_progress_text("🗃️ Modus: Textverarbeitung")
            await update_gdrive_hashes(
                service,
                folder_key,
                Settings.TEXT_EXTENSIONS,
                Path(Settings.TEXT_FILE_CACHE_DIR).parent)
            await process_text_files()

        elif folder_key in Settings.CHECKBOX_CATEGORIES:
            await update_progress_text(f"📂 Modus: Einzelne Kategorie ({folder_key})")
            await update_gdrive_hashes(service, folder_key)
            await update_local_hashes(folder_key)
            Settings.folders_loaded += 1

        else:
            await update_progress_text("📂 Modus: Alle Kategorien")
            pair_cache = Settings.CACHE.get("pair_cache")
            pair_cache.clear()

            for kategorie in Settings.kategorien:
                folder_key = kategorie["key"]
                await update_gdrive_hashes(service, folder_key)
                await update_local_hashes(folder_key)
                Settings.folders_loaded += 1

            await update_progress_text("🗃️ Modus: Textverarbeitung")
            await update_gdrive_hashes(
                service,
                folder_key,
                Settings.TEXT_EXTENSIONS,
                Settings.TEXT_FILE_CACHE_DIR.parent)
            await process_text_files()

            await process_image_folders_gdrive_progress(service, folder_key)
            await update_progress_text(f"[✓] Hash-Datei aktualisiert für {folder_key}")

    except Exception as e:
        await update_progress_text(f"❌ Fehler beim Reload-Cache: {e}")
        raise
    finally:
        await update_progress_text("✅ reloadcache_progress abgeschlossen")
        await stop_progress()


async def process_image_folders_gdrive_progress(service, folder_name: str):
    folder_id = folder_id_by_name(folder_name)
    if not folder_id:
        raise ValueError(f"Keine Folder-ID gefunden für: {folder_name}")

    await update_progress_text(f"Lade Dateien aus {folder_name}...")
    files = []
    page_token = None

    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            spaces='drive',
            fields="nextPageToken, files(id, name, md5Checksum)",
            pageSize=Settings.PAGESIZE,
            pageToken=page_token
        ).execute()

        batch = [f for f in response.get('files', [])
                 if is_valid_image(f.get('name', ''))]
        files.extend(batch)
        await update_progress_text(f"Geladen: {len(files)} Dateien...")

        page_token = response.get('nextPageToken')
        if not page_token:
            break

    gdrive_hashes = {}
    for file in files:
        try:
            name = sanitize_filename(file['name'])
            if is_valid_image(name):
                md5_drive = file.get("md5Checksum")
                if md5_drive:
                    gdrive_hashes[name] = {
                        "md5": md5_drive,
                        "id": file['id']
                    }
        except Exception as e:
            await update_progress_text(f"[Fehler] {file['name']}: {e}")

    hashfile_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name / Settings.GDRIVE_HASH_FILE
    hashfile_path.parent.mkdir(parents=True, exist_ok=True)
    with hashfile_path.open("w", encoding="utf-8") as f:
        json.dump(gdrive_hashes, f, indent=2)
    await update_progress_text(f"Hash-Datei gespeichert für {folder_name}")


async def _process_image_files_progress(image_files, folder_key, file_parents_cache, db_path):
    folder_name = label = next((k["label"] for k in Settings.kategorien if k["key"] == folder_key), None)
    total = len(image_files)
    for index, image_file in enumerate(image_files):
        await update_progress(f"Kategorie: {folder_name} : {total} Dateien ({image_file})",
                              int(index / total * 100), 0.02)
        if not image_file.is_file() or image_file.suffix.lower() not in Settings.IMAGE_EXTENSIONS:
            continue
        image_name = image_file.name.lower()
        pair = Settings.CACHE["pair_cache"].get(image_name)
        if not pair:
            await update_progress_text(
                f"[_process_image_files_progress] ⚠️ Kein Eintrag im pair_cache für: {image_name}")
            continue
        await update_progress_text(
            f"[_process_image_files_progress] ✅️ Eintrag im pair_cache für: {folder_key} / {image_name}")
        image_id = pair["image_id"]
        file_parents_cache[folder_key].append(image_id)

        save_folder_status_to_db(db_path, image_id, folder_key)


async def _load_file_parents_cache_from_db(db_path: str, file_parents_cache: dict) -> bool:
    rows = load_folder_status_from_db(db_path)
    if not rows:
        return False
    await update_progress_text("[fill_folder_cache] 📦 Lade file_parents_cache aus der Datenbank...")
    for image_id, folder_key in rows:
        if folder_key not in file_parents_cache:
            Settings.folders_loaded += 1
            file_parents_cache[folder_key] = []
            await update_progress_text(
                f"[fill_folder_cache] ✅ Cache aus DB geladen: {Settings.folders_loaded}/{Settings.folders_total} {folder_key}")
        file_parents_cache[folder_key].append(image_id)
    if Settings.folders_loaded != Settings.folders_total:
        Settings.folders_loaded = Settings.folders_total
        await update_progress_text(
            f"[fill_folder_cache] ✅ Cache aus DB geladen: {Settings.folders_loaded}/{Settings.folders_total}")
    return True


async def verify_file_cache_consistency(base_dir, folder_name: Optional[str] = None) -> dict:
    """
    Überprüft die Konsistenz zwischen dem lokalen Cache und den tatsächlichen Dateien im Ordner.

    Args:
        folder_name: Optional - Spezifischer Ordner zum Überprüfen. Wenn None, werden alle Ordner überprüft.

    Returns:
        Dict mit Statistiken und Inkonsistenzen
    """
    await init_progress_state()
    await update_progress_text("🔍 Starte lokale Cache-Konsistenzprüfung...")

    result = {
        "statistics": {
            "total_files": 0,
            "cached_files": 0,
            "missing_in_cache": [],
            "missing_in_folder": [],
            "processed_folders": 0
        },
        "inconsistencies": {}
    }

    try:
        folders_to_check = ([base_dir / folder_name] if folder_name
                            else [p for p in base_dir.iterdir() if p.is_dir()])

        total_folders = len(folders_to_check)

        for folder_idx, folder_path in enumerate(folders_to_check, 1):
            current_folder = folder_path.name
            await update_progress_text(f"📁 Prüfe Ordner: {folder_path} ({folder_idx}/{total_folders})")

            # Sammle alle Dateien im Ordner
            folder_files = set()
            try:
                folder_files = {
                    f.name.lower() for f in folder_path.iterdir()
                    if f.is_file() and f.suffix.lower() in Settings.IMAGE_EXTENSIONS
                }
            except Exception as e:
                await update_progress_text(f"⚠️ Fehler beim Lesen von {current_folder}: {e}")
                continue

            # Sammle alle Dateien im lokalen Hash-Cache
            hash_cache_path = folder_path / Settings.GALLERY_HASH_FILE
            cached_files = set()

            try:
                if hash_cache_path.exists():
                    with hash_cache_path.open('r') as f:
                        cache_data = json.load(f)
                        cached_files = {name.lower() for name in cache_data.keys()}
            except Exception as e:
                await update_progress_text(f"⚠️ Fehler beim Lesen der Cache-Datei für {current_folder}: {e}")
                continue

            # Finde Unterschiede
            missing_in_cache = folder_files - cached_files
            missing_in_folder = cached_files - folder_files

            # Aktualisiere Statistiken
            result["statistics"]["total_files"] += len(folder_files)
            result["statistics"]["cached_files"] += len(cached_files)
            result["statistics"]["processed_folders"] += 1

            if missing_in_cache or missing_in_folder:
                result["inconsistencies"][current_folder] = {
                    "missing_in_cache": list(missing_in_cache),
                    "missing_in_folder": list(missing_in_folder)
                }

                if missing_in_cache:
                    await update_progress_text(
                        f"⚠️ {len(missing_in_cache)} Dateien in {current_folder} nicht im Cache"
                    )
                if missing_in_folder:
                    await update_progress_text(
                        f"⚠️ {len(missing_in_folder)} Cache-Einträge in {current_folder} nicht im Ordner"
                    )

            result["statistics"]["missing_in_cache"].extend(
                f"{current_folder}/{f}" for f in missing_in_cache
            )
            result["statistics"]["missing_in_folder"].extend(
                f"{current_folder}/{f}" for f in missing_in_folder
            )

            # Update progress
            await update_progress(
                f"Ordner verarbeitet: {folder_idx}/{total_folders}",
                int((folder_idx / total_folders) * 100)
            )

        # Abschlussbericht
        total_inconsistencies = len(result["statistics"]["missing_in_cache"]) + \
                                len(result["statistics"]["missing_in_folder"])

        if total_inconsistencies == 0:
            await update_progress_text("✅ Lokaler Cache ist vollständig konsistent!")
        else:
            await update_progress_text(
                f"⚠️ {total_inconsistencies} Inkonsistenzen gefunden:\n"
                f"   • {len(result['statistics']['missing_in_cache'])} Dateien nicht im Cache\n"
                f"   • {len(result['statistics']['missing_in_folder'])} Cache-Einträge nicht im Ordner"
            )

    except Exception as e:
        await update_progress_text(f"❌ Fehler bei der Cache-Überprüfung: {e}")
    finally:
        await stop_progress()

    return result


async def delete_duplicates_in_gdrive_folder(service, folder_id: str) -> None:
    """
    Löscht doppelte Dateien (basierend auf MD5 Hash) in einem Google Drive Ordner.

    Args:
        service: Google Drive Service Objekt
        folder_id: ID des zu prüfenden Ordners
    """
    try:
        await update_progress_text("🔍 Suche nach Dateien im Ordner...")

        # Alle Dateien im Ordner abrufen
        files = []
        page_token = None
        while True:
            response = service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                spaces='drive',
                fields='nextPageToken, files(id, name, md5Checksum)',
                pageSize=Settings.PAGESIZE,
                pageToken=page_token
            ).execute()

            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken')
            if not page_token:
                break

        # MD5 Hashes gruppieren
        md5_groups = {}
        for file in files:
            md5 = file.get('md5Checksum')
            if md5:
                if md5 not in md5_groups:
                    md5_groups[md5] = []
                md5_groups[md5].append(file)

        # Duplikate finden und löschen
        deleted_count = 0
        for md5, file_group in md5_groups.items():
            if len(file_group) > 1:
                # Behalte die erste Datei, lösche den Rest
                original = file_group[0]
                duplicates = file_group[1:]

                await update_progress_text(f"🔍 Gefunden: {original['name']} hat {len(duplicates)} Duplikate")

                for dup in duplicates:
                    try:
                        service.files().delete(fileId=dup['id']).execute()
                        deleted_count += 1
                        await update_progress_text(f"🗑️ Gelöscht: {dup['name']}")
                    except Exception as e:
                        await update_progress_text(f"❌ Fehler beim Löschen von {dup['name']}: {str(e)}")

        await update_progress_text(f"✅ Abgeschlossen: {deleted_count} Duplikate gelöscht")

    except Exception as e:
        await update_progress_text(f"❌ Fehler: {str(e)}")


async def move_duplicates_in_folder(folder_path: str) -> dict[str, list[Path]]:
    """
    Verschiebt doppelte Bilddateien in einem lokalen Ordner und seinen Unterordnern
    in den TEMP_DIR_PATH Ordner.
    """
    files_by_md5: Dict[str, List[Path]] = {}

    try:
        base_folder = Path(folder_path)
        folder_name = base_folder.name
        await update_progress_text(f"[{folder_name}] 🔍 Initialisiere...")

        # Temp-Ordner vorbereiten
        Settings.TEMP_DIR_PATH.mkdir(parents=True, exist_ok=True)

        # Sammle alle Unterordner und Dateien vorab für bessere Fortschrittsanzeige
        folders: Dict[str, Path] = {folder_name: base_folder}
        total_files = 0
        files_to_process = []

        await update_progress_text(f"[{folder_name}] 📂 Sammle Dateien...")

        # Sammle erst alle Ordner und zähle Dateien
        for subdir in base_folder.rglob("*"):
            if subdir.is_dir():
                folders[subdir.name] = subdir
                for file_path in subdir.iterdir():
                    if file_path.is_file() and any(
                            file_path.name.lower().endswith(ext) for ext in Settings.IMAGE_EXTENSIONS):
                        total_files += 1
                        files_to_process.append(file_path)

        # Hauptordner auch durchsuchen
        for file_path in base_folder.iterdir():
            if file_path.is_file() and any(file_path.name.lower().endswith(ext) for ext in Settings.IMAGE_EXTENSIONS):
                total_files += 1
                files_to_process.append(file_path)

        await update_progress_text(f"[{folder_name}] 📂 {len(folders)} Ordner, {total_files} Dateien gefunden")

        # Verarbeite Dateien
        processed_files = 0
        last_progress = 0

        for file_path in files_to_process:
            try:
                # Dateinamen überprüfen und ggf. umbenennen
                original_name = file_path.name
                sanitized_name = sanitize_filename(original_name)

                if original_name != sanitized_name:
                    try:
                        new_path = file_path.parent / sanitized_name
                        file_path.rename(new_path)
                        file_path = new_path
                    except Exception as e:
                        await update_progress_text(
                            f"[{folder_name}] ❌ Fehler beim Umbenennen von {original_name}: {str(e)}")
                        continue

                # MD5 Hash berechnen
                with open(file_path, 'rb') as f:
                    md5 = hashlib.md5(f.read()).hexdigest()
                    if md5 not in files_by_md5:
                        files_by_md5[md5] = []
                    files_by_md5[md5].append(file_path)

                processed_files += 1
                current_progress = int((processed_files / total_files) * 100)

                # Update nur bei signifikanter Änderung
                if current_progress - last_progress >= 5:
                    await update_progress_text(
                        f"[{folder_name}] 🔍 Analysiere Dateien: {processed_files}/{total_files} ({current_progress}%)",
                        ctime=0.01)
                    last_progress = current_progress

            except Exception as e:
                await update_progress_text(
                    f"[{folder_name}] ❌ Fehler bei {file_path.name}: {str(e)}")

        # Duplikate in temp Ordner verschieben
        duplicates_found = sum(len(files) - 1 for files in files_by_md5.values() if len(files) > 1)
        await update_progress_text(f"[{folder_name}] 🎯 {duplicates_found} Duplikate gefunden")

        moved_count = 0
        for md5, file_list in files_by_md5.items():
            if len(file_list) > 1:
                original = file_list[0]
                duplicates = file_list[1:]

                for dup in duplicates:
                    try:
                        target_path = Settings.TEMP_DIR_PATH / dup.name
                        if target_path.exists():
                            base_name = target_path.stem
                            suffix = target_path.suffix
                            counter = 1
                            while target_path.exists():
                                target_path = Settings.TEMP_DIR_PATH / f"{base_name}_{counter}{suffix}"
                                counter += 1

                        dup.rename(target_path)
                        moved_count += 1

                        # Update nur bei jedem 5. Move oder bei Vielfachen von 10
                        if moved_count % 5 == 0 or moved_count % 10 == 0:
                            await update_progress_text(
                                f"[{folder_name}] 📦 {moved_count}/{duplicates_found} Duplikate verschoben",
                                ctime=0.01)
                    except Exception as e:
                        await update_progress_text(
                            f"[{folder_name}] ❌ Fehler beim Verschieben von {dup.name}: {str(e)}",
                            ctime=0.01)

        await update_progress_text(
            f"[{folder_name}] ✅ Abgeschlossen: {moved_count} von {duplicates_found} Duplikaten verschoben")

    except Exception as e:
        await update_progress_text(f"[{folder_path}] ❌ Fehler: {str(e)}")

    return files_by_md5


async def download_file(service, file_id, local_path):
    request = service.files().get_media(fileId=file_id)
    with open(local_path, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()


async def upload_file_to_gdrive(service, file_path: Path, target_folder_id: str) -> bool:
    """
    Lädt eine Datei in Google Drive hoch.

    Args:
        service: Google Drive Service Objekt
        file_path: Path Objekt zur lokalen Datei
        target_folder_id: ID des Zielordners in Google Drive

    Returns:
        bool: True wenn Upload erfolgreich, False bei Fehler
    """
    try:
        await update_progress_text(f"⬆️ Lade {file_path.name} hoch...")

        # Metadata für die Datei
        file_metadata = {
            'name': file_path.name,
            'parents': [target_folder_id]
        }

        # MediaFileUpload Objekt erstellen
        media = MediaFileUpload(
            str(file_path),
            mimetype='image/*',
            resumable=True
        )

        # Upload mit Progress-Updates
        request = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        )

        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                await update_progress(
                    f"⬆️ Upload von {file_path.name}",
                    int(status.progress() * 100)
                )

        await update_progress_text(f"✅ {file_path.name} erfolgreich hochgeladen (ID: {response.get('id')})")
        return True

    except Exception as e:
        await update_progress_text(f"❌ Fehler beim Hochladen von {file_path.name}: {e}")
        return False


async def dd(
        service: object,
        files_by_md5: Dict[str, List[Path]] | None,
        md5_groups_gdrive: dict[Any, Any] | None) -> None:
    """

    :type service: object
    """
    # Speichern der aktuellen Daten
    cache_dir = Path(Settings.IMAGE_FILE_CACHE_DIR).parent
    cache_dir.mkdir(exist_ok=True)

    files_cache = cache_dir / "files_by_md5.json"
    gdrive_cache = cache_dir / "md5_groups_gdrive.json"

    # Laden der Caches, wenn Parameter None sind
    if files_by_md5 is None:
        try:
            with files_cache.open('r') as f:
                # Konvertiere Pfad-Strings zurück zu Path-Objekten
                data = json.load(f)
                files_by_md5 = {
                    md5: [Path(p) for p in paths]
                    for md5, paths in data.items()
                }
        except (FileNotFoundError, json.JSONDecodeError):
            files_by_md5 = {}

    if md5_groups_gdrive is None:
        try:
            with gdrive_cache.open('r') as f:
                md5_groups_gdrive = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            md5_groups_gdrive = {}

    if not files_by_md5 or not md5_groups_gdrive:
        return

    try:
        # Konvertiere Path-Objekte zu Strings für JSON-Serialisierung
        serializable_files = {
            md5: [str(p) for p in paths]
            for md5, paths in files_by_md5.items()
        }
        with files_cache.open('w') as f:
            json.dump(serializable_files, f, indent=2)

        with gdrive_cache.open('w') as f:
            json.dump(md5_groups_gdrive, f, indent=2)
    except Exception as e:
        await update_progress_text(f"❌ Fehler beim Speichern der Caches: {str(e)}")

    folder_name = "Alle"
    # Nach dem Verschieben der Duplikate, Vergleich mit GDrive durchführen
    await update_progress_text(f"[{folder_name}] 🔄 Vergleiche mit GDrive-Hashes...")

    # Finde Dateien die nur lokal existieren
    local_only_hashes = set(files_by_md5.keys()) - set(md5_groups_gdrive.keys())
    if local_only_hashes:
        await update_progress_text(f"[{folder_name}] 📌 {len(local_only_hashes)} Dateien nur lokal gefunden")

        for md5 in local_only_hashes:
            local_files = files_by_md5[md5]
            for file_path in local_files:
                try:
                    # Bestimme den Zielordner aus dem Elternverzeichnis der Datei
                    target_folder = file_path.parent.name
                    target_folder_id = folder_id_by_name(target_folder)

                    if not target_folder_id:
                        await update_progress_text(
                            f"[{folder_name}] ⚠️ Keine Folder-ID für: {target_folder}")
                        continue

                    # Upload durchführen
                    success = await upload_file_to_gdrive(
                        service,
                        file_path,
                        target_folder_id
                    )

                    if success:
                        await update_progress_text(
                            f"[{folder_name}] 📤 Hochgeladen: {file_path.name} → {target_folder}")

                except Exception as e:
                    await update_progress_text(
                        f"[{folder_name}] ❌ Upload-Fehler bei {file_path.name}: {str(e)}")
                    continue

    # Finde Dateien die nur in GDrive existieren
    gdrive_only_hashes = set(md5_groups_gdrive.keys()) - set(files_by_md5.keys())
    if gdrive_only_hashes:
        await update_progress_text(f"[{folder_name}] ☁️ {len(gdrive_only_hashes)} Dateien nur in GDrive gefunden")
        for md5 in gdrive_only_hashes:
            gdrive_files = md5_groups_gdrive[md5]
            for file_info in gdrive_files:
                name = file_info.get('name');
                await update_progress_text(f"[{folder_name}] ☁️ Nur GDrive: {name}")
                try:
                    file_id = file_info.get('id')
                    local_file = Path(Settings.IMAGE_FILE_CACHE_DIR) / "recheck" / name
                    await download_file(service, file_id, local_file)
                    if local_file.exists():
                        await update_progress_text(f"📥 Heruntergeladen: {local_file}")
                except Exception as e:
                    await update_progress_text(f"❌ Download-Fehler bei {name}: {e}")

    # Finde Unterschiede bei Dateien die in beiden existieren
    common_hashes = set(files_by_md5.keys()) & set(md5_groups_gdrive.keys())
    name_mismatches = []

    for md5 in common_hashes:
        local_names = {f.name for f in files_by_md5[md5]}
        gdrive_names = {f.get('name', '') for f in md5_groups_gdrive[md5]}

        # Prüfe auf Namensunterschiede
        if local_names != gdrive_names:
            name_mismatches.append({
                'md5': md5,
                'local_names': local_names,
                'gdrive_names': gdrive_names
            })

    if name_mismatches:
        await update_progress_text(
            f"[{folder_name}] ⚠️ {len(name_mismatches)} Dateien mit unterschiedlichen Namen gefunden")
        for mismatch in name_mismatches:
            try:
                if not mismatch['local_names']:
                    await update_progress_text(
                        f"[{folder_name}] ⚠️ Keine lokalen Namen für MD5: {mismatch['md5']}",
                        ctime=0.01)
                    continue

                local_name = mismatch['local_names'].pop()
                sanitized_name = sanitize_filename(local_name)

                # Hier fehlt die GDrive File ID - wir müssen sie aus md5_groups_gdrive holen
                gdrive_files = md5_groups_gdrive[mismatch['md5']]
                for gdrive_file in gdrive_files:
                    service.files().update(
                        fileId=gdrive_file['id'],
                        body={'name': sanitized_name}
                    ).execute()
                    await update_progress_text(
                        f"[{folder_name}] ✏️ Umbenannt: {gdrive_file['name']} → {sanitized_name}")

            except Exception as e:
                await update_progress_text(
                    f"[{folder_name}] ❌ Fehler beim Umbenennen (MD5: {mismatch['md5']}): {str(e)}")
                continue

    summary = (
        f"[{folder_name}] 📊 Zusammenfassung:\n"
        f"- {len(local_only_hashes)} Dateien nur lokal\n"
        f"- {len(gdrive_only_hashes)} Dateien nur in GDrive\n"
        f"- {len(name_mismatches)} Dateien mit Namensunterschieden"
    )


async def move_duplicates_to_temp(
        service: object,
        md5_groups: dict,
        temp_folder_id: str,
        folder_name: str
) -> int:
    """
    Verschiebt Duplikate in einen temporären Ordner.

    Args:
        service: Google Drive Service
        md5_groups: Dictionary mit MD5-Hash als Key und Liste von Dateien als Value
        temp_folder_id: ID des temporären Ordners
        folder_name: Name des aktuellen Ordners für Logging

    Returns:
        int: Anzahl der verschobenen Dateien
    """
    moved_count = 0
    total_duplicates = sum(len(file_group) - 1
                           for file_group in md5_groups.values()
                           if len(file_group) > 1)

    if total_duplicates == 0:
        return 0

    await update_progress(f"[{folder_name}] 🔍 Verarbeite Duplikate", 0)

    current_count = 0
    for md5, file_group in md5_groups.items():
        if len(file_group) > 1:
            original = file_group[0]
            duplicates = file_group[1:]

            for dup in duplicates:
                try:
                    service.files().update(
                        fileId=dup['id'],
                        addParents=temp_folder_id,
                        removeParents=dup['parents'][0]
                    ).execute()
                    moved_count += 1
                    current_count += 1

                    # Update nur alle 5% oder bei jedem 10ten File
                    if current_count % 10 == 0 or (current_count / total_duplicates) * 100 % 5 == 0:
                        progress = int((current_count / total_duplicates) * 100)
                        await update_progress(
                            f"[{folder_name}] 📦 Verschiebe Duplikate ({current_count}/{total_duplicates})",
                            progress
                        )

                except Exception as e:
                    await update_progress_text(
                        f"[{folder_name}] ❌ Fehler beim Verschieben von {dup['name']}: {str(e)}")
                    current_count += 1
                    continue

    await update_progress(f"[{folder_name}] ✅ {moved_count} Duplikate verschoben", 100)
    return moved_count


async def move_duplicates_in_gdrive_folder(service, folder_id: str) -> dict[Any, Any]:
    """
    Verschiebt doppelte Bilddateien in einem Google Drive Ordner und seinen Unterordnern
    in einen 'temp' Ordner.
    """
    md5_groups = {}

    try:
        folder_name = service.files().get(fileId=folder_id, fields="name").execute().get("name", "Unbekannt")
        await update_progress_text(f"[{folder_name}] 🔍 Initialisiere Suche nach Duplikaten...")

        # Hole temp Ordner ID
        temp_folder_id = folder_id_by_name("temp")
        await update_progress_text(f"[{folder_name}] 📁 Temp Ordner ID: {temp_folder_id}")

        # Sammle erst alle Ordner
        name_to_id = {}
        id_to_name = {}
        collect_all_folders(service, folder_id, name_to_id, id_to_name)

        # Alle Ordner-IDs sammeln (inkl. Ursprungsordner)
        folder_ids = list(id_to_name.keys())
        folder_ids.append(folder_id)

        files = []
        for current_folder_id in folder_ids:
            current_folder_name = id_to_name.get(current_folder_id, "Ursprungsordner")
            try:
                await update_progress_text(f"[{folder_name}/{current_folder_name}] 📂 Verarbeite Ordner...")

                # Query für aktuellen Ordner
                query = f"'{current_folder_id}' in parents and trashed=false"

                # Dateien für aktuellen Ordner abrufen
                page_token = None
                while True:
                    response = service.files().list(
                        q=query,
                        spaces='drive',
                        fields='nextPageToken, files(id, name, md5Checksum, parents)',
                        pageSize=Settings.PAGESIZE,
                        pageToken=page_token
                    ).execute()

                    batch = response.get('files', [])
                    files.extend(batch)
                    await update_progress_text(
                        f"[{folder_name}/{current_folder_name}] 📂 {len(files)} Dateien gefunden...")

                    page_token = response.get('nextPageToken')
                    if not page_token:
                        break
            except Exception as e:
                await update_progress_text(f"[{folder_name}/{current_folder_name}] ❌ Fehler beim Verarbeiten: {str(e)}")

        # Zuerst alle Dateinamen überprüfen und ggf. umbenennen
        for file in files:
            if not any(file.get('name', '').lower().endswith(ext) for ext in Settings.IMAGE_EXTENSIONS):
                continue

            original_name = file.get('name', '')
            sanitized_name = sanitize_filename(original_name)

            if original_name != sanitized_name:
                try:
                    service.files().update(
                        fileId=file['id'],
                        body={'name': sanitized_name}
                    ).execute()
                    await update_progress_text(f"[{folder_name}] ✏️ Umbenannt: {original_name} → {sanitized_name}")
                    file['name'] = sanitized_name
                except Exception as e:
                    await update_progress_text(
                        f"[{folder_name}] ❌ Fehler beim Umbenennen von {original_name}: {str(e)}")

        # Duplikate nach MD5 gruppieren
        for file in files:
            if not any(file.get('name', '').lower().endswith(ext) for ext in Settings.IMAGE_EXTENSIONS):
                continue

            md5 = file.get('md5Checksum')
            if md5:
                if md5 not in md5_groups:
                    md5_groups[md5] = []
                md5_groups[md5].append(file)

        # Verschiebe Duplikate
        moved_count = await move_duplicates_to_temp(
            service=service,
            md5_groups=md5_groups,
            temp_folder_id=temp_folder_id,
            folder_name=folder_name
        )

        await update_progress_text(
            f"[{folder_name}] ✅ Abgeschlossen: {moved_count} Duplikate in temp Ordner verschoben")

    except Exception as e:
        await update_progress_text(f"[{folder_name}] ❌ Fehler: {str(e)}")

    return md5_groups


async def update_local_hash(directory: Path, file_name: str, file_md5: str, addordel: bool) -> None:
    """
    Aktualisiert die lokale Hash-Datei in einem Verzeichnis.

    Args:
        directory: Verzeichnispfad
        file_name: Name der Datei
        file_md5: MD5-Hash der Datei
        addordel: True zum Hinzufügen, False zum Entfernen des Hashes
    """
    try:
        hash_path = directory / Settings.GALLERY_HASH_FILE
        local_hashes = {}

        if hash_path.exists():
            try:
                with hash_path.open('r', encoding='utf-8') as f:
                    local_hashes = json.load(f)
            except json.JSONDecodeError as e:
                await update_progress_text(f"⚠️ Hash-Datei beschädigt: {e}")
                backup_path = hash_path.with_suffix('.bak')
                hash_path.rename(backup_path)

        old_hash = local_hashes.get(file_name)

        if addordel:
            if old_hash != file_md5:
                local_hashes[file_name] = file_md5
                await update_progress_text(f"➕ Hash hinzugefügt - {file_name}: {file_md5}")
        else:
            if file_name in local_hashes:
                del local_hashes[file_name]
                await update_progress_text(f"➖ Hash entfernt - {file_name}")

        await save_simple_hashes(local_hashes, hash_path)

    except Exception as e:
        await update_progress_text(f"❌ Hash-Update fehlgeschlagen - {file_name}: {str(e)}")
        raise


def p5():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    for idx, kat in enumerate(Settings.kategorien, 1):
        folder_name = kat["key"]
        result = asyncio.run(verify_file_cache_consistency(Path(Settings.IMAGE_FILE_CACHE_DIR), folder_name))
        if len(result['inconsistencies']) > 0:
            # Die Ergebnisse auswerten
            print(f"Geprüfte Dateien: {result['statistics']['total_files']}")
            print(f"Cache-Einträge: {result['statistics']['cached_files']}")
            print(f"Inkonsistente Ordner: {len(result['inconsistencies'])}")

    result = asyncio.run(
        verify_file_cache_consistency(Path(Settings.TEXT_FILE_CACHE_DIR).parent, Settings.TEXTFILES_FOLDERNAME))
    if len(result['inconsistencies']) > 0:
        # Die Ergebnisse auswerten
        print(f"Geprüfte Dateien: {result['statistics']['total_files']}")
        print(f"Cache-Einträge: {result['statistics']['cached_files']}")
        print(f"Inkonsistente Ordner: {len(result['inconsistencies'])}")


async def mache_alles(service):
    await init_progress_state()
    progress_state["running"] = True
    await update_progress_text("🔄 Starting duplicate detection")

    await update_all_local_hashes()
    await update_all_gdrive_hashes(service)
    await dd(service, None, None)

    files_by_md5 = await  move_duplicates_in_folder(Settings.IMAGE_FILE_CACHE_DIR)

    md5_groups_gdrive = await move_duplicates_in_gdrive_folder(service, folder_id_by_name("imagefiles"))

    await dd(service, files_by_md5, md5_groups_gdrive)
    await update_all_local_hashes()
    await update_all_gdrive_hashes(service)


def p6():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))

    asyncio.run(mache_alles(service))


def p7():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))

    asyncio.run(reloadcache_progress(service, Settings.TEXTFILES_FOLDERNAME))


if __name__ == "__main__":
    p7()
