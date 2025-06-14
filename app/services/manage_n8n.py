import asyncio
import os
import sqlite3
from pathlib import Path

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

from app.config import Settings
from app.config_gdrive import folder_id_by_name, SettingsGdrive, calculate_md5
from app.routes.auth import load_drive_service, load_drive_service_token
from app.utils.logger_config import setup_logger
from app.utils.progress import list_all_files

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
                SELECT status FROM external_tasks 
                WHERE file_md5 = ? AND task_type = ?
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
                    SET status = ?, 
                        drive_file_id = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE file_md5 = ? 
                    AND task_type = ?
                    """,
                    (status, drive_file_id, file_md5, task_type)
                )
            else:
                conn.execute(
                    """
                    UPDATE external_tasks 
                    SET status = ?, 
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
                VALUES (?, ?, ?, ?)
                ON CONFLICT(file_md5) DO UPDATE SET 
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


async def download_text_file(service, file_id: str, file_name: str, task_type: str) -> bool:
    """Lädt eine Textdatei aus Google Drive herunter"""
    try:
        text_file_path = Path(Settings.TEXT_FILE_CACHE_DIR) / file_name
        request = service.files().get_media(fileId=file_id)

        with open(text_file_path, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        logger.info(f"[{task_type}] Text-Datei heruntergeladen: {file_name}")
        return True
    except Exception as e:
        logger.error(f"[{task_type}] Fehler beim Herunterladen der Text-Datei {file_name}: {e}")
        return False


async def move_file_to_textfiles(service, file_id: str, file_name: str, task_type: str) -> bool:
    """Verschiebt eine Datei in den textfiles-Ordner"""
    try:
        textfiles_folder_id = folder_id_by_name("textfiles")

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


async def delete_rendered_html_files(file_md5: str, task_type: str) -> bool:
    """Löscht alle MD5-bezogenen HTML-Dateien"""
    try:
        rendered_html_dir = Settings.RENDERED_HTML_DIR
        if not rendered_html_dir.exists():
            return True

        pattern = f"{file_md5}*.j2"
        deleted = False
        for file in rendered_html_dir.glob(pattern):
            file.unlink()
            deleted = True

        if deleted:
            logger.info(f"[{task_type}] Gerenderte HTML-Dateien gelöscht für MD5: {file_md5}")
        return True
    except Exception as e:
        logger.error(f"[{task_type}] Fehler beim Löschen der HTML-Dateien für MD5 {file_md5}: {e}")
        return False


async def process_completed_task(service, file_md5: str, file_name: str, save_files_dict: dict, task_type: str) -> bool:
    """Verarbeitet einen abgeschlossenen Task"""
    try:
        # 1. Text-Datei herunterladen
        text_file_id = save_files_dict.get(f"{file_name}.txt")
        if not text_file_id or not await download_text_file(service, text_file_id, f"{file_name}.txt", task_type):
            return False

        # 2. Text-Datei in textfiles-Ordner verschieben
        if not await move_file_to_textfiles(service, text_file_id, f"{file_name}.txt", task_type):
            return False

        # 3. Bild-Datei aus Google Drive löschen
        image_file_id = save_files_dict.get(file_name)
        if not image_file_id or not await delete_drive_file(service, image_file_id, file_name, task_type):
            return False

        # 4. Gerenderte HTML-Dateien löschen
        if not await delete_rendered_html_files(file_md5, task_type):
            return False

        logger.info(f"[{task_type}] Alle Aktionen erfolgreich für: {file_name}")
        return True
    except Exception as e:
        logger.error(f"[{task_type}] Fehler bei der Verarbeitung von {file_name}: {e}")
        return False


async def check_uploading_tasks(service, task_type: str = TASK_TYPE) -> None:
    """Überprüft und verarbeitet alle Tasks mit Status 'uploaded'"""
    logger.info(f"[{task_type}] Prüfe 'uploaded' Tasks...")

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

                    if await process_completed_task(service, file_md5, file_name, save_files_dict, task_type):
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
            CREATE TABLE IF NOT EXISTS external_tasks (
                file_md5 TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_name TEXT,
                drive_file_id TEXT
            )
        """)

    while True:
        try:
            # Überprüfe uploading Tasks in jedem Durchlauf
            await check_uploading_tasks(service, task_type)

            # Hole nur erlaubte Bilddateien
            local_files = [f for f in local_gemini_path.iterdir()
                           if f.is_file() and f.suffix.lower() in Settings.IMAGE_EXTENSIONS]

            if not local_files:
                await asyncio.sleep(60)
                continue

            # Verarbeite Dateien
            moved = 0
            for file in local_files:
                # MD5 berechnen
                file_md5 = calculate_md5(file)
                try:

                    current_status = get_task_status(file_md5, task_type)

                    # Überspringe, wenn bereits erfolgreich hochgeladen
                    if current_status:
                        logger.info(f"[{task_type}] Datei bereits verarbeitet: {file.name} (MD5: {file_md5})")
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

            await asyncio.sleep(60)

        except Exception as e:
            logger.error(f"[{task_type}] Unerwarteter Fehler im Prozess: {e}")
            await asyncio.sleep(300)


def p4():
    """Konfiguration und Start des Gemini-Prozesses"""
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")
    Settings.RENDERED_HTML_DIR = "../../cache/rendered_html"

    # clear_gemini_tasks()

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))
    asyncio.run(manage_gemini_process(service))


if __name__ == "__main__":
    p4()
