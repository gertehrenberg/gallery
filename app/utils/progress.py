import asyncio
import json
import os
import sqlite3
from pathlib import Path
from typing import Dict

from app.config import Settings, score_type_map
from app.config_gdrive import folder_name_by_id, calculate_md5
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

logger.info("Initializing progress state")
progress_state = {
    "progress": 0,
    "status": "Warte auf Start...",
    "running": False
}


async def update_progress(status: str, progress: int, ctime=0.01, showlog=True):
    if isinstance(status, str) and len(status) > 0:
        progress_state["status"] = status
    progress_state["progress"] = progress
    if showlog:
        logger.info(f"{status} : {progress}")
    await asyncio.sleep(ctime)


async def update_progress_text(status: str, ctime=0.01, showlog=True):
    if isinstance(status, str) and len(status) > 0:
        progress_state["status"] = status
    if showlog:
        logger.info(f"{status}")
    await asyncio.sleep(ctime)


async def init_progress_state():
    logger.info("Initializing progress state")
    progress_state["running"] = False
    await update_progress("Warte auf Start...", 0)


async def stop_progress():
    logger.info("Stopping progress")
    progress_state["running"] = False
    await update_progress("Abgeschlossen.", 100)


async def hold_progress():
    logger.info("Hold progress")
    progress_state["running"] = False
    await update_progress_text("Warte ...")


async def list_files(folder_id, service, sign="!="):
    logger.info(f"Starting list_files with folder_id: {folder_id}, sign: {sign}")
    files = []
    page_token = None
    count = 0
    folder_name = folder_name_by_id(folder_id)
    logger.info(f"📂 Starte Dateiliste für Folder-ID: {folder_name}")
    await update_progress(f"Dateien werden aus Google Drive geladen ({folder_name})...", 0)

    while True:
        logger.info(f"📄 Lade Seite {count + 1} ...")
        logger.info(f"Making API request with pageToken: {page_token}")
        response = service.files().list(
            q=f"'{folder_id}' in parents and mimeType {sign} 'text/plain' and trashed=false",
            fields="nextPageToken, files(id, name, size, md5Checksum, parents)",
            pageSize=Settings.PAGESIZE,
            pageToken=page_token
        ).execute()

        files_batch = response.get('files', [])
        logger.info(f"🔢 {len(files_batch)} Dateien auf dieser Seite gefunden")

        files.extend(files_batch)
        count += 1

        progress_state["progress"] += 1
        if progress_state["progress"] > 100:
            logger.info("Progress reset to 0 as it exceeded 100")
            progress_state["progress"] = 0
        await asyncio.sleep(0.1)

        page_token = response.get('nextPageToken', None)
        if not page_token:
            logger.info("No more pages to process")
            break

    await update_progress(f"{len(files)} Dateien geladen.", 100)
    logger.info(f"✅ Insgesamt {len(files)} Dateien geladen aus {count} Seiten")
    await asyncio.sleep(0.5)

    return files


async def list_all_files(folder_id, service):
    logger.info(f"Starting list_files with folder_id: {folder_id}")
    files = []
    page_token = None
    count = 0
    folder_name = folder_name_by_id(folder_id)
    logger.info(f"📂 Starte Dateiliste für Folder-ID: {folder_name}")
    await update_progress(f"Dateien werden aus Google Drive geladen ({folder_name})...", 0)

    while True:
        logger.info(f"📄 Lade Seite {count + 1} ...")
        logger.info(f"Making API request with pageToken: {page_token}")
        response = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, size, md5Checksum, parents)",
            pageSize=Settings.PAGESIZE,
            pageToken=page_token
        ).execute()

        files_batch = response.get('files', [])
        logger.info(f"🔢 {len(files_batch)} Dateien auf dieser Seite gefunden")

        files.extend(files_batch)
        count += 1

        progress_state["progress"] += 1
        if progress_state["progress"] > 100:
            logger.info("Progress reset to 0 as it exceeded 100")
            progress_state["progress"] = 0
        await asyncio.sleep(0.1)

        page_token = response.get('nextPageToken', None)
        if not page_token:
            logger.info("No more pages to process")
            break

    await update_progress(f"{len(files)} Dateien geladen.", 100)
    logger.info(f"✅ Insgesamt {len(files)} Dateien geladen aus {count} Seiten")
    await asyncio.sleep(0.5)

    return files


async def write_local_hashes_progress(extensions, file_folder_dir, subfolders: bool = True):
    logger.info(f"Starting write_local_hashes_progress with dir: {file_folder_dir}, subfolders: {subfolders}")

    if Settings.TEXT_FILE_CACHE_DIR == file_folder_dir:
        text_cache = Settings.CACHE["text_cache"]
        text_cache.clear()

    with sqlite3.connect(Settings.DB_PATH) as conn:
        query = """
                UPDATE image_quality_scores
                SET image_name = SUBSTR(image_name, 1, LENGTH(image_name) - 4)
                WHERE score_type = ?
                  AND LOWER(image_name) LIKE '%.txt' \
                """
        conn.execute(query, (score_type_map['text'],))
        conn.commit()

    root = Path(file_folder_dir)
    all_dirs = [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]

    total_dirs = len(all_dirs)
    logger.info(f"Total directories to process: {total_dirs}")
    dir_counter = 0

    for subdir in all_dirs:
        logger.info(f"Processing directory: {subdir}")
        local_hashes: Dict[str, str] = {}
        image_files = [f for f in subdir.iterdir() if f.is_file() and f.suffix.lower() in extensions]

        total_files = len(image_files)
        logger.info(f"Found {total_files} files to process in {subdir}")
        file_counter = 0

        for file in image_files:
            image_name = file.name.lower()
            try:
                md5_local = calculate_md5(file)
                local_hashes[image_name] = md5_local

                # Suche nach PNG-Dateien mit den ersten 4 Zeichen des Dateinamens
                from app.services.image_processing import find_png_file
                image_name_bild = image_name[:-4]
                matching_files = find_png_file(image_name_bild)

                if matching_files:
                    # Nehme die erste gefundene Datei
                    found_file = matching_files[0]
                    found_file_str = str(found_file.name)

                    # Prüfe zuerst, ob der Text in der Datenbank vorhanden ist
                    with sqlite3.connect(Settings.DB_PATH) as conn:
                        cursor = conn.execute("""
                                              SELECT score
                                              FROM image_quality_scores
                                              WHERE image_name = ?
                                                AND score_type = ?
                                              """, (found_file_str, score_type_map['text']))
                        db_result = cursor.fetchone()

                    if db_result and db_result[0]:
                        logger.info(f"Textlänge aus DB für {image_name}: {db_result[0]} Zeichen")
                    else:
                        # Wenn nicht in DB, dann wie bisher downloaden
                        from app.services.image_processing import download_text_file
                        content = download_text_file(
                            folder_name=found_file.parent.name,
                            image_name=found_file.name,
                            cache_dir=Settings.TEXT_FILE_CACHE_DIR
                        )

                        if content:
                            logger.info(f"Textlänge für {image_name}: {len(content)}")

                            # Speichere die Textlänge in der Datenbank
                            with sqlite3.connect(Settings.DB_PATH) as conn:
                                conn.execute("""
                                    INSERT OR REPLACE INTO image_quality_scores 
                                    (image_name, score_type, score)
                                    VALUES (?, ?, ?)
                                """, (found_file_str, score_type_map['text'], len(content)))



            except Exception as e:
                logger.error(f"Fehler bei der Verarbeitung von {image_name}: {str(e)}")
            file_counter += 1

            progress = int(((dir_counter + file_counter / max(total_files, 1)) / total_dirs) * 100)
            await update_progress(f"{subdir.name}: {file_counter}/{total_files}", progress, 0.002)
            await asyncio.sleep(0.01)

        hashfile_name = Settings.GALLERY_HASH_FILE
        await update_progress_text(f"Saving hashes to file: {subdir / hashfile_name}")
        await save_simple_hashes(local_hashes, subdir / hashfile_name)
        await update_progress_text(f"✓ Lokale Hashes gespeichert: {subdir / hashfile_name}")

        dir_counter += 1


async def save_simple_hashes(hashes: Dict[str, str], hashfile_path: Path):
    await update_progress_text(f"Saving hashes to path: {hashfile_path}")
    hashfile_path.parent.mkdir(parents=True, exist_ok=True)
    await update_progress_text(f"Writing {len(hashes)} hashes to file")
    with hashfile_path.open("w", encoding="utf-8") as f:
        json.dump(hashes, f, indent=2)
    os.chmod(hashfile_path, 0o644)
    await update_progress_text(f"Successfully saved hashes to {hashfile_path}")
