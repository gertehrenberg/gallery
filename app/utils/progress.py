import asyncio
import json
import os
from pathlib import Path
from typing import Dict

from app.config import Settings
from app.config_gdrive import folder_name_by_id
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)

logger.info("Initializing progress state")
progress_state = {
    "progress": 0,
    "status": "Warte auf Start...",
    "running": False,
    "current_task": None
}

async def getlast() -> int:
    """

    :rtype: int
    """
    return int(progress_state["progress"])

async def update_progress(status: str, progress: int, ctime=0.01, showlog=True):
    if isinstance(status, str) and len(status) > 0:
        progress_state["status"] = status
    progress_state["progress"] = progress
    if showlog:
        logger.info(f"{status} : {progress}")
    await asyncio.sleep(ctime)


async def update_progress_auto(status: str, ctime=0.01, showlog=True):
    progress = await getlast() + 1
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


async def save_simple_hashes(hashes: Dict[str, str], hashfile_path: Path):
    await update_progress_text(f"Saving hashes to path: {hashfile_path}")
    hashfile_path.parent.mkdir(parents=True, exist_ok=True)
    await update_progress_text(f"Writing {len(hashes)} hashes to file")
    with hashfile_path.open("w", encoding="utf-8") as f:
        json.dump(hashes, f, indent=2)
    os.chmod(hashfile_path, 0o644)
    await update_progress_text(f"Successfully saved hashes to {hashfile_path}")
