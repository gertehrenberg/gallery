import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Dict

from app.config import Settings
from app.config_gdrive import folder_name_by_id, calculate_md5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

progress_state = {
    "progress": 0,
    "status": "Warte auf Start...",
    "running": False
}


async def update_progress(status: str, progress: int, ctime=0.1, showlog=False):
    if isinstance(status, str) and len(status) > 0:
        progress_state["status"] = status
    progress_state["progress"] = progress
    if showlog:
        logging.info(f"{status} : {progress}")
    await asyncio.sleep(ctime)  # <<< Damit der Balken Zeit zur Anzeige bekommt


async def init_progress_state():
    progress_state["running"] = False
    await update_progress("Warte auf Start...", 0)


async def stop_progress():
    progress_state["running"] = False
    await update_progress("Abgeschlossen.", 100)


async def list_files(folder_id, service, sign="!="):
    files = []
    page_token = None
    count = 0
    folder_name = folder_name_by_id(folder_id)
    logging.info(f"📂 Starte Dateiliste für Folder-ID: {folder_name}")
    await update_progress(f"Dateien werden aus Google Drive geladen ({folder_name})...", 0)
    while True:
        logging.info(f"📄 Lade Seite {count + 1} ...")
        response = service.files().list(
            q=f"'{folder_id}' in parents and mimeType {sign} 'text/plain' and trashed=false",
            fields="nextPageToken, files(id, name, size, md5Checksum, parents)",
            pageToken=page_token,
            pageSize=50
        ).execute()

        files_batch = response.get('files', [])
        logging.info(f"🔢 {len(files_batch)} Dateien auf dieser Seite gefunden")

        files.extend(files_batch)
        count += 1

        progress_state["progress"] += 1
        if progress_state["progress"] > 100:
            progress_state["progress"] = 0
        await asyncio.sleep(0.1)  # <<< Damit der Balken Zeit zur Anzeige bekommt

        page_token = response.get('nextPageToken', None)
        if not page_token:
            break

    await update_progress(f"{len(files)} Dateien geladen.", 100)
    logging.info(f"✅ Insgesamt {len(files)} Dateien geladen aus {count} Seiten")
    await asyncio.sleep(0.5)

    return files


async def write_local_hashes_progress(extensions, file_folder_dir, subfolders: bool = True):
    root = Path(file_folder_dir)
    all_dirs = [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]

    total_dirs = len(all_dirs)
    dir_counter = 0

    for subdir in all_dirs:
        local_hashes: Dict[str, str] = {}
        image_files = [f for f in subdir.iterdir() if f.is_file() and f.suffix.lower() in extensions]

        total_files = len(image_files)
        file_counter = 0

        for file in image_files:
            try:
                md5_local = calculate_md5(file)
                local_hashes[file.name] = md5_local
            except Exception as e:
                print(f"[Fehler] {file.name}: {e}")
            file_counter += 1

            progress = int(((dir_counter + file_counter / max(total_files, 1)) / total_dirs) * 100)
            await update_progress(f"{subdir.name}: {file_counter}/{total_files}", progress, 0.002)
            await asyncio.sleep(0.01)

        hashfile_name = Settings.GALLERY_HASH_FILE
        save_simple_hashes(local_hashes, subdir / hashfile_name)
        print(f"[✓] Lokale Hashes gespeichert: {subdir / hashfile_name}")

        dir_counter += 1


def save_simple_hashes(hashes: Dict[str, str], hashfile_path: Path):
    hashfile_path.parent.mkdir(parents=True, exist_ok=True)
    with hashfile_path.open("w", encoding="utf-8") as f:
        json.dump(hashes, f, indent=2)
    os.chmod(hashfile_path, 0o644)
