import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Dict, List

from tqdm import tqdm

from app.config import Settings
from app.config_gdrive import folder_id_by_name, get_all_subfolders, sanitize_filename, delete_all_hashfiles, \
    SettingsGdrive, calculate_md5
from app.database import clear_folder_status_db_by_name, load_folder_status_from_db_by_name
from app.routes.auth import load_drive_service_token
from app.tools import readimages, save_pair_cache
from app.utils.progress import save_simple_hashes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def write_local_hashes(extensions, file_folder_dir, subfolders: bool = True):
    root = Path(file_folder_dir)
    all_dirs = [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]
    with tqdm(total=len(all_dirs), desc="Erzeuge lokale Hashes", unit="Ordner") as pbar:
        for subdir in all_dirs:
            local_hashes: Dict[str, str] = {}
            image_files = [f for f in subdir.iterdir() if f.is_file() and f.suffix.lower() in extensions]
            with tqdm(total=len(image_files), desc=f"{subdir.name}", unit="Bild", leave=False) as inner:
                for file in image_files:
                    try:
                        md5_local = calculate_md5(file)
                        local_hashes[file.name] = md5_local
                    except Exception as e:
                        tqdm.write(f"[Fehler] {file.name}: {e}")
                    inner.update(1)
            hashfile_name = Settings.GALLERY_HASH_FILE
            save_simple_hashes(local_hashes, subdir / hashfile_name)
            tqdm.write(f"[✓] Lokale Hashes gespeichert: {subdir / hashfile_name}")
            pbar.update(1)


def save_structured_hashes(hashes: Dict[str, Dict[str, str]], hashfile_path: Path):
    hashfile_path.parent.mkdir(parents=True, exist_ok=True)
    with hashfile_path.open("w", encoding="utf-8") as f:
        json.dump(hashes, f, indent=2)
    os.chmod(hashfile_path, 0o644)
    print(f"[✓] Gespeichert: {hashfile_path}/{Settings.GDRIVE_HASH_FILE}")


def process_image_folders(service, extensions, file_folder_dir, folder_ids: List[str], subfolders: bool = True):
    folder_names: Dict[str, str] = {}

    for root_id in folder_ids:
        all_ids = get_all_subfolders(service, root_id) if subfolders else [root_id]
        for folder_id in all_ids:
            files = []
            page_token = None
            query = f"'{folder_id}' in parents and trashed = false"

            with tqdm(desc=f"Lade aus {folder_id[:6]}...", unit="Seite") as pbar:
                while True:
                    response = service.files().list(
                        q=query,
                        spaces='drive',
                        fields="nextPageToken, files(id, name, size, parents, md5Checksum)",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                        pageSize=Settings.PAGESIZE,
                        pageToken=page_token
                    ).execute()
                    batch = response.get('files', [])
                    files.extend([
                        f for f in batch
                        if isinstance(f.get('name'), str) and f['name'].lower().endswith(tuple(extensions))
                    ])
                    page_token = response.get('nextPageToken')
                    pbar.update(1)
                    if not page_token:
                        break

            if not files:
                continue

            folder = service.files().get(fileId=folder_id, fields="name").execute()
            folder_name = folder.get("name", "real")
            folder_names[folder_id] = folder_name

            gdrive_hashes: Dict[str, Dict[str, str]] = {}
            for file in files:
                try:
                    name = sanitize_filename(file['name'])
                    md5_drive = file.get("md5Checksum")
                    if md5_drive:
                        gdrive_hashes[name] = {
                            "md5": md5_drive,
                            "id": file['id']
                        }
                except Exception as e:
                    print(f"[Fehler] {file['name']}: {e}")

            if subfolders:
                local_dir = Path(file_folder_dir) / folder_name
            else:
                local_dir = Path(file_folder_dir)
            save_structured_hashes(gdrive_hashes, local_dir / Settings.GDRIVE_HASH_FILE)


def images(service):
    delete_all_hashfiles(Settings.IMAGE_FILE_CACHE_DIR)
    process_image_folders(service, Settings.IMAGE_EXTENSIONS, Settings.IMAGE_FILE_CACHE_DIR,
                          [folder_id_by_name("imagefiles")])
    write_local_hashes(Settings.IMAGE_EXTENSIONS, Settings.IMAGE_FILE_CACHE_DIR)


def text(service):
    delete_all_hashfiles(Settings.TEXT_FILE_CACHE_DIR, False)
    process_image_folders(service, Settings.TEXT_EXTENSIONS, Settings.TEXT_FILE_CACHE_DIR,
                          [folder_id_by_name("textfiles")], False)
    write_local_hashes(Settings.TEXT_EXTENSIONS, Settings.TEXT_FILE_CACHE_DIR, False)


def local():
    global service
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    Settings.DB_PATH = '../../gallery_local.db'

    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")
    return load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))


def onefolder(folder_id):
    pair_cache = Settings.CACHE.get("pair_cache")
    pair_cache_path_local = Settings.PAIR_CACHE_PATH

    rows = load_folder_status_from_db_by_name(Settings.DB_PATH, folder_id)
    logging.info(f"Anzahl DB: {len(rows)}")

    clear_folder_status_db_by_name(Settings.DB_PATH, folder_id)
    to_delete = [key for key, value in pair_cache.items()
                 if value.get("folder", "") == folder_id]

    for key in to_delete:
        del pair_cache[key]

    asyncio.run(readimages(Settings.IMAGE_FILE_CACHE_DIR + "/" + folder_id, pair_cache))
    save_pair_cache(pair_cache, pair_cache_path_local)
