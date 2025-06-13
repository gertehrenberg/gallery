import hashlib
import pickle
import re
from os import path
from pathlib import Path
from typing import List

from app.config import Settings
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


class SettingsGdrive:
    GDRIVE_FOLDERS_PKL = Path(Settings.DATA_DIR) / "gdrive_folders.pkl"


_cached_folder_dict = None


def sanitize_filename(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r'[\\/:*?"<>|\n\r\t]', '_', name)
    return name


def calculate_md5(file_path: Path) -> str:
    hasher = hashlib.md5()
    with file_path.open('rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def delete_all_hashfiles(file_folder_dir, subfolders: bool = True):
    root = Path(file_folder_dir)
    all_dirs = [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]
    deleted = 0
    for subdir in all_dirs:
        for file in subdir.glob("*hashes.json"):
            try:
                file.unlink()
                logger.info(f"[\U0001f5d1️] Gelöscht: {file}")
                deleted += 1
            except Exception as e:
                logger.error(f"Konnte {file} nicht löschen: {e}")
    logger.info(f"[✓] Insgesamt gelöscht: {deleted} Hash-Dateien")


def get_all_subfolders(service, parent_id: str) -> List[str]:
    folders = [parent_id]
    queue = [parent_id]
    while queue:
        current_id = queue.pop(0)
        response = service.files().list(
            q=f"'{current_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
            fields="nextPageToken, files(id)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        subfolders = response.get("files", [])
        for f in subfolders:
            folders.append(f['id'])
            queue.append(f['id'])
    return folders


def save_dict(data, file: path):
    with open(file, "wb") as f:
        pickle.dump(data, f)


def load_dict(file: path):
    if file.exists():
        with open(file, "rb") as f:
            return pickle.load(f)
    return {"name_to_id": {}, "id_to_name": {}}


def collect_all_folders(service, parent_id, name_to_id, id_to_name):
    page_token = None
    while True:
        response = service.files().list(
            q=f"mimeType = 'application/vnd.google-apps.folder' and trashed = false and '{parent_id}' in parents",
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()

        for file in response.get("files", []):
            name_to_id[file["name"]] = file["id"]
            id_to_name[file["id"]] = file["name"]
            # Rekursiv auch Unterordner sammeln
            collect_all_folders(service, file["id"], name_to_id, id_to_name)

        page_token = response.get("nextPageToken", None)
        if page_token is None:
            break


def folder_name_by_id(folder_id):
    global _cached_folder_dict
    if _cached_folder_dict is None:
        logger.info("Lade Folder-Cache aus Datei...")
        _cached_folder_dict = load_dict(SettingsGdrive.GDRIVE_FOLDERS_PKL)
    name = _cached_folder_dict.get("id_to_name", {}).get(folder_id)
    # logger.debug(f"[LOOKUP] folder_name_by_id('{folder_id}') → '{name}'")
    return name


def folder_id_by_name(folder_name):
    global _cached_folder_dict
    if _cached_folder_dict is None:
        logger.info("Lade Folder-Cache aus Datei...")
        _cached_folder_dict = load_dict(SettingsGdrive.GDRIVE_FOLDERS_PKL)
    folder_id = _cached_folder_dict.get("name_to_id", {}).get(folder_name)
    # logger.debug(f"[LOOKUP] folder_id_by_name('{folder_name}') → '{folder_id}'")
    return folder_id
