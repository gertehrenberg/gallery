import hashlib
import json
import os
import re
from pathlib import Path
from typing import List

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

ZIP_DIR = "../cache/zips"
CACHE_DATEI_PATH = Path("geocache.json")

IMAGE_EXTENSIONS = {".bmp", ".gif", ".jpg", ".jpeg", ".png"}
IMAGE_FILE_CACHE_DIR = "../cache/imagefiles"

TEXT_EXTENSIONS = {".txt"}
TEXT_FILE_CACHE_DIR = "../cache/textfiles"

TEMP_FILE_DIR = "../temp"

GALLERY_HASH_FILE = "gallery202505_hashes.json"

GDRIVE_FOLDERS_DICT = Path("../cache/gdrive_folders.pkl")

RENDERED_HTML = Path("../cache/rendered_html")

from app.routes.auth import TOKEN_FILE, SCOPES

def load_drive_service():
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    return build("drive", "v3", credentials=creds)


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
                print(f"[\U0001f5d1️] Gelöscht: {file}")
                deleted += 1
            except Exception as e:
                print(f"[Fehler] Konnte {file} nicht löschen: {e}")
    print(f"[✓] Insgesamt gelöscht: {deleted} Hash-Dateien")


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
