import hashlib
import re
from pathlib import Path
from typing import List

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from app.routes.auth import SCOPES, TOKEN_FILE


def load_drive_service():
    load_drive_service(TOKEN_FILE)


def load_drive_service(token_file):
    creds = Credentials.from_authorized_user_file(token_file, SCOPES)
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
