import io
import os
import re

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from tqdm import tqdm

# Konfiguration
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SECRET_PATH = "../secrets"
CRED_FILE = os.path.abspath(os.path.join(SECRET_PATH, "credentials.json"))
TOKEN_FILE = os.path.abspath(os.path.join(SECRET_PATH, "token.json"))
FOLDER_ID = "13U8wUpisnRJpQLNb0ecbnEcDlrz-bTzV"
DEST_DIR = os.path.abspath("../cache/textfiles")


def load_drive_service():
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    return build("drive", "v3", credentials=creds)


def list_all_txt_files(service):
    files = []
    page_token = None
    query = f"'{FOLDER_ID}' in parents and trashed = false and name contains '.txt'"
    while True:
        response = service.files().list(
            q=query,
            spaces='drive',
            fields="nextPageToken, files(id, name, size)",
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=page_token
        ).execute()
        files.extend(response.get('files', []))
        page_token = response.get('nextPageToken', None)
        if not page_token:
            break
    return files


def sanitize_filename(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r'[\\/:*?"<>|\n\r\t]', '_', name)  # ersetzt unzulässige Zeichen
    return name


def should_download(file, dest_path):
    if not os.path.exists(dest_path):
        return True
    try:
        local_size = os.path.getsize(dest_path)
        drive_size = int(file.get("size", 0))
        return drive_size > local_size
    except:
        return True


def download_files(service, files):
    os.makedirs(DEST_DIR, exist_ok=True)

    with tqdm(total=len(files), desc="Download", unit="file") as pbar:
        for file in files:
            try:
                orig_name = file["name"]
                clean_name = sanitize_filename(orig_name)
                dest_path = os.path.join(DEST_DIR, clean_name)
                dest_path = os.path.abspath(dest_path)

                if not dest_path.startswith(DEST_DIR):
                    raise ValueError(f"Ungültiger Zielpfad verhindert: {dest_path}")

                if not should_download(file, dest_path):
                    pbar.update(1)
                    continue

                request = service.files().get_media(fileId=file["id"])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)

                done = False
                while not done:
                    _, done = downloader.next_chunk()

                with open(dest_path, "wb") as f:
                    f.write(fh.getvalue())

            except Exception as e:
                print(f"[Fehler] Datei {file['name']} konnte nicht geladen werden: {e}")

            pbar.update(1)


if __name__ == "__main__":
    service = load_drive_service()
    files = list_all_txt_files(service)
    print(f"Gefundene Dateien: {len(files)}")
    download_files(service, files)
