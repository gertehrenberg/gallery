import os
import io
from tqdm import tqdm
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly"
]
SECRET_PATH = "../secrets"
TOKEN_FILE = os.path.join(SECRET_PATH, "token.json")
FOLDER_ID = "13U8wUpisnRJpQLNb0ecbnEcDlrz-bTzV"
DEST_DIR = "../cache/textfiles/"

def load_drive_service():
    if not os.path.exists(TOKEN_FILE):
        raise RuntimeError("Kein Token vorhanden. Bitte authentifizieren.")
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    return build("drive", "v3", credentials=creds)

def list_all_txt_files(service):
    query = (
        f"'{FOLDER_ID}' in parents and "
        "mimeType='text/plain' and trashed = false"
    )

    files = []
    page_token = None

    while True:
        response = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name, size)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=100,
            pageToken=page_token
        ).execute()

        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return files

def should_download(file, dest_path):
    if not os.path.exists(dest_path):
        return True
    try:
        drive_size = int(file.get("size", "0"))
        local_size = os.path.getsize(dest_path)
        return drive_size > local_size
    except Exception as e:
        print(f"[!] Fehler beim Vergleichen der Dateigröße für {file['name']}: {e}")
        return True  # zur Sicherheit erneut herunterladen

def download_files(service, files):
    os.makedirs(DEST_DIR, exist_ok=True)

    for file in tqdm(files, desc="Download", unit="file"):
        filename = file["name"].lower()
        dest_path = os.path.join(DEST_DIR, filename)

        if not should_download(file, dest_path):
            continue  # überspringen

        file_id = file["id"]
        request = service.files().get_media(fileId=file_id)

        with open(dest_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()

    print(f"Download abgeschlossen: {len(files)} Dateien geprüft.")

if __name__ == "__main__":
    service = load_drive_service()
    files = list_all_txt_files(service)
    if not files:
        print("Keine .txt-Dateien gefunden.")
    else:
        print(f"{len(files)} Dateien gefunden.")
        download_files(service, files)
