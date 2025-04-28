import sys
import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SECRET_PATH = "secrets"
CRED_FILE = os.path.join(SECRET_PATH, "credentials.json")
TOKEN_FILE = os.path.join(SECRET_PATH, "token.json")

def load_drive_service():
    if not os.path.exists(TOKEN_FILE):
        raise RuntimeError("Kein Token vorhanden. Bitte authentifizieren.")
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    service = build("drive", "v3", credentials=creds)
    return service

def find_text_file(service, image_name):
    # Bildname wird erwartet wie "dsc01234.jpg"
    # wir suchen passendes "dsc01234.jpg.txt"
    text_filename = image_name.lower() + ".txt"

    query = f"name = '{text_filename}' and trashed = false"
    results = service.files().list(
        q=query,
        spaces='drive',
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()

    files = results.get('files', [])
    if not files:
        print(f"[!] Keine Textdatei gefunden für: {text_filename}")
        return None

    file_id = files[0]['id']
    print(f"[Info] Gefunden: {files[0]['name']} (ID: {file_id})")

    request = service.files().get_media(fileId=file_id)
    content = request.execute().decode("utf-8")
    return content

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Nutzung: python3 download_textfile.py <bildname>")
        sys.exit(1)

    bildname = sys.argv[1].lower()  # Argument holen
    service = load_drive_service()

    text = find_text_file(service, bildname)
    if text:
        print("\nInhalt der Textdatei:")
        print(text)
