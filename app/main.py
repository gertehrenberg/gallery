from fastapi import FastAPI
import os
import os.path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

app = FastAPI()


SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly"
]


FOLDER_ID = os.environ.get("FOLDER_ID", "DEIN_ORDNER_ID_HIER")

# Verzeichnispfad für Credentials
SECRET_PATH = 'secrets'
CRED_FILE = os.path.join(SECRET_PATH, 'credentials.json')
TOKEN_FILE = os.path.join(SECRET_PATH, 'token.json')

def get_drive_service():
    if not os.path.exists(CRED_FILE):
        raise RuntimeError("credentials.json fehlt")

    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CRED_FILE, SCOPES)
        creds = flow.run_console()
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())

    return build('drive', 'v3', credentials=creds)

@app.get("/")
def count_files():
    try:
        service = get_drive_service()
        query = f"'{FOLDER_ID}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        return {"file_count": len(files)}
    except Exception as e:
        return {"error": str(e)}
