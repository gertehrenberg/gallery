import io
import os
import base64

from fastapi import FastAPI
from fastapi.responses import StreamingResponse, HTMLResponse

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from PIL import Image, ImageOps

app = FastAPI()

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly"
]

FOLDER_ID = os.environ.get("FOLDER_ID", "DEIN_ORDNER_ID_HIER")

SECRET_PATH = 'secrets'
CRED_FILE = os.path.join(SECRET_PATH, 'credentials.json')
TOKEN_FILE = os.path.join(SECRET_PATH, 'token.json')

# Cache: file_id -> bytes (orientierte Vollgröße)
image_cache = {}

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

def download_fullsize_oriented(file_id: str) -> bytes:
    """Lädt das Bild von Google Drive, korrigiert EXIF, speichert als JPEG-Vollformat (ohne Skalierung)."""
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)

    image = Image.open(fh)
    image = ImageOps.exif_transpose(image)

    out_fh = io.BytesIO()
    image.save(out_fh, format='JPEG')  # immer JPEG
    out_fh.seek(0)
    return out_fh.read()

def scale_image_to_400(image_data: bytes) -> bytes:
    """Nimmt orientierte Vollgröße als Bytes (JPEG), skaliert auf max. 400 px und gibt sie als Bytes zurück."""
    fh = io.BytesIO(image_data)
    img = Image.open(fh)

    max_dim = 400
    width, height = img.size
    if width > max_dim or height > max_dim:
        scale = min(max_dim / width, max_dim / height)
        new_width = int(width * scale)
        new_height = int(height * scale)
        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)

    out_fh = io.BytesIO()
    img.save(out_fh, format='JPEG')
    out_fh.seek(0)
    return out_fh.read()

@app.get("/", response_class=HTMLResponse)
def show_three_images():
    try:
        service = get_drive_service()
        query = f"'{FOLDER_ID}' in parents and trashed = false and mimeType contains 'image/'"
        results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
        files = results.get('files', [])
        if not files:
            return "<html><body><h1>Keine Bilder gefunden</h1></body></html>"

        three_files = files[:3]
        images_html_parts = []

        for f in three_files:
            file_id = f['id']
            # Lade/wiederverwende Fullsize aus dem Cache
            if file_id not in image_cache:
                # Download + EXIF-Orientierung
                try:
                    image_cache[file_id] = download_fullsize_oriented(file_id)
                except Exception as e:
                    images_html_parts.append(
                        f'<div style="margin: 5px;"><p>Fehler bei {f["name"]}: {str(e)}</p></div>'
                    )
                    continue

            # Erzeuge ein skalierter Thumbnail (400px) aus dem Fullsize
            scaled_data = scale_image_to_400(image_cache[file_id])

            # Base64-codierte Einbettung
            base64_data = base64.b64encode(scaled_data).decode('utf-8')
            # Bei Klick -> /original/{file_id}
            img_html = f'''<a href="/gallery/original/{file_id}" target="_blank"
                             style="cursor: pointer; text-decoration: none;">
                              <img src="data:image/jpeg;base64,{base64_data}"
                                   style="margin:5px; cursor:pointer;"/>
                           </a>'''
            images_html_parts.append(img_html)

        final_html = f"""
        <html>
        <head>
          <title>Drei Bilder</title>
          <style>
            img:hover {{ opacity: 0.8; }}
          </style>
        </head>
        <body>
          <h1>Drei Bilder aus Google Drive</h1>
          <div style="display:flex; flex-direction:row;">
            {''.join(images_html_parts)}
          </div>
        </body>
        </html>
        """
        return final_html
    except Exception as e:
        return f"<html><body><h1>Error: {str(e)}</h1></body></html>"

@app.get("/original/{file_id}")
def show_original_image(file_id: str):
    """Zeige das orientierte Vollbild aus dem Cache (JPEG)."""
    if file_id not in image_cache:
        return HTMLResponse(
            f"<p>Fehler: Kein Bild mit file_id={file_id} im Cache.</p>", status_code=404
        )
    return StreamingResponse(io.BytesIO(image_cache[file_id]), media_type="image/jpeg")
