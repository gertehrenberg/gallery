import io
import json
import logging
import os
import socket
import ssl
import time

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from fastapi.templating import Jinja2Templates

logging.basicConfig(level=logging.INFO)

app = FastAPI()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly"
]

FOLDER_ID = os.environ.get("FOLDER_ID", "DEIN_ORDNER_ID_HIER")

SECRET_PATH = 'secrets'
CRED_FILE = os.path.join(SECRET_PATH, 'credentials.json')
TOKEN_FILE = os.path.join(SECRET_PATH, 'token.json')

PAIR_CACHE_PATH = '/data/pair_cache.json'

IMAGES_PER_PAGE = 3

# Diese Caches sind bewusst global, da sie wiederverwendet werden sollen
image_cache = {}  # file_id -> { 'thumbnail': url }
text_cache = {}   # lowercase text filename -> content
pair_cache = {}   # lowercase image filename -> (image_id, text_id)
text_id_cache = {}  # lowercase text filename -> google file ID

service = None

@app.on_event("startup")
def init_service():
    global service

    os.environ.pop("HTTPS_PROXY", None)
    os.environ.pop("HTTP_PROXY", None)

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

    service = build('drive', 'v3', credentials=creds)
    fillcache()

def retry_google_request(callable_fn, retries: int = 7):
    for attempt in range(retries):
        try:
            return callable_fn()
        except (ssl.SSLError, socket.timeout) as e:
            logging.warning(f"[SSL] Retry {attempt + 1} für Google Request: {e}")
            time.sleep(1.5 * (attempt + 1))
        except Exception as e:
            logging.warning(f"[Retry] Fehler bei Google Request: {e}")
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"Google Request nach {retries} Versuchen fehlgeschlagen.")

def download_text_file(file_id: str, retries: int = 5) -> str:
    logging.info(f"Lade Textdatei mit ID: {file_id}")
    for attempt in range(retries):
        try:
            request = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            return fh.getvalue().decode('utf-8', errors='replace')
        except (ssl.SSLError, socket.timeout) as e:
            logging.warning(f"[SSL] Versuch {attempt + 1} fehlgeschlagen: {e}")
            time.sleep(1.0 * (attempt + 1))
        except Exception as e:
            logging.error(f"[Download] Fehler bei {file_id}: {e}")
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"Download von {file_id} nach {retries} Versuchen fehlgeschlagen.")

def download_thumbnail(file_id: str) -> str:
    logging.info(f"[Thumbnail] Abfrage für Datei-ID: {file_id}")

    if file_id in image_cache and image_cache[file_id].get("thumbnail"):
        logging.info(f"[Thumbnail] Cache-Treffer für Datei-ID: {file_id}")
        return image_cache[file_id]["thumbnail"]

    try:
        file = retry_google_request(lambda: service.files().get(fileId=file_id, fields="thumbnailLink").execute())
        thumbnail_link = file.get("thumbnailLink", "")
        logging.info(f"[Thumbnail] Erfolgreich geladen für Datei-ID {file_id}: {thumbnail_link}")
        image_cache[file_id] = {"thumbnail": thumbnail_link}
        return thumbnail_link
    except Exception as e:
        logging.warning(f"[Thumbnail] Fehler beim Abrufen für {file_id}: {e}")
        return ""

def fillcache():
    global pair_cache, text_id_cache

    if os.path.exists(PAIR_CACHE_PATH):
        try:
            with open(PAIR_CACHE_PATH, 'r') as f:
                pair_cache.update(json.load(f))
                logging.info(f"[fillcache] Pair-Cache geladen: {len(pair_cache)} Paare")
                return
        except Exception as e:
            logging.warning(f"[fillcache] Fehler beim Laden von pair_cache.json: {e}")

    image_id_cache = {}  # lowercase image filename -> google file ID

    q = f"'{FOLDER_ID}' in parents and trashed=false and (name contains '.txt' or mimeType contains 'image/')"
    logging.info(f"[p] : {q}")
    page_token = None
    while True:
        try:
            response = service.files().list(
                q=q,
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
        except Exception as e:
            logging.error(f"[fillcache] API-Fehler: {e}")
            break

        for f in response.get('files', []):
            name = f['name'].strip()
            clean_name = name.lower()
            fid = f['id']
            mimetype = f.get('mimeType', '')

            if clean_name.endswith(".txt"):
                text_id_cache[clean_name] = fid
            elif mimetype.startswith("image/"):
                image_id_cache[clean_name] = fid

        page_token = response.get('nextPageToken', None)
        if not page_token:
            break

    for img_name in image_id_cache:
        txt_name = img_name + ".txt"
        if txt_name in text_id_cache:
            pair_cache[img_name] = (image_id_cache[img_name], text_id_cache[txt_name])

    try:
        with open(PAIR_CACHE_PATH, 'w') as f:
            json.dump(pair_cache, f)
        logging.info(f"[fillcache] Pair-Cache gespeichert: {len(pair_cache)} Paare")
    except Exception as e:
        logging.warning(f"[fillcache] Fehler beim Speichern von pair_cache.json: {e}")

    logging.info(f"[fillcache] Cache vollständig aktualisiert: {len(text_id_cache)} Textdateien, {len(image_id_cache)} Bilder, {len(pair_cache)} Paare.")

@app.get("/", response_class=HTMLResponse)
def show_three_images(request: Request):
    seite_str = request.query_params.get('seite', '1')
    try:
        seite = int(seite_str)
    except ValueError:
        seite = 1

    start = (seite - 1) * IMAGES_PER_PAGE
    end = start + IMAGES_PER_PAGE
    image_keys = list(pair_cache.keys())[start:end]

    images_html_parts = []

    for img_name in image_keys:
        image_id, text_id = pair_cache[img_name]
        file_name = img_name

        thumbnail_src = download_thumbnail(image_id)

        if img_name not in text_cache:
            try:
                content = download_text_file(text_id)
                text_cache[img_name] = content
            except Exception as e:
                text_cache[img_name] = f"Fehler beim Laden: {e}"

        images_html_parts.append(
            templates.get_template("image_entry.html").render(
                file_name=file_name,
                thumbnail_src=thumbnail_src,
                text_content=text_cache.get(img_name, "Kein Text gefunden")
            )
        )

    return templates.TemplateResponse("gallery.html", {
        "request": request,
        "seite": seite,
        "next_seite": seite + 1,
        "prev_seite": max(1, seite - 1),
        "images_html": ''.join(images_html_parts)
    })

@app.get("/original/{file_id}")
def show_original_image(file_id: str):
    return HTMLResponse("<p>Originalbild-Vorschau nicht verfügbar.</p>", status_code=501)
