import io
import json
import logging
import os
import socket
import ssl
import time

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO)


def download_text_file(service, file_id: str, cache_dir: str) -> str:
    file_path = os.path.join(cache_dir, f"{file_id}.txt")

    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logging.warning(f"[download_text_file] Fehler beim Lesen von Cache-Datei: {file_path} - {e}")

    try:
        logging.info(f"Lade Textdatei mit ID: {file_id}")
        request = service.files().get_media(fileId=file_id)
        content = request.execute().decode("utf-8")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return content
    except Exception as e:
        logging.warning(f"[download_text_file] Fehler beim Laden: {e}")
        raise


def download_thumbnail(service, image_cache, file_id):
    if file_id in image_cache:
        logging.info(f"[Thumbnail] Cache-Treffer für Datei-ID: {file_id}")
        return image_cache[file_id]['thumbnail']

    try:
        logging.info(f"[Thumbnail] Abfrage für Datei-ID: {file_id}")
        file = service.files().get(fileId=file_id, fields="thumbnailLink").execute()
        thumbnail_url = file.get("thumbnailLink")
        if thumbnail_url:
            image_cache[file_id] = {'thumbnail': thumbnail_url}
            logging.info(f"[Thumbnail] Erfolgreich geladen für Datei-ID {file_id}: {thumbnail_url}")
            return thumbnail_url
    except Exception as e:
        logging.warning(f"[Thumbnail] Fehler beim Abrufen für {file_id}: {e}")

    return "https://via.placeholder.com/150?text=Kein+Bild"


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
TEXT_FILE_CACHE_DIR = '/data/textfiles'

IMAGES_PER_PAGE = 6

image_cache = {}  # file_id -> { 'thumbnail': url }
text_cache = {}  # lowercase text filename -> content
pair_cache = {}  # lowercase image filename -> (image_id, text_id)
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
    os.makedirs(TEXT_FILE_CACHE_DIR, exist_ok=True)
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

    logging.info(
        f"[fillcache] Cache vollständig aktualisiert: {len(text_id_cache)} Textdateien, {len(image_id_cache)} Bilder, {len(pair_cache)} Paare.")


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

        thumbnail_src = download_thumbnail(service, image_cache, image_id)

        if img_name not in text_cache:
            try:
                if text_id in text_cache:
                    content = text_cache[text_id]
                else:
                    content = download_text_file(service, text_id, TEXT_FILE_CACHE_DIR)
                    text_cache[img_name] = content
                    text_cache[text_id] = content
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