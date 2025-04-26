import json
import logging
import os
import socket
import sqlite3
import ssl
import time
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO)
app = FastAPI()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly"
]

kategorien = [
    {"key": "real", "label": "Alle Bilder", "icon": "💾", "folderid": "1fyE_ZYoVoGZ7ehjuWrS9Kd6WW4w2UZWy"},
    {"key": "delete", "label": "Löschen", "icon": "❌", "folderid": "1wjUj6NHZ_ZHwlahQuJUbCTf_HplqePVw"},
    {"key": "recheck", "label": "Neu beurteilen", "icon": "🔄", "folderid": "1EyrM6LLv_nEyB8s6zzGDGzf-hcPC76dg"},
    {"key": "bad", "label": "Schlecht", "icon": "⛔", "folderid": "1EkX7TxoRJlYUyeNA10T3Gzdt5Nd7yRRf"},
    {"key": "sex", "label": "Anzüglich", "icon": "🔞", "folderid": "1XCOjgEi0m0YGu11oPo3IZJizUf3p3tZg"},
    {"key": "ki", "label": "KI generiert", "icon": "🤖", "folderid": "1LWF_V26zvX-W9vRNwscmeQ6U7YeJxOuL"},
    {"key": "comfyui", "label": "ComfyUI", "icon": "🛠️", "folderid": "1UjmQV-dO3y8uhqmWjSIzU1t7w6-rQEqG"},
    {"key": "document", "label": "Dokumente", "icon": "📄", "folderid": "1oKNY7jB8hEFMEn6amA6Osrbo8K9z5jAW"},
]

CHECKBOX_CATEGORIES = [k["key"] for k in kategorien]

FOLDER_ID = next((k["folderid"] for k in kategorien if k["key"] == "real"), None)

DB_PATH = Path("checkboxen.db")

SECRET_PATH = 'secrets'
CRED_FILE = os.path.join(SECRET_PATH, 'credentials.json')
TOKEN_FILE = os.path.join(SECRET_PATH, 'token.json')
REDIRECT_URI = "https://levellevel.me/gallery/auth/callback"

PAIR_CACHE_PATH = '/data/pair_cache.json'
TEXT_FILE_CACHE_DIR = '/data/textfiles'

IMAGES_PER_PAGE = 1

image_cache = {}  # file_id -> { 'thumbnail': url }
text_cache = {}  # lowercase text filename -> content
pair_cache = {}  # lowercase image filename -> { image_id, text_id, web_link }
text_id_cache = {}  # lowercase text filename -> google file ID

service = None


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS checkbox_status (
            image_name TEXT,
            checkbox TEXT,
            checked INTEGER,
            PRIMARY KEY (image_name, checkbox)
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS text_status (
            image_name TEXT,
            field TEXT,
            value TEXT,
            PRIMARY KEY (image_name, field)
        )
        """)


logging.basicConfig(level=logging.INFO)


def save_status(image_name: str, data: dict):
    # Logge die Eingabedaten für das Speichern
    logging.info(f"[save_status] Speichern des Status für {image_name}. Eingabedaten: {data}")

    with sqlite3.connect(DB_PATH) as conn:
        for key, value in data.items():
            if key in CHECKBOX_CATEGORIES:
                checked = 1 if str(value).lower() in ["1", "true", "on"] else 0
                try:
                    # Speichern der Checkbox-Daten
                    conn.execute("""
                        INSERT OR REPLACE INTO checkbox_status (image_name, checkbox, checked)
                        VALUES (?, ?, ?)
                    """, (image_name, key, checked))
                    logging.info(
                        f"[save_status] Status für Checkbox '{key}' von {image_name} gespeichert. Wert: {checked}")
                except Exception as e:
                    logging.error(f"[save_status] Fehler beim Speichern der Checkbox '{key}' für {image_name}: {e}")
            else:
                try:
                    # Speichern der Text-Daten
                    conn.execute("""
                        INSERT OR REPLACE INTO text_status (image_name, field, value)
                        VALUES (?, ?, ?)
                    """, (image_name, key, value))
                    logging.info(f"[save_status] Textfeld '{key}' für {image_name} gespeichert. Wert: {value}")
                except Exception as e:
                    logging.error(f"[save_status] Fehler beim Speichern des Textfelds '{key}' für {image_name}: {e}")


def load_status(image_name: str):
    logging.info(f"[load_status] Laden des Status für {image_name}")

    status = {}
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Wir suchen nun nach allen Checkboxen, deren name den Dateinamen enthält, z.B. "img_5242.jpg_save"
            rows = conn.execute("""
                SELECT checkbox, checked FROM checkbox_status WHERE image_name = ?
            """, (image_name,))
            for row in rows:
                # Den Bildnamen beibehalten
                checkbox_key = row[0]  # Der Schlüssel bleibt wie er ist, z.B. "img_5242.jpg_save"
                status[checkbox_key] = bool(row[1])

            # Laden der Textstatusfelder
            rows = conn.execute("""
                SELECT field, value FROM text_status WHERE image_name = ?
            """, (image_name,))
            for row in rows:
                status[row[0]] = row[1]

        logging.info(f"[load_status] Status für {image_name} erfolgreich geladen: {status}")
    except Exception as e:
        logging.error(f"[load_status] Fehler beim Laden des Status für {image_name}: {e}")
    return status


@app.post("/save")
async def save(request: Request):
    form = await request.form()
    image_name = form.get("image_name")
    data = {key: form.get(key) for key in form if key != "image_name"}

    for key in data:
        if data[key] == "on":
            data[key] = True

    save_status(image_name, data)
    return {"status": "ok"}


@app.get("/auth")
def start_auth():
    flow = Flow.from_client_secrets_file(
        CRED_FILE,
        scopes=SCOPES,
        redirect_uri=REDIRECT_URI,
    )
    auth_url, _ = flow.authorization_url(prompt="consent", include_granted_scopes="true")
    return RedirectResponse(auth_url)


@app.get("/auth/callback")
def auth_callback(request: Request):
    code = request.query_params.get("code")
    flow = Flow.from_client_secrets_file(
        CRED_FILE,
        scopes=SCOPES,
        redirect_uri=REDIRECT_URI,
    )
    flow.fetch_token(code=code)
    creds = flow.credentials

    # Speichere das Token
    with open(TOKEN_FILE, "w") as f:
        f.write(creds.to_json())

    return {"message": "✅ Authentifizierung erfolgreich!"}


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
        logging.warning(
            "Kein Token gefunden. Bitte besuche https://levellevel.me/gallery/auth um dich zu authentifizieren.")
        return

    service = build('drive', 'v3', credentials=creds)
    os.makedirs(TEXT_FILE_CACHE_DIR, exist_ok=True)
    fillcache()
    init_db()


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

    # Falls der Cache existiert, lade ihn
    if os.path.exists(PAIR_CACHE_PATH):
        try:
            with open(PAIR_CACHE_PATH, 'r') as f:
                pair_cache.update(json.load(f))
                logging.info(f"[fillcache] Pair-Cache geladen: {len(pair_cache)} Paare")
                return
        except Exception as e:
            logging.warning(f"[fillcache] Fehler beim Laden von pair_cache.json: {e}")

    # Cache für die Bildnamen anstelle der IDs
    image_name_cache = {}

    q = f"'{FOLDER_ID}' in parents and trashed=false and (name contains '.txt' or mimeType contains 'image/')"
    logging.info(f"[p] : {q}")
    page_token = None
    while True:
        try:
            response = service.files().list(
                q=q,
                fields="nextPageToken, files(id, name, mimeType, webContentLink)",
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
        except Exception as e:
            logging.error(f"[fillcache] API-Fehler: {e}")
            break

        for f in response.get('files', []):
            name = f['name'].strip()
            clean_name = name.lower()  # Umwandlung in Kleinbuchstaben für konsistente Vergleiche
            fid = f['id']
            mimetype = f.get('mimeType', '')
            weblink = f.get('webContentLink', '')

            if clean_name.endswith(".txt"):
                text_id_cache[clean_name] = fid  # Verknüpft den Dateinamen mit der Text-ID
            elif mimetype.startswith("image/"):
                image_name_cache[clean_name] = (fid, weblink)  # Verknüpft den Bildnamen mit der ID und dem Web-Link

        page_token = response.get('nextPageToken', None)
        if not page_token:
            break

    # Verknüpfung von Bild- und Text-ID
    for image_name in image_name_cache:
        txt_name = image_name + ".txt"
        if txt_name in text_id_cache:
            image_id, web_link = image_name_cache[image_name]
            text_id = text_id_cache[txt_name]
            pair_cache[image_name] = {
                "image_id": image_id,
                "text_id": text_id,
                "web_link": web_link
            }

    # Speichern des aktualisierten Caches
    try:
        with open(PAIR_CACHE_PATH, 'w') as f:
            json.dump(pair_cache, f)
        logging.info(f"[fillcache] Pair-Cache gespeichert: {len(pair_cache)} Paare")
    except Exception as e:
        logging.warning(f"[fillcache] Fehler beim Speichern von pair_cache.json: {e}")

    logging.info(
        f"[fillcache] Cache vollständig aktualisiert: {len(text_id_cache)} Textdateien, {len(image_name_cache)} Bilder, {len(pair_cache)} Paare.")


@app.get("/", response_class=HTMLResponse)
def show_three_images(request: Request):
    page_str = request.query_params.get('page', '1')
    try:
        page = int(page_str)
        if page < 1:
            page = 1
    except ValueError:
        page = 1

    start = (page - 1) * IMAGES_PER_PAGE
    end = start + IMAGES_PER_PAGE
    image_keys = list(pair_cache.keys())[start:end]

    images_html_parts = []
    for image_name in image_keys:
        pair = pair_cache[image_name]
        image_id = pair['image_id']
        text_id = pair['text_id']

        if image_name not in text_cache:
            try:
                if text_id in text_cache:
                    content = text_cache[text_id]
                else:
                    content = download_text_file(service, text_id, TEXT_FILE_CACHE_DIR)
                    text_cache[image_name] = content
                    text_cache[text_id] = content
            except Exception as e:
                text_cache[image_name] = f"Fehler beim Laden: {e}"

        # Beispiel für die Status-Überprüfung, es kann angepasst werden
        status = {}  # Du kannst hier eine Logik hinzufügen, um den Status zu ermitteln

        thumbnail_src = download_thumbnail(service, image_cache, image_id)

        status = load_status(image_name)  # immer laden, egal in welchem Ordner

        # Übergabe der Status- und anderen Variablen an das Template
        images_html_parts.append(
            templates.get_template("image_entry.html").render(
                thumbnail_src=thumbnail_src,
                text_content=text_cache.get(image_name, "Kein Text gefunden"),
                image_id=image_id,
                image_name=image_name,
                status=status,  # Hier wird der Status übergeben
                kategorien=kategorien  # Wenn du die Kategorien dynamisch generierst
            )
        )

    return templates.TemplateResponse("image_gallery.html", {
        "request": request,
        "page": page,
        "total_pages": 1000,
        "current_folder": "sex",
        "count": IMAGES_PER_PAGE,
        "kategorien": kategorien,
        "images_html": ''.join(images_html_parts)
    })


@app.get("/original/{file_id}")
def show_original_image(file_id: str):
    try:
        html = f"""
        <html>
        <head><title>Originalbild</title></head>
        <body style="margin:0; padding:0; display:flex; justify-content:center; align-items:center; height:100vh; background:#000;">
            <img src="https://drive.google.com/uc?export=view&id={file_id}" style="max-width:100%; max-height:100%;" />
        </body>
        </html>
        """
        return HTMLResponse(content=html, status_code=200)

    except Exception as e:
        logging.error(f"[show_original_image] Fehler beim Laden des Bildes: {e}")
        return HTMLResponse("<p>Fehler beim Laden des Bildes.</p>", status_code=500)


@app.get("/grid", response_class=HTMLResponse)
def show_grid_view(request: Request):
    # Holen der aktuellen Seite und der Anzahl der Bilder pro Seite (count)
    page_str = request.query_params.get("page", "1")
    count_str = request.query_params.get("count", "12")  # Standardwert ist 6, wenn kein count angegeben wird

    try:
        page = int(page_str)
        count = int(count_str)  # Konvertiere count in eine Zahl
    except ValueError:
        page = 1
        count = 12  # Setze einen Standardwert, falls ungültige Werte übergeben werden

    # Berechne Start und Ende basierend auf der Seite und der Anzahl der Bilder pro Seite
    start = (page - 1) * count
    end = start + count
    image_keys = list(pair_cache.keys())[start:end]

    # Generiere die Grid-Einträge
    grid_entries = []
    for image_name in image_keys:
        pair = pair_cache[image_name]
        image_id = pair["image_id"]
        thumbnail_src = download_thumbnail(service, image_cache, image_id)

        grid_entries.append(
            templates.get_template("grid_entry.html").render(
                image_id=image_id,
                thumbnail_src=thumbnail_src,
                file_name=image_name,
                count=count
            )
        )

    # Rückgabe des Templates, ohne spalten, da das Grid im CSS dynamisch angepasst wird
    return templates.TemplateResponse("grid_gallery.html", {
        "request": request,
        "page": page,
        "count": count,
        "grid_entries": "".join(grid_entries)
    })
