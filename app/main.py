import json
import logging
import math
import os
import socket
import sqlite3
import ssl
import threading
import time
from pathlib import Path

import cv2
import numpy as np
from PIL import Image, ImageOps
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from skimage import feature

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
    {"key": "recheck", "label": "Neu", "icon": "🔄", "folderid": "1EyrM6LLv_nEyB8s6zzGDGzf-hcPC76dg"},
    {"key": "bad", "label": "Schlecht", "icon": "⛔", "folderid": "1EkX7TxoRJlYUyeNA10T3Gzdt5Nd7yRRf"},
    {"key": "sex", "label": "Anzüglich", "icon": "🔞", "folderid": "1XCOjgEi0m0YGu11oPo3IZJizUf3p3tZg"},
    {"key": "ki", "label": "KI", "icon": "🤖", "folderid": "1LWF_V26zvX-W9vRNwscmeQ6U7YeJxOuL"},
    {"key": "comfyui", "label": "ComfyUI", "icon": "🛠️", "folderid": "1UjmQV-dO3y8uhqmWjSIzU1t7w6-rQEqG"},
    {"key": "document", "label": "Dokumente", "icon": "📄", "folderid": "1oKNY7jB8hEFMEn6amA6Osrbo8K9z5jAW"},
]

FOLDER_ID = next((k["folderid"] for k in kategorien if k["key"] == "real"), None)

DB_PATH = Path("gallery.db")

SECRET_PATH = 'secrets'
CRED_FILE = os.path.join(SECRET_PATH, 'credentials.json')
TOKEN_FILE = os.path.join(SECRET_PATH, 'token.json')
REDIRECT_URI = "https://levellevel.me/gallery/auth/callback"

PAIR_CACHE_PATH = '/data/pair_cache.json'
TEXT_FILE_CACHE_DIR = '/data/textfiles'
IMAGE_FILE_CACHE_DIR = '/data/imagefiles'
THUMBNAIL_CACHE_DIR = '/data/thumbnailfiles300'

image_cache = {}  # file_id -> { 'thumbnail': url }
text_cache = {}  # lowercase text filename -> content
pair_cache = {}  # lowercase image filename -> { image_id, text_id, web_link }
text_id_cache = {}  # lowercase text filename -> google file ID
file_parents_cache = {}
rendered_image_cache = {}

service = None

app_ready = False

folders_total = len(kategorien)
current_loading_folder = ""
folders_loaded = 0

# Static mount für Thumbnails
app.mount("/static/thumbnails", StaticFiles(directory="/app/thumbnails"), name="thumbnails")


@app.on_event("startup")
def init_service():
    global service

    os.environ.pop("HTTPS_PROXY", None)
    os.environ.pop("HTTP_PROXY", None)

    if not os.path.exists(CRED_FILE):
        raise RuntimeError("credentials.json fehlt")

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        logging.warning("Kein Token gefunden. Bitte besuche /gallery/auth.")
        return

    service = build('drive', 'v3', credentials=creds)
    global kategorien, CHECKBOX_CATEGORIES

    kategorien = verify_folders_exist(service, kategorien)
    CHECKBOX_CATEGORIES = [k["key"] for k in kategorien]

    # Sofort Thread starten!
    threading.Thread(target=slow_start, daemon=True).start()


def slow_start():
    global app_ready

    logging.info("🏁 Starte langsames Initialisieren...")

    # erst nach dem Start langsam laden
    init_db()
    fillcache()
    fill_folder_cache()

    # move_all_images_to_real_folder()

    app_ready = True
    logging.info("🚀 Anwendung bereit!")


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS checkbox_status
                     (
                         image_name
                         TEXT,
                         checkbox
                         TEXT,
                         checked
                         INTEGER,
                         PRIMARY
                         KEY
                     (
                         image_name,
                         checkbox
                     )
                         )
                     """)
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS text_status
                     (
                         image_name
                         TEXT,
                         field
                         TEXT,
                         value
                         TEXT,
                         PRIMARY
                         KEY
                     (
                         image_name,
                         field
                     )
                         )
                     """)

        conn.execute("""
                     CREATE TABLE IF NOT EXISTS image_folder_status
                     (
                         image_id
                         TEXT
                         PRIMARY
                         KEY,
                         folder_id
                         TEXT
                     )
                     """)

        conn.execute("""
                     CREATE TABLE IF NOT EXISTS image_quality
                     (
                         image_name
                         TEXT
                         PRIMARY
                         KEY,
                         quality
                         INTEGER
                     )
                     """)


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
                                SELECT checkbox, checked
                                FROM checkbox_status
                                WHERE image_name = ?
                                """, (image_name,))
            for row in rows:
                # Den Bildnamen beibehalten
                checkbox_key = row[0]  # Der Schlüssel bleibt wie er ist, z.B. "img_5242.jpg_save"
                status[checkbox_key] = bool(row[1])

            # Laden der Textstatusfelder
            rows = conn.execute("""
                                SELECT field, value
                                FROM text_status
                                WHERE image_name = ?
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


def verify_folders_exist(service, kategorien):
    """Filtert Kategorien nach tatsächlich existierenden Ordnern."""
    valid_kategorien = []
    for kat in kategorien:
        try:
            folder = service.files().get(fileId=kat["folderid"], fields="id").execute()
            if folder.get("id") == kat["folderid"]:
                valid_kategorien.append(kat)
        except Exception as e:
            logging.warning(f"[verify_folders_exist] Ordner nicht gefunden: {kat['key']} ({kat['folderid']}) - {e}")
    return valid_kategorien


def download_text_file(image_name, file_id: str, cache_dir: str) -> str:
    file_path = os.path.join(cache_dir, f"{image_name}.txt")

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
            logging.info(f"[download_text_file] ✅ Text gespeichert: {file_path}")
        return content
    except Exception as e:
        logging.warning(f"[download_text_file] Fehler beim Laden: {e}")
        raise


def download_thumbnail(image_cache, file_id):
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
    global pair_cache
    text_id_cache = {}
    image_name_cache = {}

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
        image_name = image_name.lower()
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


@app.get("/status/{image_name}")
def get_status_for_image(image_name: str):
    return load_status(image_name)


def fill_folder_cache():
    global folders_loaded, current_loading_folder

    file_parents_cache.clear()

    with sqlite3.connect(DB_PATH) as conn:
        # Prüfen, ob schon Einträge existieren
        row = conn.execute("SELECT COUNT(*) FROM image_folder_status").fetchone()
        if row and row[0] > 0:
            logging.info("[fill_folder_cache] 📦 Lade file_parents_cache aus der Datenbank...")

            rows = conn.execute("SELECT image_id, folder_id FROM image_folder_status").fetchall()
            for image_id, folder_id in rows:
                if folder_id not in file_parents_cache:
                    file_parents_cache[folder_id] = []
                file_parents_cache[folder_id].append(image_id)

            folders_loaded = folders_total
            logging.info(f"[fill_folder_cache] ✅ Cache aus DB geladen: {folders_loaded}/{folders_total} Ordner")
            return  # <<< Fertig, nichts mehr von Google Drive laden

    # Falls KEINE Daten vorhanden → API laden
    logging.info("[fill_folder_cache] 🛰️ Keine Cache-Daten vorhanden, lade von Google Drive...")

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM image_folder_status")  # <<< Sauber löschen

        for kat in kategorien:
            folder_id = kat["folderid"]
            current_loading_folder = kat["label"]
            logging.info(f"[fill_folder_cache] Lade Dateien für Ordner: {kat['key']} ({folder_id})")

            query = f"'{folder_id}' in parents and trashed = false"
            page_token = None
            file_parents_cache[folder_id] = []

            while True:
                try:
                    response = service.files().list(
                        q=query,
                        spaces='drive',
                        fields='nextPageToken, files(id)',
                        pageToken=page_token,
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True
                    ).execute()
                except Exception as e:
                    logging.warning(f"[fill_folder_cache] Fehler beim Laden für {kat['key']} ({folder_id}): {e}")
                    break

                for file in response.get('files', []):
                    file_id = file['id']
                    file_parents_cache[folder_id].append(file_id)

                    try:
                        # Direkt in die DB speichern
                        conn.execute("""
                            INSERT OR REPLACE INTO image_folder_status (image_id, folder_id)
                            VALUES (?, ?)
                        """, (file_id, folder_id))
                    except Exception as e:
                        logging.warning(f"[fill_folder_cache] Fehler beim Speichern von {file_id} → {folder_id}: {e}")

                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break

            folders_loaded += 1
            logging.info(f"[fill_folder_cache] ✅ {folders_loaded}/{folders_total} Ordner geladen: {kat['key']}")

        conn.commit()


def init_folder_cache(FOLDER_ID):
    file_parents_cache.clear()
    file_parents_cache[FOLDER_ID] = []

    for image_name, pair in pair_cache.items():
        image_id = pair['image_id']
        file_parents_cache[FOLDER_ID].append(image_id)


def is_file_in_folder(service, file_id: str, folder_id: str) -> bool:
    """Prüft nur lokal im Cache, ob eine Datei in einem Ordner ist."""
    parents = file_parents_cache.get(folder_id, [])
    return file_id in parents


def clear_folder_parents_cache(folder_id: str):
    if folder_id in file_parents_cache:
        del file_parents_cache[folder_id]


@app.get("/loading_status")
def loading_status():
    return {
        "ready": app_ready,
        "current_folder": current_loading_folder,
        "folders_loaded": folders_loaded,
        "folders_total": folders_total
    }


def render_status(status: dict) -> str:
    html = []
    for key, checked in status.items():
        if checked:
            html.append(f'<input type="checkbox" name="{key}" checked>')
        else:
            html.append(f'<input type="checkbox" name="{key}">')
    return '\n'.join(html)


@app.get("/", response_class=HTMLResponse)
def show_images(request: Request):
    if not app_ready:
        return templates.TemplateResponse("loading.html", {"request": request}, status_code=200)

    page_str = request.query_params.get('page', '1')
    try:
        page = int(page_str)
        if page < 1:
            page = 1
    except ValueError:
        page = 1

    count_str = request.query_params.get('count', '6')
    try:
        count = int(count_str)
        if count < 1:
            count = 6
    except ValueError:
        count = 6

    folder_str = request.query_params.get('folder', 'real')
    folder_id = get_folderid_for_checkbox(folder_str)

    start = (page - 1) * count
    end = start + count

    image_keys = []
    for image_name in pair_cache.keys():
        pair = pair_cache[image_name]
        image_id = pair['image_id']

        # Prüfen, ob die Datei im gewünschten Ordner ist
        if is_file_in_folder(service, image_id, folder_id):
            image_keys.append(image_name)

    total_images = len(image_keys)
    image_keys = image_keys[start:end]

    images_html_parts = []

    for image_name in image_keys:
        image_name = image_name.lower()

        # 1. Laden der aktuellen Statusdaten
        status = load_status(image_name)

        if count > 6 and image_name in rendered_image_cache:
            images_html_parts.append(rendered_image_cache[image_name])
        elif count <= 6 and image_name + "_T" in rendered_image_cache:
            images_html_parts.append(rendered_image_cache[image_name + "_T"])
        else:
            image_data = prepare_image_data(image_name)

            if count > 6:
                text_content = ""
            else:
                text_content = text_cache.get(image_name, "Kein Text gefunden")

            rendered_html = templates.get_template("image_entry.j2").render(
                thumbnail_src=image_data["thumbnail_src"],
                text_content=text_content,
                image_id=image_data["image_id"],
                image_name=image_data["image_name"],
                status={},
                quality=image_data["quality"],
                quality_class=image_data["quality_class"],
                kategorien=kategorien
            )

            if len(text_content) == 0:
                rendered_image_cache[image_name] = rendered_html
            else:
                rendered_image_cache[image_name + "_T"] = rendered_html

            images_html_parts.append(rendered_html)

        # Status dynamisch nachschieben
        status_json = json.dumps({f"{image_name}_{key}": value for key, value in status.items()})
        safe_image_id = pair_cache[image_name]["image_id"].replace("-", "_")

        images_html_parts.append(f"""
        <script>
        const checkboxStatus_{safe_image_id} = {status_json};
        for (const key in checkboxStatus_{safe_image_id}) {{
            const checkbox = document.querySelector(`input[name="${{key}}"]`);
            if (checkbox) {{
                checkbox.checked = checkboxStatus_{safe_image_id}[key];
            }}
        }}
        </script>
        """)

    # Berechnung total_pages
    total_pages = max(1, math.ceil(total_images / count))

    return templates.TemplateResponse("image_gallery.j2", {
        "request": request,
        "page": page,
        "total_pages": total_pages,
        "current_folder": folder_str,
        "count": count,
        "kategorien": kategorien,
        "images_html": ''.join(images_html_parts)
    })


@app.get("/verarbeite/check/{checkbox}")
def verarbeite_check_checkbox(checkbox: str):
    if checkbox not in CHECKBOX_CATEGORIES:
        return {"error": "ungültige Kategorie"}
    with sqlite3.connect(DB_PATH) as conn:
        count = conn.execute("""
                             SELECT COUNT(*)
                             FROM checkbox_status
                             WHERE checked = 1
                               AND checkbox = ?
                             """, (checkbox,)).fetchone()[0]
    return {"count": count}


def move_marked_images_by_checkbox(current_folder: str, save_folder_key: str) -> int:
    logging.info(f"[move_marked_images_by_checkbox] Starte Verschieben von '{current_folder}' nach '{save_folder_key}'")

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       SELECT image_name
                       FROM checkbox_status
                       WHERE checked = 1
                         AND checkbox = ?
                       """, (save_folder_key,))
        rows = cursor.fetchall()

        logging.info(f"[move_marked_images_by_checkbox] {len(rows)} markierte Bilder gefunden für '{save_folder_key}'.")

        anzahl_verschoben = 0

        folder_id = get_folderid_for_checkbox(current_folder)
        save_folder_id = get_folderid_for_checkbox(save_folder_key)

        for (image_name,) in rows:
            image_name = image_name.lower()

            if image_name not in pair_cache:
                logging.warning(
                    f"[move_marked_images_by_checkbox] ⚠️ Bild '{image_name}' nicht im Pair-Cache gefunden, übersprungen.")
                continue

            image_id = pair_cache[image_name]["image_id"]

            try:
                move_file_db(image_id, folder_id, save_folder_id)
                conn.execute("""
                             DELETE
                             FROM checkbox_status
                             WHERE image_name = ?
                               AND checkbox = ?
                             """, (image_name, save_folder_key))
                anzahl_verschoben += 1
                logging.info(f"[move_marked_images_by_checkbox] ✅ Verschoben: {image_name} ({image_id})")
            except Exception as e:
                logging.error(f"[move_marked_images_by_checkbox] ❌ Fehler beim Verschieben von {image_name}: {e}")

        conn.commit()

    logging.info(
        f"[move_marked_images_by_checkbox] ✅ {anzahl_verschoben} Dateien erfolgreich verschoben von '{current_folder}' nach '{save_folder_key}'.")

    return anzahl_verschoben


def move_file_db(file_id: str, old_folder_id: str, new_folder_id: str):
    """Verschiebt eine Datei nur in der lokalen Datenbank von einem Ordner in einen anderen."""
    logging.info(
        f"[move_file_db] Starte Verschieben von Datei {file_id} in der Datenbank von {old_folder_id} zu {new_folder_id}")

    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Update in der Datenbank
            conn.execute("""
                         UPDATE image_folder_status
                         SET folder_id = ?
                         WHERE image_id = ?
                           AND folder_id = ?
                         """, (new_folder_id, file_id, old_folder_id))
            conn.commit()

        # Lokalen Cache anpassen
        if old_folder_id in file_parents_cache:
            try:
                file_parents_cache[old_folder_id].remove(file_id)
            except ValueError:
                logging.warning(f"[move_file_db] Datei {file_id} war nicht im Cache von {old_folder_id} vorhanden.")

        if new_folder_id not in file_parents_cache:
            file_parents_cache[new_folder_id] = []

        if file_id not in file_parents_cache[new_folder_id]:
            file_parents_cache[new_folder_id].append(file_id)

        logging.info(f"[move_file_db] 📂 Erfolgreich verschoben (nur DB): {file_id}")

    except Exception as e:
        logging.error(f"[move_file_db] ❌ Fehler beim Verschieben von {file_id}: {e}")


def move_file(file_id: str, old_folder_id: str, new_folder_id: str):
    """Verschiebt eine Datei auf Google Drive von einem Ordner in einen anderen."""
    logging.info(f"[move_file] Starte Verschieben von Datei {file_id} von {old_folder_id} zu {new_folder_id}")

    service.files().update(
        fileId=file_id,
        addParents=new_folder_id,
        removeParents=old_folder_id,
        fields='id, parents'
    ).execute()

    # Update lokaler Cache
    if old_folder_id in file_parents_cache:
        try:
            file_parents_cache[old_folder_id].remove(file_id)
        except ValueError:
            logging.warning(f"[move_file] Datei {file_id} war nicht in {old_folder_id} vorhanden.")

    if new_folder_id not in file_parents_cache:
        file_parents_cache[new_folder_id] = []

    if file_id not in file_parents_cache[new_folder_id]:
        file_parents_cache[new_folder_id].append(file_id)

    logging.info(f"[move_file] 📂 Erfolgreich verschoben: {file_id}")


from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


@app.post("/moveToFolder/{checkbox}")
async def verarbeite_checkbox(checkbox: str, request: Request):
    referer = request.headers.get("referer", "/gallery/?page=1&count=6&folder=real")
    parsed_url = urlparse(referer)
    query = parse_qs(parsed_url.query)

    # Hole aktuelle page, count und folder aus der URL
    page = query.get("page", ["1"])[0]
    count = query.get("count", ["6"])[0]
    folder = query.get("folder", ["real"])[0]

    if checkbox not in CHECKBOX_CATEGORIES:
        # Fehler: einfach sauber ohne done zurück
        clean_query = urlencode({
            "page": page,
            "count": count,
            "folder": folder
        })
        clean_url = urlunparse(parsed_url._replace(query=clean_query))
        return RedirectResponse(url=clean_url, status_code=303)

    anzahl_verschoben = move_marked_images_by_checkbox("real", checkbox)

    # Erfolgreich: mit done zurück
    done_query = urlencode({
        "page": page,
        "count": count,
        "folder": folder,
        "done": checkbox
    })
    done_url = urlunparse(parsed_url._replace(query=done_query))
    return RedirectResponse(url=done_url, status_code=303)


@app.get("/moveToFolder/{checkbox}")
def get_marked_images_count(checkbox: str):
    if checkbox not in CHECKBOX_CATEGORIES:
        return {"count": 0}
    with sqlite3.connect(DB_PATH) as conn:
        count = conn.execute("""
                             SELECT COUNT(*)
                             FROM checkbox_status
                             WHERE checked = 1
                               AND checkbox = ?
                             """, (checkbox,)).fetchone()[0]
    return {"count": count}


# Hilfsfunktion: hole folderid anhand checkbox/key
def get_folderid_for_checkbox(cat: str) -> str:
    for kategorie in kategorien:
        if kategorie["key"] == cat:
            return kategorie["folderid"]
    raise ValueError(f"Keine folderid gefunden für Checkbox '{cat}'")


def get_quality_class(quality: int) -> str:
    """Gibt eine CSS-Klasse basierend auf der Qualität zurück."""
    if quality is None:
        return "quality-unknown"
    elif quality >= 70:
        return "quality-good"
    elif quality >= 40:
        return "quality-medium"
    else:
        return "quality-bad"


def prepare_image_data(image_name: str):
    """Bereitet alle Variablen für ein einzelnes Bild vor, inkl. Qualität."""
    image_name = image_name.lower()
    pair = pair_cache[image_name]
    image_id = pair['image_id']
    text_id = pair['text_id']

    try:
        if image_name not in text_cache:
            content = download_text_file(image_name, text_id, TEXT_FILE_CACHE_DIR)
            text_cache[image_name] = content
    except Exception as e:
        text_cache[image_name] = f"Fehler beim Laden: {e}"

    local_thumbnail_path = download_and_save_image(image_id, image_name)

    if os.path.exists(local_thumbnail_path):
        thumbnail_src = f"/gallery/static/thumbnails/{image_name}"
    else:
        # Falls nicht vorhanden → Drive-Thumbnail verwenden
        thumbnail_src = download_thumbnail(image_cache, image_id)

    quality = load_image_quality(image_name)

    # Qualität Klasse bestimmen
    quality_class = get_quality_class(quality)

    return {
        "thumbnail_src": thumbnail_src,
        "image_id": image_id,
        "image_name": image_name,
        "quality": quality,
        "quality_class": quality_class
    }


def load_image_quality(image_name: str) -> int:
    """Lädt die Qualitätsbewertung (0-100) eines Bildes aus der Datenbank, case-insensitiv."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            result = conn.execute("""
                                  SELECT quality
                                  FROM image_quality
                                  WHERE LOWER(image_name) = LOWER(?)
                                  """, (image_name,)).fetchone()
            if result:
                return result[0]
    except Exception as e:
        logging.error(f"[load_image_quality] Fehler beim Laden der Qualität für {image_name}: {e}")
    return None


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


def move_all_images_to_real_folder():
    """Verschiebt ALLE Dateien aus ALLEN Ordnern in den real-Ordner, echtes Live-Lesen ohne Cache."""
    logging.info("[move_all_images_to_real_folder] 🚀 Starte Verschieben aller Dateien nach 'real' (ohne Cache)...")

    real_folder_id = get_folderid_for_checkbox("real")
    anzahl_verschoben = 0

    for folder_key in CHECKBOX_CATEGORIES:
        if folder_key == "real":
            continue  # 'real'-Ordner selbst überspringen

        source_folder_id = get_folderid_for_checkbox(folder_key)

        logging.info(f"[move_all_images_to_real_folder] 📂 Lade Dateien aus Ordner: {folder_key} ({source_folder_id})")
        page_token = None

        while True:
            try:
                response = service.files().list(
                    q=f"'{source_folder_id}' in parents and trashed = false",
                    spaces='drive',
                    fields='nextPageToken, files(id)',
                    pageToken=page_token,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                ).execute()
            except Exception as e:
                logging.error(f"[move_all_images_to_real_folder] ❌ Fehler beim Laden der Dateien: {e}")
                break

            for file in response.get('files', []):
                file_id = file['id']
                try:
                    move_file(file_id, source_folder_id, real_folder_id)
                    anzahl_verschoben += 1
                    logging.info(f"[move_all_images_to_real_folder] ✅ Datei {file_id} verschoben")
                except Exception as e:
                    logging.error(f"[move_all_images_to_real_folder] ❌ Fehler beim Verschieben von {file_id}: {e}")

            page_token = response.get('nextPageToken')
            if not page_token:
                break

    logging.info(f"[move_all_images_to_real_folder] 🎯 Insgesamt {anzahl_verschoben} Dateien erfolgreich verschoben.")


def calculate_simple_brisque(image_path):
    """Berechnet die Fake-BRISQUE (LBP-Standardabweichung)."""
    image = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
    if image is None:
        print(f"Bild {image_path} konnte nicht geladen werden.")
        return None

    lbp = feature.local_binary_pattern(image, P=8, R=1, method="uniform")
    score = np.std(lbp)
    return score


def download_and_save_image(file_id: str, image_name: str) -> str | None:
    """Lädt das Originalbild und speichert es + Thumbnail lokal."""
    image_path = os.path.join(IMAGE_FILE_CACHE_DIR, image_name)
    thumbnail_path = os.path.join(THUMBNAIL_CACHE_DIR, image_name)

    # Original speichern
    if not os.path.exists(image_path):
        try:
            logging.info(f"[download_and_save_image] Lade Originalbild für {image_name}")
            request = service.files().get_media(fileId=file_id)
            content = request.execute()
            os.makedirs(IMAGE_FILE_CACHE_DIR, exist_ok=True)
            with open(image_path, 'wb') as f:
                f.write(content)
            logging.info(f"[download_and_save_image] ✅ Originalbild gespeichert: {image_path}")
        except Exception as e:
            logging.error(f"[download_and_save_image] ❌ Fehler beim Laden von Originalbild {image_name}: {e}")
            return None

    # Thumbnail erzeugen
    if not os.path.exists(thumbnail_path):
        try:
            logging.info(f"[download_and_save_image] Erzeuge Thumbnail für {image_name}")
            img = Image.open(image_path)
            img = ImageOps.exif_transpose(img)
            img.thumbnail((300, 300),
                          Image.Resampling.LANCZOS)  # max 300x300, Seitenverhältnis wird AUTOMATISCH behalten
            os.makedirs(THUMBNAIL_CACHE_DIR, exist_ok=True)
            img.save(thumbnail_path, format="JPEG")
            logging.info(f"[download_and_save_image] ✅ Thumbnail gespeichert: {thumbnail_path}")
        except Exception as e:
            logging.error(f"[download_and_save_image] ❌ Fehler beim Erzeugen von Thumbnail {image_name}: {e}")

    return thumbnail_path
