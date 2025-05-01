import hashlib
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
from fastapi import Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from skimage import feature

KEIN_TEXT_GEFUNDEN = "Kein Text gefunden"

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

FOLDER_NAME = next((k["key"] for k in kategorien if k["key"] == "real"), None)

DB_PATH = Path("gallery_local.db")

SECRET_PATH = 'secrets'
CRED_FILE = os.path.join(SECRET_PATH, 'credentials.json')
TOKEN_FILE = os.path.join(SECRET_PATH, 'token.json')
REDIRECT_URI = "https://levellevel.me/gallery/auth/callback"

PAIR_CACHE_PATH = '/data/pair_cache_local.json'
TEXT_FILE_CACHE_DIR = '/data/textfiles'
IMAGE_FILE_CACHE_DIR = '/data/imagefiles'
THUMBNAIL_CACHE_DIR_300 = '/data/thumbnailfiles300'

image_cache = {}  # file_id -> { 'thumbnail': url }
text_cache = {}  # lowercase text filename -> content
pair_cache = {}  # lowercase image filename -> { image_id, text_id, web_link }
file_parents_cache = {}
rendered_image_cache = {}

app_ready = False

folders_total = len(kategorien)
current_loading_folder = ""
folders_loaded = 0


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
    app.mount("/static/thumbnails", StaticFiles(directory="/app/thumbnails"), name="thumbnails")
    app.mount("/static/imagefiles", StaticFiles(directory="/app/imagefiles"), name="imagefiles")

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
    init_db(DB_PATH)
    fillcache_local(PAIR_CACHE_PATH, IMAGE_FILE_CACHE_DIR)
    fill_folder_cache(DB_PATH)

    app_ready = True
    logging.info("🚀 Anwendung bereit!")


def init_db(db_path):
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS checkbox_status
                     (
                         image_id
                         TEXT,
                         checkbox
                         TEXT,
                         checked
                         INTEGER,
                         PRIMARY
                         KEY
                     (
                         image_id,
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


def fillcache_local(
        pair_cache_path_local,
        image_file_cache_dir):
    global pair_cache
    pair_cache.clear()  # Verhindert Vermischung mit alten Daten
    image_name_cache = {}

    # Falls der Cache existiert, lade ihn
    if os.path.exists(pair_cache_path_local):
        try:
            with open(pair_cache_path_local, 'r') as f:
                pair_cache.update(json.load(f))
                logging.info(f"[fillcache_local] Pair-Cache geladen: {len(pair_cache)} Paare")
                return
        except Exception as e:
            logging.warning(f"[fillcache_local] Fehler beim Laden von pair_cache.json: {e}")

    # Nur oberstes Verzeichnis einlesen (keine Unterordner)
    for name in os.listdir(image_file_cache_dir):
        full_path = os.path.join(image_file_cache_dir, name)
        if os.path.isfile(full_path) and name.lower().endswith((".jpg", ".jpeg", ".png", ".gif")):
            clean_name = name.lower()
            image_name_cache[clean_name] = (full_path, "")  # Kein Weblink, daher leerer Platzhalter

    # Verknüpfung von Bild- und Text-Datei
    for image_name in list(image_name_cache.keys()):
        image_path, _ = image_name_cache[image_name]
        if not os.path.exists(image_path):
            logging.warning(f"[fillcache_local] Bild fehlt und wird aus dem Cache entfernt: {image_name}")
            continue
        md5_hash = hashlib.md5(image_name.encode()).hexdigest()
        pair_cache[image_name] = {
            "image_id": md5_hash,
            "text_id": "",
            "web_link": ""
        }

    # Speichern des aktualisierten Caches
    try:
        with open(pair_cache_path_local, 'w') as f:
            json.dump(pair_cache, f)
        logging.info(f"[fillcache_local] Pair-Cache gespeichert: {len(pair_cache)} Paare")
    except Exception as e:
        logging.warning(f"[fillcache_local] Fehler beim Speichern von pair_cache.json: {e}")

    logging.info(
        f"[fillcache_local] Cache vollständig aktualisiert: "
        f"{len(image_name_cache)} Bilder, "
        f"{len(pair_cache)} Paare ")


def fill_folder_cache(db_path):
    global folders_loaded, current_loading_folder

    file_parents_cache.clear()

    with sqlite3.connect(db_path) as conn:
        # Prüfen, ob schon Einträge existieren
        row = conn.execute("SELECT COUNT(*) FROM image_folder_status").fetchone()
        if row and row[0] > 0:
            logging.info("[fill_folder_cache] 📦 Lade file_parents_cache aus der Datenbank...")

            rows = conn.execute("SELECT image_id, folder_id FROM image_folder_status").fetchall()
            for image_id, folder_id in rows:
                if folder_id not in file_parents_cache:
                    folders_loaded += 1
                    file_parents_cache[folder_id] = []
                    logging.info(
                        f"[fill_folder_cache] ✅ Cache aus DB geladen: {folders_loaded}/{folders_total} {folder_id}")
                file_parents_cache[folder_id].append(image_id)

            if folders_loaded != folders_total:
                folders_loaded = folders_total
                logging.info(f"[fill_folder_cache] ✅ Cache aus DB geladen: {folders_loaded}/{folders_total}")
            return  # <<< Fertig, nichts mehr von Google Drive laden

    # Falls KEINE Daten vorhanden → API laden
    logging.info("[fill_folder_cache] 🛰️ Keine Cache-Daten vorhanden, lade von lokal...")

    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM image_folder_status")  # <<< Sauber löschen

        for kat in kategorien:
            folder_name = kat["key"]
            current_loading_folder = kat["label"]
            file_parents_cache[folder_name] = []

            if (folder_name == 'real'):
                logging.info(f"[fill_folder_cache] Lade Dateien für Ordner: {kat['key']} ({folder_name})")
                for image_name in pair_cache.keys():
                    pair = pair_cache[image_name]
                    image_id = pair['image_id']
                    file_parents_cache[folder_name].append(image_id)
                    try:
                        # Direkt in die DB speichern
                        conn.execute("""
                                    INSERT OR REPLACE INTO image_folder_status (image_id, folder_id)
                                    VALUES (?, ?)
                                """, (image_id, folder_name))
                    except Exception as e:
                        logging.warning(
                            f"[fill_folder_cache] Fehler beim Speichern von {image_id} → {folder_name}: {e}")
                folders_loaded += 1
                logging.info(f"[fill_folder_cache] ✅ {folders_loaded}/{folders_total} Ordner geladen: {kat['key']}")
            folders_loaded += 1
    conn.commit()


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

    count_str = request.query_params.get('count', '1')
    try:
        count = int(count_str)
        if count < 1:
            count = 1
    except ValueError:
        count = 1

    folder_str = request.query_params.get('folder', 'real')

    start = (page - 1) * count
    end = start + count

    image_keys = []
    total_images = 0

    for image_name in pair_cache.keys():
        pair = pair_cache[image_name]
        image_id = pair['image_id']
        if is_file_in_folder(image_id, folder_str):
            if total_images >= start and total_images < end:
                image_keys.append(image_name.lower())
            total_images += 1

    images_html_parts = []

    recheck = next((k["key"] for k in kategorien if k["key"] == "recheck"), None)

    for image_name in image_keys:

        pair = pair_cache[image_name]
        image_id = pair['image_id']

        if count > 6 and image_id in rendered_image_cache:
            images_html_parts.append(rendered_image_cache[image_id])
        elif count <= 6 and image_id + "_T" in rendered_image_cache:
            images_html_parts.append(rendered_image_cache[image_id + "_T"])
        else:
            image_data = prepare_image_data(image_name)

            if count > 6:
                text_content = ""
            else:
                text_content = text_cache.get(image_id, KEIN_TEXT_GEFUNDEN)
                if KEIN_TEXT_GEFUNDEN == text_content:
                    set_status(image_id, recheck)

            rendered_html = templates.get_template("image_entry_local.j2").render(
                thumbnail_src=image_data["thumbnail_src"],
                text_content=text_content,
                image_name=image_name,
                image_id=image_id,
                status={},
                quality=image_data["quality"],
                quality_class=image_data["quality_class"],
                kategorien=kategorien
            )

            if count > 6:
                rendered_image_cache[image_id] = rendered_html
            else:
                rendered_image_cache[image_id + "_T"] = rendered_html

            images_html_parts.append(rendered_html)

        # Status dynamisch nachschieben
        status = load_status(image_id)
        status_json = json.dumps({f"{image_id}_{key}": value for key, value in status.items()})

        images_html_parts.append(f"""
        <script>
        const checkboxStatus_{image_id} = {status_json};
        for (const key in checkboxStatus_{image_id}) {{
            const checkbox = document.querySelector(`input[name="${{key}}"]`);
            if (checkbox) {{
                checkbox.checked = checkboxStatus_{image_id}[key];
            }}
        }}
        </script>
        """)

    # Berechnung total_pages
    total_pages = max(1, math.ceil(total_images / count))

    return templates.TemplateResponse("image_gallery_local.j2", {
        "request": request,
        "page": page,
        "total_pages": total_pages,
        "current_folder": folder_str,
        "count": count,
        "kategorien": kategorien,
        "images_html": ''.join(images_html_parts)
    })


def is_file_in_folder(image_id: str, folder_name: str) -> bool:
    """Prüft nur lokal im Cache, ob eine Datei in einem Ordner ist."""
    parents = file_parents_cache.get(folder_name, [])
    return image_id in parents


def prepare_image_data(image_name: str):
    """Bereitet alle Variablen für ein einzelnes Bild vor, inkl. Qualität."""
    image_name = image_name.lower()
    pair = pair_cache[image_name]
    image_id = pair['image_id']

    try:
        if image_id not in text_cache:
            content = download_text_file(image_name, TEXT_FILE_CACHE_DIR)
            text_cache[image_id] = content
    except Exception as e:
        text_cache[image_id] = f"Fehler beim Laden: {e}"

    local_thumbnail_path = download_and_save_image(image_id, image_name)

    if local_thumbnail_path and os.path.exists(local_thumbnail_path):
        thumbnail_src = f"/gallery/static/thumbnails/{image_name}"
    else:
        thumbnail_src = "https://via.placeholder.com/150?text=Kein+Bild"

    quality = load_image_quality(image_name)
    quality_class = get_quality_class(quality)

    return {
        "thumbnail_src": thumbnail_src,
        "image_id": image_id,
        "quality": quality,
        "quality_class": quality_class
    }


def download_text_file(image_name, cache_dir: str) -> str | None:
    file_path = os.path.join(cache_dir, f"{image_name}.txt")

    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logging.warning(f"[download_text_file] Fehler beim Lesen von Cache-Datei: {file_path} - {e}")
            return None
    return None


def set_status(image_id: str, key: str, checked: int = 1):
    if key == None:
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO checkbox_status (image_id, checkbox, checked)
            VALUES (?, ?, ?) ON CONFLICT(image_id, checkbox)
            DO
            UPDATE SET checked = excluded.checked
            """,
            (image_id, key, checked)
        )
        conn.commit()


def save_status(image_id: str, data: dict):
    # Logge die Eingabedaten für das Speichern
    logging.info(f"[save_status] Speichern des Status für {image_id}. Eingabedaten: {data}")

    with sqlite3.connect(DB_PATH) as conn:
        for key, value in data.items():
            if key in CHECKBOX_CATEGORIES:
                checked = 1 if str(value).lower() in ["1", "true", "on"] else 0
                try:
                    # Speichern der Checkbox-Daten
                    conn.execute("""
                        INSERT OR REPLACE INTO checkbox_status (image_id, checkbox, checked)
                        VALUES (?, ?, ?)
                    """, (image_id, key, checked))
                    logging.info(
                        f"[save_status] Status für Checkbox '{key}' von {image_id} gespeichert. Wert: {checked}")
                except Exception as e:
                    logging.error(f"[save_status] Fehler beim Speichern der Checkbox '{key}' für {image_id}: {e}")
            else:
                try:
                    # Speichern der Text-Daten
                    conn.execute("""
                        INSERT OR REPLACE INTO text_status (image_name, field, value)
                        VALUES (?, ?, ?)
                    """, (image_id, key, value))
                    logging.info(f"[save_status] Textfeld '{key}' für {image_id} gespeichert. Wert: {value}")
                except Exception as e:
                    logging.error(f"[save_status] Fehler beim Speichern des Textfelds '{key}' für {image_id}: {e}")


def load_status(image_id: str):
    conn = sqlite3.connect(DB_PATH)
    if conn.in_transaction:
        return

    logging.info(f"[load_status] Laden des Status für {image_id}")

    status = {}
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Wir suchen nun nach allen Checkboxen, deren name den Dateinamen enthält, z.B. "img_5242.jpg_save"
            rows = conn.execute("""
                                SELECT checkbox, checked
                                FROM checkbox_status
                                WHERE image_id = ?
                                """, (image_id,))
            for row in rows:
                # Den Bildnamen beibehalten
                checkbox_key = row[0]  # Der Schlüssel bleibt wie er ist, z.B. "img_5242.jpg_save"
                status[checkbox_key] = bool(row[1])

            # Laden der Textstatusfelder
            rows = conn.execute("""
                                SELECT field, value
                                FROM text_status
                                WHERE image_name = ?
                                """, (image_id,))
            for row in rows:
                status[row[0]] = row[1]

        logging.info(f"[load_status] Status für {image_id} erfolgreich geladen: {status}")
    except Exception as e:
        logging.error(f"[load_status] Fehler beim Laden des Status für {image_id}: {e}")
    return status


@app.post("/save")
async def save(request: Request):
    form = await request.form()
    image_id = form.get("image_id")
    data = {key: form.get(key) for key in form if key != "image_id"}

    for key in data:
        if data[key] == "on":
            data[key] = True

    save_status(image_id, data)
    return {"status": "ok"}


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


@app.get("/status/{image_name}")
def get_status_for_image(image_name: str):
    return load_status(image_name)


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
                       SELECT image_id
                       FROM checkbox_status
                       WHERE checked = 1
                         AND checkbox = ?
                       """, (save_folder_key,))
        rows = cursor.fetchall()

        logging.info(f"[move_marked_images_by_checkbox] {len(rows)} markierte Bilder gefunden für '{save_folder_key}'.")

        anzahl_verschoben = 0

        folder_id = current_folder
        save_folder_id = save_folder_key

        for (image_id,) in rows:
            success = move_file_db(conn, image_id, folder_id, save_folder_id)
            if success:
                try:
                    conn.execute("""
                                 DELETE
                                 FROM checkbox_status
                                 WHERE image_id = ?
                                   AND checkbox = ?
                                 """, (image_id, save_folder_key))
                    anzahl_verschoben += 1
                    logging.info(
                        f"[move_marked_images_by_checkbox] ✅ Verschoben: {image_id} ({current_folder}) -> ({save_folder_key})")
                except Exception as e:
                    logging.error(
                        f"[move_marked_images_by_checkbox] ❌ Fehler beim Entfernen der Checkbox von {image_id}: {e}")
            else:
                logging.warning(
                    f"[move_marked_images_by_checkbox] ⚠️ Verschieben von {image_id} nicht erfolgreich – überspringe Löschen.")

        conn.commit()

    logging.info(
        f"[move_marked_images_by_checkbox] ✅ {anzahl_verschoben} Dateien erfolgreich verschoben von '{current_folder}' nach '{save_folder_key}'.")

    return anzahl_verschoben


def move_file_db(conn, image_id: str, old_folder_id: str, new_folder_id: str, retries: int = 5) -> bool:
    """Verschiebt eine Datei nur in der lokalen Datenbank von einem Ordner in einen anderen."""
    logging.info(
        f"[move_file_db] Starte Verschieben von Datei {image_id} in der Datenbank von {old_folder_id} zu {new_folder_id} (Thread: {threading.get_ident()})")

    for attempt in range(retries):
        try:
            conn.execute("""
                         UPDATE image_folder_status
                         SET folder_id = ?
                         WHERE image_id = ?
                           AND folder_id = ?
                         """, (new_folder_id, image_id, old_folder_id))

            # Lokalen Cache anpassen
            if old_folder_id in file_parents_cache:
                try:
                    file_parents_cache[old_folder_id].remove(image_id)
                except ValueError:
                    logging.warning(
                        f"[move_file_db] Datei {image_id} war nicht im Cache von {old_folder_id} vorhanden.")

            if new_folder_id not in file_parents_cache:
                file_parents_cache[new_folder_id] = []

            if image_id not in file_parents_cache[new_folder_id]:
                file_parents_cache[new_folder_id].append(image_id)

            logging.info(f"[move_file_db] 📂 Erfolgreich verschoben (nur DB): {image_id}")
            return True

        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                logging.warning(f"[move_file_db] Datenbank gesperrt, Versuch {attempt + 1}/{retries}")
                time.sleep(0.3 * (attempt + 1))
            else:
                logging.error(f"[move_file_db] ❌ Unerwarteter Fehler bei {image_id}: {e}")
                return False
        except Exception as e:
            logging.error(f"[move_file_db] ❌ Fehler beim Verschieben von {image_id}: {e}")
            return False

    logging.error(f"[move_file_db] ❌ Max. Versuche erreicht für {image_id}: Datenbank bleibt gesperrt")
    return False


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


@app.post("/moveToFolder/{checkbox}")
async def verarbeite_checkbox(
        checkbox: str,
        count: str = Query("6"),
        folder: str = Query("real")
):
    if checkbox not in CHECKBOX_CATEGORIES:
        return JSONResponse(status_code=400, content={"status": "invalid checkbox"})

    anzahl = move_marked_images_by_checkbox(folder, checkbox)

    # Ziel-URL vorbereiten
    redirect_url = f"/gallery?page=1&count={count}&folder={checkbox}&done={checkbox}"
    logging.info(f"[move_file] 📂 Erfolgreich verschoben: {redirect_url}")
    return {"status": "ok", "redirect": redirect_url, "moved": anzahl}


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


def calculate_simple_brisque(image_path):
    """Berechnet die Fake-BRISQUE (LBP-Standardabweichung)."""
    image = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
    if image is None:
        print(f"Bild {image_path} konnte nicht geladen werden.")
        return None

    lbp = feature.local_binary_pattern(image, P=8, R=1, method="uniform")
    score = np.std(lbp)
    return score


def download_and_save_image(image_id: str, image_name: str) -> str | None:
    """Erzeugt ein Thumbnail aus einer lokalen Originalbilddatei."""
    image_path = os.path.join(IMAGE_FILE_CACHE_DIR, image_name)
    thumbnail_path = os.path.join(THUMBNAIL_CACHE_DIR_300, image_name)

    if not os.path.exists(image_path):
        logging.warning(f"[download_and_save_image] Originalbild nicht gefunden: {image_path}")
        return None

    if not os.path.exists(thumbnail_path):
        if not generate_thumbnail(image_path, thumbnail_path, image_name):
            return None

    return thumbnail_path


def generate_thumbnail(image_path: str, thumbnail_path: str, image_name: str) -> bool:
    try:
        logging.info(f"[generate_thumbnail] Erzeuge Thumbnail für {image_name}")
        img = Image.open(image_path)
        img = ImageOps.exif_transpose(img)
        img.thumbnail((300, 300), Image.Resampling.LANCZOS)
        os.makedirs(os.path.dirname(thumbnail_path), exist_ok=True)
        img.save(thumbnail_path, format="JPEG")
        logging.info(f"[generate_thumbnail] ✅ Thumbnail gespeichert: {thumbnail_path}")
        return True
    except Exception as e:
        logging.error(f"[generate_thumbnail] ❌ Fehler beim Erzeugen von Thumbnail {image_name}: {e}")
        return False


import subprocess
import os

CONTAINER = "gallery"
DB_PATH_IN_CONTAINER = "/app/gallery_local.db"
DUMP_FILE = "dump.sql"
LOCAL_DB = "gallery_local.db"


def run(cmd, **kwargs):
    logging.info("⚙️  %s", " ".join(cmd))
    subprocess.run(cmd, check=True, **kwargs)


def dump_from_container():
    logging.info("📤 Erzeuge Dump aus dem Container...")
    with open(DUMP_FILE, "w") as out:
        run(["docker", "exec", CONTAINER, "sqlite3", DB_PATH_IN_CONTAINER, ".dump"], stdout=out)
    logging.info("✅ Dump gespeichert in %s", DUMP_FILE)


def restore_to_local():
    logging.info("📥 Stelle Dump lokal wieder her...")
    if os.path.exists(LOCAL_DB):
        os.remove(LOCAL_DB)
        logging.info("🗑️  Alte lokale DB %s gelöscht.", LOCAL_DB)
    with open(DUMP_FILE, "rb") as f:
        run(["sqlite3", LOCAL_DB], stdin=f)
    logging.info("✅ Wiederherstellung in %s abgeschlossen.", LOCAL_DB)


def remove_db_in_container():
    logging.info("🧹 Entferne alte DB im Container...")
    run(["docker", "exec", CONTAINER, "rm", "-f", DB_PATH_IN_CONTAINER])


def restore_to_container():
    logging.info("📥 Spiele Dump in Container zurück...")
    with open(DUMP_FILE, "rb") as f:
        run(["docker", "exec", "-i", CONTAINER, "sqlite3", DB_PATH_IN_CONTAINER], stdin=f)
    logging.info("✅ Dump erfolgreich in Container-DB eingespielt.")


def sync_in():
    dump_from_container()
    restore_to_local()


def sync_out():
    remove_db_in_container()
    restore_to_container()


# Beispiel
if __name__ == "__main__":
    sync_in()

    DB_PATH = Path(LOCAL_DB)

    IMAGE_FILE_CACHE_DIR = '../cache/imagefiles'
    TEXT_FILE_CACHE_DIR = '../cache/textfiles'
    THUMBNAIL_CACHE_DIR_300 = '../cache/thumbnailfiles300'

    app_ready = True

    fillcache_local(
        '../cache/pair_cache_local.json',
        IMAGE_FILE_CACHE_DIR)

    fill_folder_cache(DB_PATH)

    scope = {
        "type": "http",
        "query_string": b"page=1&count=1&folder=real",
        "method": "GET",
        "path": "/",
        "headers": [],
    }
    request = Request(scope)

    # Direkt aufrufen
    response = show_images(request)
    print(response)

    sync_out()
