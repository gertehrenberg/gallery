import hashlib
import json
import logging
import math
import os
import shutil
import socket
import sqlite3
import ssl
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

import cv2
import numpy as np
from PIL import Image, ImageOps
from PIL.ExifTags import TAGS, GPSTAGS
from fastapi import Depends, Request
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Query
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from geopy.geocoders import Nominatim
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from skimage import feature
from starlette.middleware.sessions import SessionMiddleware

from app.auth import router as auth_router
from app.login import router as login_router

GESICHTER_TYPE = 3

script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

from config import IMAGE_EXTENSIONS, CACHE_DATEI_NAME

KEIN_TEXT_GEFUNDEN = "Kein Text gefunden"

logging.basicConfig(level=logging.INFO)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="**idefix**")
app.include_router(auth_router)
app.include_router(login_router)

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly"
]

kategorien = [
    {"key": "real", "label": "Alle Bilder", "icon": "💾", "folderid": "1fyE_ZYoVoGZ7ehjuWrS9Kd6WW4w2UZWy"},
    {"key": "top", "label": "Fast Perfekt", "icon": "💎", "folderid": "1oKNY7jB8hEFMEn7amA6Osrbo8K9z5jAX"},
    {"key": "delete", "label": "Löschen", "icon": "❌", "folderid": "1wjUj6NHZ_ZHwlahQuJUbCTf_HplqePVw"},
    {"key": "recheck", "label": "Neu", "icon": "🔄", "folderid": "1EyrM6LLv_nEyB8s6zzGDGzf-hcPC76dg"},
    {"key": "bad", "label": "Schlecht", "icon": "⛔", "folderid": "1EkX7TxoRJlYUyeNA10T3Gzdt5Nd7yRRf"},
    {"key": "sex", "label": "Anzüglich", "icon": "🔞", "folderid": "1XCOjgEi0m0YGu11oPo3IZJizUf3p3tZg"},
    {"key": "ki", "label": "KI", "icon": "🤖", "folderid": "1LWF_V26zvX-W9vRNwscmeQ6U7YeJxOuL"},
    {"key": "comfyui", "label": "ComfyUI", "icon": "🛠️", "folderid": "1UjmQV-dO3y8uhqmWjSIzU1t7w6-rQEqG"},
    {"key": "document", "label": "Dokumente", "icon": "📄", "folderid": "1oKNY7jB8hEFMEn6amA6Osrbo8K9z5jAW"},
    {"key": "double", "label": "Doppelt?", "icon": "👯", "folderid": "1oKNY7jB8hEFMEn6amA6Osrbo8K9z5jAX"},
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
GESICHTER_FILE_CACHE_DIR = '/data/facefiles'
THUMBNAIL_CACHE_DIR_300 = '/data/thumbnailfiles300'

CONTAINER = "gallery"
DB_PATH_IN_CONTAINER = "/app/gallery_local.db"
DUMP_FILE = "dump.sql"
LOCAL_DB = "gallery_local.db"

image_cache = {}  # file_id -> { 'thumbnail': url }
text_cache = {}  # lowercase text filename -> content
pair_cache = {}  # lowercase image filename -> { image_id, text_id, web_link }
file_parents_cache = {}
rendered_image_cache = {}

app_ready = False

folders_total = len(kategorien)
current_loading_folder = ""
folders_loaded = 0

face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")

# Cache laden oder leeres Dictionary erstellen
if CACHE_DATEI_NAME.exists():
    with open(CACHE_DATEI_NAME, "r", encoding="utf-8") as f:
        geo_cache = json.load(f)
else:
    geo_cache = {}


def require_login(request: Request):
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=307, headers={"Location": "/gallery/login"})
    return user


@app.get("/static/thumbnails/{file_path:path}")
async def get_thumbnail(file_path: str, request: Request, user: str = Depends(require_login)):
    file = Path("/app/thumbnails") / file_path
    if file.exists() and file.is_file():
        return FileResponse(file)
    raise HTTPException(status_code=404)


@app.get("/static/imagefiles/{file_path:path}")
async def get_imagefile(file_path: str, request: Request, user: str = Depends(require_login)):
    file = Path("/app/imagefiles") / file_path
    if file.exists() and file.is_file():
        return FileResponse(file)
    raise HTTPException(status_code=404)


@app.get("/static/facefiles/{file_path:path}")
async def get_facefile(file_path: str, request: Request, user: str = Depends(require_login)):
    file = Path("/app/facefiles") / file_path
    if file.exists() and file.is_file():
        return FileResponse(file)
    raise HTTPException(status_code=404)


@app.on_event("startup")
def init_service():
    os.environ.pop("HTTPS_PROXY", None)
    os.environ.pop("HTTP_PROXY", None)

    if not os.path.exists(CRED_FILE):
        raise RuntimeError("credentials.json fehlt")

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        logging.warning("Kein Token gefunden. Bitte besuche 'https://levellevel.me/gallery/auth'.")
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

        # image_quality(conn) # ist alt wurde mit migrate_score migriert
        # migrate_score()

        image_quality_scores(conn)


def image_quality(conn):
    conn.execute("""
                 CREATE TABLE IF NOT EXISTS image_quality
                 (
                     image_name
                     TEXT
                     PRIMARY
                     KEY,
                     scoreq1
                     INTEGER,
                     scoreq2
                     INTEGER
                 )
                 """)


def image_quality_scores(conn):
    conn.execute("""
                 CREATE TABLE IF NOT EXISTS image_quality_scores
                 (
                     image_name
                     TEXT,
                     score_type
                     INTEGER,
                     score
                     INTEGER,
                     PRIMARY
                     KEY
                 (
                     image_name,
                     score_type
                 )
                     )
                 """)


def migrate_score():
    """Migriert Daten aus image_quality nach image_quality_scores."""
    with sqlite3.connect(DB_PATH) as conn:
        # Neue Tabelle anlegen
        image_quality_scores(conn)
        conn.execute("DELETE FROM image_quality_scores")  # <<< Sauber löschen

        # Alte Daten lesen
        rows = conn.execute("SELECT image_name, scoreq1, scoreq2 FROM image_quality").fetchall()

        for image_name, scoreq1, scoreq2 in rows:
            conn.execute("""
                INSERT OR REPLACE INTO image_quality_scores (image_name, score_type, score)
                VALUES (?, ?, ?)
            """, (image_name, 1, scoreq1))

            conn.execute("""
                INSERT OR REPLACE INTO image_quality_scores (image_name, score_type, score)
                VALUES (?, ?, ?)
            """, (image_name, 2, scoreq2))

        conn.commit()
        logging.info(f"[migrate_score] ✅ {len(rows)} Einträge migriert.")

    """Löscht die alte image_quality-Tabelle dauerhaft."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DROP TABLE IF EXISTS image_quality")
        conn.commit()
        logging.info("[drop_old_quality_table] 🗑️ Tabelle image_quality gelöscht.")


def fillcache_local(
        pair_cache_path_local,
        image_file_cache_dir):
    global pair_cache
    pair_cache.clear()  # Verhindert Vermischung mit alten Daten

    # Falls der Cache existiert, lade ihn
    if os.path.exists(pair_cache_path_local):
        try:
            with open(pair_cache_path_local, 'r') as f:
                pair_cache.update(json.load(f))
                logging.info(f"[fillcache_local] Pair-Cache geladen: {len(pair_cache)} Paare")
                return
        except Exception as e:
            logging.warning(f"[fillcache_local] Fehler beim Laden von pair_cache.json: {e}")

    image_paths = []

    for name in os.listdir(image_file_cache_dir):
        full_path = os.path.join(image_file_cache_dir, name)
        if os.path.isfile(full_path) and name.lower().endswith(IMAGE_EXTENSIONS):
            image_paths.append(full_path)
        elif os.path.isdir(full_path):
            for subname in os.listdir(full_path):
                subpath = os.path.join(full_path, subname)
                if os.path.isfile(subpath) and subname.lower().endswith(IMAGE_EXTENSIONS):
                    image_paths.append(subpath)

    # Nach Anlegedatum sortieren (ctime)
    image_paths.sort(key=lambda p: os.path.getctime(p), reverse=True)

    image_name_cache = {
        os.path.basename(p).lower(): (p, "") for p in image_paths
    }

    logging.info(f"[image_name_cache] 📂 Gelesen Bilder aus: {len(image_name_cache)}")

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

            folder_path = Path(IMAGE_FILE_CACHE_DIR) / folder_name
            if not folder_path.exists():
                logging.warning(f"[fill_folder_cache] ⚠️ Ordner nicht gefunden: {folder_path}")
                continue

            logging.info(f"[fill_folder_cache] 📂 Lese Bilder aus: {folder_name}")

            for image_file in folder_path.iterdir():
                if not image_file.is_file():
                    continue
                image_name = image_file.name.lower()
                pair = pair_cache.get(image_name)
                if not pair:
                    logging.warning(f"[fill_folder_cache] ⚠️ Kein Eintrag im pair_cache für: {image_name}")
                    continue

                image_id = pair["image_id"]
                file_parents_cache[folder_name].append(image_id)

                try:
                    conn.execute("""
                                INSERT OR REPLACE INTO image_folder_status (image_id, folder_id)
                                VALUES (?, ?)
                            """, (image_id, folder_name))
                except Exception as e:
                    logging.warning(f"[fill_folder_cache] Fehler beim Speichern von {image_id} → {folder_name}: {e}")

            folders_loaded += 1
            logging.info(f"[fill_folder_cache] ✅ {folders_loaded}/{folders_total} Ordner geladen: {folder_name}")

    conn.commit()


@app.get("/images", response_class=HTMLResponse)
def show_image(
        request: Request,
        user: str = Depends(require_login)
):
    # Query-Parameter sicher parsen
    try:
        page = int(request.query_params.get('page') or 1)
        count = int(request.query_params.get('count') or 1)
    except ValueError:
        page = 1
        count = 1

    folder_name = request.query_params.get('folder', 'real')
    textflag = request.query_params.get('textflag', '1')
    image_name = request.query_params.get('image_name', '')
    image_name = unquote(image_name).strip()
    image_name = image_name.strip().lower()
    pagecounter = 0

    for image_name_l in pair_cache:
        pair = pair_cache[image_name_l]
        image_id = pair.get("image_id", "")
        if is_file_in_folder(image_id, folder_name):
            pagecounter += 1
            if image_name_l.strip().lower() == image_name:
                clean(image_name)
                return RedirectResponse(
                    url=f"/gallery/?page={pagecounter}&count=1&folder={folder_name}&textflag=2&lastpage={page}&lastcount={count}&lasttextflag={textflag}"
                )

    # Fallback, wenn Bild nicht gefunden wurde
    return RedirectResponse(
        url=f"/gallery/?page={page}&count={count}&folder={folder_name}&textflag={textflag}"
    )


@app.get("/clean")
def clean_image(image_name: str = Query(...)):
    return clean(image_name)


def clean(image_name: str):
    global rendered_image_cache, text_cache
    print(f"Bereinige Bild: {image_name}")

    # Eintrag aus Datenbank löschen
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       DELETE
                       FROM checkbox_status
                       WHERE LOWER(image_name) = LOWER(?)
                       """, (image_name,))  # <-- Tupel beachten!

        cursor = conn.cursor()
        cursor.execute("""
                       DELETE
                       FROM image_quality_scores
                       WHERE LOWER(image_name) = LOWER(?)
                       """, (image_name,))  # <-- Tupel beachten!

    # Caches bereinigen, falls Schlüssel vorhanden
    if image_name in text_cache:
        print(f"[clean_image] ✅ text_cache gelöscht {image_name}")
        text_cache.pop(image_name, None)

    image_id = find_image_id_by_name(image_name)
    for i in range(1, 5):
        key = f"{image_id}_{i}"
        if key in rendered_image_cache:
            print(f"[clean_image] ✅ rendered_image_cache gelöscht {key}")
            rendered_image_cache.pop(f"{key}", None)
        else:
            print(f"[clean_image] ❌ rendered_image_cache leer {key}")

    thumbnail_path = os.path.join(THUMBNAIL_CACHE_DIR_300, image_name)
    if os.path.exists(thumbnail_path):
        print(f"[clean_image] ✅ gelöscht {thumbnail_path}")
        os.remove(thumbnail_path)

    face_dir = Path(GESICHTER_FILE_CACHE_DIR)
    base_name = Path(image_name).stem
    # Alle passenden Dateien wie "img_0555_0.jpg", "img_0555_1.jpg", ...
    for file in face_dir.glob(f"{base_name}_*.jpg"):
        try:
            file.unlink()
            print(f"[clean_image] ✅ gelöscht {file}")
        except Exception as e:
            print(f"[clean_image] ❌ Fehler beim Löschen von {file}: {e}")

    return JSONResponse(content={"status": "ok", "image_name": image_name})


@app.get("/", response_class=HTMLResponse)
def show_images(
        request: Request,
        user: str = Depends(require_login)):
    if not app_ready:
        return templates.TemplateResponse("loading.html", {"request": request}, status_code=200)

    page = int(request.query_params.get('page', '1') or 1)
    count = int(request.query_params.get('count', '1') or 1)
    folder_name = request.query_params.get('folder', 'real')
    textflag = request.query_params.get('textflag', '1')

    try:
        lastpage = int(request.query_params.get('lastpage', 0))
    except ValueError:
        lastpage = 0

    try:
        lastcount = int(request.query_params.get('lastcount', 0))
    except ValueError:
        lastcount = 0

    try:
        lasttextflag = int(request.query_params.get('lasttextflag', 0))
    except ValueError:
        lasttextflag = 0

    start = (page - 1) * count
    end = start + count

    image_keys = []
    total_images = 0

    for image_name in pair_cache.keys():
        pair = pair_cache[image_name]
        image_id = pair['image_id']
        if is_file_in_folder(image_id, folder_name):
            if start <= total_images < end:
                image_keys.append(image_name.lower())
            total_images += 1

    images_html_parts = []

    recheck = next((k["key"] for k in kategorien if k["key"] == "recheck"), None)

    for image_name in image_keys:
        pair = pair_cache[image_name]
        image_id = pair['image_id']

        image_id_text = f"{image_id}_{textflag}"
        if image_id_text in rendered_image_cache:
            images_html_parts.append(rendered_image_cache[image_id_text])
        else:
            image_data = prepare_image_data(min(count, total_images), folder_name, image_name)

            match textflag:
                case '1':
                    # keine Anzeige
                    text_content = ""
                    textmode = "none"
                case '2':
                    # ganzer Text
                    text_content = text_cache.get(image_name, KEIN_TEXT_GEFUNDEN)
                    if KEIN_TEXT_GEFUNDEN == text_content:
                        set_status(image_name, recheck)
                case '3':
                    # nur erste Zeile
                    text_content = ""
                    textmode = "first_line"
                    text_content = text_cache.get(image_name, KEIN_TEXT_GEFUNDEN)
                    if KEIN_TEXT_GEFUNDEN == text_content:
                        set_status(image_name, recheck)
                    lines = text_content.splitlines()
                    if lines and lines[0].startswith("Aufgenommen:"):
                        text_content = lines[0]
                case '4':
                    # kein Englisch
                    text_content = text_cache.get(image_name, KEIN_TEXT_GEFUNDEN)
                    if KEIN_TEXT_GEFUNDEN == text_content:
                        set_status(image_name, recheck)

                    index1 = text_content.find("\n\nThe")
                    index2 = text_content.find("\n\nClose")

                    indices = [i for i in (index1, index2) if i != -1]

                    if indices:
                        text_content = text_content[:min(indices)]

            rendered_html = templates.get_template("image_entry_local.j2").render(
                thumbnail_src=image_data["thumbnail_src"],
                text_content=text_content,
                image_name=image_name,
                folder_name=folder_name,
                image_id=image_id,
                status={},
                scoreq1=image_data["scoreq1"],
                scoreq2=image_data["scoreq2"],
                kategorien=kategorien,
                extra_thumbnails=image_data["extra_thumbnails"]
            )

            if min(count, total_images) > 1:
                rendered_image_cache[image_id_text] = rendered_html

            images_html_parts.append(rendered_html)

        # Status dynamisch nachschieben
        status = load_status(image_name)
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

    if lastpage > 0 and lastcount > 0:
        lastcall = f"/gallery/?page={lastpage}&count={lastcount}&folder={folder_name}&textflag={lasttextflag}"
    else:
        lastcall = ""

    return templates.TemplateResponse("image_gallery_local.j2", {
        "request": request,
        "page": page,
        "total_pages": total_pages,
        "folder_name": folder_name,
        "count": count,
        "textflag": textflag,
        "kategorien": kategorien,
        "images_html": ''.join(images_html_parts),
        "lastcall": lastcall
    })


@app.post("/save")
async def save(
        request: Request,
        user: str = Depends(require_login)):
    form = await request.form()
    image_id = form.get("image_id")
    data = {key: form.get(key) for key in form if key != "image_id"}

    for key in data:
        if data[key] == "on":
            data[key] = True

    save_status(image_id, data)
    return {"status": "ok"}


@app.get("/status/{image_name}")
def get_status_for_image(
        image_name: str,
        user: str = Depends(require_login)):
    return load_status(image_name)


@app.get("/loading_status")
def loading_status(user: str = Depends(require_login)):
    return {
        "ready": app_ready,
        "folder_name": current_loading_folder,
        "folders_loaded": folders_loaded,
        "folders_total": folders_total
    }


@app.get("/verarbeite/check/{checkbox}")
def verarbeite_check_checkbox(
        checkbox: str,
        user: str = Depends(require_login)):
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


@app.post("/moveToFolder/{checkbox}")
async def verarbeite_checkbox(
        checkbox: str,
        count: str = Query("6"),
        folder: str = Query("real"),
        user: str = Depends(require_login)
):
    if checkbox not in CHECKBOX_CATEGORIES:
        return JSONResponse(status_code=400, content={"status": "invalid checkbox"})

    anzahl = move_marked_images_by_checkbox(folder, checkbox)

    # Ziel-URL vorbereiten
    redirect_url = f"/gallery?page=1&count={count}&folder={checkbox}&done={checkbox}"
    logging.info(f"[move_file] 📂 Erfolgreich verschoben: {redirect_url}")
    return {"status": "ok", "redirect": redirect_url, "moved": anzahl}


@app.get("/moveToFolder/{checkbox}")
def get_marked_images_count(
        checkbox: str,
        user: str = Depends(require_login)):
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


@app.get("/original/{file_id}")
def show_original_image(
        file_id: str,
        user: str = Depends(require_login)):
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


def is_file_in_folder(image_id: str, folder_name: str) -> bool:
    """Prüft nur lokal im Cache, ob eine Datei in einem Ordner ist."""
    parents = file_parents_cache.get(folder_name, [])
    return image_id in parents


def prepare_image_data(count: int, folder_name: str, image_name: str):
    """Bereitet alle Variablen für ein einzelnes Bild vor, inkl. Qualität."""
    image_name = image_name.lower()
    pair = pair_cache[image_name]
    image_id = pair['image_id']

    try:
        if image_name not in text_cache:
            content = download_text_file(folder_name, image_name, TEXT_FILE_CACHE_DIR)
            text_cache[image_name] = content
    except Exception as e:
        text_cache[image_name] = f"Fehler beim Laden: {e}"

    local_thumbnail_path = download_and_save_image(folder_name, image_name)

    if local_thumbnail_path and os.path.exists(local_thumbnail_path):
        if count != 1:
            thumbnail_src = f"/gallery/static/thumbnails/{image_name}"
        else:
            thumbnail_src = f"/gallery/static/imagefiles/{folder_name}/{image_name}"
    else:
        thumbnail_src = "https://via.placeholder.com/150?text=Kein+Bild"

    scoreq1, scoreq2 = load_quality(folder_name, image_name)

    extra_thumbnails = get_extra_thumbnails(folder_name, image_name)

    return {
        "thumbnail_src": thumbnail_src,
        "image_id": image_id,
        "scoreq1": scoreq1,
        "scoreq2": scoreq2,
        "extra_thumbnails": extra_thumbnails
    }


def get_exif_data(full_image_path):
    image = Image.open(full_image_path)
    exif_data = image._getexif()
    if not exif_data:
        logging.warning(f"[get_exif_data] Keine Daten: {full_image_path}")
        return None, None

    exif = {}
    gps_info = {}
    for tag, value in exif_data.items():
        decoded = TAGS.get(tag, tag)
        if decoded == "GPSInfo":
            for t in value:
                sub_decoded = GPSTAGS.get(t, t)
                gps_info[sub_decoded] = value[t]
        else:
            exif[decoded] = value

    date_taken = exif.get("DateTimeOriginal", None)
    if gps_info:
        gps_coords = get_coordinates(gps_info)
    else:
        gps_coords = None

    return date_taken, gps_coords


def get_coordinates(gps_info):
    def convert_to_degrees(value):
        try:
            d, m, s = value
            return float(d) + float(m) / 60 + float(s) / 3600
        except Exception as e:
            print(f"[Warnung] Ungültige GPS-Daten: {value} → {e}")
            return None

    lat = convert_to_degrees(gps_info.get("GPSLatitude", ()))
    if lat is not None and gps_info.get("GPSLatitudeRef") != "N":
        lat = -lat

    lon = convert_to_degrees(gps_info.get("GPSLongitude", ()))
    if lon is not None and gps_info.get("GPSLongitudeRef") != "E":
        lon = -lon

    if lat is not None and lon is not None:
        return lat, lon
    return None


def reverse_geocode(coords):
    key = f"{coords[0]:.6f},{coords[1]:.6f}"
    if key in geo_cache:
        return geo_cache[key]

    geolocator = Nominatim(user_agent="photo_exif_locator")
    try:
        location = geolocator.reverse(coords, exactly_one=True, language='de', timeout=10)
        address = location.address if location else None
        geo_cache[key] = address
        return address
    except Exception as e:
        print(f"Geocoding-Fehler: {e}")
        return None


def download_text_file(folder_name: str, image_name: str, cache_dir: str) -> str | None:
    german_date = None
    full_image_path = Path(IMAGE_FILE_CACHE_DIR) / folder_name / image_name
    date_str, gps = get_exif_data(full_image_path)
    dt = None
    if date_str:
        try:
            dt = datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S")
            german_date = dt.strftime("%d.%m.%Y %H:%M")
        except Exception as e:
            print(f"[Warnung] Ungültiges Datum in {image_name}: {e}")

    location_name = reverse_geocode(gps) if gps else None

    full_txt_path = Path(cache_dir, f"{image_name}.txt")

    lines = full_txt_path.read_text(encoding="utf-8").splitlines()
    aufnahme_info = f"Aufgenommen: {german_date}" + (f" @ {location_name}" if location_name else "")
    if lines and lines[0].startswith("Aufgenommen:"):
        lines[0] = aufnahme_info
    else:
        lines.insert(0, aufnahme_info)
    full_txt_path.write_text("\n".join(lines), encoding="utf-8")
    if dt:
        os.utime(full_txt_path, (dt.timestamp(), dt.timestamp()))
        os.utime(full_image_path, (dt.timestamp(), dt.timestamp()))

    if os.path.exists(full_txt_path):
        try:
            with open(full_txt_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logging.warning(f"[download_text_file] Fehler beim Lesen von Cache-Datei: {full_txt_path} - {e}")
            return None
    return None


def set_status(image_name: str, key: str, checked: int = 1):
    if key == None:
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO checkbox_status (image_name, checkbox, checked)
            VALUES (?, ?, ?) ON CONFLICT(image_name, checkbox)
            DO
            UPDATE SET checked = excluded.checked
            """,
            (image_name, key, checked)
        )
        conn.commit()


def save_status(image_id: str, data: dict):
    image_name = find_image_name_by_id(image_id)
    logging.info(f"[save_status] Speichern des Status für {image_name}. Eingabedaten: {data}")

    with sqlite3.connect(DB_PATH) as conn:
        for key, value in data.items():
            if key in CHECKBOX_CATEGORIES:
                checked = 1 if str(value).lower() in ["1", "true", "on"] else 0
                try:
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
    conn = sqlite3.connect(DB_PATH)
    if conn.in_transaction:
        return

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


def verify_folders_exist(service, kategorien):
    valid_kategorien = kategorien
    return valid_kategorien


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


def clear_folder_parents_cache(folder_id: str):
    if folder_id in file_parents_cache:
        del file_parents_cache[folder_id]


def render_status(status: dict) -> str:
    html = []
    for key, checked in status.items():
        if checked:
            html.append(f'<input type="checkbox" name="{key}" checked>')
        else:
            html.append(f'<input type="checkbox" name="{key}">')
    return '\n'.join(html)


def move_marked_images_by_checkbox(current_folder: str, new_folder: str) -> int:
    logging.info(f"[move_marked_images_by_checkbox] Starte Verschieben von '{current_folder}' nach '{new_folder}'")

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       SELECT image_name
                       FROM checkbox_status
                       WHERE checked = 1
                         AND checkbox = ?
                       """, (new_folder,))
        rows = cursor.fetchall()

        logging.info(f"[move_marked_images_by_checkbox] {len(rows)} markierte Bilder gefunden für '{new_folder}'.")

        anzahl_verschoben = 0

        folder_id = current_folder
        save_folder_id = new_folder

        for (image_name,) in rows:
            if not image_name:
                continue
            logging.info(f"[move_marked_images_by_checkbox] Bild gefunden '{image_name}'.")
            success = move_file_db(conn, image_name, folder_id, save_folder_id)
            if success:
                try:
                    conn.execute("""
                                 DELETE
                                 FROM checkbox_status
                                 WHERE image_name = ?
                                   AND checkbox = ?
                                 """, (image_name, new_folder))

                    current_folder_path = Path(IMAGE_FILE_CACHE_DIR) / current_folder / image_name
                    new_folder_path = Path(IMAGE_FILE_CACHE_DIR) / new_folder / image_name
                    try:
                        (Path(IMAGE_FILE_CACHE_DIR) / new_folder).mkdir(parents=True, exist_ok=True)
                        shutil.move(current_folder_path, new_folder_path)
                    except:
                        logging.error(
                            f"[move_marked_images_by_checkbox] ❌ Fehler beim Verschieben {current_folder_path} ->  {new_folder_path}")

                    anzahl_verschoben += 1
                    logging.info(
                        f"[move_marked_images_by_checkbox] ✅ Verschoben: {image_name} ({current_folder}) -> ({new_folder})")
                except Exception as e:
                    logging.error(
                        f"[move_marked_images_by_checkbox] ❌ Fehler beim Entfernen der Checkbox von {image_name}: {e}")
            else:
                logging.warning(
                    f"[move_marked_images_by_checkbox] ⚠️ Verschieben von {image_name} nicht erfolgreich – überspringe Löschen.")

        conn.commit()

    logging.info(
        f"[move_marked_images_by_checkbox] ✅ {anzahl_verschoben} Dateien erfolgreich verschoben von '{current_folder}' nach '{new_folder}'.")

    return anzahl_verschoben


def move_file_db(conn, image_name: str, old_folder_id: str, new_folder_id: str, retries: int = 5) -> bool:
    """Verschiebt eine Datei nur in der lokalen Datenbank von einem Ordner in einen anderen."""
    logging.info(
        f"[move_file_db] Starte Verschieben von Datei {image_name} in der Datenbank von {old_folder_id} zu {new_folder_id} (Thread: {threading.get_ident()})")

    image_name = image_name.lower()
    pair = pair_cache.get(image_name)
    if not pair:
        logging.warning(f"[move_file_db] ⚠️ Kein Eintrag im pair_cache für: {image_name}")
        return
    image_id = pair["image_id"]

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
                        f"[move_file_db] Datei {image_name} war nicht im Cache von {old_folder_id} vorhanden.")

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


def calculateq1andq2(image_path):
    """Berechnet die Fake-BRISQUE (LBP-Standardabweichung und einfache Bildästhetik).
    :return: Tuple[int, int] → (scoreq1, scoreq2), oder (None, None) bei Fehler
    """
    image = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
    if image is None:
        print(f"Bild {image_path} konnte nicht geladen werden.")
        return None, None

    if image.shape[0] < 16 or image.shape[1] < 16:
        print(f"Bild zu klein für Analyse: {image_path}")
        return None, None

    lbp = feature.local_binary_pattern(image, P=8, R=1, method="uniform")
    scoreq1 = min(scale_score_to_0_100(np.std(lbp)), 100)

    image = cv2.imread(image_path)
    h, w = image.shape[:2]
    if h < 16 or w < 16:
        print(f"Bild zu klein für Analyse: {image_path}")
        return None, None

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    _, thresh = cv2.threshold(blur, 127, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        print("Keine Konturen erkannt.")
        return None, None

    largest = max(contours, key=cv2.contourArea)
    M = cv2.moments(largest)
    if M["m00"] == 0:
        print("Ungültige Momentberechnung.")
        return None, None

    cx = int(M["m10"] / M["m00"])
    cy = int(M["m01"] / M["m00"])

    # Goldener Schnitt
    gx, gy = int(w * 0.618), int(h * 0.618)
    dist_golden = np.hypot(cx - gx, cy - gy) / np.hypot(w, h)
    score_golden = max(0, 1 - dist_golden)

    # Drittelregel
    thirds_x = [w // 3, 2 * w // 3]
    thirds_y = [h // 3, 2 * h // 3]
    min_dist_thirds = min([abs(cx - x) for x in thirds_x]) + min([abs(cy - y) for y in thirds_y])
    score_thirds = max(0, 1 - (min_dist_thirds / max(w, h)))

    # Symmetrie (vertikal)
    left = image[:, :w // 2]
    right = cv2.flip(image[:, w - w // 2:], 1)
    diff = cv2.absdiff(left, right)
    denom = h * w * 3 * 255
    score_symmetry = 1 - (np.sum(diff) / denom) if denom > 0 else 0

    # Kontrast
    contrast = gray.std() / 128
    score_contrast = min(contrast, 1.0)

    # Gesamtbewertung (q2)
    scoreq2 = int(round(np.mean([score_golden, score_thirds, score_symmetry, score_contrast]), 2) * 100)

    return scoreq1, scoreq2


def find_file_by_name(root_dir: Path, image_name: str):
    return list(root_dir.rglob(image_name))


def download_and_save_image(folder_name: str, image_name: str) -> str | None:
    """Erzeugt ein Thumbnail aus einer lokalen Originalbilddatei."""
    image_path = Path(IMAGE_FILE_CACHE_DIR) / folder_name / image_name
    thumbnail_path = os.path.join(THUMBNAIL_CACHE_DIR_300, image_name)

    if not os.path.exists(image_path):
        treffer = find_file_by_name(Path(IMAGE_FILE_CACHE_DIR), image_name)
        for path in treffer:
            try:
                shutil.move(path, image_path)
            except Exception as e:
                logging.warning(f"[download_and_save_image] Originalbild nicht gefunden: {image_path}")
                return None
            break

    if not os.path.exists(image_path):
        logging.warning(f"[download_and_save_image] Originalbild nicht gefunden: {image_path}")
        return None

    if not os.path.exists(thumbnail_path):
        if not generate_thumbnail(image_path, thumbnail_path, image_name):
            return None

    return thumbnail_path


def generate_thumbnail(image_path: Path, thumbnail_path: str, image_name: str) -> bool:
    try:
        logging.info(f"[generate_thumbnail] Erzeuge Thumbnail für {image_name}")
        img = Image.open(image_path)
        img = ImageOps.exif_transpose(img)
        img.thumbnail((300, 300), Image.Resampling.LANCZOS)
        os.makedirs(os.path.dirname(thumbnail_path), exist_ok=True)
        img.convert("RGB").save(thumbnail_path, format="JPEG")  # <-- Fix hier
        logging.info(f"[generate_thumbnail] ✅ Thumbnail gespeichert: {thumbnail_path}")
        return True
    except Exception as e:
        logging.error(f"[generate_thumbnail] ❌ Fehler beim Erzeugen von Thumbnail {image_name}: {e}")
        return False


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


def egal():
    global DB_PATH, IMAGE_FILE_CACHE_DIR, TEXT_FILE_CACHE_DIR, THUMBNAIL_CACHE_DIR_300, app_ready
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


def get_extra_thumbnails(folder_name: str, image_name: str) -> list[dict]:
    full_path = Path(IMAGE_FILE_CACHE_DIR) / folder_name / image_name
    faces(folder_name, image_name)

    stem = full_path.stem
    face_dir = Path(GESICHTER_FILE_CACHE_DIR)
    base_url = "/static/facefiles"
    return [
        {
            "src": f"/gallery{base_url}/{thumb.name}",
            "link": f"/gallery{base_url}/{thumb.name}",
            "image_name": f"{thumb.name}"
        }
        for thumb in sorted(face_dir.glob(f"{stem}_*.jpg"))
    ]  # Beispiel


def scale_score_to_0_100(score):
    """Skaliert den LBP-Score präzise auf 0–100."""
    scaled = (score / 5.0) * 100  # 5.0 ist ein erfahrener Maximalwert für LBP-std
    scaled = max(0, min(100, scaled))  # Clamping
    return int(round(scaled))


def load_quality(folder_name: str, image_name: str):
    """Lädt die Qualitätsbewertung (0–100) eines Bildes aus der neuen Tabelle image_quality_scores."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute("""
                                SELECT score_type, score
                                FROM image_quality_scores
                                WHERE LOWER(image_name) = LOWER(?)
                                """, (image_name,)).fetchall()

            scores = {score_type: score for score_type, score in rows}
            if 1 in scores and 2 in scores:
                return scores[1], scores[2]

            logging.info(f"[load_quality] nicht vollständig in DB für {image_name}")

        full_path = Path(IMAGE_FILE_CACHE_DIR) / folder_name / image_name
        scoreq1, scoreq2 = calculateq1andq2(full_path)
        if scoreq1 is not None and scoreq2 is not None:
            save_image_quality(image_name, scoreq1, scoreq2)
            return scoreq1, scoreq2

    except Exception as e:
        logging.error(f"[load_quality] Fehler bei {image_name}: {e}")

    return None, None


def faces(folder_name: str, image_name: str) -> int | None:
    score_type = 3
    """
    Analysiert ein Bild, erkennt Gesichter und speichert die Anzahl der erkannten Gesichter
    in der Datenbank. Zusätzlich werden die erkannten Gesichter als separate Bilddateien gespeichert.

    Args:
        folder_name (str): Name des Unterordners im Bild-Cache-Verzeichnis.
        image_name (str): Name der Bilddatei.

    Returns:
        int | None: Anzahl der erkannten Gesichter oder None bei einem Fehler.
    """
    try:
        # Datenbankverbindung herstellen
        with sqlite3.connect(DB_PATH) as conn:
            image_path = Path(IMAGE_FILE_CACHE_DIR) / folder_name / image_name

            cursor = conn.cursor()
            cursor.execute("""
                           SELECT score
                           FROM image_quality_scores
                           WHERE score_type = ?
                             AND LOWER(image_name) = LOWER(?)
                           """, (score_type, image_name))
            row = cursor.fetchone()
            if row:
                logging.info(f"[gesichter] wurden schon verarbeitet: {image_path}")
                return row[0]

            # Bildpfad erstellen
            if not image_path.is_file():
                logging.warning(f"[gesichter] Datei nicht gefunden: {image_path}")
                return None

            # Bild laden
            img = cv2.imread(str(image_path))
            if img is None:
                logging.warning(f"[gesichter] Bild konnte nicht geladen werden: {image_path}")
                return None

            # Bild in Graustufen konvertieren
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

            # Gesichter erkennen
            min_gesichtsgroesse = (200, 300)
            gesichter = face_cascade.detectMultiScale(
                gray,
                scaleFactor=1.1,
                minNeighbors=5,
                minSize=min_gesichtsgroesse,
                flags=cv2.CASCADE_SCALE_IMAGE
            )

            # Anzahl der erkannten Gesichter
            count = len(gesichter)

            # Gesichter extrahieren und speichern
            ziel_verzeichnis = Path(GESICHTER_FILE_CACHE_DIR)
            ziel_verzeichnis.mkdir(parents=True, exist_ok=True)

            for i, (x, y, w, h) in enumerate(gesichter):
                gesicht_img = img[y:y + h, x:x + w]
                ziel_datei = ziel_verzeichnis / f"{image_path.stem}_{i}.jpg"
                cv2.imwrite(str(ziel_datei), gesicht_img)

            # Ergebnis in der Datenbank speichern
            cursor.execute("""
                INSERT OR REPLACE INTO image_quality_scores (image_name, score_type, score)
                VALUES (?, ?, ?)
            """, (image_name, score_type, count))
            conn.commit()

            return count

    except Exception as e:
        logging.exception(f"[gesichter] Fehler bei der Verarbeitung von {image_name}: {e}")
        return None


def save_image_quality(image_name, scoreq1, scoreq2):
    """Speichert die Qualitätswerte als separate Zeilen in der neuen Tabelle."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO image_quality_scores (image_name, score_type, score)
            VALUES (?, ?, ?)
        """, (image_name, 1, scoreq1))

        conn.execute("""
            INSERT OR REPLACE INTO image_quality_scores (image_name, score_type, score)
            VALUES (?, ?, ?)
        """, (image_name, 2, scoreq2))


def ensure_category_dirs(image_file_cache_dir, kategorien):
    image_file_cache_dir = Path(image_file_cache_dir)  # absichern
    for k in kategorien:
        folder = image_file_cache_dir / k["key"]
        folder.mkdir(parents=True, exist_ok=True)


def list_all_images_one_level(image_file_cache_dir: Path):
    image_file_cache_dir = Path(image_file_cache_dir)
    files = list(image_file_cache_dir.glob("*.*"))  # direkt im Hauptverzeichnis
    files += list(image_file_cache_dir.glob("*/*.*"))  # genau eine Ebene tiefer
    return files


def find_image_id_by_name(image_name):
    pair = pair_cache.get(image_name)
    if pair:
        return pair.get("image_id")
    return None  # nicht gefunden


def find_image_name_by_id(image_id):
    for image_name, pair in pair_cache.items():
        if pair.get("image_id") == image_id:
            return image_name
    return None  # nicht gefunden


def find_category_by_image_id(file_parents_cache, target_image_id):
    for category_key, image_ids in file_parents_cache.items():
        if target_image_id in image_ids:
            return category_key
    return None


def print_file_counts(image_file_cache_dir, kategorien):
    base_dir = Path(image_file_cache_dir)

    for k in kategorien:
        folder = base_dir / k["key"]
        count = len(list(folder.glob("*"))) if folder.exists() else 0
        print(f"{k['key']:<10} ({k['label']:<12}): {count:>4} Dateien {folder}")


def mmmm():
    IMAGE_FILE_CACHE_DIR = '../cache/imagefiles'
    DB_PATH = "../gallery_local.db"

    ensure_category_dirs(IMAGE_FILE_CACHE_DIR, kategorien)

    fillcache_local(PAIR_CACHE_PATH, IMAGE_FILE_CACHE_DIR)

    fill_folder_cache(DB_PATH)

    for category_key, image_ids in file_parents_cache.items():
        print(f"Kategorie: {category_key} ({len(image_ids)} Bilder)")

    allimages = list_all_images_one_level(IMAGE_FILE_CACHE_DIR)
    print(f"allimages: {len(allimages)} Bilder")

    alle = len(allimages);

    for image_path in allimages:
        pair = pair_cache.get(image_path.name)
        if not pair:
            logging.error(
                f"[Verschieben] ❌ Fehler finden pair {image_path}")
            if (image_path.suffix == ".html" or
                image_path.suffix == ".log" or
                image_path.suffix == ".log") and image_path.exists():
                image_path.unlink()
                print("Datei gelöscht:", image_path)
            else:
                print("Datei existiert nicht:", image_path)
            continue
        image_id = pair["image_id"]
        if not image_id:
            logging.error(
                f"[Verschieben] ❌ Fehler finden image_id {image_path}")
            continue
        cat = find_category_by_image_id(file_parents_cache, image_id)
        if not cat:
            logging.error(
                f"[Verschieben] ❌ Fehler finden cat {image_path}")
            continue
        else:
            topath = Path(IMAGE_FILE_CACHE_DIR) / cat / image_path.name
            if (not topath.exists()):
                logging.info(f"✅ Verschieben nach {topath}.")
                shutil.move(image_path, topath)
            alle -= 1

    logging.info(f"✅ Rest {alle}.")


def batch_generate_thumbnails(cats):
    global THUMBNAIL_CACHE_DIR_300

    from tqdm import tqdm

    THUMBNAIL_CACHE_DIR_300 = '../cache/thumbnailfiles300'

    for k in kategorien:
        folder_name = k["key"]
        folder_path = Path(IMAGE_FILE_CACHE_DIR) / folder_name

        if not folder_path.exists():
            print("Ordner nicht gefunden:", folder_path)
            return

        image_files = [f.name for f in folder_path.iterdir() if f.is_file()]

        for image in tqdm(image_files):
            result = download_and_save_image(folder_name, image)
            if not result:
                print("Fehler bei batch_generate_thumbnails:", image)


def batch_generate_faces(cats):
    from tqdm import tqdm

    for k in kategorien:
        folder_name = k["key"]
        folder_path = Path(IMAGE_FILE_CACHE_DIR) / folder_name

        if not folder_path.exists():
            print("Ordner nicht gefunden:", folder_path)
            return

        image_files = [f.name for f in folder_path.iterdir() if f.is_file()]

        for image in tqdm(image_files):
            result = faces(folder_name, image)
            if not result:
                print("Fehler bei batch_generate_faces:", image)


if __name__ == "__main__":
    IMAGE_FILE_CACHE_DIR = '../cache/imagefiles'
    GESICHTER_FILE_CACHE_DIR = '../cache/facefiles'
    DB_PATH = Path("../gallery_local.db")

    print_file_counts(IMAGE_FILE_CACHE_DIR, kategorien)

    # batch_generate_thumbnails(kategorien)
    batch_generate_faces(kategorien)
