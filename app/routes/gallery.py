import json
import logging
import math
import os
from pathlib import Path
from urllib.parse import unquote

from fastapi import APIRouter, Depends, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import JSONResponse

from app.config import Settings  # Importiere die Settings-Klasse
from app.database import set_status, load_status, save_status, \
    move_marked_images_by_checkbox, get_checkbox_count  # Importiere die benötigten Funktionen
from app.dependencies import require_login
from app.services.cache_management import load_rendered_html_file, save_rendered_html_file
from app.services.image_processing import prepare_image_data, clean

DEFAULT_COUNT: str = "6"
DEFAULT_FOLDER: str = "real"

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))
logger = logging.getLogger(__name__)

Settings.app_ready = False


def is_file_in_folder(image_id: str, folder_name: str) -> bool:
    """Prüft nur lokal im Cache, ob eine Datei in einem Ordner ist."""
    parents = Settings.CACHE["file_parents_cache"].get(folder_name, [])  # Verwende Caches aus Settings
    return image_id in parents


@router.get("/images", response_class=HTMLResponse)
def show_image_redirect(
        request: Request,
        user: str = Depends(require_login)
):
    """
    Diese Funktion handhabt die Weiterleitung zu einer bestimmten Seite oder einem bestimmten Bild,
    wenn ein Bildname in der Anfrage enthalten ist.
    """
    try:
        page = int(request.query_params.get('page') or 1)
        count = int(request.query_params.get('count') or DEFAULT_COUNT)
    except ValueError:
        page = 1
        count = DEFAULT_COUNT

    folder_name = request.query_params.get('folder', DEFAULT_FOLDER)
    textflag = request.query_params.get('textflag', '1')
    image_name = unquote(request.query_params.get('image_name', '')).strip().lower()

    pagecounter = 0
    # TODO: Refactor this
    for image_name_l in Settings.CACHE["pair_cache"]:  # Verwende Caches aus Settings
        pair = Settings.CACHE["pair_cache"][image_name_l]  # Verwende Caches aus Settings
        image_id = pair.get("image_id", "")
        if is_file_in_folder(image_id, folder_name):
            pagecounter += 1
            if image_name_l.strip().lower() == image_name:
                clean(image_name)  # Entfernt, da die Funktion nicht definiert ist.
                return RedirectResponse(
                    url=f"/gallery/?page={pagecounter}&count=1&folder={folder_name}&textflag=2&lastpage={page}&lastcount={count}&lasttextflag={textflag}"
                )

    return RedirectResponse(
        url=f"/gallery/?page={page}&count={count}&folder={folder_name}&textflag={textflag}"
    )


@router.get("/", response_class=HTMLResponse)
def show_images_gallery(
        request: Request,
        user: str = Depends(require_login)
):
    """
    Zeigt eine Galerie von Bildern an, mit Paginierung, Filtern und Textanzeigeoptionen.
    """
    if not Settings.app_ready:
        return templates.TemplateResponse("loading.html", {"request": request}, status_code=200)

    page = int(request.query_params.get('page', '1') or 1)
    count = int(request.query_params.get('count', DEFAULT_COUNT) or 1)
    folder_name = request.query_params.get('folder', DEFAULT_FOLDER)
    textflag = request.query_params.get('textflag', '1')
    checkboxstr = request.query_params.get('checkbox', None)

    try:
        lastindex = int(request.query_params.get('lastindex', 0))
    except ValueError:
        lastindex = 0
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

    if lastindex > 0:
        page = (lastindex // count) + 1

    start = (page - 1) * count
    end = start + count

    image_keys = []
    total_images = 0

    for image_name in Settings.CACHE["pair_cache"].keys():  # Verwende Caches aus Settings
        pair = Settings.CACHE["pair_cache"][image_name]  # Verwende Caches aus Settings
        image_id = pair['image_id']
        if is_file_in_folder(image_id, folder_name):
            if start <= total_images < end:
                image_keys.append(image_name.lower())
            total_images += 1

    images_html_parts = []
    recheck_category = next((k["key"] for k in Settings.kategorien if k["key"] == "recheck"),
                            None)  # Verwende kategorien aus Settings

    for image_name in image_keys:
        pair = Settings.CACHE["pair_cache"][image_name]  # Verwende Caches aus Settings
        image_id = pair['image_id']

        image_id_text = f"{image_id}_{textflag}"
        if rendered_html := load_rendered_html_file(Settings.RENDERED_HTML_DIR, image_id_text):
            images_html_parts.append(rendered_html)
        else:
            image_data = prepare_image_data(min(count, total_images), folder_name, image_name)
            text_content = ""  # Standardwert
            match textflag:
                case '1':
                    text_content = ""
                case '2':
                    text_content = Settings.CACHE["text_cache"].get(image_name,
                                                                    Settings.KEIN_TEXT_GEFUNDEN)  # Verwende Caches aus Settings
                    if Settings.KEIN_TEXT_GEFUNDEN == text_content:
                        set_status(image_name, recheck_category)
                case '3':
                    text_content = Settings.CACHE["text_cache"].get(image_name,
                                                                    Settings.KEIN_TEXT_GEFUNDEN)  # Verwende Caches aus Settings
                    if Settings.KEIN_TEXT_GEFUNDEN == text_content:
                        set_status(image_name, recheck_category)
                    lines = text_content.splitlines()
                    if lines and lines[0].startswith("Aufgenommen:"):
                        text_content = lines[0]
                case '4':
                    text_content = Settings.CACHE["text_cache"].get(image_name,
                                                                    Settings.KEIN_TEXT_GEFUNDEN)  # Verwende Caches aus Settings
                    if Settings.KEIN_TEXT_GEFUNDEN == text_content:
                        set_status(image_name, recheck_category)

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
                quality_scores=image_data["quality_scores"],
                nsfw_scores=image_data["nsfw_scores"],
                kategorien=Settings.kategorien,
                extra_thumbnails=image_data["extra_thumbnails"]
            )
            images_html_parts.append(rendered_html)
            save_rendered_html_file(Settings.RENDERED_HTML_DIR, image_id_text, rendered_html)

        # Status dynamisch nachschieben
        status = load_status(image_name)
        if checkboxstr:
            status[checkboxstr] = True
        else:
            value = Settings.CACHE["text_cache"].get(image_name, "")  # Verwende Caches aus Settings
            if isinstance(value, str) and "Error 2" in value:
                status["recheck"] = True

        status_json = json.dumps({f"{image_id}_{key}": value for key, value in status.items()})

        images_html_parts.append(f"""
        <script>
        const checkboxStatus_{image_id} = {status_json};
        for (const key in checkboxStatus_{image_id}) {{
            const checkbox = document.querySelector(`input[name="${{key}}"]`);  // <-- RICHTIG: Backticks!
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
        "total_images": total_images,
        "folder_name": folder_name,
        "count": count,
        "textflag": textflag,
        "kategorien": Settings.kategorien,
        "images_html": ''.join(images_html_parts),
        "lastcall": lastcall
    })


@router.post("/save")
async def save(
        request: Request,
        user: str = Depends(require_login)):
    form = await request.form()
    image_id = form.get("image_id")
    data = {key: form.get(key) for key in form if key != "image_id"}

    save_status(image_id, data)
    return {"status": "ok"}


@router.get("/status/{image_name}")
def get_status_for_image(image_name: str, user: str = Depends(require_login)):
    logger.info(f"📥 Lade Status für Bild: {image_name}")
    return load_status(image_name)


def find_file_by_name(root_dir: Path, image_name: str):
    return list(root_dir.rglob(image_name))


@router.get("/loading_status")
def loading_status(user: str = Depends(require_login)):
    logger.info("🔄 Abfrage: loading_status")
    return {
        "ready": Settings.app_ready,
        "folder_name": Settings.current_loading_folder,
        "folders_loaded": Settings.folders_loaded,
        "folders_total": Settings.folders_total
    }


@router.post("/moveToFolder/{checkbox}")
async def verarbeite_checkbox(
        checkbox: str,
        count: str = Query(DEFAULT_COUNT),
        folder: str = Query(DEFAULT_FOLDER),
        user: str = Depends(require_login)):
    logger.info(f"📦 Starte move_marked_images_by_checkbox() von '{folder}' nach '{checkbox}'")
    if checkbox not in Settings.CHECKBOX_CATEGORIES:
        logger.warning("❌ Ungültige Checkbox-Kategorie")
        return JSONResponse(status_code=400, content={"status": "invalid checkbox"})

    anzahl = move_marked_images_by_checkbox(folder, checkbox)
    redirect_url = f"/gallery?page=1&count={count}&folder={checkbox}&done={checkbox}"
    logger.info(f"✅ Erfolgreich verschoben: {anzahl} Dateien -> {redirect_url}")
    return {"status": "ok", "redirect": redirect_url, "moved": anzahl}


@router.get("/verarbeite/check/{checkbox}")
def verarbeite_check_checkbox(checkbox: str, user: str = Depends(require_login)):
    logger.info(f"📊 Zähle markierte Bilder für Kategorie: {checkbox}")
    return get_checkbox_count(checkbox)


@router.get("/moveToFolder/{checkbox}")
def get_marked_images_count(checkbox: str, user: str = Depends(require_login)):
    logger.info(f"📊 Abfrage markierter Bilder für: {checkbox}")
    return get_checkbox_count(checkbox)
