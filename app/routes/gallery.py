import asyncio
import json
import math
import os
import sqlite3
import time
from pathlib import Path
from urllib.parse import unquote

import httpx
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Form
from fastapi import Query
from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import JSONResponse

from app.routes.hashes import download_file
from .auth import load_drive_service_token
from .dashboard import load_rendered_html_file
from .dashboard import save_rendered_html_file
from ..config import Settings  # Importiere die Settings-Klasse
from ..config import score_type_map
from ..config_gdrive import SettingsGdrive
from ..config_gdrive import calculate_md5
from ..database import get_scores_filtered_by_expr
from ..dependencies import require_login
from ..scores.texte import search_recoll
from ..services.image_processing import clean
from ..services.image_processing import prepare_image_data
from ..tools import newpaircache
from ..utils.db_utils import load_comfyui_count
from ..utils.logger_config import setup_logger
from ..utils.move_utils import get_checkbox_count
from ..utils.move_utils import move_marked_images_by_checkbox
from ..utils.move_utils import move_single_image
from ..utils.progress import init_progress_state
from ..utils.progress import progress_state
from ..utils.progress import stop_progress
from ..utils.progress import update_progress
from ..utils.progress import update_progress_text
from ..utils.score_parser import parse_score_expression
from ..utils.status_utils import load_status
from ..utils.status_utils import save_status
from ..utils.status_utils import set_status

DEFAULT_COUNT: str = "6"
DEFAULT_FOLDER: str = "real"

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))

logger = setup_logger(__name__)


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

    pair_cache = newpaircache(folder_name)
    pagecounter = 0
    for image_name_l in pair_cache:
        pagecounter += 1
        if image_name_l.strip().lower() == image_name:
            clean(image_name)
            url = f"/gallery/?page={pagecounter}&count=1&folder={folder_name}&textflag=2&lastpage={page}&lastcount={count}&lasttextflag={textflag}"
            return RedirectResponse(url=url)

    url = f"/gallery/?page={page}&count={count}&folder={folder_name}&textflag={textflag}"
    return RedirectResponse(url=url)


@router.get("/", response_class=HTMLResponse)
def show_images_gallery(
        request: Request,
        user: str = Depends(require_login)
):
    """
    Zeigt eine Galerie von Bildern an, mit Paginierung, Filtern und Textanzeigeoptionen.
    """
    logger.info(f"[Gallery] Anfrage für Benutzer {user}")

    if not Settings.app_ready:
        logger.warning("[Gallery] Anwendung noch nicht bereit, zeige Ladebildschirm")
        return templates.TemplateResponse("loading.html", {"request": request}, status_code=200)

    page = int(request.query_params.get('page', '1') or 1)
    count = int(request.query_params.get('count', DEFAULT_COUNT) or 1)
    folder_name = request.query_params.get('folder', DEFAULT_FOLDER)
    textflag = request.query_params.get('textflag', '1')

    logger.info(f"[Gallery] Anfrageparameter: Seite={page}, Anzahl={count}, Ordner={folder_name}, Textflag={textflag}")

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

    if SettingsFilter.FILTER_TEXT:
        score_expr_raw = SettingsFilter.FILTER_TEXT
    else:
        score_expr_raw = None

    logger.info(f"[Gallery] Beginne Verarbeitung mit Score-Filter: {score_expr_raw}")

    # Prüfe, ob ein sinnvoller Ausdruck übergeben wurde
    score_expr = None
    if score_expr_raw and score_expr_raw.lower() != "none":
        try:
            # Versuch, Ausdruck zu parsen (wirft ValueError als ungültig)
            dummy_scores = dict.fromkeys(score_type_map.keys(), 0)
            parse_score_expression(score_expr_raw, dummy_scores)
            score_expr = score_expr_raw
        except Exception as e:
            logger.warning(f"[score_filter] Ungültiger Score-Ausdruck ignoriert: {score_expr_raw} ({e})")

    # 2. Ausdruck verarbeiten und ggf. Trefferliste cachen
    filtered_names = None
    if score_expr:
        cache_key = score_expr_raw.strip().lower()
        if cache_key in Settings.CACHE["score_filter_result"]:
            filtered_names = Settings.CACHE["score_filter_result"][cache_key]
            logger.info(f"[score_filter] ⚡ Treffer aus Cache: {len(filtered_names)} Bilder für '{score_expr}'")
        else:
            try:
                all_scores = get_scores_filtered_by_expr(Settings.DB_PATH, score_expr)
                filtered_names = [
                    name for name, scores in all_scores.items()
                    if parse_score_expression(score_expr, scores)
                ]
                logger.info(f"[Gallery] filtered_names {filtered_names}")
                Settings.CACHE["score_filter_result"][cache_key] = filtered_names
                logger.info(f"[score_filter] 🧮 Neu berechnet: {len(filtered_names)} Bilder für '{score_expr}'")
            except Exception as e:
                logger.warning(f"[score_filter] ⚠️ Fehler beim Score-Filter '{score_expr}': {e}")
                score_expr = None
                filtered_names = None

    if SettingsFilter.SEARCH_TEXT:
        search_results = asyncio.run(search_recoll(SettingsFilter.SEARCH_TEXT))
        search_results_lower = {name.lower() for name in search_results}

        if filtered_names is None:
            # Wenn noch keine Einschränkung existiert, nur Textsuche verwenden
            filtered_names = list(search_results_lower)
        else:
            # Wenn bereits eine Einschränkung existiert, Schnittmenge bilden
            filtered_names = [name for name in filtered_names if name.lower() in search_results_lower]

        score_expr = "textsearch"  # Markieren dass gefiltert wurde
        logger.info(f"[Gallery] Nach Textsuche: {len(filtered_names)} Bilder")

    pair_cache = newpaircache(folder_name)

    # 3. Hauptschleife über alle Bilder
    logger.info("[Gallery] Starte Hauptschleife über Bilder")
    for image_name in pair_cache.keys():
        # Bei Textsuche: Prüfe ob Bildname den Suchtext enthält
        if SettingsFilter.SEARCH_TEXT:
            text_match = SettingsFilter.SEARCH_TEXT.lower() in image_name.lower()
            if text_match:
                if start <= total_images < end:
                    image_keys.append(image_name.lower())
                total_images += 1
                continue

        # Bei Score-Filter: Prüfe ob Bild in gefilterten Namen enthalten
        if score_expr and filtered_names is not None:
            if image_name.lower() not in filtered_names:
                continue

        # Füge Bild zur aktuellen Seite hinzu wenn im richtigen Bereich
        if start <= total_images < end:
            image_keys.append(image_name.lower())
        total_images += 1

    logger.info(f"[Gallery] Gefunden: {total_images} Bilder gesamt, {len(image_keys)} auf aktueller Seite")

    images_html_parts = []

    logger.info("[Gallery] Beginne HTML-Generierung")
    for image_name in image_keys:
        pair = pair_cache[image_name]  # Verwende Caches aus Settings
        image_id = pair['image_id']

        if Settings.is_admin():
            image_id_text = f"{image_id}_{textflag}"
        else:
            image_id_text = f"{image_id}_{textflag}_{Settings.get_user_type()}"

        if Settings.COMFYUI == folder_name:
            image_id_text = f"{folder_name}_{image_id_text}"

        if rendered_html := load_rendered_html_file(Settings.RENDERED_HTML_DIR, image_id_text):
            logger.debug(f"[Gallery] Cache-Hit für {image_id_text}")
            images_html_parts.append(rendered_html)
        else:
            logger.debug(f"[Gallery] Cache-Miss für {image_id_text}, generiere neu")
            image_data = prepare_image_data(min(count, total_images), folder_name, image_name)
            text_content = ""  # Standardwert
            match textflag:
                case '1':
                    text_content = ""
                case '2':
                    text_content = Settings.CACHE["text_cache"].get(image_name,
                                                                    Settings.KEIN_TEXT_GEFUNDEN)
                    if Settings.KEIN_TEXT_GEFUNDEN == text_content:
                        logger.warning(f"[Gallery] Kein Text gefunden für Bild {image_name}")
                        set_status(image_name, Settings.RECHECK)
                case '3':
                    text_content = Settings.CACHE["text_cache"].get(image_name,
                                                                    Settings.KEIN_TEXT_GEFUNDEN)
                    if Settings.KEIN_TEXT_GEFUNDEN == text_content:
                        logger.warning(f"[Gallery] Kein Text gefunden für Bild {image_name}")
                        set_status(image_name, Settings.RECHECK)

                    if isinstance(text_content, str):
                        lines = text_content.splitlines()
                        if lines and lines[0].startswith("Aufgenommen:"):
                            text_content = lines[0]

                case '4':
                    text_content = Settings.CACHE["text_cache"].get(image_name,
                                                                    Settings.KEIN_TEXT_GEFUNDEN)
                    if Settings.KEIN_TEXT_GEFUNDEN == text_content:
                        logger.warning(f"[Gallery] Kein Text gefunden für Bild {image_name}")
                        set_status(image_name, Settings.RECHECK)

                    if isinstance(text_content, str):
                        index1 = text_content.find("\n\nThe")
                        index2 = text_content.find("\n\nClose")
                        indices = [i for i in (index1, index2) if i != -1]
                        if indices:
                            text_content = text_content[:min(indices)]

            # Stelle sicher, dass quality_scores ein Dictionary ist
            quality_scores = image_data.get("quality_scores", {})
            if not isinstance(quality_scores, dict):
                quality_scores = {}

            civitai = None
            url = f"https://civitai.com/images/{image_name.split('.')[0]}"
            try:
                with httpx.Client() as client:
                    response = client.head(url, timeout=2.0, follow_redirects=True)
                    if response.status_code == 200:
                        civitai = url
            except:
                pass

            rendered_html = templates.get_template("image_entry_local.j2").render(
                thumbnail_src=image_data["thumbnail_src"],
                text_content=text_content,
                image_name=image_name,
                civitai=civitai,
                folder_name=folder_name,
                image_id=image_id,
                status={},
                quality_scores=quality_scores,
                nsfw_scores=image_data["nsfw_scores"],
                kategorien=Settings.kategorien(),
                extra_thumbnails=image_data["extra_thumbnails"]
            )
            images_html_parts.append(rendered_html)
            save_rendered_html_file(Settings.RENDERED_HTML_DIR, image_id_text, rendered_html)

        # Status dynamisch nachschieben
        status = load_status(image_name)
        value = Settings.CACHE["text_cache"].get(image_name, "")  # Verwende Caches aus Settings
        if isinstance(value, str):
            if "Error 2" in value:
                status[Settings.RECHECK] = True

        status_json = json.dumps({f"{image_id}_{key}": value for key, value in status.items()})

        comfyui_count = load_comfyui_count(Settings.DB_PATH, image_id)

        images_html_parts.append(f"""
        <script>
        const checkboxStatus_{image_id} = {status_json};
        for (const key in checkboxStatus_{image_id}) {{
            const checkbox = document.querySelector(`input[name=\"${{key}}\"]`);  // <-- RICHTIG: Backticks!
            if (checkbox) {{
                checkbox.checked = checkboxStatus_{image_id}[key];
            }}
        }}
        const input_{image_id} = document.getElementById('comfyui_count_{image_id}');
        if (input_{image_id}) {{
            input_{image_id}.value = {comfyui_count};
        }}
        </script>
        """)

    # Berechnung total_pages
    total_pages = max(1, math.ceil(total_images / count))

    lastcall = ""
    if lastpage > 0 and lastcount > 0:
        lastcall = f"/gallery/?page={lastpage}&count={lastcount}&folder={folder_name}&textflag={lasttextflag}"

    logger.info(f"[Gallery] Seite erfolgreich generiert: {total_images} Bilder, {total_pages} Seiten")

    return templates.TemplateResponse("image_gallery_local.j2", {
        "request": request,
        "page": page,
        "total_pages": total_pages,
        "total_images": total_images,
        "folder_name": folder_name,
        "count": count,
        "textflag": textflag,
        "kategorien": Settings.kategorien(),
        "images_html": ''.join(images_html_parts),
        "lastcall": lastcall,
        "last_texts": SettingsFilter.FILTER_HISTORY,
        "filter_text": SettingsFilter.FILTER_TEXT,
        "search_history": SettingsFilter.SEARCH_HISTORY,
        "search_text": SettingsFilter.SEARCH_TEXT
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


@router.post("/savegencount")
async def save_gen_count(
        image_id: str = Query(..., description="ID des Bildes"),
        comfyui_count: int = Query(..., description="Anzahl der Generierungen"),
        user: dict = Depends(require_login)
) -> JSONResponse:
    """
    Speichert die ComfyUI-Generierungsanzahl für ein Bild.
    """
    logger.info(f"🔄 savegencount({image_id}{comfyui_count})")

    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            key = "comfyui_count"
            conn.execute("""
                INSERT OR REPLACE INTO text_status (image_name, field, value)
                VALUES (?, ?, ?)
            """, (image_id, key, comfyui_count))
            logger.info(f"[savegencount] ✅ Textfeld '{key}' für {image_id} gespeichert. Wert: {comfyui_count}")
            conn.commit()
    except sqlite3.Error as e:
        logger.error(f"[savegencount] ❌ Fehler beim Speichern des Status für {image_id}: {e}")
        raise

    return JSONResponse(content={
        "status": "ok"
    })


@router.post("/moveToFolder/{checkbox}")
async def verarbeite_checkbox(
        checkbox: str,
        count: str = Query(DEFAULT_COUNT),
        folder: str = Query(DEFAULT_FOLDER),
        user: str = Depends(require_login)
) -> JSONResponse:
    logger.info(f"📦 Starte Massenverschiebung von '{folder}' nach '{checkbox}'")

    # Validiere Zielordner
    if checkbox not in Settings.checkbox_categories():
        error_msg = f"Ungültige Checkbox-Kategorie: {checkbox}"
        logger.warning(f"❌ {error_msg}")
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "message": error_msg
            }
        )

    try:
        # Verschieben der markierten Bilder
        anzahl = await move_marked_images_by_checkbox(folder, checkbox)

        # Generiere Redirect-URL
        redirect_url = f"/gallery?page=1&count={count}&folder={checkbox}&done={checkbox}"

        logger.info(f"✅ Erfolgreich verschoben: {anzahl} Dateien -> {redirect_url}")

        return JSONResponse(content={
            "status": "ok",
            "redirect": redirect_url,
            "moved": anzahl,
            "source": folder,
            "target": checkbox
        })

    except Exception as e:
        error_msg = f"Fehler beim Verschieben der Bilder: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": error_msg,
                "source": folder,
                "target": checkbox
            }
        )


from pydantic import BaseModel


class MoveResponse(BaseModel):
    status: str
    redirect: str | None = None
    moved: int | None = None
    message: str | None = None


@router.post("/moveToFolder/{checkbox}/{image_name}", response_model=MoveResponse)
async def verarbeite_einzelnes_bild(
        checkbox: str,
        image_name: str,
        current_folder: str = Query(..., alias="folder"),
        count: str = Query("6"),
        page: str = Query("1"),
        textflag: str = Query("1"),
        user: str = Depends(require_login)
) -> MoveResponse:
    folder_name = checkbox
    logger.info(f"📦 Starte verarbeite_einzelnes_bild() für '{image_name}' von '{current_folder}' nach '{folder_name}'")

    if folder_name not in Settings.checkbox_categories():
        logger.warning("❌ Ungültige Checkbox-Kategorie")
        return MoveResponse(status="invalid checkbox")

    success = await move_single_image(image_name, current_folder, folder_name)

    if Settings.COMFYUI == folder_name:
        page = 1
        count = 6
        current_folder = Settings.COMFYUI
        textflag = 4

    redirect_url = f"/gallery/?page={page}&count={count}&folder={current_folder}&textflag={textflag}"

    logger.info(f"📦 URL {redirect_url}")

    if success:
        logger.info(f"✅ Bild erfolgreich verschoben: {image_name} -> {folder_name}")
        return MoveResponse(
            status="ok",
            redirect=redirect_url,
            moved=1
        )
    else:
        logger.error(f"❌ Fehler beim Verschieben von {image_name}")
        return MoveResponse(
            status="error",
            message=f"Fehler beim Verschieben von {image_name}",
            redirect=redirect_url,
            moved=0
        )


@router.get("/verarbeite/check/{checkbox}")
def verarbeite_check_checkbox(checkbox: str, user: str = Depends(require_login)):
    logger.info(f"📊 Zähle markierte Bilder für Kategorie: {checkbox}")
    return get_checkbox_count(checkbox)


@router.get("/moveToFolder/{checkbox}")
def get_marked_images_count(checkbox: str, user: str = Depends(require_login)):
    logger.info(f"📊 Abfrage markierter Bilder für: {checkbox}")
    return get_checkbox_count(checkbox)


@router.post("/dashboard/multi/gen_pages")
async def _gen_pages(folder: str = Form(...), direction: str = Form(...)):
    """Startet die Generierung von Seiten für einen Ordner."""
    if not progress_state["running"]:
        try:
            await init_progress_state()
            task = asyncio.create_task(gen_pages(folder))
            progress_state["current_task"] = task
        finally:
            await stop_progress()
    return {"status": "ok"}


class SettingsFilter:
    # Filter-Status
    FILTER_TEXT = None
    FILTER_HISTORY = []

    SEARCH_TEXT = None
    SEARCH_HISTORY = []


@router.post("/filter/update_history")
async def update_text_history(text: str = Form(...)):
    """Aktualisiert die Text-Historie"""
    text = text.strip().replace(":", " > ")  # Ersetze ":" durch ">"
    logger.info(f'[update_text_history] Filter-Text: "{text}"')

    if text:
        dummy_scores = {key: 0 for key in score_type_map.keys()}
        try:
            parse_score_expression(text, dummy_scores)
        except ValueError as e:
            # Fange die spezifische ValueError ab
            error_msg = str(e)
            if "Unbekannter Score-Schlüssel" in error_msg:
                # Extrahiere die erlaubten Schlüssel für das Log
                allowed_keys = error_msg.split("erlaubt: ")[1].strip(")")
                msg = f"Ungültiger Schlüssel verwendet.\nErlaubte Schlüssel sind:\n{allowed_keys}"
                logger.warning(f'[update_text_history] {msg}')
                return {
                    "status": f"❌ {msg}"
                }
            msg = f'Validierungsfehler: {error_msg}'
            logger.warning(f'[update_text_history] {msg}')
            return {
                "status": f"❌ {msg}"
            }
        except Exception as e:
            msg = f'Unerwarteter Fehler: {str(e)}'
            logger.error(f'[update_text_history] {msg}')
            return {
                "status": f"❌ {msg}"
            }
        if text in SettingsFilter.FILTER_HISTORY:
            SettingsFilter.FILTER_HISTORY.remove(text)
            logger.debug(f'[update_text_history] Filter-Text aus Historie entfernt: "{text}"')

        SettingsFilter.FILTER_HISTORY.insert(0, text)
        SettingsFilter.FILTER_HISTORY = SettingsFilter.FILTER_HISTORY[:10]  # Auf 10 Einträge begrenzen
        SettingsFilter.FILTER_TEXT = text
        logger.info(f'[update_text_history] Filter-Text, neue Historie: {SettingsFilter.FILTER_HISTORY}')
    else:
        logger.info('[update_text_history] Filter-Text zurückgesetzt (leer)')
        SettingsFilter.FILTER_TEXT = None

    return {
        "status": "ok",
        "last_texts": SettingsFilter.FILTER_HISTORY,
        "filter_text": SettingsFilter.FILTER_TEXT
    }


@router.post("/search/update_history")
async def update_search_history(text: str = Form(...)):
    """Aktualisiert die Text-Historie"""
    text = text.strip().replace(":", " > ")  # Ersetze ":" durch ">"
    logger.info(f'[update_search_history] Search-Text: "{text}"')

    text = text.strip()
    if text:

        # Prüfe auf Sonderzeichen (außer Leerzeichen)
        has_special_chars = any(not c.isalnum() and not c.isspace() for c in text)

        if has_special_chars:
            msg = "Eingabe enthält Sonderzeichen"
            logger.warning(f'[update_search_history] {msg}')
            return {
                "status": f"❌ {msg}"
            }

        if text in SettingsFilter.SEARCH_HISTORY:
            SettingsFilter.SEARCH_HISTORY.remove(text)
            logger.debug(f'[update_search_history] Search-Text aus Historie entfernt: "{text}"')

        SettingsFilter.SEARCH_HISTORY.insert(0, text)
        SettingsFilter.SEARCH_HISTORY = SettingsFilter.SEARCH_HISTORY[:10]  # Auf 10 Einträge begrenzen
        SettingsFilter.SEARCH_TEXT = text
        logger.info(f'[update_search_history] Search-Text aktualisiert, neue Historie: {SettingsFilter.SEARCH_HISTORY}')
    else:
        logger.info('[update_search_history] Search-Text zurückgesetzt (leer)')
        SettingsFilter.SEARCH_TEXT = None

    return {
        "status": "ok",
        "search_history": SettingsFilter.SEARCH_HISTORY,
        "search_text": SettingsFilter.SEARCH_TEXT
    }


END_PAGE = 1
TEXT_FLAGS: list[int] = [1, 2, 3, 4]


async def get_total_images_from_cache(folder_key: str) -> int:
    await update_progress_text(f"🔍 get_total_images_from_cache(folder_key={folder_key})")

    count = 0

    folder_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_key
    if folder_path.exists():
        gallery_hash_file = folder_path / Settings.GALLERY_HASH_FILE

        try:
            if gallery_hash_file.exists():
                with open(gallery_hash_file, 'r') as f:
                    local_hashes = json.load(f)
                    count = len(local_hashes)

        except Exception as e:
            await update_progress_text(f"❌ Fehler beim Lesen/Schreiben der Cache-Dateien: {e}")

    await update_progress_text(f"📊 get_total_images_from_cache → {count} Bilder für Folder '{folder_key}'")
    return count


def calculate_start_page(total_images: int, images_per_page: int = 24) -> int:
    logger.debug(f"🧮 calculate_start_page(total_images={total_images}, images_per_page={images_per_page})")
    result = math.ceil(total_images / images_per_page)
    logger.debug(f"📝 calculate_start_page → {result}")
    return result


async def gen_pages(folder_name: str):
    await update_progress_text(f"🚀 gen_pages({folder_name})")

    if hasattr(gen_pages, 'is_running') and gen_pages.is_running:
        await update_progress_text("⚠️ gen_pages läuft bereits!")
        return

    gen_pages.is_running = True

    try:
        for kategorie in Settings.kategorien():
            if folder_name:
                if folder_name != kategorie["key"]:
                    continue
            total_images = await get_total_images_from_cache(kategorie["key"])
            start_page = calculate_start_page(total_images)

            await update_progress_text(
                f"📁 Verarbeite Kategorie: {kategorie} (Seiten: {start_page}-{END_PAGE}, Flags: 1-1)")
            await process_pages(kategorie["key"], start_page, [1])
    finally:
        gen_pages.is_running = False
        await stop_progress()


async def process_pages(folder_key: str, start_page: int, text_flags=TEXT_FLAGS):
    """Verarbeitet Seiten direkt ohne HTTP"""
    await update_progress_text(f"📥 process_pages(folder_key={folder_key}, start_page={start_page})")

    total_start = time.time()
    total_pages = start_page - END_PAGE + 1
    total_operations = total_pages * len(text_flags)
    operation_count = 0

    for textflag in text_flags:
        for page in range(start_page, END_PAGE - 1, -1):
            page_start = time.time()

            try:
                mock_request = Request(scope={
                    'type': 'http',
                    'method': 'GET',
                    'path': '/',
                    'query_string': f'page={page}&folder={folder_key}&textflag={textflag}'.encode(),
                })

                # Direkte Funktion aufrufen mit korrekten Parametern
                show_images_gallery(
                    request=mock_request,
                    user="test_user"  # oder der entsprechende User-Wert
                )
                success = True
            except Exception as e:
                logger.error(f"❌ Fehler: {str(e)}")
                success = False

            total_page_time = time.time() - page_start
            operation_count += 1

            await update_progress(
                f"[{folder_key}] Flag {textflag}/4 - Seite {page} von {start_page}-{END_PAGE}",
                int(operation_count / total_operations * 100),
                0.02
            )

            if success:
                await update_progress_text(
                    f"✅ [{folder_key}|Flag {textflag}] Seite {page} vollständig – "
                    f"Gesamt: {total_page_time:.2f}s"
                )
            else:
                await update_progress_text(
                    f"⚠️ [{folder_key}|Flag {textflag}] Seite {page} fehlgeschlagen – "
                    f"Gesamt: {total_page_time:.2f}s"
                )
                await asyncio.sleep(5)

    total_duration = time.time() - total_start
    await update_progress_text(f"🏁 [{folder_key}] Gesamtzeit: {total_duration:.1f} Sekunden")


def p4():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    asyncio.run(gen_pages("delete"))


async def download_comfyui_gifs(service):
    """Downloads GIF files from Google Drive folder and saves them in DATA_DIR/comfyui_gif"""

    try:

        # ID des Quell-Ordners
        folder_id = "1dQwLa3tZYY10-BJ7L_3N02AwuopiAHuM"

        # Suche nach GIF Dateien im Ordner
        response = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='image/gif' and trashed=false",
            spaces='drive',
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        files = response.get('files', [])

        # Lade jedes GIF herunter
        for file in files:
            file_name = file['name']
            file_id = file['id']
            target_path = Settings.GIF_FILE_CACHE_PATH / file_name
            target_path.parent.mkdir(parents=True, exist_ok=True)

            target_path.unlink()

            if not target_path.exists():
                logger.info(f"Lade {file_name} herunter...")
                # Direktes Herunterladen mit der download_file Funktion aus move_local_files_from_gdrive.py
                try:
                    await download_file(service, file_id, target_path)
                    logger.info(f"✅ {file_name} erfolgreich heruntergeladen")

                    # Suche nach entsprechender Datei im IMAGE_FILE_CACHE_DIR
                    base_name = file_name[:-4]  # Entferne .gif Endung
                    cache_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
                    matching_files = list(cache_dir.rglob(base_name + "*"))

                    if len(matching_files) == 1:
                        image_id = calculate_md5(matching_files[0])
                        clean(matching_files[0].name, image_id)

                except Exception as e:
                    logger.error(f"❌ Fehler beim Herunterladen von {file_name}: {e}")
            else:
                logger.info(f"⏩ {file_name} existiert bereits, überspringe...")

    except Exception as e:
        logger.error(f"Fehler beim Herunterladen der GIF-Dateien: {e}")


def p5():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    Settings.GIF_FILE_CACHE_PATH = Path("../../cache/comfyui_gif")

    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))

    asyncio.run(download_comfyui_gifs(service))


if __name__ == "__main__":
    p5()
