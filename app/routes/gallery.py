import asyncio
import json
import math
import os
import time
from pathlib import Path
from typing import Dict
from urllib.parse import unquote

from fastapi import APIRouter, Depends, Request, Query, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import JSONResponse

from app.config import Settings, score_type_map  # Importiere die Settings-Klasse
from app.database import set_status, load_status, save_status, \
    move_marked_images_by_checkbox, get_checkbox_count, \
    get_scores_filtered_by_expr  # Importiere die benötigten Funktionen
from app.dependencies import require_login
from app.routes.dashboard import load_rendered_html_file, save_rendered_html_file
from app.scores.texte import search_recoll
from app.services.image_processing import prepare_image_data, clean
from app.utils.logger_config import setup_logger
from app.utils.progress import update_progress, stop_progress, progress_state
from app.utils.score_parser import parse_score_expression

DEFAULT_COUNT: str = "6"
DEFAULT_FOLDER: str = "real"

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))

logger = setup_logger(__name__)

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
    for image_name_l in Settings.CACHE["pair_cache"]:
        pair = Settings.CACHE["pair_cache"][image_name_l]
        image_id = pair.get("image_id", "")
        if is_file_in_folder(image_id, folder_name):
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
            # Versuch, Ausdruck zu parsen (wirft ValueError wenn ungültig)
            dummy_scores = {key: 0 for key in score_type_map.keys()}
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


    # 3. Hauptschleife über alle Bilder
    logger.info("[Gallery] Starte Hauptschleife über Bilder")
    for image_name in Settings.CACHE["pair_cache"].keys():
        pair = Settings.CACHE["pair_cache"][image_name]
        image_id = pair['image_id']
        if not is_file_in_folder(image_id, folder_name):
            continue

        # Nur Bilder mit positivem Score-Match weiterverarbeiten
        if score_expr and filtered_names is not None:
            if image_name.lower() not in filtered_names:
                continue

        if start <= total_images < end:
            image_keys.append(image_name.lower())
        total_images += 1

    logger.info(f"[Gallery] Gefunden: {total_images} Bilder gesamt, {len(image_keys)} auf aktueller Seite")

    images_html_parts = []
    recheck_category = next((k["key"] for k in Settings.kategorien if k["key"] == "recheck"),
                            None)  # Verwende kategorien aus Settings

    logger.info("[Gallery] Beginne HTML-Generierung")
    for image_name in image_keys:
        pair = Settings.CACHE["pair_cache"][image_name]  # Verwende Caches aus Settings
        image_id = pair['image_id']

        image_id_text = f"{image_id}_{textflag}"
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
                        set_status(image_name, recheck_category)
                case '3':
                    text_content = Settings.CACHE["text_cache"].get(image_name,
                                                                    Settings.KEIN_TEXT_GEFUNDEN)
                    if Settings.KEIN_TEXT_GEFUNDEN == text_content:
                        logger.warning(f"[Gallery] Kein Text gefunden für Bild {image_name}")
                        set_status(image_name, recheck_category)

                    if isinstance(text_content, str):
                        lines = text_content.splitlines()
                        if lines and lines[0].startswith("Aufgenommen:"):
                            text_content = lines[0]

                case '4':
                    text_content = Settings.CACHE["text_cache"].get(image_name,
                                                                    Settings.KEIN_TEXT_GEFUNDEN)
                    if Settings.KEIN_TEXT_GEFUNDEN == text_content:
                        logger.warning(f"[Gallery] Kein Text gefunden für Bild {image_name}")
                        set_status(image_name, recheck_category)

                    if isinstance(text_content, str):
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
        value = Settings.CACHE["text_cache"].get(image_name, "")  # Verwende Caches aus Settings
        if isinstance(value, str):
            if "Error 2" in value:
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
        "kategorien": Settings.kategorien,
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


@router.post("/dashboard/multi/gen_pages")
async def _gen_pages(folder: str = Form(...), direction: str = Form(...)):
    if not progress_state["running"]:
        asyncio.create_task(gen_pages())
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
    logger.info(f'[update_history] Filter-Text: "{text}"')

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
                logger.warning(f'[update_history] {msg}')
                return {
                    "status": f"❌ {msg}"
                }
            msg = f'Validierungsfehler: {error_msg}'
            logger.warning(f'[update_history] {msg}')
            return {
                "status": f"❌ {msg}"
            }
        except Exception as e:
            msg = f'Unerwarteter Fehler: {str(e)}'
            logger.error(f'[update_history] {msg}')
            return {
                "status": f"❌ {msg}"
            }
        if text in SettingsFilter.FILTER_HISTORY:
            SettingsFilter.FILTER_HISTORY.remove(text)
            logger.debug(f'[update_history] Filter-Text aus Historie entfernt: "{text}"')

        SettingsFilter.FILTER_HISTORY.insert(0, text)
        SettingsFilter.FILTER_HISTORY = SettingsFilter.FILTER_HISTORY[:10]  # Auf 10 Einträge begrenzen
        SettingsFilter.FILTER_TEXT = text
        logger.info(f'[update_history] Filter-Text, neue Historie: {SettingsFilter.FILTER_HISTORY}')
    else:
        logger.info('[update_history] Filter-Text zurückgesetzt (leer)')
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
    logger.info(f'[update_history] Search-Text: "{text}"')

    if text:
        if text in SettingsFilter.SEARCH_HISTORY:
            SettingsFilter.SEARCH_HISTORY.remove(text)
            logger.debug(f'[update_history] Search-Text aus Historie entfernt: "{text}"')

        SettingsFilter.SEARCH_HISTORY.insert(0, text)
        SettingsFilter.SEARCH_HISTORY = SettingsFilter.SEARCH_HISTORY[:10]  # Auf 10 Einträge begrenzen
        SettingsFilter.SEARCH_TEXT = text
        logger.info(f'[update_history] Search-Text aktualisiert, neue Historie: {SettingsFilter.SEARCH_HISTORY}')
    else:
        logger.info('[update_history] Search-Text zurückgesetzt (leer)')
        SettingsFilter.SEARCH_TEXT = None

    return {
        "status": "ok",
        "search_history": SettingsFilter.SEARCH_HISTORY,
        "search_text": SettingsFilter.SEARCH_TEXT
    }



END_PAGE = 1
TEXT_FLAGS = range(1, 5)  # 1 bis 4


def get_total_images_from_cache(folder_key: str) -> int:
    logger.debug(f"🔍 get_total_images_from_cache(folder_key={folder_key})")
    pair_cache: Dict = Settings.CACHE.get("pair_cache", {})
    count = sum(1 for img_data in pair_cache.values() if img_data.get("folder") == folder_key)
    logger.debug(f"📊 get_total_images_from_cache → {count} Bilder für Folder '{folder_key}'")
    return count


def calculate_start_page(total_images: int, images_per_page: int = 24) -> int:
    logger.debug(f"🧮 calculate_start_page(total_images={total_images}, images_per_page={images_per_page})")
    result = math.ceil(total_images / images_per_page)
    logger.debug(f"📝 calculate_start_page → {result}")
    return result


async def gen_pages():
    logger.info("🚀 gen_pages()")

    if hasattr(gen_pages, 'is_running') and gen_pages.is_running:
        logger.warning("⚠️ gen_pages läuft bereits!")
        return

    gen_pages.is_running = True

    try:
        for kategorie in Settings.kategorien:
            total_images = get_total_images_from_cache(kategorie["key"])
            start_page = calculate_start_page(total_images)

            logger.info(f"📁 Verarbeite Kategorie: {kategorie} (Seiten: {start_page}-{END_PAGE}, Flags: 1-4)")
            await process_pages(kategorie["key"], start_page)
    finally:
        gen_pages.is_running = False
        await stop_progress()


async def process_pages(folder_key: str, start_page: int):
    """Verarbeitet Seiten direkt ohne HTTP"""
    logger.debug(f"📥 process_pages(folder_key={folder_key}, start_page={start_page})")

    total_start = time.time()
    total_pages = start_page - END_PAGE + 1
    total_operations = total_pages * len(TEXT_FLAGS)
    operation_count = 0

    for textflag in TEXT_FLAGS:
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
                logger.info(
                    f"✅ [{folder_key}|Flag {textflag}] Seite {page} vollständig – "
                    f"Gesamt: {total_page_time:.2f}s"
                )
            else:
                logger.warning(
                    f"⚠️ [{folder_key}|Flag {textflag}] Seite {page} fehlgeschlagen – "
                    f"Gesamt: {total_page_time:.2f}s"
                )
                await asyncio.sleep(5)

    total_duration = time.time() - total_start
    logger.info(f"🏁 [{folder_key}] Gesamtzeit: {total_duration:.1f} Sekunden")
