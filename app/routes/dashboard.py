import asyncio
import calendar
import csv
import json
import os
import shutil
import sqlite3
import time
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict

from fastapi import APIRouter, Request
from fastapi import Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from google.cloud import bigquery
from starlette.responses import JSONResponse

from app.config import Settings, score_type_map
from app.config_gdrive import sanitize_filename, calculate_md5, folder_id_by_name, SettingsGdrive
from app.routes import what
from app.routes.auth import load_drive_service, load_drive_service_token
from app.routes.cost_openai_api import load_openai_costs_from_dir
from app.routes.cost_runpod import load_runpod_costs_from_dir
from app.routes.hashes import reloadcache_progress, download_file, update_gdrive_hashes
from app.routes.manage_image_files import move_gdrive_files_by_local
from app.scores.comfyUI import reload_comfyui
from app.scores.faces import reload_faces
from app.scores.nsfw import reload_nsfw
from app.scores.quality import reload_quality
from app.scores.texte import reload_texte
from app.tools import readimages, fill_pair_cache
from app.utils.db_utils import delete_all_checkbox_status, delete_all_external_tasks
from app.utils.folder_utils import count_folder_entries
from app.utils.logger_config import setup_logger
from app.utils.progress import init_progress_state, progress_state, update_progress, stop_progress, \
    update_progress_text
from app.utils.progress import list_files
from app.utils.progress_detail import detail_state

DASHBOARD_PROGRESS = "/gallery/dashboard/progress"

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))

logger = setup_logger(__name__)

router.include_router(what.router)


@router.get("/dashboard/progress")
async def get_multi_progress():
    """Gibt den aktuellen Fortschrittszustand zurück."""
    return JSONResponse({
        "progress": progress_state["progress"],
        "status": progress_state["status"],
        "details": {
            "status": detail_state["status"],
            "progress": detail_state["progress"]
        }
    })


_BASE = "/gallery/dashboard"


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, year: int = None, month: int = None):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/innate-setup-454010-i9-f92b1b6a1c44.json"

    today = datetime.today()
    year = year or today.year
    month = month or today.month
    current = datetime(year, month, 1)

    prev_month = (current.replace(day=1) - timedelta(days=1)).replace(day=1)
    next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)

    dataset = "gcp_billing_export_n8n"
    table = "gcp_billing_export_resource_v1_01003C_0EEFF2_E60D9D"
    gcp_daily = get_daily_costs(dataset, table, year, month)
    gcp_map = {d["tag"]: float(d["kosten_chf"]) for d in gcp_daily}

    openai_daily = load_openai_costs_from_dir(Path(Settings.COSTS_FILE_DIR), year, month)
    openai_map = {d["tag"]: float(d["kosten_chf"]) for d in openai_daily}

    runpod_daily = load_runpod_costs_from_dir(year, month)
    runpod_map = {d["tag"]: float(d["kosten_chf"]) for d in runpod_daily}

    # Lade die Standard-Kosten
    from app.routes.cost_default import load_default_costs
    default_daily = load_default_costs(year, month)
    default_map = {d["tag"]: float(d["kosten_chf"]) for d in default_daily}

    # Kombiniere alle Tags von allen Diensten
    all_tags = sorted(set(gcp_map) | set(openai_map) | set(runpod_map) | set(default_map))
    labels = [datetime.strptime(tag, "%Y-%m-%d").strftime("%d.%m.") for tag in all_tags]

    values_gcp = [
        gcp_map.get(tag, 0.0) if tag >= "2025-05-06" else 0.0
        for tag in all_tags
    ]
    values_openai = [openai_map.get(tag, 0.0) for tag in all_tags]
    values_runpod = [runpod_map.get(tag, 0.0) for tag in all_tags]
    values_default = [default_map.get(tag, 0.0) for tag in all_tags]

    if all_tags:
        first_day = datetime.strptime(all_tags[0], "%Y-%m-%d").strftime("%d.%m.%Y")
        last_day = datetime.strptime(all_tags[-1], "%Y-%m-%d").strftime("%d.%m.%Y")
        total_chf = round(
            sum(values_gcp) + sum(values_openai) +
            sum(values_runpod) + sum(values_default),
            2
        )
        info = [
            {"from_to": f"{first_day}–{last_day}"},
            {"kosten_chf": f"CHF {total_chf}"}
        ]
    else:
        info = [{"from_to": "unbekannt"}, {"kosten_chf": "CHF 0.0"}]

    # GDrive Statistiken
    gdrive_stats1 = compare_hashfile_counts_dash(Settings.IMAGE_FILE_CACHE_DIR, subfolders=True)
    gdrive_stats2 = compare_hashfile_counts_dash(Settings.TEXT_FILE_CACHE_DIR, subfolders=False)

    help_links = [
        {"label": "n8n", "url": "http://localhost", "icon": "⚙️"},
        {"label": "ComfyUi", "url": "http://comfyui.local:8188", "icon": "🎨"},
        {"label": "n8n-server", "url": "https://nw1cs857bq8z5p-5678.proxy.runpod.net/", "icon": "⚙️"},
        {"label": "openai", "url": "https://platform.openai.com/settings/organization/usage", "icon": "🧠"},
        {"label": "runpod", "url": "https://console.runpod.io/user/billing", "icon": "☁️"}
    ]

    # Tool Links Definition
    tool_links = [
        {"label": 'Sync mit "Save" (GDrive)', "url": f"{_BASE}/test?folder=save&direction=manage_save", "icon": "🔄"},
        {"label": "Reload pair & File-hashes", "url": f"{_BASE}/test?direction=reloadcache", "icon": "🔄"},
        {"label": "Lösche File Cache(s)", "url": f"{_BASE}/what?what=reloadfilecache", "icon": "🗑️"},
        {"label": "Repair DB", "url": f"{_BASE}/test?direction=repair_db", "icon": "👤"},
        {"label": "Reload Gesichter", "url": f"{_BASE}/test?direction=reload_faces", "icon": "👤"},
        {"label": "Reload Quality-Scores", "url": f"{_BASE}/test?direction=reload_quality", "icon": "📊"},
        {"label": "Reload NSFW-Scores", "url": f"{_BASE}/test?direction=reload_nsfw", "icon": "🔞"},
        {"label": "Reload Texte", "url": f"{_BASE}/test?direction=reload_texte", "icon": "📝"},
        {"label": 'Reload ComfyUI nur in "KI"', "url": f"{_BASE}/test?direction=reload_comfyui", "icon": "🤖"},
        {"label": "Lösche Doppelte Bilder", "url": f"{_BASE}/test?direction=del_double_images", "icon": "🎯"},
        {"label": "Gen Pages", "url": f"{_BASE}/test?direction=gen_pages", "icon": "📄"}
    ]

    cost_datasets = [
        {
            'label': 'Google',
            'data': values_gcp,
            'color': 'rgba(54, 162, 235, 0.6)'  # Blau
        },
        {
            'label': 'OpenAI (+ChatGPT)',
            'data': values_openai,
            'color': 'rgba(255, 99, 132, 0.6)'  # Rot
        },
        {
            'label': 'RunPod',
            'data': values_runpod,
            'color': 'rgba(75, 192, 192, 0.6)'  # Türkis
        },
        {
            'label': 'JetBrains, Wingo, One',
            'data': values_default,
            'color': 'rgba(153, 102, 255, 0.6)'  # Violett
        }
    ]

    return templates.TemplateResponse("dashboard.j2", {
        "request": request,
        "gdrive_stats": gdrive_stats1 + gdrive_stats2,
        "info": info,
        "labels": labels,
        "cost_datasets": cost_datasets,
        "help_links": help_links,
        "tool_links": tool_links,
        "nav": {
            "current": current.strftime("%Y-%m"),
            "prev": f"{_BASE}?year={prev_month.year}&month={prev_month.month}",
            "next": f"{_BASE}?year={next_month.year}&month={next_month.month}"
        }
    })


def compare_hashfile_counts_dash(file_folder_dir, subfolders: bool = True):
    """Compare hash file counts across different storage locations."""
    icon_map = {k["key"]: (k["icon"], k["label"]) for k in Settings.kategorien()}
    all_dirs = get_directories(file_folder_dir, subfolders)
    result = []

    for subdir in sorted(all_dirs):
        hash_data = load_hash_files(subdir)
        db_count = get_db_count(file_folder_dir, subdir.name)
        result_entry = create_result_entry(
            subdir,
            hash_data,
            db_count,
            icon_map,
            subfolders
        )
        if result_entry:
            result.append(result_entry)

    return sorted(result, key=lambda x: x["local_count"], reverse=True)


def get_directories(file_folder_dir, subfolders):
    """Get the list of directories to process."""
    root = Path(file_folder_dir)
    return [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]


def load_hash_files(directory):
    """Load and parse hash files from the directory."""
    gdrive_data = load_json_file(directory / "hashes.json")
    local_data = load_json_file(directory / Settings.GALLERY_HASH_FILE)
    return {
        'gdrive': gdrive_data,
        'local': local_data,
        'gdrive_count': len(gdrive_data),
        'local_count': len(local_data)
    }


def load_json_file(path):
    """Load and parse JSON file with error handling."""
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except:
        return {}


def get_db_count(file_folder_dir, subdir_name):
    """Get count from the database."""
    if Settings.TEXT_FILE_CACHE_DIR == file_folder_dir:
        return get_text_db_count()
    return count_folder_entries(Settings.DB_PATH, subdir_name)


def get_text_db_count():
    """Get text entry count from the database."""
    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM image_quality_scores WHERE score_type = ?",
                (score_type_map['text'],)
            )
            count_result = cursor.fetchone()
            return count_result[0] if count_result else 0
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return 0


def create_result_entry(subdir, hash_data, db_count, icon_map, subfolders):
    """Create the result entry dictionary."""
    entry = icon_map.get(subdir.name)
    if entry:
        icon, label = entry
    elif not subfolders:
        icon, label = "📄", "Textfiles"
    else:
        return None

    return {
        "icon": icon,
        "label": label,
        "key": subdir.name,
        "gdrive_count": hash_data['gdrive_count'],
        "local_count": hash_data['local_count'],
        "db_count": db_count,
        "has_count_mismatch": hash_data['local_count'] != db_count,
        "has_gdrive_mismatch": hash_data['gdrive_count'] != hash_data['local_count']
    }


def get_monthly_costs(dataset: str, table: str, start: str, end: str):
    client = bigquery.Client()
    query = f"""
    SELECT
      project.id AS projekt_id,
      ROUND(SUM(cost), 2) AS kosten_chf
    FROM
      `{dataset}.{table}`
    WHERE
      usage_start_time >= '{start}'
      AND usage_start_time < '{end}'
    GROUP BY
      projekt_id
    ORDER BY
      kosten_chf DESC
    """
    query_job = client.query(query)
    results = query_job.result()
    return [
        {"projekt_id": row["projekt_id"], "kosten_chf": row["kosten_chf"]}
        for row in results
    ]


def get_daily_costs(dataset: str, table: str, year: int, month: int):
    client = bigquery.Client()
    start = date(year, month, 1).strftime("%Y-%m-%d")
    last_day = calendar.monthrange(year, month)[1]
    end = date(year, month, last_day).strftime("%Y-%m-%d")

    query = f"""
    SELECT
      DATE(usage_start_time) AS tag,
      ROUND(SUM(cost), 2) AS kosten_chf
    FROM
      `{dataset}.{table}`
    WHERE
      usage_start_time >= '{start}' AND usage_start_time <= '{end}'
    GROUP BY
      tag
    ORDER BY
      tag
    """
    query_job = client.query(query)
    results = query_job.result()
    return [{"tag": row["tag"].strftime("%Y-%m-%d"), "kosten_chf": row["kosten_chf"]} for row in results]


calls = {
    "del_double_images": {
        "label": "Finde und Lösche doppelte Bilder ...",
        "start_url": "/gallery/dashboard/multi/del_double_images",
        "progress_url": DASHBOARD_PROGRESS
    },
    "gen_pages": {
        "label": "Erzeuge die internen Seiten ...",
        "start_url": "/gallery/dashboard/multi/gen_pages",
        "progress_url": DASHBOARD_PROGRESS
    },
    "reload_comfyui": {
        "label": "Kopiere Bilder mit Workflow in ComfyUI ...",
        "start_url": "/gallery/dashboard/multi/reload_comfyui",
        "progress_url": DASHBOARD_PROGRESS
    },
    "reload_quality": {
        "label": "Erstell die Quality-Scores neu ...",
        "start_url": "/gallery/dashboard/multi/reload_quality",
        "progress_url": DASHBOARD_PROGRESS
    },
    "reload_nsfw": {
        "label": "Erstell die NSFW-Scores neu ...",
        "start_url": "/gallery/dashboard/multi/reload_nsfw",
        "progress_url": DASHBOARD_PROGRESS
    },
    "reload_texte": {
        "label": "Bilder ohne Text nach \"Neu\"",
        "start_url": "/gallery/dashboard/multi/reload_texte",
        "progress_url": DASHBOARD_PROGRESS
    },
    "repair_db": {
        "label": "Bereinige Datenbank ...",
        "start_url": "/gallery/dashboard/multi/repair_db",
        "progress_url": DASHBOARD_PROGRESS
    },
    "reload_faces": {
        "label": "Erstell die Gesichter neu ...",
        "start_url": "/gallery/dashboard/multi/reload_faces",
        "progress_url": DASHBOARD_PROGRESS
    },
    "manage_save": {
        "label": "Verarbeite Dateien aus Save (GDrive/lokal) ...",
        "start_url": "/gallery/dashboard/multi/manage_save",
        "progress_url": DASHBOARD_PROGRESS
    },
    "reloadcache": {
        "label": lambda folder_key: (
            f'Reload für "{next((k["label"] for k in Settings.kategorien() if k["key"] == folder_key), folder_key)}" pair & File-hashes ...'
            if folder_key else 'Reload für "Alle" pair & File-hashes'
        ),
        "start_url": "/gallery/dashboard/multi/reloadcache",
        "progress_url": DASHBOARD_PROGRESS
    },
    "lokal_zu_gdrive": {
        "label": lambda folder_key: (
            f'Passe für "{next((k["label"] for k in Settings.kategorien() if k["key"] == folder_key), folder_key)}" lokal -> GDrive ...'
            if folder_key else ""
        ),
        "start_url": "/gallery/dashboard/start",
        "progress_url": DASHBOARD_PROGRESS
    },
    "gdrive_zu_local": {
        "label": lambda folder_key: (
            f'Passe für "{next((k["label"] for k in Settings.kategorien() if k["key"] == folder_key), folder_key)}" GDrive -> lokal ...'
            if folder_key else "GDrive -> lokal"
        ),
        "start_url": "/gallery/dashboard/start",
        "progress_url": DASHBOARD_PROGRESS
    }
}


@router.get("/dashboard/test", response_class=HTMLResponse)
async def dashboard_progress(request: Request):
    folder_key = request.query_params.get("folder")
    direction = request.query_params.get("direction")
    logger.info(f"🔄 dashboard_progress: {folder_key} {direction}")

    call = calls.get(direction)
    if not call:
        return HTMLResponse("Ungültige direction", status_code=400)

    label = call["label"]
    if callable(label):
        label = label(folder_key)

    return templates.TemplateResponse("dashboard_progress.j2", {
        "request": request,
        "button_text": label,
        "folder_name": folder_key,
        "direction": direction,
        "start_url": call["start_url"],
        "progress_url": call["progress_url"]
    })


@router.post("/dashboard/start")
async def start_progress(folder: str = Form(...), direction: str = Form(...)):
    logger.info(f"🔄 start_progress: {folder} {direction}")

    # Validierung der Eingabeparameter
    if not folder or not direction:
        return JSONResponse(
            content={"error": "Fehlende Parameter"},
            status_code=400
        )

    if folder != Settings.TEXTFILES_FOLDERNAME and folder not in Settings.checkbox_categories():
        return JSONResponse(
            content={"error": f"Ungültiger Ordner: {folder}"},
            status_code=400
        )

    # Prüfe, ob die Richtung gültig ist
    valid_directions = ("lokal_zu_gdrive", "gdrive_zu_local")
    if direction not in valid_directions:
        return JSONResponse(
            content={"error": f"Ungültige Richtung: {direction}"},
            status_code=400
        )

    # Prüfe, ob bereits ein Prozess läuft
    if progress_state.get("running"):
        return JSONResponse(
            content={"error": "Es läuft bereits ein Synchronisierungsprozess"},
            status_code=409
        )

    await init_progress_state()

    try:
        # Google Drive Service initialisieren
        service = load_drive_service()
        if not service:
            raise RuntimeError("Konnte Google Drive Service nicht initialisieren")

        # Synchronisation starten
        if direction == "lokal_zu_gdrive":
            task = asyncio.create_task(move_gdrive_files_by_local(service, folder))
        else:
            task = asyncio.create_task(copy_or_move_local_by_gdrive(service, folder))
        progress_state["current_task"] = task

    except Exception as e:
        error_msg = f"❌ Fehler bei {direction} für Ordner {folder}: {str(e)}"
        logger.error(error_msg)
        await update_progress_text(error_msg)
        raise

    finally:
        await stop_progress()

    # Task-ID generieren und speichern
    task_id = f"{folder}_{direction}_{int(time.time())}"

    return JSONResponse(
        content={
            "status": "started",
            "task_id": task_id,
            "folder": folder,
            "direction": direction
        },
        status_code=202
    )


def load_all_gdrive_hashes(cache_dir: Path) -> Dict[str, Dict[str, str]]:
    global_hashes = {}
    hashfiles = list(cache_dir.rglob("hashes.json"))
    for hashfile in hashfiles:
        try:
            with hashfile.open("r", encoding="utf-8") as f:
                data = json.load(f)
                for name, entry in data.items():
                    if isinstance(entry, dict) and 'md5' in entry and 'id' in entry:
                        global_hashes[entry['md5']] = {
                            "name": name,
                            "id": entry['id'],
                            "md5": entry['md5']
                        }
        except Exception as e:
            logger.info(f"[Fehler] {hashfile}: {e}")
    return global_hashes


def move_file_to_folder_new(service, file_id, old_parents, new_parent):
    service.files().update(
        fileId=file_id,
        addParents=new_parent,
        removeParents=",".join(old_parents),
        fields='id, parents'
    ).execute()


async def save_gdrive_hashes(folder_name: str, gdrive_hashes: dict) -> None:
    """Save the generated hashes to a file."""
    await update_progress_text(f"💾 Speichere Hash-Datei für {folder_name}...")
    cache_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
    hashfile_path = cache_dir / folder_name / Settings.GDRIVE_HASH_FILE
    hashfile_path.parent.mkdir(parents=True, exist_ok=True)

    with hashfile_path.open("w", encoding="utf-8") as f:
        json.dump(gdrive_hashes, f, indent=2)

    await update_progress_text(f"✅ GDrive-Hashes aktualisiert für Ordner: {folder_name}")


async def copy_or_move_local_by_gdrive(service, folder_name: str):
    await update_progress_text(f"🔄 Starte GDrive zu Local für: {folder_name}")
    try:
        base_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
        if folder_name:
            all_local_folders = [base_dir / folder_name]
        else:
            all_local_folders = [p for p in base_dir.iterdir() if p.is_dir()]

        for folder_path in sorted(all_local_folders):
            current_folder = folder_path.name
            await update_progress_text(f"📂 Verarbeite Ordner: {current_folder}")

            # Initialisiere Hashes für diesen Ordner
            folder_gallery_hashes = {}

            # Hole Hash-Datei für diesen Ordner
            hash_file_path = folder_path / Settings.GDRIVE_HASH_FILE
            if not hash_file_path.exists():
                await update_progress_text(f"⚠️ Keine Hash-Datei gefunden für: {current_folder}")
                continue

            with hash_file_path.open("r", encoding="utf-8") as f:
                entries = json.load(f)
                total = len(entries)

                for idx, (name, entry) in enumerate(entries.items()):
                    current_progress = int((idx / total) * 100)
                    await update_progress(f"⚡ Verarbeite: {name}", current_progress)

                    md5 = None
                    file_id = None
                    if isinstance(entry, dict):
                        md5 = entry.get("md5")
                        file_id = entry.get("id")
                    elif isinstance(entry, str):
                        md5 = entry

                    if not md5:
                        continue

                    local_target = folder_path / name
                    all_matches = list(base_dir.rglob(name))
                    best_match = None
                    for match in all_matches:
                        if match.resolve() != local_target.resolve() and match.parent != local_target.parent:
                            best_match = match
                            break

                    if local_target.exists():
                        folder_gallery_hashes[name] = {"md5": md5, "id": file_id} if file_id else md5
                    elif best_match:
                        try:
                            shutil.move(str(best_match), str(local_target))
                            if local_target.exists():
                                await update_progress_text(f"📦 Verschoben: {name} → {current_folder}")
                                folder_gallery_hashes[name] = {"md5": md5, "id": file_id} if file_id else md5
                            else:
                                await update_progress_text(f"❌ Fehler: Ziel existiert nach Move nicht: {name}")
                        except Exception as e:
                            await update_progress_text(f"❌ Move-Fehler bei {name}: {e}")
                            raise SystemExit(
                                f"Abbruch: Datei konnte nicht verschoben werden: {best_match} → {local_target}")
                    elif file_id:
                        try:
                            await download_file(service, file_id, local_target)
                            if local_target.exists():
                                await update_progress_text(f"📥 Heruntergeladen: {name} → {current_folder}")
                                folder_gallery_hashes[name] = {"md5": md5, "id": file_id} if file_id else md5
                        except Exception as e:
                            await update_progress_text(f"❌ Download-Fehler bei {name}: {e}")
                    elif best_match:
                        await update_progress_text(f"⚠️ {name}: Kein Download möglich, aber lokal gefunden")

            # Speichere Hash-Datei für diesen Ordner
            await update_progress_text(f"💾 Speichere Gallery-Hash-Datei für: {current_folder}")
            gallery_hash_path = folder_path / Settings.GALLERY_HASH_FILE
            with gallery_hash_path.open("w", encoding="utf-8") as f:
                json.dump(folder_gallery_hashes, f, indent=2)

        await update_progress_text("✅ Verarbeitung abgeschlossen")

    except Exception as e:
        await update_progress_text(f"❌ Fehler bei gdrive_zu_local: {e}")

    await stop_progress()


@router.post("/dashboard/multi/reloadcache")
async def _reloadcache(folder: str = Form(...), direction: str = Form(...)):
    if not progress_state["running"]:
        task = asyncio.create_task(reloadcache_progress(load_drive_service(), folder))
        # Store the task in a way that prevents garbage collection
        asyncio.current_task().reloadcache_task = task
    return {"status": "ok"}


@router.post("/dashboard/multi/manage_save")
async def _manage_save(folder: str = Form(...), direction: str = Form(...)):
    if not progress_state["running"]:
        asyncio.create_task(manage_save_progress(None))
    return {"status": "ok"}


def delete_file(service, file_id):
    service.files().delete(fileId=file_id).execute()


async def manage_save_progress(service: None):
    await init_progress_state()

    if not service:
        service = load_drive_service()
    from_folder_name = "save"
    to_folder_name = Settings.RECHECK

    from_folder_id = folder_id_by_name(from_folder_name)
    from_files = await list_files(from_folder_id, service, "!=")
    if len(from_files) > 0:
        to_folder_id = folder_id_by_name(to_folder_name)
        to_files = await list_files(to_folder_id, service, "!=")

        existing_hashes = {f['md5Checksum'] for f in to_files if 'md5Checksum' in f}

        downloaded = await perform_local_sync(
            service,
            from_files,
            Path(Settings.IMAGE_FILE_CACHE_DIR) / to_folder_name,
            existing_hashes)
        moved, deleted = await perform_gdrive_sync(
            service,
            from_files,
            to_files,
            existing_hashes,
            to_folder_id)

        await fill_pair_cache_folder(
            to_folder_name,
            Settings.IMAGE_FILE_CACHE_DIR,
            Settings.CACHE.get("pair_cache"),
            Settings.PAIR_CACHE_PATH)

        logger.info("Zusammenfassung Images:")
        logger.info(f"🔢 Zu verarbeiten: {len(from_files)}")
        logger.info(f"📥 Heruntergeladen lokal: {downloaded}")
        logger.info(f"📦 Verschoben nach GDrive: {moved}")
        logger.info(f"🗑️  Gelöscht auf GDrive: {deleted}")

    from_files = await list_files(from_folder_id, service, "=")
    if len(from_files) > 0:
        to_folder_id = folder_id_by_name(Settings.TEXTFILES_FOLDERNAME)
        to_files = await list_files(to_folder_id, service, "=")

        existing_hashes = {f['md5Checksum'] for f in to_files if 'md5Checksum' in f}

        downloaded = await perform_local_sync(
            service,
            from_files,
            Settings.TEXT_FILE_CACHE_DIR,
            existing_hashes)

        moved, deleted = await perform_gdrive_sync(
            service,
            from_files,
            to_files,
            existing_hashes,
            to_folder_id)

        logger.info("Zusammenfassung Text:")
        logger.info(f"🔢 Zu verarbeiten: {len(from_files)}")
        logger.info(f"📥 Heruntergeladen lokal: {downloaded}")
        logger.info(f"📦 Verschoben nach GDrive: {moved}")
        logger.info(f"🗑️  Gelöscht auf GDrive: {deleted}")

    await stop_progress()


async def perform_gdrive_sync(service, save_files, _files, existing_hashes, to_folder_id):
    moved = 0
    deleted = 0
    total = len(save_files)

    for index, file in enumerate(save_files, start=1):
        original_name = file['name']
        file_id = file['id']
        remote_md5 = file.get('md5Checksum')

        await update_progress(f"GDrive: {original_name}", int(index / total * 100))

        status = []
        if remote_md5 in existing_hashes:
            status.append("✅ bereits vorhanden (MD5 match)")
            remote_size = int(file.get("size", 0))
            target_file = next(
                (f for f in _files if f.get("md5Checksum") == remote_md5 and f.get("name") == file.get("name")),
                None)
            target_size = int(target_file.get("size", 0)) if target_file else 0

            if remote_size > target_size:
                move_file_to_folder_new(service, file_id, file['parents'], to_folder_id)
                moved += 1
                status.append("📦 verschoben (größer)")
            else:
                delete_file(service, file_id)
                deleted += 1
                status.append("🗑️ gelöscht (nicht größer oder gleichnamig)")
        else:
            move_file_to_folder_new(service, file_id, file['parents'], to_folder_id)
            moved += 1
            status.append("📦 verschoben (neuer Hash)")

        logger.info(f"{original_name}: {', '.join(status)}")
        await asyncio.sleep(0.05)  # sichtbare Aktualisierung

    await update_progress(f"{moved} Dateien verschoben, {deleted} Dateien gelöscht.", 100)
    await asyncio.sleep(0.5)

    return moved, deleted


async def perform_local_sync(service, save_files, local_file_dir, existing_hashes):
    total = len(save_files)
    downloaded = 0

    # Erzeuge vollständigen Pfad mit Zeitstempel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = Path(f"{Settings.SAVE_LOG_FILE}{timestamp}.csv")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("w", newline='', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "original_name", "action", "local_path"])

        for index, file in enumerate(save_files, start=1):
            original_name = file['name']
            sanitized_name = sanitize_filename(original_name)
            file_id = file['id']
            remote_md5 = file.get('md5Checksum')
            local_path = local_file_dir / sanitized_name

            await update_progress(f"Lokal: {original_name}", int(index / total * 100))
            entry_time = datetime.now().isoformat(timespec='seconds')

            status = []
            action = None

            if remote_md5 in existing_hashes:
                status.append("✅ bereits vorhanden (MD5 match)")
            elif local_path.exists():
                local_md5 = calculate_md5(local_path)
                if remote_md5 == local_md5:
                    status.append("✅ lokal identisch")
                else:
                    await download_file(service, file_id, local_path)
                    downloaded += 1
                    action = "Aktualisiert"
                    status.append("🔁 lokal aktualisiert")
            else:
                await download_file(service, file_id, local_path)
                downloaded += 1
                action = "Heruntergeladen"
                status.append("⬇️ heruntergeladen")

            if action:
                logger.info(f"{action}: {original_name} → {local_path}")
                writer.writerow([entry_time, original_name, action, str(local_path)])

            logger.info(f"{original_name}: {', '.join(status)}")
            await asyncio.sleep(0.05)

    await update_progress(f"{downloaded} Dateien geladen.", 100)
    logger.info(f"✅ Insgesamt {downloaded} Dateien geladen.")
    await asyncio.sleep(0.5)

    return downloaded


def is_today(filepath: Path) -> bool:
    """Prüft, ob die Datei heute erstellt oder zuletzt geändert wurde."""
    stat = filepath.stat()
    # Verwende die letzte Änderungszeit (ctime ist auf Unix oft Change-Time, nicht Creation-Time)
    modified_time = stat.st_mtime
    today_start = time.mktime(time.strptime(time.strftime("%Y-%m-%d"), "%Y-%m-%d"))
    tomorrow_start = today_start + 86400
    return today_start <= modified_time < tomorrow_start


def load_today_files_with_progress(directory: Path):
    today_files = []
    all_files = list(directory.rglob("*.txt"))  # rekursiv
    total = len(all_files)

    for index, file in enumerate(all_files):
        if file.is_file() and is_today(file):
            today_files.append(file)

        update_progress(f"Lokal: {file.name}", int(index / total * 100))

    update_progress(f"Fertig: {len(today_files)} Dateien gefunden", 100)

    return today_files


def delete_files_with_prefix(html_path: Path, image_id: str):
    count = 0
    for file in html_path.iterdir():
        if file.is_file() and file.name.startswith(image_id):
            count += 1
            file.unlink()
    return count


@router.post("/dashboard/multi/repair_db")
async def _repair_db(folder: str = Form(...), direction: str = Form(...)):
    if not progress_state["running"]:
        task = asyncio.create_task(repair_db())
        # Store the task in a way that prevents garbage collection
        asyncio.current_task().repair_db_task = task
    return {"status": "ok"}


async def repair_db():
    logger.info("🚀 Starting repair DB")

    try:
        await init_progress_state()
        await update_progress_text("🔄 Starting delete_all_checkbox_status")
        delete_all_checkbox_status()
        await update_progress_text("🔄 Starting delete_all_external_tasks")
        delete_all_external_tasks()
    except Exception as e:
        error_msg = f"Error in repair_db: {e}"
        logger.error(error_msg)
        await update_progress_text(f"❌ {error_msg}")

    finally:
        await stop_progress()


@router.post("/dashboard/multi/reload_faces")
async def _reload_faces(folder: str = Form(...), direction: str = Form(...)):
    if not progress_state["running"]:
        asyncio.create_task(reload_faces())
    return {"status": "ok"}


@router.post("/dashboard/multi/reload_nsfw")
async def _reload_nsfw(folder: str = Form(...), direction: str = Form(...)):
    if not progress_state["running"]:
        asyncio.create_task(reload_nsfw())
    return {"status": "ok"}


@router.post("/dashboard/multi/reload_texte")
async def _reload_texte(folder: str = Form(...), direction: str = Form(...)):
    if not progress_state["running"]:
        asyncio.create_task(reload_texte())
    return {"status": "ok"}


@router.post("/dashboard/multi/reload_quality")
async def _reload_quality(folder: str = Form(...), direction: str = Form(...)):
    if not progress_state["running"]:
        asyncio.create_task(reload_quality())
    return {"status": "ok"}


@router.post("/dashboard/multi/reload_comfyui")
async def _reload_comfyui(folder: str = Form(...), direction: str = Form(...)):
    if not progress_state["running"]:
        asyncio.create_task(reload_comfyui())
    return {"status": "ok"}


def load_rendered_html_file(file_dir: Path, file_name: str) -> str | None:
    file_path = file_dir / (file_name + ".j2")
    if file_path.is_file():
        try:
            logger.info(f"[load_rendered_html_file] ✅ {file_path}")
            return file_path.read_text(encoding='utf-8')
        except Exception as e:
            logger.error(f"Fehler beim Laden der Datei {file_path}: {e}")
            return None
    else:
        logger.info(f"[load_rendered_html_file] ⚠️ {file_path}")
        return None


def save_rendered_html_file(file_dir: Path, file_name: str, content: str) -> bool:
    file_path = file_dir / (file_name + ".j2")
    try:
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding="utf-8")
        return True
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Datei {file_path}: {e}")
        return False


def delete_rendered_html_file(file_dir: Path, file_name: str) -> bool:
    file_path = file_dir / (file_name + ".j2")
    if file_path.is_file():
        try:
            file_path.unlink()
            return True
        except Exception as e:
            logger.error(f"Fehler beim Löschen der Datei {file_path}: {e}")
            return False
    return False


@router.post("/dashboard/multi/del_double_images")
async def _del_double_images(folder: str = Form(...), direction: str = Form(...)):
    if not progress_state["running"]:
        asyncio.create_task(handle_duplicates())
    return {"status": "ok"}


async def handle_duplicates():
    logger.info("🚀 Starting duplicate handling")
    try:
        await init_progress_state()

        # Setup
        temp_dir = Settings.TEMP_DIR_PATH
        temp_dir.mkdir(parents=True, exist_ok=True)
        md5_map = {}
        filename_map = {}

        # Scan files
        await scan_files(md5_map, filename_map)

        # Process duplicates
        moved_count = await handle_md5_duplicates(md5_map, temp_dir)
        renamed_count = await handle_filename_duplicates(filename_map)

        # Final status
        final_message = f"✅ Completed! Moved {moved_count} MD5 duplicates to {temp_dir} and renamed {renamed_count} filename duplicates"
        await update_progress_text(final_message)

    except Exception as e:
        logger.error(f"Error in handle_duplicates: {e}")
        await update_progress_text(f"❌ Error in handle_duplicates: {e}")
    finally:
        await stop_progress()


async def scan_files(md5_map, filename_map):
    await update_progress_text("🔄 Starting duplicate detection")

    for index, kat in enumerate(Settings.kategorien(), 1):
        folder_name = kat["key"]
        local_files = {}

        await update_progress(f"Scanning category: {folder_name}", int(index * 100 / len(Settings.kategorien())), 0.02)
        await readimages(str(Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name), local_files)

        for image_name, entry in local_files.items():
            full_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name / image_name
            file_info = {
                "path": full_path,
                "image_name": image_name,
                "category": folder_name,
                "image_id": entry.get("image_id", None)
            }

            if "image_id" in entry:
                md5_map.setdefault(entry["image_id"], []).append(file_info)
            filename_map.setdefault(image_name, []).append(file_info)


async def handle_md5_duplicates(md5_map, temp_dir):
    moved_count = 0
    duplicates = [files for files in md5_map.values() if len(files) > 1]
    await update_progress_text(f"Found {len(duplicates)} groups of MD5 duplicate images")

    for files in duplicates:
        for duplicate in files[1:]:
            try:
                source_path = duplicate["path"]
                dest_path = temp_dir / f"{duplicate['category']}_{duplicate['image_name']}"
                source_path.rename(dest_path)
                moved_count += 1
            except Exception as e:
                logger.error(f"Error moving file {source_path}: {e}")

    return moved_count


async def handle_filename_duplicates(filename_map):
    renamed_count = 0
    duplicates = [files for files in filename_map.values() if len(files) > 1]
    await update_progress_text(f"Found {len(duplicates)} groups of filename duplicates")

    for files in duplicates:
        files.sort(key=lambda x: str(x["path"]))
        for duplicate in files[1:]:
            if not duplicate["image_id"]:
                continue
            try:
                source_path = duplicate["path"]
                new_name = f"{duplicate['image_id']}_{duplicate['image_name']}"
                new_path = source_path.parent / new_name
                source_path.rename(new_path)
                renamed_count += 1
            except Exception as e:
                logger.error(f"Error renaming file {source_path}: {e}")

    return renamed_count


def localp2():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.RENDERED_HTML_DIR = "../../cache/rendered_html"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../../secrets/innate-setup-454010-i9-f92b1b6a1c44.json"
    return load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))


def p2():
    localp2()

    gdrive_stats1 = compare_hashfile_counts_dash(Settings.IMAGE_FILE_CACHE_DIR, subfolders=True)
    gdrive_stats2 = compare_hashfile_counts_dash(Settings.TEXT_FILE_CACHE_DIR, subfolders=False)

    gdrive_stats = gdrive_stats1 + gdrive_stats2

    for entry in gdrive_stats:
        logger.info(f"{entry['label']:<15}{entry['gdrive_count']:>15}{entry['local_count']:>15}{entry['db_count']:>15}")


def localp3():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.RENDERED_HTML_DIR = "../../cache/rendered_html"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.SAVE_LOG_FILE = "../../cache/from_save_"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../../secrets/innate-setup-454010-i9-f92b1b6a1c44.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")
    return load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))


def p3():
    asyncio.run(manage_save_progress(localp3()))


def p4():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    fill_pair_cache(Settings.IMAGE_FILE_CACHE_DIR, Settings.CACHE.get("pair_cache"), Settings.PAIR_CACHE_PATH)

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))

    asyncio.run(copy_or_move_local_by_gdrive(service, Settings.RECHECK))


def p5():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))
    asyncio.run(update_gdrive_hashes(service, "ki", Settings.IMAGE_EXTENSIONS, Path(Settings.IMAGE_FILE_CACHE_DIR)))

def p_lokal_zu_gdrive():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))
    asyncio.run(move_gdrive_files_by_local(service, "delete"))

if __name__ == "__main__":
    p_lokal_zu_gdrive()
