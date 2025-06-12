import asyncio
import calendar
import csv
import json
import logging
import os
import shutil
import time
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, Optional

from fastapi import APIRouter, Request
from fastapi import Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from google.cloud import bigquery
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from starlette.responses import JSONResponse

from app.config import Settings
from app.config_gdrive import sanitize_filename, calculate_md5, folder_id_by_name, SettingsGdrive
from app.database import clear_folder_status_db, load_folder_status_from_db, save_folder_status_to_db, \
    clear_folder_status_db_by_name
from app.database import count_folder_entries
from app.routes import what
from app.routes.auth import load_drive_service, load_drive_service_token
from app.routes.cost_openai_api import load_openai_costs_from_dir
from app.routes.cost_runpod import load_runpod_costs_from_dir
from app.routes.dashboard_help import _prepare_folder, _process_image_files
from app.scores.comfyUI import reload_comfyui
from app.scores.faces import reload_faces
from app.scores.nsfw import reload_nsfw
from app.scores.quality import reload_quality
from app.tools import readimages, save_pair_cache, fill_pair_cache
from app.utils.progress import init_progress_state, progress_state, update_progress, stop_progress, \
    write_local_hashes_progress
from app.utils.progress import list_files

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

kategorientabelle = {k["key"]: k for k in Settings.kategorien}

router.include_router(what.router)


@router.get("/dashboard/progress")
async def get_multi_progress():
    return JSONResponse({
        "progress": progress_state["progress"],
        "status": progress_state["status"]
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

    # Tool Links Definition
    tool_links = [
        {"label": "n8n", "url": "http://localhost", "icon": "🧩"},
        {"label": 'Sync mit "Save" (GDrive)', "url": f"{_BASE}/test?folder=save&direction=manage_save", "icon": "🔄"},
        {"label": "Reload Caches", "url": f"{_BASE}/test?direction=reloadcache", "icon": "🧹"},
        {"label": "Lösche File Cache(s)", "url": f"{_BASE}/what?what=reloadfilecache", "icon": "🗑️"},
        {"label": "Reload Gesichter", "url": f"{_BASE}/test?direction=reload_faces", "icon": "😶"},
        {"label": "Reload Quality-Scores", "url": f"{_BASE}/test?direction=reload_quality", "icon": "⭐"},
        {"label": "Reload NSFW-Scores", "url": f"{_BASE}/test?direction=reload_nsfw", "icon": "🚫"},
        {"label": 'Reload ComfyUI nur in "KI"', "url": f"{_BASE}/test?direction=reload_comfyui", "icon": "🖼️"},
        {"label": "Gen Pages", "url": f"{_BASE}/test?direction=gen_pages", "icon": "📘"}
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
            'label': 'JetBrains, Wingo',
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
        "tool_links": tool_links,
        "nav": {
            "current": current.strftime("%Y-%m"),
            "prev": f"{_BASE}?year={prev_month.year}&month={prev_month.month}",
            "next": f"{_BASE}?year={next_month.year}&month={next_month.month}"
        }
    })

def compare_hashfile_counts_dash(file_folder_dir, subfolders: bool = True):
    icon_map = {k["key"]: (k["icon"], k["label"]) for k in Settings.kategorien}

    root = Path(file_folder_dir)
    all_dirs = [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]
    result = []

    for subdir in sorted(all_dirs):
        gdrive_path = subdir / "hashes.json"
        local_path = subdir / Settings.GALLERY_HASH_FILE

        try:
            with gdrive_path.open("r", encoding="utf-8") as f:
                gdrive_data = json.load(f)
                gdrive_data = gdrive_data if isinstance(gdrive_data, dict) else {}
        except:
            gdrive_data = {}

        try:
            with local_path.open("r", encoding="utf-8") as f:
                local_data = json.load(f)
                local_data = local_data if isinstance(local_data, dict) else {}
        except:
            local_data = {}

        db_count = count_folder_entries(Settings.DB_PATH, subdir.name)

        entry = icon_map.get(subdir.name)
        if entry:
            icon, label = entry
            result.append({
                "icon": icon,
                "label": label,
                "key": subdir.name,
                "gdrive_count": len(gdrive_data),
                "local_count": len(local_data),
                "db_count": db_count
            })
        elif not subfolders:
            result.append({
                "icon": "📄",
                "label": "Textfiles",
                "key": subdir.name,
                "gdrive_count": len(gdrive_data),
                "local_count": len(local_data),
                "db_count": db_count
            })
    return sorted(result, key=lambda x: x["local_count"], reverse=True)


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
    "gen_pages": {
        "label": "Erzeuge die internen Seiten ...",
        "start_url": "/gallery/dashboard/multi/gen_pages",
        "progress_url": "/gallery/dashboard/progress"
    },
    "reload_comfyui": {
        "label": "Kopiere Bilder mit Workflow in ComfyUI ...",
        "start_url": "/gallery/dashboard/multi/reload_comfyui",
        "progress_url": "/gallery/dashboard/progress"
    },
    "reload_quality": {
        "label": "Erstell die Quality-Scores neu ...",
        "start_url": "/gallery/dashboard/multi/reload_quality",
        "progress_url": "/gallery/dashboard/progress"
    },
    "reload_nsfw": {
        "label": "Erstell die NSFW-Scores neu ...",
        "start_url": "/gallery/dashboard/multi/reload_nsfw",
        "progress_url": "/gallery/dashboard/progress"
    },
    "reload_faces": {
        "label": "Erstell die Gesichter neu ...",
        "start_url": "/gallery/dashboard/multi/reload_faces",
        "progress_url": "/gallery/dashboard/progress"
    },
    "manage_save": {
        "label": "Verarbeite Dateien aus Save (GDrive/lokal) ...",
        "start_url": "/gallery/dashboard/multi/manage_save",
        "progress_url": "/gallery/dashboard/progress"
    },
    "reloadcache": {
        "label": lambda folder_key: (
            f'Aktualisiere für "{next((k["label"] for k in Settings.kategorien if k["key"] == folder_key), folder_key)}" internen Caches ...'
            if folder_key else "Aktualisiere alle internen Caches"
        ),
        "start_url": "/gallery/dashboard/multi/reloadcache",
        "progress_url": "/gallery/dashboard/progress"
    },
    "gdrive_from_lokal": {
        "label": lambda folder_key: (
            f'Passe für "{next((k["label"] for k in Settings.kategorien if k["key"] == folder_key), folder_key)}" lokal so an wie GDrive ...'
            if folder_key else "Passe lokal so an wie GDrive"
        ),
        "start_url": "/gallery/dashboard/start",
        "progress_url": "/gallery/dashboard/progress"
    },
    "lokal_from_gdrive": {
        "label": lambda folder_key: (
            f'Passe für "{next((k["label"] for k in Settings.kategorien if k["key"] == folder_key), folder_key)}" GDrive so an wie lokal ...'
            if folder_key else "Passe GDrive so an wie lokal"
        ),
        "start_url": "/gallery/dashboard/start",
        "progress_url": "/gallery/dashboard/progress"
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
    import threading

    logger.info(f"🔄 start_progress: {folder} {direction}")

    kategorientabelle = {k["key"]: k for k in Settings.kategorien}
    kat = kategorientabelle.get(folder)

    if not kat or direction not in ("gdrive_from_lokal", "lokal_from_gdrive"):
        return JSONResponse(content={"error": "Ungültiger Parameter"}, status_code=400)

    def runner():
        asyncio.run(init_progress_state())
        try:
            if direction == "gdrive_from_lokal":
                gdrive_from_lokal(load_drive_service(), folder)
            elif direction == "lokal_from_gdrive":
                lokal_from_gdrive(load_drive_service(), folder)
        finally:
            asyncio.run(stop_progress())

    threading.Thread(target=runner).start()
    return {"started": True}


def gdrive_from_lokal(service, folder_name: str):
    logger.info(f"gdrive_from_lokal: {folder_name}")

    cache_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)

    global_gdrive_hashes = load_all_gdrive_hashes(cache_dir)
    folder_id_map = build_folder_id_map(service)
    hashfiles = list(cache_dir.rglob("gallery202505_hashes.json"))

    for gallery_hashfile in hashfiles:
        try:
            with gallery_hashfile.open("r", encoding="utf-8") as f:
                local_hashes = json.load(f)
        except Exception:
            continue

    for gallery_hashfile in hashfiles:
        folder_path = gallery_hashfile.parent
        folder = folder_path.name
        logger.info(f"folder: {folder}")
        if not (folder == folder_name):
            continue

        gdrive_hashfile = folder_path / "hashes.json"

        try:
            with gallery_hashfile.open("r", encoding="utf-8") as f:
                local_hashes = json.load(f)
        except Exception as e:
            print(f"[Fehler] {gallery_hashfile}: {e}")
            continue

        try:
            with gdrive_hashfile.open("r", encoding="utf-8") as f:
                gdrive_hashes = json.load(f)
        except Exception:
            gdrive_hashes = {}

        updated = False
        count = 0
        progress_state["progress"] = 0

        total = len(local_hashes)
        if total == 0:
            stop_progress()
            return

        for name, md5 in local_hashes.items():
            existing = gdrive_hashes.get(name)
            current_md5 = existing.get("md5") if isinstance(existing, dict) else existing

            if name not in gdrive_hashes or current_md5 != md5:
                file_info = global_gdrive_hashes.get(md5)
                if file_info:
                    print(f"[✓] {name} fehlt in {folder}, aber global vorhanden als: {file_info['name']}")
                    file_id = file_info.get("id")
                    if file_id:
                        target_folder_id = folder_id_map.get(folder)
                        if not target_folder_id:
                            print(f"[!] Keine Ordner-ID für {folder} gefunden")
                            count += 1
                            progress_state["progress"] = int((count / total) * 100)
                            continue
                        try:
                            move_file_to_folder(service, file_id, target_folder_id)
                            gdrive_hashes[name] = {
                                "md5": file_info["md5"],
                                "id": file_id
                            }
                            updated = True
                        except Exception as e:
                            print(f"[Fehler beim Verschieben] {name}: {e}")
                else:
                    local_file = folder_path / name
                    if local_file.exists():
                        target_folder_id = folder_id_map.get(folder)
                        if target_folder_id:
                            try:
                                file_metadata = {"name": name, "parents": [target_folder_id]}
                                media = MediaFileUpload(str(local_file), resumable=True)
                                uploaded = service.files().create(
                                    body=file_metadata,
                                    media_body=media,
                                    fields="id"
                                ).execute()
                                gdrive_hashes[name] = {
                                    "md5": md5,
                                    "id": uploaded["id"]
                                }
                                updated = True
                                print(f"[↑] {name} hochgeladen in {folder}")
                            except Exception as e:
                                print(f"[Fehler beim Hochladen] {name}: {e}")
                        else:
                            print(f"[!] Keine Zielordner-ID für {folder} gefunden")
                    else:
                        print(f"[!] {name} fehlt in {folder} und global nicht gefunden")

            count += 1
            if total > 0:
                progress_state["progress"] = int((count / total) * 100)

        if updated:
            with gdrive_hashfile.open("w", encoding="utf-8") as f:
                json.dump(gdrive_hashes, f, indent=2)
            print(f"[↑] hashes.json aktualisiert für Ordner {folder}")

    asyncio.run(stop_progress())


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
            print(f"[Fehler] {hashfile}: {e}")
    return global_hashes


def build_folder_id_map(service) -> Dict[str, str]:
    folder_map = {}
    page_token = None
    while True:
        response = service.files().list(
            q="mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=1000,
            pageToken=page_token
        ).execute()
        for file in response.get("files", []):
            folder_map[file["name"]] = file["id"]
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return folder_map


def move_file_to_folder(service, file_id: str, target_folder_id: str):
    file = service.files().get(fileId=file_id, fields="parents").execute()
    previous_parents = ",".join(file.get("parents", []))
    service.files().update(
        fileId=file_id,
        addParents=target_folder_id,
        removeParents=previous_parents,
        fields="id, parents"
    ).execute()


def move_file_to_folder_new(service, file_id, old_parents, new_parent):
    service.files().update(
        fileId=file_id,
        addParents=new_parent,
        removeParents=",".join(old_parents),
        fields='id, parents'
    ).execute()


def lokal_from_gdrive(service, folder_name: str):
    logger.info(f"lokal_from_gdrive: {folder_name}")
    try:
        progress_state["progress"] = 0
        count = 0
        base_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
        if folder_name:
            all_local_folders = [base_dir / folder_name]
        else:
            all_local_folders = [p for p in base_dir.iterdir() if p.is_dir()]

        all_entries = []

        for folder_path in sorted(all_local_folders):
            hash_file_path = folder_path / "hashes.json"
            if not hash_file_path.exists():
                continue
            with open(hash_file_path, "r", encoding="utf-8") as f:
                entries = json.load(f)
                all_entries.extend([(folder_path, name, entry) for name, entry in entries.items()])

        total = len(all_entries)
        if total == 0:
            stop_progress()
            return

        processed_files = set()
        gallery_hashes = {}

        for folder_path, name, entry in all_entries:
            if name in processed_files:
                continue

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
                processed_files.add(name)
                gallery_hashes[name] = {"md5": md5, "id": file_id} if file_id else md5
            elif best_match:
                try:
                    shutil.move(str(best_match), str(local_target))
                    if local_target.exists():
                        print(f"[MOVE] {name} → {folder_name}")
                        processed_files.add(name)
                        gallery_hashes[name] = {"md5": md5, "id": file_id} if file_id else md5
                    else:
                        raise RuntimeError("Ziel existiert nach Move nicht")
                except Exception as e:
                    print(f"[MOVE-FEHLER] {name}: {e}")
                    raise SystemExit(f"Abbruch: Datei konnte nicht verschoben werden: {best_match} → {local_target}")
            elif file_id:
                try:
                    download_file(service, file_id, local_target)
                    if local_target.exists():
                        print(f"[DL] {name} ↓ {folder_name}")
                        processed_files.add(name)
                        gallery_hashes[name] = {"md5": md5, "id": file_id} if file_id else md5
                except Exception as e:
                    print(f"[Fehler beim Herunterladen] {name}: {e}")
            elif best_match:
                print(f"\033[94m[FEHLT] {name} → kein Download möglich, aber lokal gefunden\033[0m")

            if total > 0:
                progress_state["progress"] = int((count / total) * 100)

        gallery_hash_path = base_dir / Settings.GALLERY_HASH_FILE
        with open(gallery_hash_path, "w", encoding="utf-8") as f:
            json.dump(gallery_hashes, f, indent=2)

        asyncio.run(stop_progress())
    except Exception as e:
        logger.error(f"Fehler bei map_gdrive_to_local (mit Fortschritt): {e}")
        asyncio.run(stop_progress())


def download_file(service, file_id, local_path):
    request = service.files().get_media(fileId=file_id)
    with open(local_path, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()


@router.post("/dashboard/multi/reloadcache")
async def _reloadcache(folder: str = Form(...), direction: str = Form(...)):
    if not progress_state["running"]:
        asyncio.create_task(reloadcache_progress(folder))
    return {"status": "ok"}


@router.post("/dashboard/multi/manage_save")
async def _manage_save(folder: str = Form(...), direction: str = Form(...)):
    if not progress_state["running"]:
        asyncio.create_task(manage_save_progress(None))
    return {"status": "ok"}


async def fill_pair_cache_folder(folder_name: str, image_file_cache_dir, pair_cache, pair_cache_path_local):
    folder_path = os.path.join(image_file_cache_dir, folder_name)

    if not os.path.isdir(folder_path):
        logging.warning(f"[fill_pair_cache] Kein gültiger Ordner: {folder_path}")
        return

    logging.info(f"[fill_pair_cache] Aktualisiere Cache für Ordner: {folder_name}")

    # Entferne nur die Paare aus dem angegebenen Ordner
    keys_to_delete = [k for k in pair_cache if k.startswith(f"{folder_name}/") or f"/{folder_name}/" in k]
    for k in keys_to_delete:
        del pair_cache[k]

    for name in os.listdir(folder_path):
        subpath = os.path.join(folder_path, name)
        if os.path.isfile(subpath):
            if any(subpath.lower().endswith(key) for key in [folder_name]):
                readimages(folder_path, pair_cache)
    try:
        with open(pair_cache_path_local, 'w') as f:
            json.dump(pair_cache, f)
        logging.info(f"[fill_pair_cache] Pair-Cache gespeichert: {len(pair_cache)} Paare")
    except Exception as e:
        logging.warning(f"[fill_pair_cache] Fehler beim Speichern von pair_cache.json: {e}")

    logging.info(f"[fill_pair_cache] Cache für {folder_name} aktualisiert.")


def delete_file(service, file_id):
    service.files().delete(fileId=file_id).execute()


async def manage_save_progress(service: None):
    await init_progress_state()
    progress_state["running"] = True

    if not service:
        service = load_drive_service()
    from_folder_name = "save"
    to_folder_name = "recheck"

    from_folder_id = folder_id_by_name(from_folder_name)
    from_files = await list_files(from_folder_id, service, "!=")
    if len(from_files) > 0:
        to_folder_id = folder_id_by_name(to_folder_name)
        to_files = await list_files(to_folder_id, service, "!=")

        existing_hashes = {f['md5Checksum'] for f in to_files if 'md5Checksum' in f}

        downloaded = await perform_local_sync(service, from_files, Path(Settings.IMAGE_FILE_CACHE_DIR) / to_folder_name,
                                              existing_hashes)
        moved, deleted = await perform_gdrive_sync(service, from_files, to_files, existing_hashes, to_folder_id,
                                                   from_folder_id)

        await fill_pair_cache_folder(
            to_folder_name,
            Settings.IMAGE_FILE_CACHE_DIR,
            Settings.CACHE.get("pair_cache"),
            Settings.PAIR_CACHE_PATH)

        print("Zusammenfassung Images:")
        print(f"🔢 Zu verarbeiten: {len(from_files)}")
        print(f"📥 Heruntergeladen lokal: {downloaded}")
        print(f"📦 Verschoben nach GDrive: {moved}")
        print(f"🗑️  Gelöscht auf GDrive: {deleted}")

    from_files = await list_files(from_folder_id, service, "=")
    if len(from_files) > 0:
        to_folder_id = folder_id_by_name("textfiles")
        to_files = await list_files(to_folder_id, service, "=")

        existing_hashes = {f['md5Checksum'] for f in to_files if 'md5Checksum' in f}

        downloaded = await perform_local_sync(service, from_files, Settings.TEXT_FILE_CACHE_DIR, existing_hashes)

        moved, deleted = await perform_gdrive_sync(service, from_files, to_files, existing_hashes, to_folder_id,
                                                   from_folder_id)

        print("Zusammenfassung Text:")
        print(f"🔢 Zu verarbeiten: {len(from_files)}")
        print(f"📥 Heruntergeladen lokal: {downloaded}")
        print(f"📦 Verschoben nach GDrive: {moved}")
        print(f"🗑️  Gelöscht auf GDrive: {deleted}")

    await stop_progress()


async def perform_gdrive_sync(service, save_files, _files, existing_hashes, to_folder_id, from_folder_id):
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
                    download_file(service, file_id, local_path)
                    downloaded += 1
                    action = "Aktualisiert"
                    status.append("🔁 lokal aktualisiert")
            else:
                download_file(service, file_id, local_path)
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


def _load_file_parents_cache_from_db(db_path: str, file_parents_cache: dict) -> bool:
    rows = load_folder_status_from_db(db_path)
    if not rows:
        return False
    logging.info("[fill_folder_cache] 📦 Lade file_parents_cache aus der Datenbank...")
    for image_id, folder_key in rows:
        if folder_key not in file_parents_cache:
            Settings.folders_loaded += 1
            file_parents_cache[folder_key] = []
            logging.info(
                f"[fill_folder_cache] ✅ Cache aus DB geladen: {Settings.folders_loaded}/{Settings.folders_total} {folder_key}")
        file_parents_cache[folder_key].append(image_id)
    if Settings.folders_loaded != Settings.folders_total:
        Settings.folders_loaded = Settings.folders_total
        logging.info(
            f"[fill_folder_cache] ✅ Cache aus DB geladen: {Settings.folders_loaded}/{Settings.folders_total}")
    return True


async def _process_image_files_progress(image_files, folder_key, file_parents_cache, db_path):
    folder_name = label = next((k["label"] for k in Settings.kategorien if k["key"] == folder_key), None)
    total = len(image_files)
    for index, image_file in enumerate(image_files):
        await update_progress(f"Kategorie: {folder_name} : {total} Dateien ({image_file})",
                              int(index / total * 100), 0.02)
        if not image_file.is_file() or image_file.suffix.lower() not in Settings.IMAGE_EXTENSIONS:
            continue
        image_name = image_file.name.lower()
        pair = Settings.CACHE["pair_cache"].get(image_name)
        if not pair:
            logging.warning(f"[_process_image_files_progress] ⚠️ Kein Eintrag im pair_cache für: {image_name}")
            continue
        logging.info(f"[_process_image_files_progress] ✅️ Eintrag im pair_cache für: {folder_key} / {image_name}")
        image_id = pair["image_id"]
        file_parents_cache[folder_key].append(image_id)
        save_folder_status_to_db(db_path, image_id, folder_key)


def fillcache_local(pair_cache_path_local: str, image_file_cache_dir: str):
    pair_cache = Settings.CACHE["pair_cache"]
    pair_cache.clear()

    logging.info(f"[fillcache_local] 📂 Lesen: {pair_cache_path_local}")

    if os.path.exists(pair_cache_path_local):
        try:
            with open(pair_cache_path_local, 'r') as f:
                pair_cache.update(json.load(f))
                logging.info(f"[fillcache_local] Pair-Cache geladen: {len(pair_cache)} Paare")
                return
        except Exception as e:
            logging.warning(f"[fillcache_local] Fehler beim Laden von pair_cache.json: {e}")

    fill_pair_cache(image_file_cache_dir, pair_cache, pair_cache_path_local)


def fill_file_parents_cache(db_path: str):
    file_parents_cache = Settings.CACHE["file_parents_cache"]
    file_parents_cache.clear()

    if _load_file_parents_cache_from_db(db_path, file_parents_cache):
        return

    logging.info("[fill_folder_cache] 🚀 Keine Cache-Daten vorhanden, lade von lokal...")
    clear_folder_status_db(db_path)

    for kat in Settings.kategorien:
        folder_name = kat["key"]
        file_parents_cache[folder_name] = []
        folder_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name
        if not _prepare_folder(folder_path):
            continue
        logging.info(f"[fill_folder_cache] 📂 Lese Bilder aus: {folder_name}")
        image_files = list(folder_path.iterdir())
        _process_image_files(image_files, folder_name, file_parents_cache, db_path)
        Settings.folders_loaded += 1
        logging.info(
            f"[fill_folder_cache] ✅ {Settings.folders_loaded}/{Settings.folders_total} Ordner geladen: {folder_name}")


def load_rendered_html_file(file_dir: Path, file_name: str) -> str | None:
    file_path = file_dir / (file_name + ".j2")
    if file_path.is_file():
        try:
            logging.info(f"[load_rendered_html_file] ✅ {file_path}")
            return file_path.read_text(encoding='utf-8')
        except Exception as e:
            logging.error(f"Fehler beim Laden der Datei {file_path}: {e}")
            return None
    else:
        logging.info(f"[load_rendered_html_file] ⚠️ {file_path}")
        return None


def save_rendered_html_file(file_dir: Path, file_name: str, content: str) -> bool:
    file_path = file_dir / (file_name + ".j2")
    try:
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding="utf-8")
        return True
    except Exception as e:
        logging.error(f"Fehler beim Speichern der Datei {file_path}: {e}")
        return False


def delete_rendered_html_file(file_dir: Path, file_name: str) -> bool:
    file_path = file_dir / (file_name + ".j2")
    if file_path.is_file():
        try:
            file_path.unlink()
            return True
        except Exception as e:
            logging.error(f"Fehler beim Löschen der Datei {file_path}: {e}")
            return False
    return False


async def reloadcache_progress(folder_key: Optional[str] = None):
    """
    Reloads the cache for folders based on the folder_key parameter.

    Args:
        folder_key: Optional folder key.
                   If None, processes all categories.
                   If in CHECKBOX_CATEGORIES, processes only that folder.
                   If "textfiles", processes text files.
    """
    logger.info(f"🔄 Starte reloadcache_progress für Ordner: {folder_key}")

    try:
        await init_progress_state()
        progress_state["running"] = True
        Settings.folders_loaded = 0

        if folder_key == "textfiles":
            logger.info("🗃️ Modus: Textverarbeitung")
            await process_text_files()

        elif folder_key in Settings.CHECKBOX_CATEGORIES:
            logger.info(f"📂 Modus: Einzelne Kategorie ({folder_key})")
            folder_name = next(
                (k["label"] for k in Settings.kategorien if k["key"] == folder_key),
                folder_key
            )
            await process_category(folder_key, folder_name)
            Settings.folders_loaded += 1

        else:
            logger.info("📂 Modus: Alle Kategorien")
            for kategorie in Settings.kategorien:
                await process_category(kategorie["key"], kategorie["label"])
                Settings.folders_loaded += 1

    except Exception as e:
        logger.error(f"❌ Fehler beim Reload-Cache: {e}")
        raise
    finally:
        await stop_progress()
        logger.info("✅ reloadcache_progress abgeschlossen")


async def process_category(folder_key: str, folder_name: str):
    """
    Processes a single category folder.

    Args:
        folder_key: The key of the folder to process
        folder_name: The display name of the folder
    """
    logger.info(f"📂 Verarbeite Kategorie: {folder_name} ({folder_key})")

    pair_cache = Settings.CACHE.get("pair_cache")

    # Update progress
    await update_progress(f"{folder_name}: fillcache_local ...", 33)

    # Clear existing entries
    to_delete = [
        key for key, value in pair_cache.items()
        if value.get("folder", "") == folder_key
    ]

    logger.info(f"🧹 Entferne {len(to_delete)} bestehende Einträge aus pair_cache für {folder_key}")
    for key in to_delete:
        del pair_cache[key]

    # Process images
    image_dir = f"{Settings.IMAGE_FILE_CACHE_DIR}/{folder_key}"
    logger.info(f"📸 Lese Bilder aus {image_dir}")
    readimages(image_dir, pair_cache)

    # Save cache
    save_pair_cache(pair_cache, Settings.PAIR_CACHE_PATH)
    logger.info(f"💾 pair_cache gespeichert: {Settings.PAIR_CACHE_PATH}")

    await update_progress(f"{folder_name}: fillcache_local fertig", 100)
    await asyncio.sleep(1.0)

    # Update database
    logger.info("🔄 Aktualisiere Elternpfade in DB")
    await fill_file_parents_cache_progress(Settings.DB_PATH, folder_key)

    # Write hashes
    logger.info("🧮 Schreibe lokale Hashes (Bilder)")
    await write_local_hashes_progress(Settings.IMAGE_EXTENSIONS, image_dir, False)


async def process_text_files():
    """Processes text files in the text directory."""
    logger.info("🧮 Schreibe lokale Hashes (Texte)")
    await write_local_hashes_progress(
        Settings.TEXT_EXTENSIONS,
        Settings.TEXT_FILE_CACHE_DIR,
        False
    )


async def fill_file_parents_cache_progress(db_path: str, folder_key: str | None):
    if folder_key:

        file_parents_cache = Settings.CACHE["file_parents_cache"]
        if folder_key in file_parents_cache:
            del file_parents_cache[folder_key]

        folder_name = next((k["label"] for k in Settings.kategorien if k["key"] == folder_key), None)

        clear_folder_status_db_by_name(db_path, folder_key)

        logging.info("[fill_folder_cacFhe] 🚀 Keine Cache-Daten vorhanden, lade von lokal...")

        file_parents_cache[folder_key] = []
        folder_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_key
        if not _prepare_folder(folder_path):
            return
        image_files = list(folder_path.iterdir())
        await update_progress(f"{folder_name}: Kategorie: {folder_key} : {len(image_files)} Dateien", 0)
        await _process_image_files_progress(image_files, folder_key, file_parents_cache, db_path)
        Settings.folders_loaded += 1
        logging.info(
            f"[fill_folder_cache] ✅ {Settings.folders_loaded}/{Settings.folders_total} Ordner geladen: {folder_key}")
    else:
        file_parents_cache = Settings.CACHE["file_parents_cache"]
        file_parents_cache.clear()

        if _load_file_parents_cache_from_db(db_path, file_parents_cache):
            return

        logging.info("[fill_folder_cache] 🚀 Keine Cache-Daten vorhanden, lade von lokal...")
        clear_folder_status_db(db_path)

        for kat in Settings.kategorien:
            if folder_key and kat != folder_key:
                continue
            folder_key = kat["key"]
            file_parents_cache[folder_key] = []
            folder_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_key
            if not _prepare_folder(folder_path):
                continue
            logging.info(f"[fill_folder_cache] 📂 Lese Bilder aus: {folder_key}")
            image_files = list(folder_path.iterdir())
            await update_progress(f"Kategorie: {folder_key} : {len(image_files)} Dateien", 0)
            await _process_image_files_progress(image_files, folder_key, file_parents_cache, db_path)
            Settings.folders_loaded += 1
            logging.info(
                f"[fill_folder_cache] ✅ {Settings.folders_loaded}/{Settings.folders_total} Ordner geladen: {folder_key}")


def localp1():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.RENDERED_HTML_DIR = "../../cache/rendered_html"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"


def p1():
    localp1()

    fill_pair_cache(Settings.IMAGE_FILE_CACHE_DIR, Settings.CACHE.get("pair_cache"), Settings.PAIR_CACHE_PATH)

    asyncio.run(reloadcache_progress("recheck"))


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
        print(f"{entry['label']:<15}{entry['gdrive_count']:>15}{entry['local_count']:>15}{entry['db_count']:>15}")


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


if __name__ == "__main__":
    p3()
