import calendar
import json
import logging
import os
from datetime import datetime, date
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi import Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from google.cloud import bigquery
from googleapiclient.http import MediaIoBaseDownload
from starlette.responses import JSONResponse

from app.config import Settings
from app.routes.auth import load_drive_service

router = APIRouter()
progress = {"value": 0}
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))
logger = logging.getLogger(__name__)


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/innate-setup-454010-i9-f92b1b6a1c44.json"

    dataset = "gcp_billing_export_n8n"
    table = "gcp_billing_export_resource_v1_01003C_0EEFF2_E60D9D"

    today = datetime.today()

    daily_info = [d for d in get_daily_costs(dataset, table, today.year, today.month) if d["tag"] >= "2025-05-11"]

    if daily_info:
        first_day = datetime.strptime(daily_info[0]["tag"], "%Y-%m-%d").strftime("%d.%m.%Y")
        last_day = datetime.strptime(daily_info[-1]["tag"], "%Y-%m-%d").strftime("%d.%m.%Y")
        total = round(sum(float(d["kosten_chf"]) for d in daily_info), 2)
        info = [
            {"from_to": f"{first_day}–{last_day}"},
            {"kosten_chf": f"CHF {total}"}
        ]
    else:
        info = [
            {"from_to": f"unbekannt"},
            {"kosten_chf": f"CHF 0.0"}
        ]

    labels = [datetime.strptime(d["tag"], "%Y-%m-%d").strftime("%d.%m.") for d in daily_info]
    values = [float(d["kosten_chf"]) for d in daily_info]

    logger.info(info)

    gdrive_stats = compare_hashfile_counts_dash(Settings.IMAGE_FILE_CACHE_DIR, subfolders=True)

    return templates.TemplateResponse("dashboard.j2", {
        "request": request,
        "gdrive_stats": gdrive_stats,
        "info": info,
        "labels": labels,
        "values": values
    })


def compare_hashfile_counts_dash(file_folder_dir, subfolders: bool = True):
    icon_map = {k["key"]: (k["icon"], k["label"]) for k in Settings.kategorien}

    root = Path(file_folder_dir)
    all_dirs = [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]
    result = []

    for subdir in sorted(all_dirs):
        gdrive_path = subdir / "hashes.json"
        local_path = subdir / "gallery202505_hashes.json"

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

        entry = icon_map.get(subdir.name)
        if entry:
            icon, label = entry
            result.append({
                "icon": icon,
                "label": label,
                "key": subdir.name,
                "gdrive_count": len(gdrive_data),
                "local_count": len(local_data)
            })
    return sorted(result, key=lambda x: x["local_count"], reverse=True)


def compare_hashfile_counts(file_folder_dir, subfolders: bool = True):
    results = compare_hashfile_counts_dash(file_folder_dir, subfolders=subfolders)

    header = f"{'Ordner':<15}{'GDrive-Hashes':>15}{'Lokal-Hashes':>15}"
    print(header)
    print("-" * len(header))

    for entry in results:
        print(f"{entry['ordner']:<15}{entry['gdrive_count']:>15}{entry['local_count']:>15}")


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


kategorientabelle = {k["key"]: k for k in Settings.kategorien}


@router.get("/dashboard/test", response_class=HTMLResponse)
async def dashboard_test(request: Request):
    folder_name = request.query_params.get("folder", None)
    direction = request.query_params.get('direction', None)
    logger.info(f"🔄 dashboard_test: {folder_name} {direction}")

    kat = kategorientabelle.get(folder_name)
    if kat:
        if direction == "gdrive2lokal":
            button_text = f'Übertrage "{kat["label"]}" von GDrive zu lokal'
        elif direction == "lokal2gdrive":
            button_text = f'Übertrage "{kat["label"]}" von lokal zu GDrive'
        else:
            return None
    else:
        return None

    return templates.TemplateResponse("dashboard_test.j2", {
        "request": request,
        "button_text": button_text,
        "folder_name": folder_name,
        "direction": direction
    })


@router.get("/dashboard/progress")
async def get_progress():
    return {"progress": progress["value"]}


@router.post("/dashboard/start")
async def start_progress(folder: str = Form(...), direction: str = Form(...)):
    import threading

    logger.info(f"🔄 start_progress: {folder} {direction}")

    kategorientabelle = {k["key"]: k for k in Settings.kategorien}
    kat = kategorientabelle.get(folder)

    if not kat or direction not in ("gdrive2lokal", "lokal2gdrive"):
        return JSONResponse(content={"error": "Ungültiger Parameter"}, status_code=400)

    def runner():
        progress["value"] = 0
        try:
            if direction == "gdrive2lokal":
                gdrive2lokal(folder)
            elif direction == "lokal2gdrive":
                lokal2gdrive(folder)
        finally:
            progress["value"] = 100

    threading.Thread(target=runner).start()
    return {"started": True}


import time


def gdrive2lokal(folder_name: str):
    logger.info(f"Starte GDrive → Lokal für: {folder_name}")
    steps = 50
    for i in range(1, steps + 1):
        progress["value"] = int(i * 100 / steps)
        logger.info(f"Fortschritt {progress['value']}% bei {folder_name}")
        time.sleep(0.5)  # kurze Pause, z. B. 100 ms
    logger.info(f"Fertig mit GDrive → Lokal für: {folder_name}")


def lokal2gdrive(folder_name: str):
    logger.info(f"Starte Lokal → GDrive für: {folder_name}")
    try:
        progress["value"] = 0
        count = 0
        base_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
        service = load_drive_service()
        all_local_folders = [p for p in base_dir.iterdir() if p.is_dir() and p.name != "real"]
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
            progress["value"] = 100
            return

        processed_files = set()
        gallery_hashes = {}

        for folder_path, name, entry in all_entries:
            count += 1
            logger.info(f"[{count}/{total}] Verarbeitung: {name}")

            md5 = None
            file_id = None
            if isinstance(entry, dict):
                md5 = entry.get("md5")
                file_id = entry.get("id")
            elif isinstance(entry, str):
                md5 = entry

            if not md5:
                logger.warning(f"Kein MD5 für Datei {name} gefunden – übersprungen")
                continue

            local_target = folder_path / name
            all_matches = list(base_dir.rglob(name))
            logger.debug(f"Gefundene Matches für {name}: {[str(p) for p in all_matches]}")

            best_match = None
            for match in all_matches:
                if match.resolve() != local_target.resolve() and match.parent != local_target.parent:
                    best_match = match
                    break

            if local_target.exists():
                logger.info(f"Ziel existiert bereits: {local_target}")
            elif best_match:
                logger.info(f"Würde verschieben: {best_match} → {local_target}")
            else:
                logger.info(f"Kein Match für {name}, würde ggf. herunterladen (id: {file_id})")

            progress["value"] = int((count / total) * 100)

        # Optional: spätere Speicherung von gallery_hashes
        # with open(base_dir / "gallery_hashes_simuliert.json", "w", encoding="utf-8") as f:
        #     json.dump(gallery_hashes, f, indent=2)

        progress["value"] = 100
    except Exception as e:
        logger.error(f"Fehler bei map_gdrive_to_local (mit Fortschritt): {e}")
        progress["value"] = 100
    # TODO: Implementiere den eigentlichen Ablauf hier


def download_file(service, file_id, local_path):
    request = service.files().get_media(fileId=file_id)
    with open(local_path, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()


def local():
    global service
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../../secrets/innate-setup-454010-i9-f92b1b6a1c44.json"


if __name__ == "__main__":
    local()

    dataset = "gcp_billing_export_n8n"
    table = "gcp_billing_export_resource_v1_01003C_0EEFF2_E60D9D"
    start = "2025-05-01"
    end = "2025-05-31"

    print(get_monthly_costs(dataset, table, start, end))
