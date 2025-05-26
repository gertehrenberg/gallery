import calendar
import json
import logging
import os
import shutil
from datetime import datetime, date
from pathlib import Path
from typing import Dict

from fastapi import APIRouter, Request
from fastapi import Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from google.cloud import bigquery
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from starlette.responses import JSONResponse

from app.config import Settings
from app.config_gdrive import folder_id_by_name
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
        if direction == "gdrive_from_lokal":
            button_text = f'Passe für "{kat["label"]}" lokal so an wie GDrive'
        elif direction == "lokal_from_gdrive":
            button_text = f'Passe für "{kat["label"]}" GDrive so an wie lokal'
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

    if not kat or direction not in ("gdrive_from_lokal", "lokal_from_gdrive"):
        return JSONResponse(content={"error": "Ungültiger Parameter"}, status_code=400)

    def runner():
        progress["value"] = 0
        try:
            if direction == "gdrive_from_lokal":
                gdrive_from_lokal(folder)
            elif direction == "lokal_from_gdrive":
                lokal_from_gdrive(folder)
        finally:
            progress["value"] = 100

    threading.Thread(target=runner).start()
    return {"started": True}


def gdrive_from_lokal(folder_name: str):
    logger.info(f"gdrive_from_lokal: {folder_name}")

    cache_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)

    global_gdrive_hashes = load_all_gdrive_hashes(cache_dir)
    service = load_drive_service()
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
        processed_files = 0
        progress["value"] = 0
        total_files = len(local_hashes)

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
                            processed_files += 1
                            progress["value"] = int((processed_files / total_files) * 100)
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

            processed_files += 1
            if total_files > 0:
                progress["value"] = int((processed_files / total_files) * 100)

        if updated:
            with gdrive_hashfile.open("w", encoding="utf-8") as f:
                json.dump(gdrive_hashes, f, indent=2)
            print(f"[↑] hashes.json aktualisiert für Ordner {folder}")

    progress["value"] = 100


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


def lokal_from_gdrive(folder_name: str):
    logger.info(f"lokal_from_gdrive: {folder_name}")
    try:
        progress["value"] = 0
        count = 0
        base_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
        service = load_drive_service()
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
            progress["value"] = 100
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

            progress["value"] = int((count / total) * 100)

        gallery_hash_path = base_dir / Settings.GALLERY_HASH_FILE
        with open(gallery_hash_path, "w", encoding="utf-8") as f:
            json.dump(gallery_hashes, f, indent=2)

        progress["value"] = 100
    except Exception as e:
        logger.error(f"Fehler bei map_gdrive_to_local (mit Fortschritt): {e}")
        progress["value"] = 100


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
