import os

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..config import Settings
from ..config_gdrive import folder_id_by_name
from ..routes.auth import load_drive_service
from ..utils.logger_config import setup_logger

logger = setup_logger(__name__)

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "../templates")
)


# =====================================================================
#  GOOGLE DRIVE: ALLE DATEIEN EINES ORDNER LADEN (KEINE UNTERORDNER)
# =====================================================================
async def gdrive_list_folder(service, folder_id: str):
    query = f"'{folder_id}' in parents and trashed = false"

    files = []
    page = None

    while True:
        resp = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id,name,md5Checksum,size)",
            pageToken=page,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        files.extend(resp.get("files", []))
        page = resp.get("nextPageToken")

        if not page:
            break

    return files


# =====================================================================
#  FINDE CASE-DUPLIKATE (Uppercase löschen → lowercase behalten)
# =====================================================================
async def find_case_duplicates(service, folder_name: str):
    folder_id = folder_id_by_name(folder_name)

    if not folder_id:
        logger.warning(f"GDrive: Ordner '{folder_name}' wurde NICHT gefunden!")
        return {
            "folder": folder_name,
            "folder_id": None,
            "num_results": 0,
            "results": [],
        }

    files = await gdrive_list_folder(service, folder_id)
    logger.info(f"{folder_name}: {len(files)} Dateien")

    md5_groups = {}
    name_groups = {}

    # === gruppieren ===
    for f in files:
        name = f["name"]
        size = int(f.get("size", 0)) if f.get("size") else None
        md5 = f.get("md5Checksum")

        if md5:
            md5_groups.setdefault(md5, []).append(f)
        else:
            name_groups.setdefault(name.lower(), []).append(f)

    results = []

    # === 1. MD5-basierte Duplikate ===
    for md5, group in md5_groups.items():
        if len(group) < 2:
            continue

        lowercase_versions = [g for g in group if g["name"] == g["name"].lower()]
        if not lowercase_versions:
            # Keine lowercase-Version → nix löschen
            continue

        keep = lowercase_versions[0]

        for f in group:
            if f["id"] == keep["id"]:
                continue
            if f["name"] != f["name"].lower():
                results.append({
                    "folder": folder_name,
                    "md5": md5,
                    "delete": f["name"],
                    "delete_id": f["id"],
                    "keep": keep["name"],
                    "keep_id": keep["id"],
                })

    # === 2. Name/Größe Fallback für Dateien ohne MD5 ===
    for key, group in name_groups.items():
        if len(group) < 2:
            continue

        sizes = {g.get("size") for g in group}
        if len(sizes) != 1:
            continue  # Verschiedene Dateien

        lowercase_versions = [g for g in group if g["name"] == g["name"].lower()]
        if not lowercase_versions:
            continue

        keep = lowercase_versions[0]

        for f in group:
            if f["id"] == keep["id"]:
                continue
            if f["name"] != f["name"].lower():
                results.append({
                    "folder": folder_name,
                    "md5": "(no md5)",
                    "delete": f["name"],
                    "delete_id": f["id"],
                    "keep": keep["name"],
                    "keep_id": keep["id"],
                })

    return {
        "folder": folder_name,
        "folder_id": folder_id,
        "num_results": len(results),
        "results": results,
    }


# =====================================================================
#  FASTAPI ENDPOINT: Pro Kategorie GENAU EINEN Ordner scannen
# =====================================================================
@router.get("/gdrive_cleanup", response_class=HTMLResponse)
async def gdrive_cleanup(request: Request):

    service = load_drive_service()
    categories_output = []

    for k in Settings.kategorien():
        key = k["key"]
        label = k["label"]
        icon = k["icon"]

        if(key == "real"):
            continue

        logger.info(f"Scanne GDrive-Ordner für Kategorie: {label} ({key})")

        folder_results = await find_case_duplicates(service, key)

        categories_output.append({
            "key": key,
            "label": label,
            "icon": icon,
            "folder_id": folder_results["folder_id"],
            "num_results": folder_results["num_results"],
            "results": folder_results["results"],
        })

    return templates.TemplateResponse(
        "gdrive_cleanup.j2",
        {
            "request": request,
            "categories": categories_output,
            "dry_run": True,
        }
    )
