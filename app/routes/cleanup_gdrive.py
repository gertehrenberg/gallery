import os
import asyncio
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
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

# ============================================================
#  GLOBALER CACHE
# ============================================================
GDRIVE_CLEANUP_CACHE = None

# ============================================================
#  GOOGLE DRIVE SERVICE
# ============================================================
DRIVE_SERVICE = None
def get_drive_service():
    global DRIVE_SERVICE
    if DRIVE_SERVICE is None:
        DRIVE_SERVICE = load_drive_service()
    return DRIVE_SERVICE


# ============================================================
#  FORTSCHRITT
# ============================================================
CLEANUP_PROGRESS = {
    "status": "Bereit",
    "progress": 0,
    "details": {"status": "-", "progress": 0}
}
PROGRESS_LOCK = asyncio.Lock()


async def set_progress(status, progress, detail_status=None, detail_progress=None):
    async with PROGRESS_LOCK:
        CLEANUP_PROGRESS["status"] = status
        CLEANUP_PROGRESS["progress"] = progress
        if detail_status is not None:
            CLEANUP_PROGRESS["details"]["status"] = detail_status
        if detail_progress is not None:
            CLEANUP_PROGRESS["details"]["progress"] = min(detail_progress, 20)


def reset_progress():
    CLEANUP_PROGRESS["status"] = "Bereit"
    CLEANUP_PROGRESS["progress"] = 0
    CLEANUP_PROGRESS["details"] = {"status": "-", "progress": 0}


# =====================================================================
#  GOOGLE DRIVE: ALLE DATEIEN EINES ORDNER LADEN
# =====================================================================
async def gdrive_list_folder(service, folder_id: str):
    query = f"'{folder_id}' in parents and trashed = false"

    files = []
    page = None
    page_counter = 0

    while True:
        page_counter += 1

        await set_progress(
            CLEANUP_PROGRESS["status"],
            CLEANUP_PROGRESS["progress"],
            f"Seite {page_counter}",
            page_counter
        )

        resp = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id,name,md5Checksum,size)",
            pageSize=Settings.PAGESIZE,
            pageToken=page,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()

        files.extend(resp.get("files", []))
        page = resp.get("nextPageToken")
        if not page:
            break

        await asyncio.sleep(0.1)

    return files


# =====================================================================
#  FINDE CASE-DUPLIKATE
# =====================================================================
async def find_case_duplicates(service, folder_name: str):
    folder_id = folder_id_by_name(folder_name)

    if not folder_id:
        return {
            "folder": folder_name,
            "folder_id": None,
            "num_results": 0,
            "results": [],
        }

    files = await gdrive_list_folder(service, folder_id)

    md5_groups = {}
    name_groups = {}

    for f in files:
        name = f["name"]
        size = int(f.get("size", 0)) if f.get("size") else None
        md5 = f.get("md5Checksum")

        if md5:
            md5_groups.setdefault(md5, []).append(f)
        else:
            name_groups.setdefault(name.lower(), []).append(f)

    results = []

    # MD5-basierte Duplikate
    for md5, group in md5_groups.items():
        if len(group) < 2:
            continue

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
                    "md5": md5,
                    "delete": f["name"],
                    "delete_id": f["id"],
                    "keep": keep["name"],
                    "keep_id": keep["id"],
                })

    # Namensfallback
    for key, group in name_groups.items():
        if len(group) < 2:
            continue

        sizes = {g.get("size") for g in group}
        if len(sizes) != 1:
            continue

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
#  WORKER → kompletter Scan mit Fortschritt
# =====================================================================
async def scan_cleanup():
    global GDRIVE_CLEANUP_CACHE

    reset_progress()
    await set_progress("Verbinde mit Google Drive…", 5, "Initialisiere…", 0)
    await asyncio.sleep(0.2)

    service = get_drive_service()
    categories = Settings.kategorien()
    total = len(categories)

    output = []

    for idx, cat in enumerate(categories, start=1):
        key = cat["key"]
        label = cat["label"]
        icon = cat["icon"]

        main_progress = int(idx / total * 100)

        await set_progress(f"Scanne Ordner '{label}'…", main_progress, "Lese Dateien…", 0)

        res = await find_case_duplicates(service, key)

        if res["num_results"] > 0:
            output.append({
                "key": key,
                "label": label,
                "icon": icon,
                "folder_id": res["folder_id"],
                "num_results": res["num_results"],
                "results": res["results"],
            })

    GDRIVE_CLEANUP_CACHE = output
    await set_progress("Fertig", 100, "Analyse abgeschlossen", 20)


# =====================================================================
#  HTML SEITE (scan wird nicht hier gestartet)
# =====================================================================
@router.get("/cleanup_gdrive", response_class=HTMLResponse)
async def cleanup_gdrive(request: Request):
    return templates.TemplateResponse(
        "cleanup_gdrive.j2",
        {
            "request": request,
            "categories": GDRIVE_CLEANUP_CACHE,
            "dry_run": True,
        }
    )


# =====================================================================
#  START-SCAN
# =====================================================================
@router.post("/cleanup_gdrive_start")
async def cleanup_gdrive_start():
    reset_progress()
    asyncio.create_task(scan_cleanup())
    return JSONResponse({"started": True})


# =====================================================================
#  PROGRESS
# =====================================================================
@router.get("/cleanup_gdrive_progress")
async def cleanup_gdrive_progress():
    return JSONResponse(CLEANUP_PROGRESS)


# =====================================================================
#  RELOAD
# =====================================================================
@router.get("/cleanup_gdrive_reload")
async def cleanup_gdrive_reload():
    global GDRIVE_CLEANUP_CACHE
    GDRIVE_CLEANUP_CACHE = None
    reset_progress()
    return RedirectResponse("/gallery/cleanup_gdrive", status_code=302)


# =====================================================================
#  DELETE
# =====================================================================
@router.post("/cleanup_gdrive_delete", response_class=HTMLResponse)
async def cleanup_gdrive_delete(request: Request,
                                delete_ids: list[str] = Form(default=[])):
    global GDRIVE_CLEANUP_CACHE

    service = get_drive_service()
    deleted = []
    errors = []

    for file_id in delete_ids:
        try:
            service.files().delete(fileId=file_id).execute()
            deleted.append(file_id)
        except Exception as e:
            errors.append({"id": file_id, "error": str(e)})

    # Cache aktualisieren
    if GDRIVE_CLEANUP_CACHE:
        for cat in GDRIVE_CLEANUP_CACHE:
            cat["results"] = [
                r for r in cat["results"]
                if r["delete_id"] not in deleted
            ]
            cat["num_results"] = len(cat["results"])

    return templates.TemplateResponse(
        "cleanup_gdrive_done.j2",
        {
            "request": request,
            "deleted": deleted,
            "errors": errors,
        }
    )
