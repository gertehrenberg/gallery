import os
import asyncio
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..utils.logger_config import setup_logger
from ..config import Settings
from ..routes.auth import load_drive_service
from ..config_gdrive import folder_id_by_name

logger = setup_logger(__name__)
router = APIRouter()

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "../templates")
)

# ============================================================================
# Globale Variablen
# ============================================================================

CROSSFOLDER_RESULTS = []     # [{md5, files:[...]}]
CROSSFOLDER_PROGRESS = {
    "status": "Bereit",
    "progress": 0,
    "details": {"status": "-", "progress": 0}
}
PROGRESS_LOCK = asyncio.Lock()


# ============================================================================
# Fortschritt (threadsafe)
# ============================================================================

async def set_progress(status, progress, detail_status=None, detail_progress=None):
    async with PROGRESS_LOCK:
        CROSSFOLDER_PROGRESS["status"] = status
        CROSSFOLDER_PROGRESS["progress"] = progress
        if detail_status is not None:
            CROSSFOLDER_PROGRESS["details"]["status"] = detail_status
        if detail_progress is not None:
            CROSSFOLDER_PROGRESS["details"]["progress"] = min(detail_progress, 20)


def reset_progress():
    CROSSFOLDER_PROGRESS["status"] = "Bereit"
    CROSSFOLDER_PROGRESS["progress"] = 0
    CROSSFOLDER_PROGRESS["details"] = {"status": "-", "progress": 0}


# ============================================================================
# Dateien eines Ordners laden
# ============================================================================

async def load_drive_files_for_folder(service, folder_name, folder_id):
    query = f"'{folder_id}' in parents and trashed = false"

    all_files = []
    page = None
    page_idx = 0

    while True:
        page_idx += 1

        await set_progress(
            f"Ordner „{folder_name}“",
            CROSSFOLDER_PROGRESS["progress"],
            f"Seite {page_idx}",
            page_idx
        )

        resp = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id,name,md5Checksum,size)",
            pageSize=1000,
            pageToken=page,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        all_files.extend(resp.get("files", []))

        page = resp.get("nextPageToken")
        if not page:
            break

        await asyncio.sleep(0.1)

    return all_files


# ============================================================================
# Ordner + Dateien lesen → MD5 gruppieren
# ============================================================================

async def load_drive_folders_and_md5():
    await set_progress("Verbinde mit Google Drive…", 5, "Initialisiere…", 0)
    await asyncio.sleep(0.2)

    service = load_drive_service()

    categories = [k for k in Settings.kategorien() if k["key"] != "real"]
    total = len(categories)

    md5_groups = {}

    for idx, cat in enumerate(categories, start=1):
        name = cat["key"]
        folder_id = folder_id_by_name(name)
        main_progress = int(idx / total * 100)

        await set_progress(
            f"Lese Ordner '{name}'…",
            main_progress,
            "Hole Folder-ID…",
            0
        )

        if not folder_id:
            continue

        files = await load_drive_files_for_folder(service, name, folder_id)

        for f in files:
            md5 = f.get("md5Checksum")
            if not md5:
                continue

            md5_groups.setdefault(md5, []).append({
                "folder": name,
                "label": cat["label"],
                "icon": cat["icon"],
                "id": f["id"],
                "name": f["name"]
            })

    # ============================================================================
    # FIX: Cross-Folder Ergebnisstruktur erweitern
    # ============================================================================
    results = []
    for md5, flist in md5_groups.items():
        folders = {f["folder"] for f in flist}

        if len(folders) > 1:   # echte Cross-Folder-Duplikate
            results.append({
                "md5": md5,
                "folders": list(folders),   # <-- hinzugefügt
                "files": flist              # f enthält folder/label/icon
            })
    # ============================================================================

    global CROSSFOLDER_RESULTS
    CROSSFOLDER_RESULTS = results

    await set_progress("Fertig", 100, "MD5 analysiert", 20)


# ============================================================================
# Start Button
# ============================================================================

@router.post("/gdrive_crossduplicates_start")
async def gdrive_crossduplicates_start():
    reset_progress()
    global CROSSFOLDER_RESULTS
    CROSSFOLDER_RESULTS = []

    asyncio.create_task(load_drive_folders_and_md5())

    return JSONResponse({"started": True})


# ============================================================================
# Progress Endpoint
# ============================================================================

@router.get("/gdrive_crossduplicates_progress")
async def gdrive_crossduplicates_progress():
    return JSONResponse(CROSSFOLDER_PROGRESS)


# ============================================================================
# Seite anzeigen
# ============================================================================

@router.get("/gdrive_crossduplicates", response_class=HTMLResponse)
async def gdrive_crossduplicates(request: Request):
    return templates.TemplateResponse(
        "gdrive_crossduplicates.j2",
        {"request": request, "results": CROSSFOLDER_RESULTS}
    )


# ============================================================================
# Reload
# ============================================================================

@router.get("/gdrive_crossduplicates_reload")
async def gdrive_crossduplicates_reload():
    global CROSSFOLDER_RESULTS
    CROSSFOLDER_RESULTS = []
    reset_progress()
    return RedirectResponse("/gallery/gdrive_crossduplicates")


# ============================================================================
# Löschen (Simulation)
# ============================================================================

@router.post("/gdrive_crossduplicates_delete", response_class=HTMLResponse)
async def gdrive_crossduplicates_delete(request: Request,
                                        delete_ids: list[str] = Form(default=[])):
    return templates.TemplateResponse(
        "gdrive_crossduplicates_done.j2",
        {"request": request, "deleted": delete_ids, "errors": []}
    )
