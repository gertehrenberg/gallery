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

CROSSFOLDER_RESULTS = []
BACKGROUND_TASK = None

CROSSFOLDER_PROGRESS = {
    "status": "Bereit",
    "progress": 0,
    "details": {"status": "-", "progress": 0}
}

PROGRESS_LOCK = asyncio.Lock()


# ============================================================================
# Fortschritt setzen (threadsafe)
# ============================================================================

async def set_progress(status, progress, detail_status=None, detail_progress=None):
    async with PROGRESS_LOCK:
        CROSSFOLDER_PROGRESS["status"] = status
        CROSSFOLDER_PROGRESS["progress"] = progress

        if detail_status is not None:
            CROSSFOLDER_PROGRESS["details"]["status"] = detail_status

        if detail_progress is not None:
            # Detailbalken max = 20
            CROSSFOLDER_PROGRESS["details"]["progress"] = min(20, detail_progress)


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
            fields="nextPageToken, files(id,name,mimeType,size)",
            pageSize=1000,
            pageToken=page,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        all_files.extend(resp.get("files", []))
        page = resp.get("nextPageToken")

        if not page:
            break

        await asyncio.sleep(0.05)

    return all_files


# ============================================================================
# Alle Ordner laden
# ============================================================================

async def load_drive_folders_and_files():
    await set_progress("Verbinde mit Google Drive…", 5, "Initialisiere…", 0)
    await asyncio.sleep(0.1)

    service = load_drive_service()

    categories = [k["key"] for k in Settings.kategorien() if k["key"] != "real"]
    total = len(categories)

    results = []

    for idx, name in enumerate(categories, start=1):

        await set_progress(
            f"Lese Ordner '{name}'…",
            int(idx / total * 100),
            "Hole Folder-ID…",
            0
        )

        folder_id = folder_id_by_name(name)

        if not folder_id:
            results.append({"folder": name, "folder_id": None, "files": []})
            continue

        files = await load_drive_files_for_folder(service, name, folder_id)

        results.append({
            "folder": name,
            "folder_id": folder_id,
            "files": files
        })

    global CROSSFOLDER_RESULTS
    CROSSFOLDER_RESULTS = results

    await set_progress("Fertig", 100, "Ordner + Dateien geladen", 20)


# ============================================================================
# START – mit TASK-SCHUTZ!
# ============================================================================

@router.post("/gdrive_crossduplicates_start")
async def gdrive_crossduplicates_start():
    global BACKGROUND_TASK, CROSSFOLDER_RESULTS

    # Falls ein Task läuft → nicht neu starten, nur weiter pollen
    if BACKGROUND_TASK and not BACKGROUND_TASK.done():
        return JSONResponse({"started": False, "running": True})

    reset_progress()
    CROSSFOLDER_RESULTS = []

    BACKGROUND_TASK = asyncio.create_task(load_drive_folders_and_files())

    return JSONResponse({"started": True, "running": False})


# ============================================================================
# PROGRESS
# ============================================================================

@router.get("/gdrive_crossduplicates_progress")
async def gdrive_crossduplicates_progress():
    return JSONResponse(CROSSFOLDER_PROGRESS)


# ============================================================================
# PAGE
# ============================================================================

@router.get("/gdrive_crossduplicates", response_class=HTMLResponse)
async def gdrive_crossduplicates(request: Request):
    return templates.TemplateResponse(
        "gdrive_crossduplicates.j2",
        {"request": request, "results": CROSSFOLDER_RESULTS}
    )


# ============================================================================
# RELOAD
# ============================================================================

@router.get("/gdrive_crossduplicates_reload")
async def gdrive_crossduplicates_reload():
    global CROSSFOLDER_RESULTS, BACKGROUND_TASK
    CROSSFOLDER_RESULTS = []
    BACKGROUND_TASK = None
    reset_progress()
    return RedirectResponse("/gallery/gdrive_crossduplicates")


# ============================================================================
# DELETE (Simulation)
# ============================================================================

@router.post("/gdrive_crossduplicates_delete", response_class=HTMLResponse)
async def gdrive_crossduplicates_delete(request: Request,
                                        delete_ids: list[str] = Form(default=[])):
    deleted = delete_ids
    errors = []
    return templates.TemplateResponse(
        "gdrive_crossduplicates_done.j2",
        {"request": request, "deleted": deleted, "errors": errors}
    )
