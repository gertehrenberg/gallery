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
CROSSFOLDER_CACHE = None     # optionaler Cache (analog zu cleanup)
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

    # Nur MD5 mit mehreren Ordnern
    results = []
    for md5, flist in md5_groups.items():
        folders = {f["folder"] for f in flist}
        if len(folders) > 1:
            results.append({
                "md5": md5,
                "files": flist
            })

    global CROSSFOLDER_RESULTS, CROSSFOLDER_CACHE
    CROSSFOLDER_RESULTS = results
    CROSSFOLDER_CACHE = results.copy()   # Cache speichern

    await set_progress("Fertig", 100, "MD5 analysiert", 20)


# ============================================================================
# START
# ============================================================================

@router.post("/gdrive_crossduplicates_start")
async def gdrive_crossduplicates_start():
    reset_progress()
    global CROSSFOLDER_RESULTS
    CROSSFOLDER_RESULTS = []

    asyncio.create_task(load_drive_folders_and_md5())
    return JSONResponse({"started": True})


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
    global CROSSFOLDER_RESULTS, CROSSFOLDER_CACHE
    CROSSFOLDER_RESULTS = []
    CROSSFOLDER_CACHE = None
    reset_progress()
    return RedirectResponse("/gallery/gdrive_crossduplicates")


# ============================================================================
# DELETE – **Dry-Run + Cache-Update**
# ============================================================================

@router.post("/gdrive_crossduplicates_delete", response_class=HTMLResponse)
async def gdrive_crossduplicates_delete(request: Request,
                                        delete_ids: list[str] = Form(default=[])):
    """
    NICHT LÖSCHEN — nur simulieren.
    ABER: Aus Tabelle und Cache entfernen.
    """
    global CROSSFOLDER_RESULTS, CROSSFOLDER_CACHE

    delete_ids = set(delete_ids)

    def filter_md5_groups(groups):
        new_groups = []
        for g in groups:
            new_files = [f for f in g["files"] if f["id"] not in delete_ids]
            if new_files:
                new_groups.append({"md5": g["md5"], "files": new_files})
        return new_groups

    CROSSFOLDER_RESULTS = filter_md5_groups(CROSSFOLDER_RESULTS)

    if CROSSFOLDER_CACHE:
        CROSSFOLDER_CACHE = filter_md5_groups(CROSSFOLDER_CACHE)

    return templates.TemplateResponse(
        "gdrive_crossduplicates_done.j2",
        {
            "request": request,
            "deleted": list(delete_ids),
            "errors": [],
            "dry_run": True
        }
    )
