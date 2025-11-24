import asyncio
import os

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

# ============================================================================
# Globale Variablen
# ============================================================================

CROSSFOLDER_RESULTS = []  # [{md5, files:[...]}]
CROSSFOLDER_CACHE = None  # optionaler Cache (analog zu cleanup)
CROSSFOLDER_PROGRESS = {
    "status": "Bereit",
    "progress": 0,
    "details": {"status": "-", "progress": 0}
}
PROGRESS_LOCK = asyncio.Lock()

# Neuer globaler Drive-Service
DRIVE_SERVICE = None


# ============================================================================
# Drive-Service nur EINMAL laden
# ============================================================================

def get_drive_service():
    global DRIVE_SERVICE
    if DRIVE_SERVICE is None:
        DRIVE_SERVICE = load_drive_service()
    return DRIVE_SERVICE


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
            pageSize=Settings.PAGESIZE,
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

    service = get_drive_service()

    categories = Settings.kategorien()

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
    CROSSFOLDER_CACHE = results.copy()  # Cache speichern

    await set_progress("Fertig", 100, "MD5 analysiert", 20)


# ============================================================================
# START
# ============================================================================

@router.post("/crossduplicates_gdrive_start")
async def crossduplicates_gdrive_start():
    reset_progress()
    global CROSSFOLDER_RESULTS
    CROSSFOLDER_RESULTS = []

    asyncio.create_task(load_drive_folders_and_md5())
    return JSONResponse({"started": True})


# ============================================================================
# PROGRESS
# ============================================================================

@router.get("/crossduplicates_gdrive_progress")
async def crossduplicates_gdrive_progress():
    return JSONResponse(CROSSFOLDER_PROGRESS)


# ============================================================================
# PAGE
# ============================================================================

@router.get("/crossduplicates_gdrive", response_class=HTMLResponse)
async def crossduplicates_gdrive(request: Request):
    return templates.TemplateResponse(
        "crossduplicates_gdrive.j2",
        {"request": request, "results": CROSSFOLDER_RESULTS}
    )


# ============================================================================
# RELOAD
# ============================================================================

@router.get("/crossduplicates_gdrive_reload")
async def crossduplicates_gdrive_reload():
    global CROSSFOLDER_RESULTS, CROSSFOLDER_CACHE
    CROSSFOLDER_RESULTS = []
    CROSSFOLDER_CACHE = None
    reset_progress()
    return RedirectResponse("/gallery/crossduplicates_gdrive")


# ============================================================================
# DELETE – echtes Löschen + Cache-Update
# ============================================================================

@router.post("/crossduplicates_gdrive_delete", response_class=HTMLResponse)
async def crossduplicates_gdrive_delete(request: Request,
                                        delete_ids: list[str] = Form(default=[])):
    global CROSSFOLDER_RESULTS, CROSSFOLDER_CACHE

    service = get_drive_service()

    deleted = []
    errors = []

    # ------------------------------
    # 1) Wirklich löschen
    # ------------------------------
    for file_id in delete_ids:
        try:
            service.files().delete(fileId=file_id).execute()
            deleted.append(file_id)
        except Exception as e:
            errors.append({"id": file_id, "error": str(e)})

    deleted_set = set(deleted)

    # ------------------------------
    # 2) MD5s herausfinden, die gelöscht wurden
    # ------------------------------
    md5_to_remove = set()

    for g in CROSSFOLDER_RESULTS:
        for f in g["files"]:
            if f["id"] in deleted_set:
                md5_to_remove.add(g["md5"])

    # ------------------------------
    # 3) Neue Liste: komplette MD5-Gruppen rauswerfen
    # ------------------------------
    def filter_groups(groups):
        return [g for g in groups if g["md5"] not in md5_to_remove]

    CROSSFOLDER_RESULTS = filter_groups(CROSSFOLDER_RESULTS)

    if CROSSFOLDER_CACHE:
        CROSSFOLDER_CACHE = filter_groups(CROSSFOLDER_CACHE)

    # ------------------------------
    # 4) Ergebnis-Seite anzeigen
    # ------------------------------
    return templates.TemplateResponse(
        "crossduplicates_gdrive_done.j2",
        {"request": request, "deleted": deleted, "errors": errors}
    )
