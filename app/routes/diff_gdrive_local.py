import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from .cleanup_local import local_list_folder, compute_md5_file
from ..config import Settings
from ..config_gdrive import folder_name_by_id, folder_id_by_name
from ..routes.auth import load_drive_service
from ..utils.logger_config import setup_logger

VERSION = 168
logger = setup_logger(__name__)
logger.info(f"🟦 Starte diff_gdrive_local.py v{VERSION}")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "../templates")
)

_DRIVE = None


def get_drive():
    """Google Drive Service lazy laden"""
    global _DRIVE
    if _DRIVE is None:
        _DRIVE = load_drive_service()
    return _DRIVE


# ======================================================================
# PROGRESS
# ======================================================================

PROGRESS = {"status": "Bereit", "progress": 0, "details": {"status": "Bereit", "progress": 0}}
PROGRESS_LOCK = asyncio.Lock()


async def set_progress(status, progress, detail_status=None, detail_progress=None):
    async with PROGRESS_LOCK:
        PROGRESS["status"] = status
        PROGRESS["progress"] = progress
        if detail_status is not None:
            PROGRESS["details"]["status"] = detail_status
        if detail_progress is not None:
            PROGRESS["details"]["progress"] = detail_progress


async def set_progress_detail(detail_status=None, detail_progress=None):
    async with PROGRESS_LOCK:
        if detail_status is not None:
            PROGRESS["details"]["status"] = detail_status
        if detail_progress is not None:
            PROGRESS["details"]["progress"] = detail_progress

def reset_progress():
    PROGRESS["status"] = "Bereit"
    PROGRESS["progress"] = 0
    PROGRESS["details"] = {"status": "Bereit", "progress": 0}


EXECUTOR = ThreadPoolExecutor(max_workers=8)

# ======================================================================
# GLOBALER GDRIVE-MD5-INDEX
# ======================================================================

GDRIVE_MD5_INDEX = {}  # md5 → [ {folder, name, id} ]


# ======================================================================
# ORDNER-SCAN
# ======================================================================

async def find_case_duplicates(folder_name: str, idx: int, total: int, debug: bool = False):
    logger.info(f"🔍 Scanne Kategorie: {folder_name}")

    files_gdrive = await gdrive_list_folder(folder_name)
    files_local = await local_list_folder(folder_name)

    gdrive_map = {
        f["name"]: {
            "id": f["id"],
            "md5": f.get("md5Checksum"),
            "raw": f
        }
        for f in files_gdrive
    }

    local_map = {
        f["name"]: {
            "path": f["path"],
            "raw": f
        }
        for f in files_local
    }

    results = []
    loop = asyncio.get_running_loop()

    total_files = len(gdrive_map)
    processed = 0

    await set_progress(
        f"Verarbeite Dateien in GDrive Ordner: {folder_name}",
        min(PROGRESS["progress"] + 1, 99),
        "Bereit",
        0
    )

    # ---------------------------------------------------
    # 1) Dateien, die im GDrive existieren
    # ---------------------------------------------------
    for name, gf in gdrive_map.items():
        processed += 1

        await set_progress_detail(
            f"{processed}/{total_files}",
            int((processed / max(total_files, 1)) * 100)
        )

        g_md5 = gf["md5"]

        # → nur GDrive
        if name not in local_map:
            results.append({
                "folder": folder_name,
                "filename": name,
                "gdrive_id": gf["id"],
                "local_path": "",
                "type": "nur_gdrive",
            })
            continue

        # → existiert lokal → MD5 berechnen
        local_path = local_map[name]["path"]

        l_md5 = await loop.run_in_executor(
            EXECUTOR,
            compute_md5_file,
            local_path
        )

        if g_md5 == l_md5:
            continue

        # Verschiedene MD5 → Unterschied
        results.append({
            "folder": folder_name,
            "filename": name,
            "gdrive_id": gf["id"],
            "local_path": local_path,
            "type": "md5_unterschied",
        })

    # ---------------------------------------------------
    # 2) Dateien, die nur lokal existieren
    # ---------------------------------------------------
    for name, lf in local_map.items():
        if name in gdrive_map:
            continue

        local_path = lf["path"]

        # lokalen MD5 holen
        local_md5 = await loop.run_in_executor(
            EXECUTOR,
            compute_md5_file,
            local_path
        )

        # globalen GDrive-Index abfragen
        md5_match = GDRIVE_MD5_INDEX.get(local_md5)

        if md5_match:
            # Copy irgendwo im Drive
            results.append({
                "folder": folder_name,
                "filename": name,
                "gdrive_id": md5_match[0]["id"],
                "local_path": local_path,
                "type": "nur_local_md5_match",
                "match_info": md5_match,
            })
        else:
            # echte lokale Datei
            results.append({
                "folder": folder_name,
                "filename": name,
                "gdrive_id": "",
                "local_path": local_path,
                "type": "nur_local",
            })

    return {
        "key": folder_name,
        "label": folder_name,
        "folder_id": folder_name,
        "results": results,
    }


async def gdrive_list_folder(folder_name: str):
    folder_id = folder_id_by_name(folder_name)
    if not folder_id:
        return []

    service = get_drive()
    query = (
        f"'{folder_id}' in parents "
        f"and trashed = false "
        f"and mimeType != 'application/vnd.google-apps.folder' "
        f"and mimeType != 'application/vnd.google-apps.shortcut'"
    )

    files = []
    token = None
    page = 0

    await set_progress(
        f"Lese GDrive Ordner: {folder_name}",
        min(PROGRESS["progress"] + 1, 99),
        "Bereit",
        0
    )

    while True:
        page += 1

        await set_progress_detail(
            f"Seite {page}",
            min(page*4, 100)
        )

        resp = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id,name,md5Checksum,size,parents)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=token,
            pageSize=Settings.PAGESIZE,
        ).execute()

        files.extend(resp.get("files", []))
        token = resp.get("nextPageToken")

        if not token:
            break

        await asyncio.sleep(0.05)

    return files


SCAN_CACHE = None


async def run_full_scan():
    global SCAN_CACHE
    global GDRIVE_MD5_INDEX

    reset_progress()

    categories = [c for c in Settings.kategorien() if c["key"] != "real"]
    total = len(categories)

    logger.info("🔄 Baue globalen GDrive-MD5-Index auf...")

    GDRIVE_MD5_INDEX = {}
    all_files = []

    # ---- alle GDrive-Dateien einsammeln
    for cat in categories:
        flist = await gdrive_list_folder(cat["key"])
        all_files.extend(flist)

    # ---- MD5-Index aufbauen
    for f in all_files:
        md5 = f.get("md5Checksum")
        if not md5:
            continue

        # den Ordner als parent-ID setzen
        folder_id = (f.get("parents") or ["?"])[0]

        GDRIVE_MD5_INDEX.setdefault(md5, []).append({
            "folder": folder_name_by_id(folder_id),
            "folder_id": folder_id,
            "name": f["name"],
            "id": f["id"],
        })

    logger.info(f"🟢 MD5-Index fertig ({len(GDRIVE_MD5_INDEX)} eindeutige Hashes)")

    # ---- jetzt pro Kategorie den Scan durchführen
    out = []
    idx = 0

    for cat in categories:
        r = await find_case_duplicates(cat["key"], idx, total, False)
        out.append(r)
        idx += 1

    SCAN_CACHE = out
    await set_progress(
        "Fertig",
        100,
        "Fertig",
        100
    )
    logger.info("🟢 Scan fertig")


# ======================================================================
# ROUTES
# ======================================================================

@router.get("/diff_gdrive_local", response_class=HTMLResponse)
async def diff_gdrive_local(request: Request):
    return templates.TemplateResponse(
        "diff_gdrive_local.j2",
        {
            "request": request,
            "categories": SCAN_CACHE,
            "version": VERSION,
        }
    )


@router.post("/diff_gdrive_local_start")
async def diff_gdrive_local_start():
    reset_progress()
    asyncio.get_running_loop().create_task(run_full_scan())
    return JSONResponse({"started": True})


@router.get("/diff_gdrive_local_progress")
async def diff_gdrive_local_progress():
    return JSONResponse(PROGRESS)


@router.get("/diff_gdrive_local_reload")
async def diff_gdrive_local_reload():
    return RedirectResponse("/gallery/diff_gdrive_local")


# ======================================================================
# (Legacy) DELETE ROUTE — leer, damit Template nicht crasht
# ======================================================================

@router.post("/diff_gdrive_local_delete", response_class=HTMLResponse)
async def diff_gdrive_local_delete(
        request: Request,
        delete_ids: list[str] = Form(default=[]),
        lower_ids: list[str] = Form(default=[]),
):
    return templates.TemplateResponse(
        "cleanup_done.j2",
        {
            "request": request,
            "version": VERSION,
            "deleted": [],
            "renamed": [],
            "errors": [],
        }
    )


# ======================================================================
# TEST
# ======================================================================

@router.get("/diff_gdrive_local_test")
async def diff_gdrive_local_test():
    return RedirectResponse("/gallery/diff_gdrive_local", status_code=302)
