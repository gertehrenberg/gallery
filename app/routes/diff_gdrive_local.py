# Refactored diff_gdrive_local.py with full logging
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from .cleanup_local import local_list_folder, compute_md5_file
from ..config import Settings
from ..config_gdrive import folder_id_by_name
from ..routes.auth import load_drive_service
from ..utils.logger_config import setup_logger

VERSION = 201
logger = setup_logger(__name__)
logger.info(f"🟦 Starte diff_gdrive_local_refactor.py v{VERSION}")

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))

_DRIVE = None


def get_drive():
    global _DRIVE
    if _DRIVE is None:
        _DRIVE = load_drive_service()
    return _DRIVE


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

# GLOBAL INDEX
global SCAN_CACHE
SCAN_CACHE = {"categories": [], "invalid_md5": []}
GLOBAL_MD5_INDEX = {}  # md5 -> {"local": [...], "gdrive": [...]}


async def gdrive_list_folder(folder_name: str):
    folder_id = folder_id_by_name(folder_name)
    if not folder_id:
        logger.warning(f"⚠️ Kein Folder ID für Kategorie {folder_name}")
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

    while True:
        page += 1
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

    logger.info(f"📁 GDrive Folder {folder_name}: {len(files)} Dateien")
    return files


async def find_case_duplicates(folder_name: str, idx: int, total: int):
    logger.info(f"🔍 Scanne Kategorie: {folder_name}")

    # Fortschritt
    await set_progress(f"Kategorie {idx + 1}/{total}: {folder_name}", int((idx / total) * 70))

    # GDRIVE
    gfiles = await gdrive_list_folder(folder_name)
    g_insert_before = sum(len(v.get("gdrive", [])) for v in GLOBAL_MD5_INDEX.values())

    for f in gfiles:
        if f["name"].lower().endswith(".json"):
            continue
        md5 = f.get("md5Checksum")
        if not md5:
            continue

        folder_id = (f.get("parents") or ["?"])[0]

        GLOBAL_MD5_INDEX.setdefault(md5, {"local": [], "gdrive": []})
        GLOBAL_MD5_INDEX[md5]["gdrive"].append({
            "folder": folder_name,
            "folder_id": folder_id,
            "name": f["name"],
            "id": f["id"],
        })

    g_insert_after = sum(len(v.get("gdrive", [])) for v in GLOBAL_MD5_INDEX.values())
    logger.info(f"📥 GDRIVE Insert: vorher={g_insert_before}, nachher={g_insert_after}")

    # LOCAL
    lfiles = await local_list_folder(folder_name)
    l_insert_before = sum(len(v.get("local", [])) for v in GLOBAL_MD5_INDEX.values())

    loop = asyncio.get_running_loop()

    for lf in lfiles:
        if lf["name"].lower().endswith(".json"):
            continue
        md5 = await loop.run_in_executor(EXECUTOR, compute_md5_file, lf["path"])

        GLOBAL_MD5_INDEX.setdefault(md5, {"local": [], "gdrive": []})
        GLOBAL_MD5_INDEX[md5]["local"].append({
            "folder": folder_name,
            "path": lf["path"],
            "name": lf["name"],
        })

    l_insert_after = sum(len(v.get("local", [])) for v in GLOBAL_MD5_INDEX.values())
    logger.info(f"📥 LOCAL Insert: vorher={l_insert_before}, nachher={l_insert_after}")

    return {"folder": folder_name, "results": []}


async def run_full_scan():
    global SCAN_CACHE, GLOBAL_MD5_INDEX

    reset_progress()
    GLOBAL_MD5_INDEX = {}

    categories = [c["key"] for c in Settings.kategorien() if c["key"] != "XXXX"]
    total = len(categories)

    out = []

    for idx, cat in enumerate(categories):
        result = await find_case_duplicates(cat, idx, total)
        out.append(result)

    # MD5 VALIDIERUNG
    invalid_md5 = []

    for md5, entry in GLOBAL_MD5_INDEX.items():
        lc = len(entry["local"])
        gc = len(entry["gdrive"])

        if lc != 1 or gc != 1:
            invalid_md5.append({
                "md5": md5,
                "local": entry["local"],
                "gdrive": entry["gdrive"],
                "status": f"{lc}x local, {gc}x gdrive",
            })

    logger.info(f"❗ Ungueltige MD5 Eintraege: {len(invalid_md5)}")
    logger.info(f"📊 Gesamtindex: {len(GLOBAL_MD5_INDEX)} MD5-Hashes")

    SCAN_CACHE = {"categories": out, "invalid_md5": invalid_md5}
    await set_progress("Fertig", 100, "Fertig", 100)
    logger.info("🟢 Globaler MD5-Scan abgeschlossen")


# ROUTES
@router.get("/diff_gdrive_local", response_class=HTMLResponse)
async def diff_gdrive_local(request: Request):
    return templates.TemplateResponse(
        "diff_gdrive_local.j2",
        {
            "request": request,
            "categories": SCAN_CACHE.get("categories", []),
            "invalid_md5": SCAN_CACHE.get("invalid_md5", []),
            "version": VERSION,
        },
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
