import asyncio
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ..config import Settings
from ..config_gdrive import sanitize_filename
from ..utils.logger_config import setup_logger

VERSION = 111
logger = setup_logger(__name__)
logger.info(f"🟦 Starte cleanup_local.py (VARIANTE C – Cache-Update) v{VERSION}")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "../templates")
)

LOCAL_BASE = Settings.IMAGE_FILE_CACHE_DIR

# ============================================================
# THREADPOOL
# ============================================================

EXECUTOR = ThreadPoolExecutor(max_workers=8)


def compute_md5_file(path: str):
    """MD5-Berechnung im ThreadPool."""
    hasher = hashlib.md5()
    try:
        with open(path, "rb") as fp:
            for chunk in iter(lambda: fp.read(1024 * 1024), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception:
        return None


# ============================================================
# PROGRESS SYSTEM
# ============================================================

PROGRESS = {
    "status": "Bereit",
    "progress": 0,
    "details": {"status": "-", "progress": 0}
}
LOCK = asyncio.Lock()


async def set_progress(status, progress, detail_status=None, detail_progress=None):
    async with LOCK:
        PROGRESS["status"] = status
        PROGRESS["progress"] = progress
        if detail_status is not None:
            PROGRESS["details"]["status"] = detail_status
        if detail_progress is not None:
            PROGRESS["details"]["progress"] = detail_progress


def reset_progress():
    PROGRESS["status"] = "Bereit"
    PROGRESS["progress"] = 0
    PROGRESS["details"] = {"status": "-", "progress": 0}


# ============================================================
# LOKALE DATEIEN LADEN
# ============================================================

async def local_list_folder(folder_name: str):
    folder_path = os.path.join(LOCAL_BASE, folder_name)
    if not os.path.isdir(folder_path):
        return []

    result = []
    for filename in os.listdir(folder_path):
        full = os.path.join(folder_path, filename)
        if os.path.isfile(full):
            result.append({
                "id": f"{folder_name}/{filename}",
                "folder": folder_name,
                "name": filename,
                "path": full,
                "size": os.path.getsize(full)
            })
    return result


# ============================================================
# DUPE-SUCHE
# ============================================================

async def find_case_duplicates(folder_name: str, folder_idx: int, folder_total: int):
    logger.info(f"🔍 Starte Duplikatscan: {folder_name}")

    loop = asyncio.get_running_loop()
    files = await local_list_folder(folder_name)

    total_files = len(files)
    processed = 0

    md5_groups = {}

    for f in files:
        md5 = await loop.run_in_executor(EXECUTOR, compute_md5_file, f["path"])
        f["md5Checksum"] = md5

        if md5:
            md5_groups.setdefault(md5, []).append(f)

        processed += 1
        percent = int((processed / total_files) * 100)

        await set_progress(
            f"Scanne {folder_name}",
            int((folder_idx / folder_total) * 100),
            f"MD5 {processed}/{total_files}",
            percent
        )

    results = []
    weird = []

    # Gruppierung auswerten
    for md5, group in md5_groups.items():

        # Nur ein File mit diesem MD5
        if len(group) == 1:
            f = group[0]
            if f["name"] != f["name"].lower():
                weird.append({
                    "folder": folder_name,
                    "name": f["name"],
                    "sanitized": sanitize_filename(f["name"]),
                })
            continue

        # KEEP bestimmen (kleingeschrieben + alphabetisch)
        lower = [g for g in group if g["name"] == g["name"].lower()]

        if lower:
            keep = sorted(lower, key=lambda x: x["name"])[0]
        else:
            # kein lowercase → weird
            for f in group:
                weird.append({
                    "folder": folder_name,
                    "name": f["name"],
                    "sanitized": sanitize_filename(f["name"]),
                })
            continue

        # DELETE-Liste bestimmen
        delete_list = sorted(
            [f for f in group if f["id"] != keep["id"]],
            key=lambda x: x["name"].lower()
        )

        for f in delete_list:
            results.append({
                "folder": folder_name,
                "delete": f["name"],
                "delete_id": f["id"],
                "keep": keep["name"],
                "keep_id": keep["id"],
            })

    return {
        "key": folder_name,
        "label": folder_name,
        "folder_id": folder_name,
        "num_results": len(results),
        "results": results,
        "weird": weird,
    }


# ============================================================
# BACKGROUND-SCAN
# ============================================================

SCAN_CACHE = None


async def run_full_scan():
    global SCAN_CACHE
    reset_progress()

    categories = [k for k in Settings.kategorien() if k["key"] != "real"]
    total = len(categories)
    out = []

    idx = 0
    for cat in categories:
        idx += 1

        await set_progress(
            f"Starte {cat['key']}",
            int(((idx - 1) / total) * 100),
            "Warte…", 0
        )

        r = await find_case_duplicates(cat["key"], idx - 1, total)
        out.append(r)

    SCAN_CACHE = out

    await set_progress("Fertig", 100, "Complete", 20)
    logger.info("🟢 Lokaler Scan abgeschlossen!")


# ============================================================
# CACHE UPDATE NACH LÖSCHEN
# ============================================================

def update_cache_after_delete(delete_ids: list[str]):
    """Entfernt gelöschte Dateien aus SCAN_CACHE."""
    global SCAN_CACHE
    if not SCAN_CACHE:
        return

    remaining = []

    for cat in SCAN_CACHE:
        folder = cat["key"]

        # Results aktualisieren
        new_results = [
            r for r in cat["results"]
            if r["delete_id"] not in delete_ids
        ]

        # Weird aktualisieren
        new_weird = [
            w for w in cat["weird"]
            if f"{folder}/{w['name']}" not in delete_ids
        ]

        cat["results"] = new_results
        cat["weird"] = new_weird
        cat["num_results"] = len(new_results)

        remaining.append(cat)

    SCAN_CACHE = remaining


# ============================================================
# ROUTES
# ============================================================

@router.get("/cleanup_local", response_class=HTMLResponse)
async def cleanup_local(request: Request):
    return templates.TemplateResponse(
        "cleanup_local.j2",
        {
            "request": request,
            "categories": SCAN_CACHE,
            "version": VERSION,
            "dry_run": True,
        }
    )


@router.post("/cleanup_local_start")
async def cleanup_local_start():
    reset_progress()
    loop = asyncio.get_running_loop()
    loop.create_task(run_full_scan())
    return JSONResponse({"started": True})


@router.get("/cleanup_local_progress")
async def cleanup_local_progress():
    return JSONResponse(PROGRESS)


@router.get("/cleanup_local_reload")
async def cleanup_local_reload():
    global SCAN_CACHE
    SCAN_CACHE = None
    reset_progress()
    return RedirectResponse("/gallery/cleanup_local")


# ============================================================
# DELETE
# ============================================================

@router.post("/cleanup_local_delete", response_class=HTMLResponse)
async def cleanup_local_delete(request: Request, delete_ids: list[str] = Form(default=[])):
    errors = []
    deleted = []

    for file_id in delete_ids:
        folder, fname = file_id.split("/", 1)
        path = os.path.join(LOCAL_BASE, folder, fname)

        try:
            os.remove(path)
            deleted.append(file_id)
        except Exception as e:
            errors.append({"id": file_id, "error": str(e)})

    # Cache nur aktualisieren, nicht löschen!
    update_cache_after_delete(delete_ids)

    return templates.TemplateResponse(
        "cleanup_local_done.j2",
        {
            "request": request,
            "version": VERSION,
            "deleted": deleted,
            "errors": errors,
        }
    )


# ============================================================
# TESTDATA
# ============================================================

@router.get("/cleanup_local_test")
async def cleanup_local_test():
    try:
        from .cleanup_local_testdata import generate_test_data
        generate_test_data()
        logger.info("🧪 Testdaten erfolgreich erzeugt!")
    except Exception as e:
        logger.error(f"Fehler beim Erzeugen der Testdaten: {e}")

    global SCAN_CACHE
    SCAN_CACHE = None

    return RedirectResponse("/gallery/cleanup_local", status_code=302)
