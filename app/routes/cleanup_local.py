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

VERSION = 120
logger = setup_logger(__name__)
logger.info(f"🟦 Starte cleanup_local.py (Unified Model) v{VERSION}")

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
    """MD5 Berechnung über ThreadPool."""
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

PROGRESS = {"status": "Bereit", "progress": 0, "details": {"status": "-", "progress": 0}}
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
    PROGRESS["details"] = {"status": "", "progress": 0}


# ============================================================
# DATEIEN LADEN
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
                "size": os.path.getsize(full),
            })
    return result


# ============================================================
# CASE-DUPLIKATE / WEIRD → EINHEITLICHES MODELL
# ============================================================

async def find_case_duplicates(folder_name: str, idx: int, total: int):
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
            int((idx / total) * 100),
            f"MD5 {processed}/{total_files}",
            percent
        )

    results = []

    for md5, group in md5_groups.items():
        # FALL A: nur 1 Datei → kann weird sein
        if len(group) == 1:
            f = group[0]
            if f["name"] != f["name"].lower():
                # weird = Datei hat theoretisch eine lowercase-Version
                lower_name = sanitize_filename(f["name"])
                results.append({
                    "folder": folder_name,
                    "delete": f["name"],
                    "delete_id": f["id"],
                    "keep": lower_name,
                    "keep_id": f"{folder_name}/{lower_name}",
                    "type": "weird",
                })
            continue

        # FALL B: mehrere Dateien mit gleichem MD5 → Duplikate
        lower = [g for g in group if g["name"] == g["name"].lower()]
        if not lower:
            # alle Upper/Weird → alle erzeugen weird-Einträge
            for f in group:
                lower_name = sanitize_filename(f["name"])
                results.append({
                    "folder": folder_name,
                    "delete": f["name"],
                    "delete_id": f["id"],
                    "keep": lower_name,
                    "keep_id": f"{folder_name}/{lower_name}",
                    "type": "weird",
                })
            continue

        # echten keep bestimmen (alphabetisch)
        keep = sorted(lower, key=lambda x: x["name"])[0]

        for f in group:
            if f["id"] == keep["id"]:
                continue
            results.append({
                "folder": folder_name,
                "delete": f["name"],
                "delete_id": f["id"],
                "keep": keep["name"],
                "keep_id": keep["id"],
                "type": "delete",
            })

    return {
        "key": folder_name,
        "label": folder_name,
        "folder_id": folder_name,
        "results": results,
    }


# ============================================================
# BACKGROUND SCAN
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
        r = await find_case_duplicates(cat["key"], idx, total)
        out.append(r)
        idx += 1

    SCAN_CACHE = out
    await set_progress("Fertig", 100, "Complete", 20)
    logger.info("🟢 Lokaler Scan abgeschlossen!")


# ============================================================
# CACHE-AKTUALISIERUNG
# ============================================================

def update_cache(delete_ids, lower_ids):
    """
    Aktualisiert SCAN_CACHE:
    - entfernt nur wirklich gelöschte Einträge
    - aktualisiert umbenannte Einträge statt sie zu entfernen
    """
    global SCAN_CACHE
    if not SCAN_CACHE:
        return

    # Umbenennungen vorbereiten: Mapping alt → neu
    rename_map = {}
    for fid in lower_ids:
        folder, fname = fid.split("/", 1)
        new_name = sanitize_filename(fname)
        rename_map[fid] = f"{folder}/{new_name}"

    for cat in SCAN_CACHE:
        new_results = []
        for r in cat["results"]:

            # 1️⃣ komplett entfernen, wenn gelöscht
            if r["delete_id"] in delete_ids:
                continue

            # 2️⃣ bei Umbenennung: Eintrag updaten
            if r["delete_id"] in rename_map:
                new_id = rename_map[r["delete_id"]]

                r["delete"] = r["keep"]  # neue Basiswerte setzen
                r["delete_id"] = new_id

                # keep_id muss bleiben wie gehabt
                new_results.append(r)
                continue

            # 3️⃣ sonst unverändertes Ergebnis übernehmen
            new_results.append(r)

        cat["results"] = new_results


# ============================================================
# ROUTES
# ============================================================

@router.get("/cleanup_local", response_class=HTMLResponse)
async def cleanup_local(request: Request):
    return templates.TemplateResponse(
        "cleanup_local.j2",
        {"request": request, "categories": SCAN_CACHE, "version": VERSION}
    )


@router.post("/cleanup_local_start")
async def cleanup_local_start():
    reset_progress()
    asyncio.get_running_loop().create_task(run_full_scan())
    return JSONResponse({"started": True})


@router.get("/cleanup_local_progress")
async def cleanup_local_progress():
    return JSONResponse(PROGRESS)


@router.get("/cleanup_local_reload")
async def cleanup_local_reload():
    return RedirectResponse("/gallery/cleanup_local")


# ============================================================
# DELETE + LOWERCASE UMBENENNEN
# ============================================================

@router.post("/cleanup_local_delete", response_class=HTMLResponse)
async def cleanup_local_delete(
        request: Request,
        delete_ids: list[str] = Form(default=[]),
        lower_ids: list[str] = Form(default=[]),
):
    errors = []
    deleted = []
    renamed = []

    # --- löschen ---
    for file_id in delete_ids:
        folder, fname = file_id.split("/", 1)
        try:
            os.remove(os.path.join(LOCAL_BASE, folder, fname))
            deleted.append(file_id)
        except Exception as e:
            errors.append({"id": file_id, "error": str(e)})

    # --- umbenennen ---
    for file_id in lower_ids:
        folder, fname = file_id.split("/", 1)
        src = os.path.join(LOCAL_BASE, folder, fname)
        dst_name = sanitize_filename(fname)
        dst = os.path.join(LOCAL_BASE, folder, dst_name)
        try:
            os.rename(src, dst)
            renamed.append({"from": file_id, "to": f"{folder}/{dst_name}"})
        except Exception as e:
            errors.append({"id": file_id, "error": str(e)})

    # Cache aktualisieren
    update_cache(delete_ids, lower_ids)

    return templates.TemplateResponse(
        "cleanup_local_done.j2",
        {
            "request": request,
            "version": VERSION,
            "deleted": deleted,
            "renamed": renamed,
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

    return RedirectResponse("/gallery/cleanup_local", status_code=302)
