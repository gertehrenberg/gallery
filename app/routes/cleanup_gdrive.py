import asyncio
import io
import os

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from googleapiclient.http import MediaIoBaseDownload
from ..config import Settings
from ..config_gdrive import folder_id_by_name, sanitize_filename
from ..routes.auth import load_drive_service
from ..utils.logger_config import setup_logger

VERSION = 122
logger = setup_logger(__name__)
logger.info(f"🟦 Starte cleanup_gdrive.py (Unified Model) v{VERSION}")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "../templates")
)

_DRIVE = None


def get_drive():
    """Load Google Drive API service once."""
    global _DRIVE
    if _DRIVE is None:
        _DRIVE = load_drive_service()
    return _DRIVE


# ======================================================================
# PROGRESS SYSTEM
# ======================================================================

PROGRESS = {"status": "Bereit", "progress": 0, "details": {"status": "", "progress": 0}}
PROGRESS_LOCK = asyncio.Lock()


async def set_progress(status, progress, detail_status=None, detail_progress=None):
    async with PROGRESS_LOCK:
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


# ======================================================================
# GOOGLE DRIVE FOLDER LISTING
# ======================================================================

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

    while True:
        page += 1

        await set_progress(
            f"Lese Dateien: {folder_name}",
            PROGRESS["progress"],
            f"Seite {page}",
            min(page, 100))

        resp = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id,name,md5Checksum,size)",
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


# ======================================================================
# DUPLICATE & CASE ANALYSIS
# ======================================================================

async def find_case_duplicates(folder_name: str, idx: int, total: int):
    logger.info(f"🔍 Scanne GDrive-Ordner: {folder_name}")

    files = await gdrive_list_folder(folder_name)

    md5_groups = {}
    processed = 0
    total_files = len(files)

    results = []

    for f in files:
        name = f["name"]
        ext = os.path.splitext(name.lower())[1]
        md5 = f.get("md5Checksum")

        if md5:
            md5_groups.setdefault(md5, []).append(f)

        processed += 1
        percent = int((processed / max(total_files, 1)) * 100)

        await set_progress(
            f"Scanne {folder_name}",
            int((idx / total) * 100),
            f"Seite {processed}/{total_files}",
            percent
        )

        # if folder_name != "delete" and ext in Settings.IMAGE_EXTENSIONS:
        #     if not is_real_image(
        #             f["id"],
        #             name,
        #             f.get("size"),
        #             md5
        #     ):
        #         results.append({
        #             "folder": folder_name,
        #             "delete": name,
        #             "delete_id": f["id"],
        #             "keep": None,
        #             "keep_id": None,
        #             "type": "gdrive",
        #         })
        #         continue

    for md5, group in md5_groups.items():

        # WEIRD: single file with uppercase name
        if len(group) == 1:
            f = group[0]
            if f["name"] != f["name"].lower():
                lower = sanitize_filename(f["name"])
                results.append({
                    "folder": folder_name,
                    "delete": f["name"],
                    "delete_id": f["id"],
                    "keep": lower,
                    "keep_id": f["id"],  # same ID on rename
                    "type": "weird",
                })
            continue

        # multi-file duplicate set
        lower = [g for g in group if g["name"] == g["name"].lower()]

        if not lower:
            # all weird
            for f in group:
                lower = sanitize_filename(f["name"])
                results.append({
                    "folder": folder_name,
                    "delete": f["name"],
                    "delete_id": f["id"],
                    "keep": lower,
                    "keep_id": f["id"],
                    "type": "weird",
                })
            continue

        # pick alphabetically first lowercase version
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


def is_real_image(file_id: str, filename: str, size: int = None, md5: str = None) -> bool:
    """
    Schneller Bild-Check:
    1) superschnelle Heuristiken über Dateigröße & md5Checksum
    2) nur falls nötig: Header-Check (erste 64 Bytes)
    """

    ext = os.path.splitext(filename.lower())[1]

    # Nicht-Bilder nicht prüfen
    if ext not in Settings.IMAGE_EXTENSIONS:
        return True

    # -----------------------------
    # 1️⃣ Ultra-schnelle Checks
    # -----------------------------

    size = int(size or 0)

    # Größe 0 → niemals ein Bild
    if size == 0:
        return False

    # Minimale sinnvolle Bildgrößen
    if ext in (".jpg", ".jpeg") and size < 2000:
        return False

    if ext == ".png" and size < 150:
        return False

    if ext == ".gif" and size < 40:
        return False

    if ext == ".webp" and size < 40:
        return False

    # md5Checksum immer vorhanden bei validen Dateien
    if md5 is None or md5 == "":
        return False

    # Wenn Datei groß genug ist → sehr wahrscheinlich ein Bild → kein Header nötig
    if size > 10_000:  # > 10 KB → fast sicher echtes Bild
        return True

    # -----------------------------
    # 2️⃣ Header-Check (Fallback)
    # -----------------------------
    try:
        req = get_drive().files().get_media(fileId=file_id)
        bio = io.BytesIO()
        downloader = MediaIoBaseDownload(bio, req)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        data = bio.getvalue()[:64]

    except Exception as e:
        logger.warning(f"⚠ Fehler beim Laden von {filename}: {e}")
        return False

    # JPEG
    if ext in (".jpg", ".jpeg"):
        return data.startswith(b"\xFF\xD8")

    # PNG
    if ext == ".png":
        return data.startswith(b"\x89PNG")

    # GIF
    if ext == ".gif":
        return data.startswith(b"GIF87a") or data.startswith(b"GIF89a")

    # WEBP
    if ext == ".webp":
        return data.startswith(b"RIFF") and b"WEBP" in data[8:16]

    # generischer Fallback
    return len(data) > 10


# ======================================================================
# FULL SCAN + CACHE
# ======================================================================

SCAN_CACHE = None


async def run_full_scan():
    global SCAN_CACHE
    reset_progress()

    categories = [c for c in Settings.kategorien() if c["key"] != "XXXX"]
    total = len(categories)
    out = []

    idx = 0
    for cat in categories:
        r = await find_case_duplicates(cat["key"], idx, total)
        out.append(r)
        idx += 1

    SCAN_CACHE = out
    await set_progress("Fertig", 100, "Complete", 100)
    logger.info("🟢 GDrive Scan abgeschlossen!")


# ======================================================================
# CACHE UPDATE — FINAL FIX
# ======================================================================

def update_cache(delete_ids, lower_ids):
    global SCAN_CACHE
    if not SCAN_CACHE:
        logger.info("⚠ update_cache: SCAN_CACHE ist leer!")
        return

    logger.info("🔧 update_cache START")
    logger.info(f"🔹 delete_ids = {delete_ids}")
    logger.info(f"🔹 lower_ids  = {lower_ids}")

    for cat in SCAN_CACHE:
        logger.info(f"🗂 Kategorie: {cat['key']}")

        before = len(cat["results"])
        logger.info(f"   🔸 Vorher: {before} Einträge")

        new_results = []

        for r in cat["results"]:
            did = r["delete_id"]
            kid = r.get("keep_id")

            # logger.info(f"   ▶ Prüfe Eintrag: delete_id={did} keep_id={kid}")

            # -------------------------------------------------------
            # 1️⃣ DELETE → Eintrag entfernen
            # -------------------------------------------------------
            if did in delete_ids:
                # logger.info(f"      🗑️ DELETE-Match: entferne → {did}")
                continue

            # -------------------------------------------------------
            # 2️⃣ RENAME → Eintrag AUCH entfernen(!)
            #    (Weil weird-Einträge nach dem Rename NICHT mehr nötig sind)
            # -------------------------------------------------------
            if did in lower_ids or kid in lower_ids:
                # logger.info(f"      ✏️ RENAME-Match: entferne Eintrag komplett → {did}")
                continue

            # -------------------------------------------------------
            # 3️⃣ behalten
            # -------------------------------------------------------
            # logger.info("      ✔ behalten")
            new_results.append(r)

        cat["results"] = new_results

        after = len(new_results)
        logger.info(f"   🔹 Nachher: {after} Einträge (Δ = {after - before})")

    logger.info("🔧 update_cache DONE\n")


# ======================================================================
# ROUTES
# ======================================================================

@router.get("/cleanup_gdrive", response_class=HTMLResponse)
async def cleanup_gdrive(request: Request):
    return templates.TemplateResponse(
        "cleanup.j2",
        {
            "request": request,
            "categories": SCAN_CACHE,
            "version": VERSION,
            "mode": "gdrive"
        }
    )


@router.post("/cleanup_gdrive_start")
async def cleanup_gdrive_start():
    reset_progress()
    asyncio.get_running_loop().create_task(run_full_scan())
    return JSONResponse({"started": True})


@router.get("/cleanup_gdrive_progress")
async def cleanup_gdrive_progress():
    return JSONResponse(PROGRESS)


@router.get("/cleanup_gdrive_reload")
async def cleanup_gdrive_reload():
    return RedirectResponse("/gallery/cleanup_gdrive")


# ======================================================================
# DELETE + RENAME ACTIONS
# ======================================================================

@router.post("/cleanup_gdrive_delete", response_class=HTMLResponse)
async def cleanup_gdrive_delete(
        request: Request,
        delete_ids: list[str] = Form(default=[]),
        lower_ids: list[str] = Form(default=[]),
):
    errors = []
    deleted = []
    renamed = []

    service = get_drive()

    # REAL DELETES
    for file_id in delete_ids:
        try:
            service.files().delete(fileId=file_id).execute()
            deleted.append(file_id)
        except Exception as e:
            errors.append({"id": file_id, "error": str(e)})

    # REAL RENAMES
    for file_id in lower_ids:
        try:
            # find original entry in cache
            old_name = None
            new_name = None
            for cat in SCAN_CACHE or []:
                for r in cat["results"]:
                    if r["delete_id"] == file_id or r.get("keep_id") == file_id:
                        old_name = r["delete"]
                        new_name = r["keep"]
                        break

            if not new_name:
                raise Exception("Rename entry not found in cache")

            service.files().update(
                fileId=file_id,
                body={"name": new_name},
            ).execute()

            renamed.append({"from": old_name, "to": new_name})

        except Exception as e:
            errors.append({"id": file_id, "error": str(e)})

    # FIXED CACHE UPDATE
    update_cache(delete_ids, lower_ids)

    return templates.TemplateResponse(
        "cleanup_done.j2",
        {
            "request": request,
            "version": VERSION,
            "deleted": deleted,
            "renamed": renamed,
            "errors": errors,
            "mode": "gdrive",
        }
    )


# ============================================================
# TESTDATA
# ============================================================

@router.get("/cleanup_gdrive_test")
async def cleanup_gdrive_test():
    try:
        from .cleanup_gdrive_testdata import generate_gdrive_test_data
        from .cleanup_gdrive_testdata import delete_gdrive_testdata_files_from_store
        delete_gdrive_testdata_files_from_store()
        #generate_gdrive_test_data(folder_id_by_name("imagefiles"))
        logger.info("🧪 Testdaten erfolgreich erzeugt!")
    except Exception as e:
        logger.error(f"Fehler beim Erzeugen der Testdaten: {e}")

    return RedirectResponse("/gallery/cleanup_gdrive", status_code=302)
