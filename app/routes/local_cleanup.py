import os
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from ..config import Settings
from ..utils.logger_config import setup_logger

# ============================================================
# 🔵 VERSION — BEI JEDEM UPDATE ERHÖHEN!
# ============================================================
VERSION = 101

logger = setup_logger(__name__)
logger.info(f"🟦 Starte local_cleanup.py v{VERSION}")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "../templates")
)

# Lokaler Basisordner für Dateien
LOCAL_BASE = Settings.IMAGE_FILE_CACHE_DIR

# Cache
GDRIVE_CLEANUP_CACHE = None


# =====================================================================
#  LOKAL: ALLE DATEIEN EINES ORDNER LADEN
# =====================================================================

async def local_list_folder(folder_name: str):
    logger.info(f"📂 [v{VERSION}] Scanne lokalen Ordner: {folder_name}")
    folder_path = os.path.join(LOCAL_BASE, folder_name)
    if not os.path.isdir(folder_path):
        return []

    files = []
    for f in os.listdir(folder_path):
        full = os.path.join(folder_path, f)
        if os.path.isfile(full):
            files.append({
                "id": f"{folder_name}/{f}",
                "name": f,
                "path": full,
                "size": os.path.getsize(full)
            })

    return files


# =====================================================================
#  FINDE CASE-DUPLIKATE (lokal)
# =====================================================================

async def find_case_duplicates(folder_name: str):
    import hashlib

    logger.info(f"🔍 [v{VERSION}] Starte Case-Duplikatsuche in: {folder_name}")
    files = await local_list_folder(folder_name)
    logger.info(f"📁 [v{VERSION}] {folder_name}: {len(files)} Dateien geladen")

    # MD5-Gruppen
    md5_groups = {}
    for f in files:
        try:
            with open(f["path"], "rb") as fp:
                f["md5Checksum"] = hashlib.md5(fp.read()).hexdigest()
        except:
            f["md5Checksum"] = None

        md5_groups.setdefault(f["md5Checksum"], []).append(f)

    results = []

    for md5, group in md5_groups.items():
        if len(group) < 2:
            continue

        # Echte lowercase-Dateien
        lowercase = [g for g in group if g["name"] == g["name"].lower()]

        if not lowercase:
            logger.info(f"⚠️ [v{VERSION}] Keine lowercase-Datei in Gruppe → skip")
            continue

        # KEEP = alphabetisch kleinste lowercase-Datei
        keep = sorted(lowercase, key=lambda x: x["name"])[0]
        logger.info(f"🟢 [v{VERSION}] KEEP = {keep['name']}")

        # DELETE = alle anderen Varianten
        delete_list = [g for g in group if g["id"] != keep["id"]]
        delete_list = sorted(delete_list, key=lambda x: x["name"].lower())

        logger.info(f"🔴 [v{VERSION}] DELETE = {[d['name'] for d in delete_list]}")

        for d in delete_list:
            results.append({
                "folder": folder_name,
                "md5": md5,
                "delete": d["name"],
                "delete_id": d["id"],
                "delete_path": d["path"],
                "keep": keep["name"],
                "keep_id": keep["id"],
            })

    logger.info(f"📊 [v{VERSION}] {folder_name}: {len(results)} Duplikate gefunden")

    return {
        "folder": folder_name,
        "folder_id": folder_name,
        "num_results": len(results),
        "results": results,
    }


# =====================================================================
# GET – CACHED ANSICHT
# =====================================================================

@router.get("/local_cleanup", response_class=HTMLResponse)
async def gdrive_cleanup(request: Request):
    global GDRIVE_CLEANUP_CACHE

    if GDRIVE_CLEANUP_CACHE is not None:
        return templates.TemplateResponse(
            "local_cleanup.j2",
            {
                "request": request,
                "categories": GDRIVE_CLEANUP_CACHE,
                "dry_run": True,
                "version": VERSION,
             }
        )

    categories_output = []

    for k in Settings.kategorien():
        key = k["key"]
        label = k["label"]

        if key == "real":
            continue

        res = await find_case_duplicates(key)
        if res["num_results"] > 0:
            categories_output.append({
                "key": key,
                "label": label,
                "folder_id": res["folder_id"],
                "num_results": res["num_results"],
                "results": res["results"],
            })

    GDRIVE_CLEANUP_CACHE = categories_output

    return templates.TemplateResponse(
        "local_cleanup.j2",
        {"request": request, "categories": categories_output, "dry_run": True}
    )


# =====================================================================
# Reload
# =====================================================================

@router.get("/local_cleanup_reload")
async def gdrive_cleanup_reload():
    global GDRIVE_CLEANUP_CACHE
    GDRIVE_CLEANUP_CACHE = None
    return RedirectResponse("/gallery/local_cleanup", status_code=302)


# =====================================================================
#  POST → Löschen lokal
# =====================================================================

@router.post("/local_cleanup_delete", response_class=HTMLResponse)
async def gdrive_cleanup_delete(request: Request, delete_ids: list[str] = Form(default=[])):
    global GDRIVE_CLEANUP_CACHE

    deleted = []
    errors = []

    for file_id in delete_ids:
        folder, fname = file_id.split("/", 1)
        path = os.path.join(LOCAL_BASE, folder, fname)
        try:
            os.remove(path)
            deleted.append(file_id)
        except Exception as e:
            errors.append({"id": file_id, "error": str(e)})

    # Cache aktualisieren
    if GDRIVE_CLEANUP_CACHE:
        for cat in GDRIVE_CLEANUP_CACHE:
            cat["results"] = [r for r in cat["results"] if r["delete_id"] not in deleted]
            cat["num_results"] = len(cat["results"])

    return templates.TemplateResponse(
        "local_cleanup_done.j2",
        {"request": request, "deleted": deleted, "errors": errors}
    )
