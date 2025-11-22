import os

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
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

# ============================================================
#  GLOBALER CACHE → kein erneuter Scan nach "Zurück"
# ============================================================
GDRIVE_CLEANUP_CACHE = None

# ============================================================
#  GLOBALER Google-Drive-Service (neu)
# ============================================================
DRIVE_SERVICE = None

def get_drive_service():
    global DRIVE_SERVICE
    if DRIVE_SERVICE is None:
        DRIVE_SERVICE = load_drive_service()
    return DRIVE_SERVICE


# =====================================================================
#  GOOGLE DRIVE: ALLE DATEIEN EINES ORDNER LADEN (KEINE UNTERORDNER)
# =====================================================================
async def gdrive_list_folder(service, folder_id: str):
    query = f"'{folder_id}' in parents and trashed = false"

    files = []
    page = None

    while True:
        resp = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id,name,md5Checksum,size)",
            pageToken=page,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        files.extend(resp.get("files", []))
        page = resp.get("nextPageToken")

        if not page:
            break

    return files


# =====================================================================
#  FINDE CASE-DUPLIKATE
# =====================================================================
async def find_case_duplicates(service, folder_name: str):
    folder_id = folder_id_by_name(folder_name)

    if not folder_id:
        logger.warning(f"GDrive: Ordner '{folder_name}' wurde NICHT gefunden!")
        return {
            "folder": folder_name,
            "folder_id": None,
            "num_results": 0,
            "results": [],
        }

    files = await gdrive_list_folder(service, folder_id)
    logger.info(f"{folder_name}: {len(files)} Dateien")

    md5_groups = {}
    name_groups = {}

    for f in files:
        name = f["name"]
        size = int(f.get("size", 0)) if f.get("size") else None
        md5 = f.get("md5Checksum")

        if md5:
            md5_groups.setdefault(md5, []).append(f)
        else:
            name_groups.setdefault(name.lower(), []).append(f)

    results = []

    # MD5-basierte Duplikate
    for md5, group in md5_groups.items():
        if len(group) < 2:
            continue

        lowercase_versions = [g for g in group if g["name"] == g["name"].lower()]
        if not lowercase_versions:
            continue

        keep = lowercase_versions[0]

        for f in group:
            if f["id"] == keep["id"]:
                continue
            if f["name"] != f["name"].lower():
                results.append({
                    "folder": folder_name,
                    "md5": md5,
                    "delete": f["name"],
                    "delete_id": f["id"],
                    "keep": keep["name"],
                    "keep_id": keep["id"],
                })

    # Fallback: Name + Größe
    for key, group in name_groups.items():
        if len(group) < 2:
            continue

        sizes = {g.get("size") for g in group}
        if len(sizes) != 1:
            continue

        lowercase_versions = [g for g in group if g["name"] == g["name"].lower()]
        if not lowercase_versions:
            continue

        keep = lowercase_versions[0]

        for f in group:
            if f["id"] == keep["id"]:
                continue
            if f["name"] != f["name"].lower():
                results.append({
                    "folder": folder_name,
                    "md5": "(no md5)",
                    "delete": f["name"],
                    "delete_id": f["id"],
                    "keep": keep["name"],
                    "keep_id": keep["id"],
                })

    return {
        "folder": folder_name,
        "folder_id": folder_id,
        "num_results": len(results),
        "results": results,
    }


# =====================================================================
#  GET → Seite anzeigen (mit Cache)
# =====================================================================
@router.get("/gdrive_cleanup", response_class=HTMLResponse)
async def gdrive_cleanup(request: Request):
    global GDRIVE_CLEANUP_CACHE

    # Cache verwenden
    if GDRIVE_CLEANUP_CACHE is not None:
        logger.info("➡ Verwende GDrive-Cache (kein Scan)")
        return templates.TemplateResponse(
            "gdrive_cleanup.j2",
            {
                "request": request,
                "categories": GDRIVE_CLEANUP_CACHE,
                "dry_run": True,
            }
        )

    # Kein Cache → scannen
    logger.info("➡ Scanne GDrive (erster Aufruf)")
    service = get_drive_service()
    categories_output = []

    for k in Settings.kategorien():
        key = k["key"]
        label = k["label"]
        icon = k["icon"]

        if key == "real":
            continue

        res = await find_case_duplicates(service, key)

        # ❗ nur Kategorien mit Ergebnissen
        if res["num_results"] > 0:
            categories_output.append({
                "key": key,
                "label": label,
                "icon": icon,
                "folder_id": res["folder_id"],
                "num_results": res["num_results"],
                "results": res["results"],
            })

    if categories_output:
        GDRIVE_CLEANUP_CACHE = categories_output
        logger.info("➡ Cache gespeichert (%d Kategorien)", len(categories_output))

    return templates.TemplateResponse(
        "gdrive_cleanup.j2",
        {
            "request": request,
            "categories": categories_output,
            "dry_run": True,
        }
    )


# =====================================================================
#  GET → Cache leeren & neu scannen (Reload)
# =====================================================================
from fastapi.responses import RedirectResponse


@router.get("/gdrive_cleanup_reload")
async def gdrive_cleanup_reload():
    global GDRIVE_CLEANUP_CACHE
    logger.info("🔄 Cache geleert → neuer Scan")
    GDRIVE_CLEANUP_CACHE = None
    return RedirectResponse("/gallery/gdrive_cleanup", status_code=302)


# =====================================================================
#  POST → echtes Löschen + Cache-Update
# =====================================================================
@router.post("/gdrive_cleanup_delete", response_class=HTMLResponse)
async def gdrive_cleanup_delete(request: Request,
                                delete_ids: list[str] = Form(default=[])):
    global GDRIVE_CLEANUP_CACHE

    service = get_drive_service()
    deleted = []
    errors = []

    for file_id in delete_ids:
        try:
            service.files().delete(fileId=file_id).execute()
            deleted.append(file_id)
        except Exception as e:
            errors.append({"id": file_id, "error": str(e)})

    # Cache aktualisieren
    if GDRIVE_CLEANUP_CACHE:
        for cat in GDRIVE_CLEANUP_CACHE:
            cat["results"] = [
                r for r in cat["results"]
                if r["delete_id"] not in deleted
            ]
            cat["num_results"] = len(cat["results"])

    return templates.TemplateResponse(
        "gdrive_cleanup_done.j2",
        {
            "request": request,
            "deleted": deleted,
            "errors": errors,
        }
    )
