# Refactored diff_gdrive_local.py with full logging
import asyncio
import hashlib
import io
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from ..config import Settings, UserType
from ..config_gdrive import folder_id_by_name, sanitize_filename
from ..routes.auth import load_drive_service
from ..utils.logger_config import setup_logger
from ..config_gdrive import calculate_md5

VERSION = 201
logger = setup_logger(__name__)
logger.info(f"🟦 Starte diff_gdrive_local_refactor.py v{VERSION}")

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))

PROGRESS = {"status": "Bereit", "progress": 0, "details": {"status": "Bereit", "progress": 0}}
PROGRESS_LOCK = asyncio.Lock()
LOCAL_BASE = Settings.IMAGE_FILE_CACHE_DIR
EXECUTOR = ThreadPoolExecutor(max_workers=8)
GLOBAL_MD5_INDEX = {}  # md5 -> {"local": [...], "gdrive": [...]}
_DRIVE = None

SCAN_CACHE = {
    "categories": [],
    "invalid_md5": [],
    "invalid_names": []  # <--- NEU
}

def get_drive():
    global _DRIVE
    if _DRIVE is None:
        _DRIVE = load_drive_service()
    return _DRIVE

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

def compute_md5_file(path: str):
    return calculate_md5(Path(path))


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

        clean = sanitize_filename(f["name"])  # <--- NEU
        invalid_name = (clean != f["name"])  # <--- NEU

        GLOBAL_MD5_INDEX.setdefault(md5, {"local": [], "gdrive": []})
        GLOBAL_MD5_INDEX[md5]["gdrive"].append({
            "folder": folder_name,
            "folder_id": folder_id,
            "name": f["name"],
            "id": f["id"],
            "sanitized_name": clean,  # <--- NEU
            "is_invalid_name": invalid_name  # <--- NEU
        })

        if invalid_name:  # <--- NEU
            SCAN_CACHE["invalid_names"].append({
                "source": "gdrive",
                "folder": folder_name,
                "orig_name": f["name"],
                "clean_name": clean,
                "id": f["id"],
                "md5": md5,
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

        clean = sanitize_filename(lf["name"])  # <--- NEU
        invalid_name = (clean != lf["name"])  # <--- NEU

        GLOBAL_MD5_INDEX.setdefault(md5, {"local": [], "gdrive": []})
        GLOBAL_MD5_INDEX[md5]["local"].append({
            "folder": folder_name,
            "path": lf["path"],
            "name": lf["name"],
            "sanitized_name": clean,  # <--- NEU
            "is_invalid_name": invalid_name  # <--- NEU
        })

        if invalid_name:  # <--- NEU
            SCAN_CACHE["invalid_names"].append({
                "source": "local",
                "folder": folder_name,
                "orig_name": lf["name"],
                "clean_name": clean,
                "path": lf["path"],
                "md5": md5,
            })

    l_insert_after = sum(len(v.get("local", [])) for v in GLOBAL_MD5_INDEX.values())
    logger.info(f"📥 LOCAL Insert: vorher={l_insert_before}, nachher={l_insert_after}")

    return {"folder": folder_name, "results": []}


async def run_full_scan():
    global SCAN_CACHE, GLOBAL_MD5_INDEX

    reset_progress()
    GLOBAL_MD5_INDEX = {}

    # <--- wichtig: invalid_names zurücksetzen
    SCAN_CACHE = {
        "categories": [],
        "invalid_md5": [],
        "invalid_names": []  # <--- NEU
    }

    Settings._user_type = UserType.ADMIN
    categories = [c["key"] for c in Settings.kategorien() if c["key"] != "real"]
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

    SCAN_CACHE["categories"] = out
    SCAN_CACHE["invalid_md5"] = invalid_md5

    await set_progress("Fertig", 100, "Fertig", 100)
    logger.info("🟢 Globaler MD5-Scan abgeschlossen")


@router.get("/diff_gdrive_local", response_class=HTMLResponse)
async def diff_gdrive_local(request: Request):
    return templates.TemplateResponse(
        "diff_gdrive_local.j2",
        {
            "request": request,
            "categories": SCAN_CACHE.get("categories", []),
            "invalid_md5": SCAN_CACHE.get("invalid_md5", []),
            "invalid_names": SCAN_CACHE.get("invalid_names", []),  # <--- NEU
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

@router.post("/diff_gdrive_local_delete")
async def diff_gdrive_local_delete(request: Request):
    """
    Löscht ausgewählte Dateien (local oder gdrive)
    UND synchronisiert ausgewählte sync_ids.
    OHNE Ordner anzulegen – alle Ordner müssen existieren!
    Danach werden betroffene Einträge aus SCAN_CACHE entfernt.
    """

    form = await request.form()
    delete_ids = form.getlist("delete_ids")
    sync_ids = form.getlist("sync_ids")

    deleted_local = []
    deleted_gdrive = []
    synced_local = []
    synced_gdrive = []
    errors = []

    drive = get_drive()

    # ======================================================
    # 0) HILFSFUNKTIONEN
    # ======================================================

    def resolve_drive_path(drive, path_segments):
        """
        Gibt die ID des Zielordners zurück.
        Legt NIE Ordner an.
        """
        try:
            parent_id = folder_id_by_name("imagefiles")
        except Exception:
            raise Exception("Drive Basisordner 'imagefiles' nicht gefunden!")

        for seg in path_segments:
            query = (
                f"name='{seg}' and mimeType='application/vnd.google-apps.folder' "
                f"and '{parent_id}' in parents and trashed=false"
            )
            res = drive.files().list(q=query, fields="files(id)").execute()
            folders = res.get("files", [])

            if not folders:
                raise Exception(f"GDrive Unterordner fehlt: {seg}")

            parent_id = folders[0]["id"]

        return parent_id

    async def sync_from_gdrive(file_id: str):
        """GDrive → Local (Ordner muss existieren!)"""
        try:
            meta = drive.files().get(fileId=file_id, fields="name").execute()
            filename = meta["name"]

            # Ordner über GLOBAL_MD5_INDEX finden
            target_folder = None
            for md5, entry in GLOBAL_MD5_INDEX.items():
                for g in entry["gdrive"]:
                    if g["id"] == file_id:
                        target_folder = g["folder"]
                        break

            if target_folder is None:
                raise Exception(f"Kein Ordner für GDrive-ID {file_id} im MD5-Index!")

            local_target_dir = os.path.join(LOCAL_BASE, target_folder)

            if not os.path.isdir(local_target_dir):
                raise Exception(f"Lokaler Ordner fehlt: {local_target_dir}")

            local_target = os.path.join(local_target_dir, filename)

            # Download
            request_dl = drive.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request_dl)
            done = False
            while not done:
                _, done = downloader.next_chunk()

            fh.seek(0)

            with open(local_target, "wb") as f:
                f.write(fh.read())

            logger.info(f"⬇️ Sync GDrive → Local: {file_id} → {local_target}")
            return local_target

        except Exception as e:
            msg = f"Fehler Sync GDrive→Local {file_id}: {e}"
            logger.error(msg)
            errors.append(msg)
            return None

    async def sync_to_gdrive(local_path: str):
        """Local → GDrive (Ordner muss existieren im GDrive!)"""
        try:
            # relativer Pfad unter imagefiles
            rel = os.path.relpath(local_path, LOCAL_BASE)
            parts = rel.split("/")
            folder_parts = parts[:-1]
            filename = parts[-1]

            # GDrive-Ordner nachschlagen
            parent_id = resolve_drive_path(drive, folder_parts)

            metadata = {
                "name": filename,
                "parents": [parent_id],
            }

            media = MediaFileUpload(local_path, resumable=True)

            new_file = drive.files().create(
                body=metadata,
                media_body=media,
                fields="id"
            ).execute()

            new_id = new_file["id"]
            logger.info(
                f"⬆️ Sync Local → GDrive: {local_path} → {new_id} (Ordner: {'/'.join(folder_parts)})"
            )
            return new_id

        except Exception as e:
            msg = f"Fehler Sync Local→GDrive {local_path}: {e}"
            logger.error(msg)
            errors.append(msg)
            return None

    # ======================================================
    # 1) SYNC verarbeiten
    # ======================================================

    for sid in sync_ids:

        # A) GDrive → Local
        if "/" not in sid:
            res = await sync_from_gdrive(sid)
            if res:
                synced_local.append(res)

        # B) Local → GDrive
        else:
            res = await sync_to_gdrive(sid)
            if res:
                synced_gdrive.append(res)

    # ======================================================
    # 2) SCAN_CACHE Einträge für gesyncte Dateien entfernen
    # ======================================================

    if sync_ids:
        old_invalid = SCAN_CACHE.get("invalid_md5", [])
        new_invalid = []

        for item in old_invalid:
            md5 = item["md5"]

            local_paths = [x.get("path") for x in item["local"]]
            gdrive_ids = [x.get("id") for x in item["gdrive"]]

            # wenn irgendein Teil dieses Eintrags gesynct wurde → entfernen
            if any(sid in local_paths or sid in gdrive_ids for sid in sync_ids):
                logger.info(f"🧹 Entferne aus SCAN_CACHE wegen Sync: {md5}")
                continue

            new_invalid.append(item)

        SCAN_CACHE["invalid_md5"] = new_invalid

    # ======================================================
    # 3) DELETE verarbeiten
    # ======================================================

    for did in delete_ids:

        # lokal
        if did.startswith("/") and os.path.exists(did):
            try:
                os.remove(did)
                deleted_local.append(did)
                logger.info(f"🗑 Lokal gelöscht: {did}")
            except Exception as e:
                msg = f"Fehler lokales Löschen {did}: {e}"
                errors.append(msg)

        # GDrive
        else:
            try:
                drive.files().delete(fileId=did).execute()
                deleted_gdrive.append(did)
                logger.info(f"🗑 GDrive gelöscht: {did}")
            except Exception as e:
                msg = f"Fehler GDrive-Löschen {did}: {e}"
                errors.append(msg)

    # ======================================================
    # 4) SCAN_CACHE nach DELETE aktualisieren
    # ======================================================

    old_invalid = SCAN_CACHE.get("invalid_md5", [])
    new_invalid = []

    for item in old_invalid:
        md5 = item["md5"]
        local = [x for x in item["local"] if x.get("path") not in deleted_local]
        gdrive = [x for x in item["gdrive"] if x.get("id") not in deleted_gdrive]

        lc = len(local)
        gc = len(gdrive)

        # UI reduzieren
        if (lc == 0 and gc == 0) or (lc == 1 and gc == 1):
            logger.info(f"🧹 Entfernt aus SCAN_CACHE (Delete): {md5}")
            continue

        new_invalid.append({
            "md5": md5,
            "local": local,
            "gdrive": gdrive,
            "status": f"{lc}x local, {gc}x gdrive",
        })

    SCAN_CACHE["invalid_md5"] = new_invalid

    # ======================================================
    # 5) invalid_names nach Delete filtern
    # ======================================================

    new_names = []
    for item in SCAN_CACHE.get("invalid_names", []):
        if item["source"] == "local" and item["path"] in deleted_local:
            continue
        if item["source"] == "gdrive" and item["id"] in deleted_gdrive:
            continue
        new_names.append(item)

    SCAN_CACHE["invalid_names"] = new_names

    # ======================================================
    # 6) Ergebnis
    # ======================================================

    return templates.TemplateResponse(
        "diff_gdrive_local_done.j2",
        {
            "request": request,
            "version": VERSION,
            "deleted_local": deleted_local,
            "deleted_gdrive": deleted_gdrive,
            "synced_local": synced_local,
            "synced_gdrive": synced_gdrive,
            "errors": errors,
        },
    )
