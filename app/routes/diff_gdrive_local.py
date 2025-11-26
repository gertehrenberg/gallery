# Refactored diff_gdrive_local.py with full logging
import asyncio
import io
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from ..config import Settings, UserType
from ..config_gdrive import calculate_md5
from ..config_gdrive import sanitize_filename
from ..routes.auth import load_drive_service
from ..utils.logger_config import setup_logger

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

SCAN_CACHE = {
    "categories": [],
    "invalid_md5": [],
    "invalid_names": [],
    "filename_collisions": []
}

UID_CACHE = {}
# Globaler Fortschritt für Dateiscans
processed_files = 0
total_files = 0

_cached_folder_dict = None


def warmup_drive():
    try:
        logger.info("🔧 Initialisiere GDrive Client (Warm-Up)…")
        s = load_drive_service()
        # Kleiner Aufruf erzwingt die Verbindung:
        s.files().list(pageSize=1, fields="files(id)").execute()
        logger.info("🔧 GDrive Warm-Up erfolgreich")
    except Exception as e:
        logger.error(f"❌ GDrive Warm-Up Fehler: {e}")


warmup_drive()


async def prepare_total_file_count(categories):
    """Zählt alle Dateien in allen Kategorien (local + gdrive)."""
    total_files = 0
    for cat in categories:
        # lokale Dateien
        lfiles = await local_list_folder(cat)
        total_files += len(lfiles)

        # gdrive Dateien
        gfiles = await gdrive_list_folder(cat)
        total_files += len(gfiles)

    return total_files


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
            # logger.info(f"detail_status: {detail_status}")
        if detail_progress is not None:
            PROGRESS["details"]["progress"] = detail_progress
            # logger.info(f"detail_progress: {detail_progress}")


async def set_progress_detail(detail_status=None, detail_progress=None):
    async with PROGRESS_LOCK:
        if detail_status is not None:
            PROGRESS["details"]["status"] = detail_status
            # logger.info(f"detail_status: {detail_status}")
        if detail_progress is not None:
            PROGRESS["details"]["progress"] = detail_progress
            # logger.info(f"detail_progress: {detail_progress}")


async def update_file_progress(processed_files, total_files, source):
    """Aktualisiert den Detail-Fortschritt je gescannte Datei."""
    percent = int(processed_files / total_files * 100)
    await set_progress_detail(
        detail_status=f"Scanne {source} ({processed_files}/{total_files})",
        detail_progress=percent
    )


def reset_progress():
    PROGRESS["status"] = "Bereit"
    PROGRESS["progress"] = 0
    PROGRESS["details"] = {"status": "Bereit", "progress": 0}


import asyncio

FOLDER_MAP = {
    "real": "1fyE_ZYoVoGZ7ehjuWrS9Kd6WW4w2UZWy",
    "top": "1uw14kdlhFbbEfobToLCP2A-NYH6QRfXF",
    "delete": "1wjUj6NHZ_ZHwlahQuJUbCTf_HplqePVw",
    "recheck": "1Ub8ULCBzQI5DvcJjKQbB7wEepa52Wmmj",
    "bad": "1EkX7TxoRJlYUyeNA10T3Gzdt5Nd7yRRf",
    "ki": "1LWF_V26zvX-W9vRNwscmeQ6U7YeJxOuL",
    "comfyui": "1UjmQV-dO3y8uhqmWjSIzU1t7w6-rQEqG",
    "document": "1oKNY7jB8hEFMEn6amA6Osrbo8K9z5jAW",
    "double": "16GyqMDHTCw-bdDjM3lYxoycn1P-pFa7s",
    "gemini": "1dO98jTeGTbQdwbBfyGj4IYcP1TTF9kaS",
    "sex": "1aaArEgGubDIpQJRZw3MaLWuRVDat5oWg",
}


async def folder_id_by_name(name: str):
    return FOLDER_MAP.get(name)


async def gdrive_list_folder(folder_name: str):
    folder_id = await folder_id_by_name(folder_name)
    if not folder_id:
        logger.warning(f"⚠️ Kein Folder ID für Kategorie {folder_name}")
        return []

    service = load_drive_service()
    query = (
        f"'{folder_id}' in parents "
        f"and trashed = false "
        f"and mimeType != 'application/vnd.google-apps.folder' "
        f"and mimeType != 'application/vnd.google-apps.shortcut'"
    )

    # Die echte Arbeit kommt in einen Thread!
    def do_request():
        files = []
        token = None
        while True:
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
        return files

    loop = asyncio.get_running_loop()
    files = await loop.run_in_executor(EXECUTOR, do_request)

    logger.info(f"📁 GDrive Folder {folder_name}: {len(files)} Dateien")
    return files


async def find_case_duplicates(folder_name: str, idx: int, total: int):
    global processed_files, total_files

    logger.info(f"🔍 Scanne Kategorie: {folder_name}")

    # Hauptfortschritt (Kategoriebalken)
    await set_progress(
        f"Kategorie {idx + 1}/{total}: {folder_name}",
        int((idx / total) * 80),
        detail_status="Initialisiere GDrive…",
        detail_progress=int(processed_files / total_files * 100) if total_files else 0
    )

    # 🔥 Sofort sichtbar, bevor erster API-Call blockiert
    await set_progress_detail(
        detail_status=f"{folder_name}: lade GDrive Dateien…",
        detail_progress=int(processed_files / total_files * 100) if total_files else 0
    )

    # ----------------------------------------------------------
    # 📁 GDRIVE SCAN
    # ----------------------------------------------------------
    gfiles = await gdrive_list_folder(folder_name)
    g_insert_before = sum(len(v.get("gdrive", [])) for v in GLOBAL_MD5_INDEX.values())

    for f in gfiles:
        if f["name"].lower().endswith(".json"):
            continue
        md5 = f.get("md5Checksum")
        if not md5:
            continue

        folder_id = (f.get("parents") or ["?"])[0]

        clean = sanitize_filename(f["name"])
        invalid_name = (clean != f["name"])

        GLOBAL_MD5_INDEX.setdefault(md5, {"local": [], "gdrive": []})
        GLOBAL_MD5_INDEX[md5]["gdrive"].append({
            "folder": folder_name,
            "folder_id": folder_id,
            "name": f["name"],
            "id": f["id"],
            "sanitized_name": clean,
            "is_invalid_name": invalid_name
        })

        if invalid_name:
            SCAN_CACHE["invalid_names"].append({
                "source": "gdrive",
                "folder": folder_name,
                "orig_name": f["name"],
                "clean_name": clean,
                "id": f["id"],
                "md5": md5,
            })

        # Fortschritt pro Datei
        processed_files += 1
        await set_progress_detail(
            detail_status=f"Scanne GDrive ({processed_files}/{total_files})",
            detail_progress=int(processed_files / total_files * 100)
        )

    g_insert_after = sum(len(v.get("gdrive", [])) for v in GLOBAL_MD5_INDEX.values())
    logger.info(f"📥 GDRIVE Insert: vorher={g_insert_before}, nachher={g_insert_after}")

    # ----------------------------------------------------------
    # 🖥 LOCAL SCAN
    # ----------------------------------------------------------
    lfiles = await local_list_folder(folder_name)
    l_insert_before = sum(len(v.get("local", [])) for v in GLOBAL_MD5_INDEX.values())

    loop = asyncio.get_running_loop()

    # Vor Beginn sichtbar machen
    await set_progress_detail(f"{folder_name}: scanne lokale Dateien…")

    for lf in lfiles:
        if lf["name"].lower().endswith(".json"):
            continue

        md5 = await loop.run_in_executor(EXECUTOR, compute_md5_file, lf["path"])

        clean = sanitize_filename(lf["name"])
        invalid_name = (clean != lf["name"])

        GLOBAL_MD5_INDEX.setdefault(md5, {"local": [], "gdrive": []})
        GLOBAL_MD5_INDEX[md5]["local"].append({
            "folder": folder_name,
            "path": lf["path"],
            "name": lf["name"],
            "sanitized_name": clean,
            "is_invalid_name": invalid_name
        })

        if invalid_name:
            uid = uuid4().hex
            UID_CACHE[uid] = {
                "source": "local",
                "folder": folder_name,
                "path": lf["path"],
                "orig_name": lf["name"],
                "clean_name": clean,
                "md5": md5,
            }
            SCAN_CACHE["invalid_names"].append({
                "source": "local",
                "folder": folder_name,
                "orig_name": lf["name"],
                "clean_name": clean,
                "path": lf["path"],
                "md5": md5,
                "uid": uid,
            })

        processed_files += 1
        await set_progress_detail(
            detail_status=f"Scanne Local ({processed_files}/{total_files})",
            detail_progress=int(processed_files / total_files * 100)
        )

    l_insert_after = sum(len(v.get("local", [])) for v in GLOBAL_MD5_INDEX.values())
    logger.info(f"📥 LOCAL Insert: vorher={l_insert_before}, nachher={l_insert_after}")

    # Kollisionen
    await filename_collision(folder_name)

    return {"folder": folder_name, "results": []}


async def filename_collision(folder_name: str):
    # -------------------------------------------------------
    # NEU: Dateinamen-Kollisionen erkennen
    # -------------------------------------------------------
    logger.info(f"🔍 Starte Filename-Kollisionsscan für Ordner: {folder_name}")

    name_map = {}  # name -> list of (md5, source, entry)

    # 1) lokale Dateien sammeln
    for md5, entry in GLOBAL_MD5_INDEX.items():
        for item in entry["local"]:
            name_map.setdefault(item["name"], [])
            name_map[item["name"]].append({
                "md5": md5,
                "source": "local",
                "entry": item
            })

    logger.info(f"📁 Lokale Dateien gesammelt: {sum(len(v) for v in name_map.values())}")

    # 2) gdrive Dateien sammeln
    for md5, entry in GLOBAL_MD5_INDEX.items():
        for item in entry["gdrive"]:
            name_map.setdefault(item["name"], [])
            name_map[item["name"]].append({
                "md5": md5,
                "source": "gdrive",
                "entry": item
            })

    logger.info(
        f"📁 Lokale + GDrive-Dateien total gesammelt für Namensmapping: "
        f"{sum(len(v) for v in name_map.values())}"
    )

    # 3) Kollisionen finden
    count = 50
    for filename, items in name_map.items():
        md5_values = {x["md5"] for x in items}

        if len(md5_values) > 1:
            # <<< HIER WICHTIG: md5 mit in die Einträge aufnehmen >>>
            local_entries = [
                {**x["entry"], "md5": x["md5"]}
                for x in items if x["source"] == "local"
            ]

            gdrive_entries = [
                {**x["entry"], "md5": x["md5"]}
                for x in items if x["source"] == "gdrive"
            ]

            logger.warning(
                f"🔥 Kollision erkannt: '{filename}' in Ordner '{folder_name}' → "
                f"{len(local_entries)} lokal, {len(gdrive_entries)} gdrive, "
                f"MD5s={list(md5_values)}"
            )

            SCAN_CACHE["filename_collisions"].append({
                "folder": folder_name,
                "name": filename,
                "local": local_entries,
                "gdrive": gdrive_entries,
                "md5_list": list(md5_values)
            })
            count -= 1
            if count < 0:
                break


async def run_full_scan():
    global SCAN_CACHE, GLOBAL_MD5_INDEX, processed_files, total_files

    reset_progress()

    GLOBAL_MD5_INDEX.clear()
    UID_CACHE.clear()

    SCAN_CACHE = {
        "categories": [],
        "invalid_md5": [],
        "invalid_names": [],
        "filename_collisions": [],
    }

    Settings._user_type = UserType.ADMIN
    categories = [c["key"] for c in Settings.kategorien() if c["key"] != "XXXX"]
    total_categories = len(categories)

    processed_files = 0
    total_files = 0

    # ----------------------------------------------------------
    # PHASE 1 — Dateizählung (0–10%)
    # ----------------------------------------------------------
    await set_progress(
        "Zähle Dateien…",
        1,
        detail_status="Vorbereitung…",
        detail_progress=0
    )

    for idx, cat in enumerate(categories):
        await set_progress(
            f"Zähle Dateien ({idx + 1}/{total_categories}): {cat}",
            int((idx / total_categories) * 10),
            detail_status=f"Scanne Ordnerliste für {cat}",
            detail_progress=0
        )

        lfiles = await local_list_folder(cat)
        total_files += len(lfiles)

        gfiles = await gdrive_list_folder(cat)
        total_files += len(gfiles)

    logger.info(f"📊 Total Files to scan: {total_files}")

    # ----------------------------------------------------------
    # PHASE 2 — eigentlicher Scan (10–90%)
    # ----------------------------------------------------------
    out = []

    for idx, cat in enumerate(categories):
        main_progress = 10 + int((idx / total_categories) * 80)
        await set_progress(
            f"Scanne Kategorie {idx + 1}/{total_categories}: {cat}",
            main_progress,
            detail_status=f"Verarbeite… {processed_files}/{total_files}",
            detail_progress=int((processed_files / total_files) * 100)
        )

        result = await find_case_duplicates(cat, idx, total_categories)
        out.append(result)

    # ----------------------------------------------------------
    # PHASE 3 — MD5 Validierung (90–100%)
    # ----------------------------------------------------------
    await set_progress(
        "Prüfe MD5-Konsistenz…",
        90,
        detail_status="Analysiere Hash-Anzahl…",
        detail_progress=100
    )

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

    SCAN_CACHE["categories"] = out
    SCAN_CACHE["invalid_md5"] = invalid_md5

    # ----------------------------------------------------------
    # PHASE 4 — Fertig
    # ----------------------------------------------------------
    await set_progress("Fertig", 100, "Fertig", 100)
    logger.info("🟢 Globaler MD5-Scan abgeschlossen")


@router.get("/diff_gdrive_local", response_class=HTMLResponse)
async def diff_gdrive_local(request: Request):
    categories = SCAN_CACHE.get("categories", [])
    invalid_md5 = SCAN_CACHE.get("invalid_md5", [])
    invalid_names = SCAN_CACHE.get("invalid_names", [])
    filename_collisions = SCAN_CACHE.get("filename_collisions", [])

    logger.info(f"categories   : {len(categories)}")
    logger.info(f"invalid_md5  : {len(invalid_md5)}")
    logger.info(f"invalid_names: {len(invalid_names)}")
    logger.info(f"filename_collisions: {len(filename_collisions)}")

    return templates.TemplateResponse(
        "diff_gdrive_local.j2",
        {
            "request": request,
            "categories": categories,
            "invalid_md5": invalid_md5,
            "invalid_names": invalid_names,
            "filename_collisions": filename_collisions,
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


async def resolve_drive_path(drive, path_segments):
    """
    Gibt die ID des Zielordners zurück.
    Legt NIE Ordner an.
    """
    try:
        parent_id = await folder_id_by_name("imagefiles")
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

    drive = load_drive_service()

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
        return None


async def sync_to_gdrive(local_path: str):
    """Local → GDrive (Ordner muss existieren im GDrive!)"""

    drive = load_drive_service()

    try:
        # relativer Pfad unter imagefiles
        rel = os.path.relpath(local_path, LOCAL_BASE)
        parts = rel.split("/")
        folder_parts = parts[:-1]
        filename = parts[-1]

        # GDrive-Ordner nachschlagen
        parent_id = await resolve_drive_path(drive, folder_parts)

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
        return None


@router.post("/diff_gdrive_local_delete")
async def diff_gdrive_local_delete(request: Request):
    """
    Löscht ausgewählte Dateien (local oder gdrive)
    UND synchronisiert ausgewählte sync_ids.
    OHNE Ordner anzulegen – alle Ordner müssen existieren!
    Danach werden betroffene Einträge aus SCAN_CACHE entfernt.
    """

    form = await request.form()
    drive = load_drive_service()

    rename_ids = form.getlist("rename_ids")
    renamed_local = []
    renamed_gdrive = []

    for source_id in rename_ids:
        # ----------------------------------------------------------
        # A) LOCAL rename (source_id = UID!)
        # ----------------------------------------------------------
        if source_id in UID_CACHE:
            info = UID_CACHE[source_id]

            real_path = info["path"]  # absoluter echter Dateipfad
            new_name = info["clean_name"]
            folder = os.path.dirname(real_path)
            new_path = os.path.join(folder, new_name)

            try:
                os.rename(real_path, new_path)
                renamed_local.append({"old": real_path, "new": new_path})
                logger.info(f"✏️ Lokal umbenannt: {real_path} → {new_path}")
            except Exception as e:
                logger.error(f"Fehler beim lokalen Umbenennen: {real_path}: {e}")
                continue

            del UID_CACHE[source_id]

            SCAN_CACHE["invalid_names"] = [
                x for x in SCAN_CACHE["invalid_names"]
                if not (x["source"] == "local" and x.get("uid") == source_id)
            ]

            # MD5-Index aktualisieren
            for md5, entry in GLOBAL_MD5_INDEX.items():
                for item in entry["local"]:
                    if item.get("path") == real_path:
                        item["name"] = new_name
                        item["path"] = new_path
                        item["sanitized_name"] = sanitize_filename(new_name)
                        item["is_invalid_name"] = (item["sanitized_name"] != new_name)

        # ----------------------------------------------------------
        # B) GDRIVE rename
        # ----------------------------------------------------------
        else:
            match = next(
                (x for x in SCAN_CACHE["invalid_names"]
                 if x["source"] == "gdrive" and x["id"] == source_id),
                None
            )

            if not match:
                logger.error(f"Kein clean_name für GDrive-ID {source_id} gefunden!")
                continue

            new_name = match["clean_name"]
            try:
                drive.files().update(
                    fileId=source_id,
                    body={"name": new_name},
                    fields="id,name"
                ).execute()

                renamed_gdrive.append({"id": source_id, "new": new_name})
                logger.info(f"✏️ GDrive umbenannt: {source_id} → {new_name}")

            except Exception as e:
                logger.error(f"GDrive-Umbenennfehler bei {source_id}: {e}")
                continue

            # SCAN_CACHE invalid_names aktualisieren
            SCAN_CACHE["invalid_names"] = [
                x for x in SCAN_CACHE["invalid_names"]
                if not (x["source"] == "gdrive" and x["id"] == source_id)
            ]

            # MD5-Index aktualisieren
            for md5, entry in GLOBAL_MD5_INDEX.items():
                for item in entry["gdrive"]:
                    if item.get("id") == source_id:
                        item["name"] = new_name
                        item["sanitized_name"] = sanitize_filename(new_name)
                        item["is_invalid_name"] = (item["sanitized_name"] != new_name)

    deleted_local = []
    deleted_gdrive = []
    synced_local = []
    synced_gdrive = []
    errors = []

    # ======================================================
    # 1) SYNC verarbeiten
    # ======================================================
    sync_ids = form.getlist("sync_ids")
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
    # 3.a) DELETE verarbeiten
    # ======================================================

    delete_ids = form.getlist("delete_ids")
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
    # 3.b) UNIQUE FILENAMES → echte Umbenennung
    # ======================================================

    unique_ids = form.getlist("unique_filename_ids")
    renamed_unique = []  # optional für UI

    for uid in unique_ids:

        # 🔍 1) passende Collision-Gruppe finden
        collision_group = next(
            (cg for cg in SCAN_CACHE.get("filename_collisions", [])
             if any(x.get("path") == uid for x in cg["local"])
             or any(x.get("id") == uid for x in cg["gdrive"])),
            None
        )

        if not collision_group:
            logger.error(f"❌ Keine Collision-Gruppe für {uid} gefunden!")
            continue

        filename = collision_group["name"]
        md5 = None

        # herausfinden, welche Datei es ist
        entry_local = next((x for x in collision_group["local"] if x.get("path") == uid), None)
        entry_gdrive = next((x for x in collision_group["gdrive"] if x.get("id") == uid), None)

        if entry_local:
            md5 = entry_local["md5"]
        if entry_gdrive:
            md5 = entry_gdrive["md5"]

        if not md5:
            logger.error(f"❌ Kein MD5 gefunden für {uid}")
            continue

        new_name = f"{md5}_{filename}"

        # ====================================================
        # A) LOKAL
        # ====================================================
        if entry_local:
            old_path = entry_local["path"]
            folder = os.path.dirname(old_path)
            new_path = os.path.join(folder, new_name)

            try:
                os.rename(old_path, new_path)
                logger.info(f"🔁 Lokal eindeutig umbenannt: {old_path} → {new_path}")

                renamed_unique.append({"old": old_path, "new": new_path})

                # UPDATE GLOBAL_MD5_INDEX
                for md5_key, grp in GLOBAL_MD5_INDEX.items():
                    for item in grp["local"]:
                        if item.get("path") == old_path:
                            item["name"] = new_name
                            item["path"] = new_path

            except Exception as e:
                errors.append(f"Fehler beim lokalen eindeutigen Umbenennen {old_path}: {e}")
                continue

        # ====================================================
        # B) GDRIVE
        # ====================================================
        if entry_gdrive:
            file_id = entry_gdrive["id"]
            try:
                drive.files().update(
                    fileId=file_id,
                    body={"name": new_name},
                    fields="id,name"
                ).execute()

                logger.info(f"🔁 GDrive eindeutig umbenannt: {file_id} → {new_name}")
                renamed_unique.append({"id": file_id, "new": new_name})

                # UPDATE GLOBAL_MD5_INDEX
                for md5_key, grp in GLOBAL_MD5_INDEX.items():
                    for item in grp["gdrive"]:
                        if item.get("id") == file_id:
                            item["name"] = new_name

            except Exception as e:
                errors.append(f"Fehler GDrive eindeutiges Umbenennen {file_id}: {e}")
                continue

    # Nachher die Collision-Gruppe löschen, da kein Konflikt mehr
    SCAN_CACHE["filename_collisions"] = [
        cg for cg in SCAN_CACHE["filename_collisions"]
        if not any(uid == x.get("path") or uid == x.get("id")
                   for x in cg["local"] + cg["gdrive"])
    ]

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
    # Filename-Collisions nach Operationen aktualisieren
    # ======================================================

    old_cols = SCAN_CACHE.get("filename_collisions", [])
    new_cols = []

    # Alle IDs, die in dieser Operation angefasst wurden:
    touched_ids = set(delete_ids) | set(unique_ids) | set(rename_ids)

    for item in old_cols:

        # Wenn irgendein Eintrag der Gruppe angefasst wurde → ganze Gruppe entfernen
        group_ids = set()

        for x in item["local"]:
            if "path" in x:
                group_ids.add(x["path"])

        for x in item["gdrive"]:
            if "id" in x:
                group_ids.add(x["id"])

        if group_ids & touched_ids:
            logger.info(f"🧹 Filename-Kollision vollständig entfernt: {item['name']}")
            continue  # ganze Gruppe skippen → wird gelöscht

        # --- Falls nicht berührt, wird sie übernommen ---
        new_cols.append(item)

    SCAN_CACHE["filename_collisions"] = new_cols

    # ======================================================
    # 6) Ergebnis
    # ======================================================

    return templates.TemplateResponse(
        "diff_gdrive_local_done.j2",
        {
            "request": request,
            "version": VERSION,
            "renamed_local": renamed_local,
            "renamed_gdrive": renamed_gdrive,
            "deleted_local": deleted_local,
            "deleted_gdrive": deleted_gdrive,
            "synced_local": synced_local,
            "synced_gdrive": synced_gdrive,
            "errors": errors,
        },
    )
