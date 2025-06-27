import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import Dict, List

from googleapiclient.http import MediaFileUpload
from tqdm import tqdm

from app.config import Settings
from app.config_gdrive import sanitize_filename, folder_id_by_name, get_all_subfolders, SettingsGdrive, calculate_md5
from app.database import load_folder_status_from_db
from app.routes.auth import load_drive_service_token
from app.routes.dashboard_help import fillcache_local
from app.tools import find_image_name_by_id
from app.utils.logger_config import setup_logger
from app.utils.progress import init_progress_state, progress_state, update_progress, stop_progress, save_simple_hashes, \
    update_progress_text

logger = setup_logger(__name__)


def move_file_to_folder(service, file_id: str, target_folder_id: str):
    file = service.files().get(fileId=file_id, fields="parents").execute()
    previous_parents = ",".join(file.get("parents", []))
    service.files().update(
        fileId=file_id,
        addParents=target_folder_id,
        removeParents=previous_parents,
        fields="id, parents"
    ).execute()


def move_file_to_folder_new(service, file_id, old_parents, new_parent):
    service.files().update(
        fileId=file_id,
        addParents=new_parent,
        removeParents=",".join(old_parents),
        fields='id, parents'
    ).execute()


async def gdrive_from_lokal(service, folder_name: str):
    logger.info(f"gdrive_from_lokal: {folder_name}")

    cache_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)

    global_gdrive_hashes = load_all_gdrive_hashes(cache_dir)
    folder_id_map = build_folder_id_map(service)
    hashfiles = list(cache_dir.rglob("gallery202505_hashes.json"))

    for gallery_hashfile in hashfiles:
        try:
            with gallery_hashfile.open("r", encoding="utf-8") as f:
                local_hashes = json.load(f)
        except Exception:
            continue

    for gallery_hashfile in hashfiles:
        folder_path = gallery_hashfile.parent
        folder = folder_path.name
        await update_progress_text(f"folder: {folder}")
        if not (folder == folder_name):
            continue

        gdrive_hashfile = folder_path / Settings.GDRIVE_HASH_FILE

        try:
            with gallery_hashfile.open("r", encoding="utf-8") as f:
                local_hashes = json.load(f)
        except Exception as e:
            logger.error(f"[Fehler] {gallery_hashfile}: {e}")
            continue

        try:
            with gdrive_hashfile.open("r", encoding="utf-8") as f:
                gdrive_hashes = json.load(f)
        except Exception:
            gdrive_hashes = {}

        updated = False

        await init_progress_state()
        progress_state["progress"] = 0

        total = len(local_hashes)
        if total > 0:
            count = 0
            for name, md5 in local_hashes.items():
                existing = gdrive_hashes.get(name)
                current_md5 = existing.get("md5") if isinstance(existing, dict) else existing

                if name not in gdrive_hashes or current_md5 != md5:
                    file_info = global_gdrive_hashes.get(md5)
                    if file_info:
                        await update_progress_text(
                            f"[✓] {name} fehlt in {folder}, aber global vorhanden als: {file_info['name']}")
                        file_id = file_info.get("id")
                        if file_id:
                            target_folder_id = folder_id_map.get(folder)
                            if not target_folder_id:
                                logger.warning(f"[!] Keine Ordner-ID für {folder} gefunden")
                                count += 1
                                progress_state["progress"] = int((count / total) * 100)
                                continue
                            try:
                                move_file_to_folder(service, file_id, target_folder_id)
                                gdrive_hashes[name] = {
                                    "md5": file_info["md5"],
                                    "id": file_id
                                }
                                updated = True
                            except Exception as e:
                                logger.error(f"[Fehler beim Verschieben] {name}: {e}")
                    else:
                        local_file = folder_path / name
                        if local_file.exists():
                            target_folder_id = folder_id_map.get(folder)
                            if target_folder_id:
                                try:
                                    file_metadata = {"name": name, "parents": [target_folder_id]}
                                    media = MediaFileUpload(str(local_file), resumable=True)
                                    uploaded = service.files().create(
                                        body=file_metadata,
                                        media_body=media,
                                        fields="id"
                                    ).execute()
                                    gdrive_hashes[name] = {
                                        "md5": md5,
                                        "id": uploaded["id"]
                                    }
                                    updated = True
                                    await update_progress_text(f"[↑] {name} hochgeladen in {folder}")
                                except Exception as e:
                                    logger.error(f"[Fehler beim Hochladen] {name}: {e}")
                            else:
                                logger.warning(f"[!] Keine Zielordner-ID für {folder} gefunden")
                        else:
                            logger.warning(f"[!] {name} fehlt in {folder} und global nicht gefunden")

                count += 1
                if total > 0:
                    progress_state["progress"] = int((count / total) * 100)

            if updated:
                save_gdrive_hashes(gdrive_hashes, gdrive_hashfile, folder)

    if await lokal_from_gdrive_move(service, folder_name):
        await process_image_folders_gdrive(
            service,
            Settings.IMAGE_EXTENSIONS,
            Settings.IMAGE_FILE_CACHE_DIR,
            [folder_name])

    await stop_progress()


def save_gdrive_hashes(gdrive_hashes: Dict, hashfile_path: Path, folder: str) -> None:
    """
    Speichert die Google Drive Hashes sicher in einer JSON-Datei.
    """
    import fcntl  # Für File-Locking

    # Temporäre Datei im gleichen Verzeichnis
    temp_path = hashfile_path.with_suffix('.json.tmp')

    try:
        # Schreibe erst in temporäre Datei
        with temp_path.open("w", encoding="utf-8") as f:
            # File-Lock setzen
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                # Validiere und schreibe Daten
                for key in gdrive_hashes:
                    if isinstance(gdrive_hashes[key], dict):
                        # Stelle sicher, dass md5 und id vorhanden sind
                        gdrive_hashes[key]["md5"] = gdrive_hashes[key].get("md5", "")
                        gdrive_hashes[key]["id"] = gdrive_hashes[key].get("id", "")

                json.dump(gdrive_hashes, f, indent=2)
                f.flush()  # Stelle sicher, dass alles geschrieben wurde
                os.fsync(f.fileno())  # Synchronisiere mit Festplatte
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

        # Atomar umbenennen
        temp_path.replace(hashfile_path)
        logger.info(f"[✓] {Settings.GDRIVE_HASH_FILE} aktualisiert für Ordner {folder}")

    except Exception as e:
        logger.error(f"[Fehler] Konnte Hashes nicht in {hashfile_path} speichern: {e}")
        if temp_path.exists():
            temp_path.unlink()  # Lösche temporäre Datei bei Fehler


async def save_structured_hashes(hashes: Dict[str, Dict[str, str]], hashfile_path: Path):
    """
    Speichert strukturierte Hashes in einer JSON-Datei.

    Args:
        hashes: Dictionary mit den zu speichernden Hashes
        hashfile_path: Path-Objekt zum Speicherort der Datei
    """
    try:
        hashfile_path.parent.mkdir(parents=True, exist_ok=True)
        with hashfile_path.open("w", encoding="utf-8") as f:
            json.dump(hashes, f, indent=2)
        os.chmod(hashfile_path, 0o644)
        logger.info(f"[✓] Gespeichert: {hashfile_path}/{Settings.GDRIVE_HASH_FILE}")
    except Exception as e:
        logger.error(f"[Fehler] Konnte Hashes nicht in {hashfile_path} speichern: {e}")


async def process_image_folders_gdrive(service, extensions, file_folder_dir, folder_names: List[str]):
    subfolders = False

    for folder_name in folder_names:
        root_id = folder_id_by_name(folder_name)
        all_ids = get_all_subfolders(service, root_id) if subfolders else [root_id]
        for folder_id in all_ids:
            files = []
            page_token = None
            query = f"'{folder_id}' in parents and trashed = false"

            with tqdm(desc=f"Lade aus {folder_id[:6]}...", unit="Seite") as pbar:
                while True:
                    response = service.files().list(
                        q=query,
                        spaces='drive',
                        fields="nextPageToken, files(id, name, size, parents, md5Checksum)",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                        pageSize=Settings.PAGESIZE,
                        pageToken=page_token
                    ).execute()
                    batch = response.get('files', [])
                    files.extend([
                        f for f in batch
                        if isinstance(f.get('name'), str) and f['name'].lower().endswith(tuple(extensions))
                    ])
                    page_token = response.get('nextPageToken')
                    pbar.update(1)
                    if not page_token:
                        break

            gdrive_hashes: Dict[str, Dict[str, str]] = {}
            if files:
                folder = service.files().get(fileId=folder_id, fields="name").execute()

                for file in files:
                    try:
                        name = sanitize_filename(file['name'])
                        md5_drive = file.get("md5Checksum")
                        if md5_drive:
                            gdrive_hashes[name] = {
                                "md5": md5_drive,
                                "id": file['id']
                            }
                    except Exception as e:
                        logger.error(f"[Fehler] {file['name']}: {e}")

            # Convert file_folder_dir to Path if it isn't already
            base_dir = Path(file_folder_dir)
            local_dir = base_dir / folder_name

            # Ensure directory exists
            local_dir.mkdir(parents=True, exist_ok=True)

            # Create the final path for the hash file
            hash_file_path = local_dir / Settings.GDRIVE_HASH_FILE

            # Save the hashes
            await save_structured_hashes(gdrive_hashes, hash_file_path)


def load_all_gdrive_hashes(cache_dir: Path) -> Dict[str, Dict[str, str]]:
    global_hashes = {}
    hashfiles = list(cache_dir.rglob(Settings.GDRIVE_HASH_FILE))
    for hashfile in hashfiles:
        try:
            with hashfile.open("r", encoding="utf-8") as f:
                data = json.load(f)
                for name, entry in data.items():
                    if isinstance(entry, dict) and 'md5' in entry and 'id' in entry:
                        global_hashes[entry['md5']] = {
                            "name": name,
                            "id": entry['id'],
                            "md5": entry['md5']
                        }
        except Exception as e:
            logger.error(f"[Fehler] {hashfile}: {e}")
    return global_hashes


def build_folder_id_map(service) -> Dict[str, str]:
    folder_map = {}
    page_token = None
    while True:
        response = service.files().list(
            q="mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=Settings.PAGESIZE,
            pageToken=page_token
        ).execute()
        for file in response.get("files", []):
            folder_map[file["name"]] = file["id"]
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return folder_map


def move_file_in_gdrive(service, file_id: str, file_name: str, target_folder_name: str, folder_id_map: dict) -> bool:
    """
    Verschiebt eine Datei in Google Drive in einen anderen Ordner.

    Args:
        service: Google Drive Service Objekt
        file_id: ID der zu verschiebenden Datei
        file_name: Name der Datei (für Logging)
        target_folder_name: Name des Zielordners
        folder_id_map: Dictionary mit Ordnernamen zu Folder IDs

    Returns:
        bool: True wenn erfolgreich verschoben, False bei Fehler
    """
    try:
        target_folder_id = folder_id_map.get(target_folder_name)
        if not target_folder_id:
            logger.error(f"[MOVE-ERROR] Could not find folder ID for {target_folder_name}")
            return False

        # Get current file parents
        file = service.files().get(
            fileId=file_id,
            fields='parents'
        ).execute()

        current_parents = file.get('parents', [])

        # Check if file is already in target folder
        if target_folder_id in current_parents:
            logger.info(f"[SKIP] {file_name} already in target folder {target_folder_name}")
            return True

        previous_parents = ",".join(current_parents)

        # Move the file to the new folder
        file = service.files().update(
            fileId=file_id,
            addParents=target_folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()

        logger.info(f"[MOVED] {file_name} to folder {target_folder_name}")
        return True

    except Exception as e:
        logger.error(f"[MOVE-ERROR] Failed to move {file_name}: {e}")
        return False


async def lokal_from_gdrive_move(service, folder_name: str) -> bool:
    """
    Überprüft und verschiebt Dateien in Google Drive basierend auf lokalen Hash-Vergleichen.

    Args:
        service: Google Drive Service Objekt
        folder_name: Name des zu überprüfenden Ordners
    """
    logger.info(f"lokal_from_gdrive_move: {folder_name}")
    await init_progress_state()
    progress_state["running"] = True

    def find_local(md5: str, hash_cache: dict) -> str | None:
        """
        Sucht nach einer Datei mit gegebenem MD5-Hash in allen Galleries.
        """
        logger.debug(f"find_local: {md5}")

        for folder_name, hashes in hash_cache.items():
            if md5 in hashes.values():
                logger.debug(f"[FOUND] MD5 {md5} in {folder_name}")
                return folder_name
        return None

    # Initialization phase
    await update_progress("Initialisiere...", 0)
    cache_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
    hashfiles = list(cache_dir.rglob("gallery202505_hashes.json"))

    # Build hash cache
    await update_progress("Lade Hash-Cache...", 10)
    hash_cache = {}
    for i, gallery_hashfile in enumerate(hashfiles):
        try:
            with gallery_hashfile.open("r", encoding="utf-8") as f:
                folder_name = gallery_hashfile.parent.name
                hash_cache[folder_name] = json.load(f)
        except Exception as e:
            logger.error(f"[Fehler] {gallery_hashfile}: {e}")
            continue
        progress = int(10 + (i / len(hashfiles) * 20))  # 10-30%
        await update_progress("Lade Hash-Cache...", progress, 0.01)

    # Build folder ID map
    await update_progress("Lade Google Drive Ordner...", 30)
    folder_id_map = {}
    results = service.files().list(
        q="mimeType='application/vnd.google-apps.folder'",
        fields="files(id, name)"
    ).execute()
    for folder in results.get('files', []):
        folder_id_map[folder['name']] = folder['id']

    # Count total files to process
    await update_progress("Zähle zu verarbeitende Dateien...", 40)
    total_files = 0
    for gallery_hashfile in hashfiles:
        if gallery_hashfile.parent.name == folder_name:
            gdrive_hashfile = gallery_hashfile.parent / Settings.GDRIVE_HASH_FILE
            try:
                with gdrive_hashfile.open("r", encoding="utf-8") as f:
                    gdrive_hashes = json.load(f)
                    total_files = len(gdrive_hashes)
                    break
            except Exception:
                continue

    # Main processing
    processed_files = 0
    moved_count = 0
    missing_count = 0

    for gallery_hashfile in hashfiles:
        folder_path = gallery_hashfile.parent
        folder = folder_path.name
        if not (folder == folder_name):
            continue

        local_hashes = hash_cache.get(folder, {})
        gdrive_hashfile = folder_path / Settings.GDRIVE_HASH_FILE

        try:
            with gdrive_hashfile.open("r", encoding="utf-8") as f:
                gdrive_hashes = json.load(f)
        except Exception:
            gdrive_hashes = {}

        logger.info(f"Prüfe Dateien in GDrive für Ordner {folder_name}...")

        for name, entry in gdrive_hashes.items():
            gdrive_md5 = entry.get("md5") if isinstance(entry, dict) else entry
            file_id = entry.get("id") if isinstance(entry, dict) else None

            # Check if file exists in local hashes with same md5
            found = False
            for local_name, local_md5 in local_hashes.items():
                if local_md5 == gdrive_md5:
                    found = True
                    break

            if not found:
                curr_folder = find_local(gdrive_md5, hash_cache)
                if curr_folder and file_id:
                    if curr_folder == folder_name:
                        logger.info(f"[SKIP] {name} bereits in korrektem Ordner {curr_folder}")
                        continue
                    logger.info(f"[VERSCHIEBE] {name} nach {curr_folder}")
                    if move_file_in_gdrive(service, file_id, name, curr_folder, folder_id_map):
                        moved_count += 1
                else:
                    logger.warning(f"[FEHLT] {name}")
                missing_count += 1

            processed_files += 1
            if total_files > 0:
                progress = int(40 + (processed_files / total_files * 60))  # 40-100%
                status = (f"Verarbeite Dateien... {processed_files}/{total_files} "
                          f"({moved_count} verschoben, {missing_count} fehlend)")
                await update_progress(status, progress, 0.01)

    logger.info(f"Zusammenfassung für {folder_name}:")
    logger.info(f"- {missing_count} Dateien fehlen")
    logger.info(f"- {moved_count} Dateien wurden verschoben")

    return missing_count > 0 or moved_count > 0


def write_local_hashes(extensions, file_folder_dir, subfolders: bool = True):
    root = Path(file_folder_dir)
    all_dirs = [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]
    with tqdm(total=len(all_dirs), desc="Erzeuge lokale Hashes", unit="Ordner") as pbar:
        for subdir in all_dirs:
            local_hashes: Dict[str, str] = {}
            image_files = [f for f in subdir.iterdir() if f.is_file() and f.suffix.lower() in extensions]
            with tqdm(total=len(image_files), desc=f"{subdir.name}", unit="Bild", leave=False) as inner:
                for file in image_files:
                    try:
                        md5_local = calculate_md5(file)
                        local_hashes[file.name] = md5_local
                    except Exception as e:
                        tqdm.write(f"[Fehler] {file.name}: {e}")
                    inner.update(1)
            hashfile_name = Settings.GALLERY_HASH_FILE
            save_simple_hashes(local_hashes, subdir / hashfile_name)
            tqdm.write(f"[✓] Lokale Hashes gespeichert: {subdir / hashfile_name}")
            pbar.update(1)


def find_image_file(image_name: str) -> Path | None:
    """
    Sucht rekursiv nach einer Bilddatei im IMAGE_FILE_CACHE_DIR.

    Args:
        image_name: Name der zu suchenden Datei

    Returns:
        Path Objekt zur gefundenen Datei oder None wenn nicht gefunden
    """
    cache_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
    for file_path in cache_dir.rglob(image_name):
        if file_path.is_file():
            return file_path
    return None


def p1():
    # Basis-Pfade
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"

    # Token-Pfad für Google Drive
    token_path = os.path.abspath(os.path.join("../../secrets", "token.json"))

    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    Settings.DB_PATH = '../../gallery_local.db'
    Settings.RENDERED_HTML_DIR = "../../cache/rendered_html"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.SAVE_LOG_FILE = "../../cache/from_save_"

    fillcache_local(Settings.PAIR_CACHE_PATH, Settings.IMAGE_FILE_CACHE_DIR)

    write_local_hashes(Settings.IMAGE_EXTENSIONS, Settings.IMAGE_FILE_CACHE_DIR)

    hash_cache = {}

    def find_local(md5: str, hash_cache: dict) -> str | None:
        """
        Sucht nach einer Datei mit gegebenem MD5-Hash in allen Galleries.
        """
        logger.debug(f"find_local: {md5}")

        for folder_name, hashes in hash_cache.items():
            if md5 in hashes.values():
                logger.debug(f"[FOUND] MD5 {md5} in {folder_name}")
                return folder_name
        return None

    for eintrag in Settings.kategorien:
        folder_key = eintrag["key"]

        local_path = Path(Settings.IMAGE_FILE_CACHE_DIR + "/" + folder_key) / Settings.GALLERY_HASH_FILE

        try:
            with local_path.open("r", encoding="utf-8") as f:
                dir_cache = json.load(f)
                dir_cache = dir_cache if isinstance(dir_cache, dict) else {}
                hash_cache[folder_key] = dir_cache  # Store the directory cache with folder key
        except Exception as e:
            logger.error(f"Error reading {local_path}: {e}")
            hash_cache[folder_key] = {}

    rows = load_folder_status_from_db(Settings.DB_PATH)
    logger.info(f"Anzahl DB: {len(rows)}")

    count = 0
    if rows:
        for image_id, folder_key in tqdm(rows, desc="Processing images", unit="image"):
            image_name = find_image_name_by_id(image_id)
            if not image_name:
                logger.info(f"not image_name: {image_name}")
                continue
            file_path = find_image_file(image_name)
            if not file_path:
                logger.info(f"not image_name: {image_name}")
                continue
            else:
                # Prüfen, ob das aktuelle Verzeichnis vom gewünschten abweicht
                if folder_key != file_path.parent.name:
                    # Neues Zielverzeichnis erstellen (falls nicht vorhanden)
                    new_dir = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_key
                    new_dir.mkdir(parents=True, exist_ok=True)

                    new_file_path = new_dir / image_name
                    try:
                        # Datei verschieben
                        shutil.move(str(file_path), str(new_file_path))
                        logger.info(f"Verschoben: {file_path} → {new_file_path}")
                    except Exception as e:
                        logger.error(f"Fehler beim Verschieben von {file_path} nach {new_file_path}: {e}")
                        continue
                count = count + 1
            # continue
            # curr_folder = find_local(image_id, hash_cache)
            # if not curr_folder:
            #     logging.info(f"not image_id: {image_id}")
        logger.info(f"count: {count}")

    if count > 0:
        write_local_hashes(Settings.IMAGE_EXTENSIONS, Settings.IMAGE_FILE_CACHE_DIR)

    # asyncio.run(gdrive_from_lokal(
    #     load_drive_service_token(token_path),
    #     "recheck"
    # ))


def p2():
    # Basis-Pfade
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"

    # Token-Pfad für Google Drive
    token_path = os.path.abspath(os.path.join("../../secrets", "token.json"))

    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    Settings.DB_PATH = '../../gallery_local.db'
    Settings.RENDERED_HTML_DIR = "../../cache/rendered_html"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.SAVE_LOG_FILE = "../../cache/from_save_"

    service = load_drive_service_token(token_path)

    fillcache_local(Settings.PAIR_CACHE_PATH, Settings.IMAGE_FILE_CACHE_DIR)

    hash_cache = {}

    for eintrag in Settings.kategorien:
        folder_key = eintrag["key"]

        local_path = Path(Settings.IMAGE_FILE_CACHE_DIR + "/" + folder_key) / Settings.GDRIVE_HASH_FILE

        try:
            # Read and parse the hash file
            with local_path.open("r", encoding="utf-8") as f:
                dir_cache = json.load(f)
                dir_cache = dir_cache if isinstance(dir_cache, dict) else {}
                hash_cache[folder_key] = dir_cache

        except Exception as e:
            logger.error(f"Error reading {local_path}: {e}")
            # Generate new hash file for this folder
            try:
                logger.info(f"Attempting to regenerate hash file for folder: {folder_key}")
                process_image_folders_gdrive(service, Settings.IMAGE_EXTENSIONS, Settings.IMAGE_FILE_CACHE_DIR,
                                             [folder_id_by_name(folder_key)])

                # Try to read the newly generated file
                if local_path.exists():
                    with local_path.open("r", encoding="utf-8") as f:
                        dir_cache = json.load(f)
                        dir_cache = dir_cache if isinstance(dir_cache, dict) else {}
                        hash_cache[folder_key] = dir_cache
                        logger.info(f"Successfully regenerated and loaded hash file for {folder_key}")
                else:
                    logger.error(f"Failed to generate hash file for {folder_key}")
                    hash_cache[folder_key] = {}
            except Exception as regen_error:
                logger.error(f"Error regenerating hash file for {folder_key}: {regen_error}")
                hash_cache[folder_key] = {}

    # Log the number of entries for each folder
    for folder_key, cache_data in hash_cache.items():
        logger.info(f"Anzahl Einträge in {folder_key}: {len(cache_data)}")

    logger.info(f"Anzahl Ordner gesamt: {len(hash_cache)}")

    process_image_folders_gdrive(service, Settings.IMAGE_EXTENSIONS, Settings.TEXT_FILE_CACHE_DIR,
                                 ["textfiles"])


if __name__ == "__main__":
    p2()
