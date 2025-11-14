import asyncio
import json
import os
from pathlib import Path
from typing import Dict
from typing import Set
from typing import Tuple

from .auth import load_drive_service_token
from .hashes import delete_duplicates_in_gdrive_folder
from .hashes import update_gdrive_hashes
from .hashes import upload_file_to_gdrive
from .manage_text_files import check_and_move_gdrive_files
from .manage_text_files import gdrive_textfiles_files_by_local
from .manage_text_files import move_file_to_folder
from ..config import Settings
from ..config_gdrive import SettingsGdrive
from ..config_gdrive import folder_id_by_name
from ..utils.logger_config import setup_logger
from ..utils.progress import init_progress_state
from ..utils.progress import stop_progress
from ..utils.progress import update_progress_auto
from ..utils.progress import update_progress_text
from ..utils.progress_detail import calc_detail_progress
from ..utils.progress_detail import start_detail_progress
from ..utils.progress_detail import stop_detail_progress
from ..utils.progress_detail import update_detail_progress
from ..utils.progress_detail import update_detail_status

logger = setup_logger(__name__)


def load_local_hashes(folder_path: Path) -> Dict[str, str]:
    """Load local hashes for the given folder path."""
    try:
        with (folder_path / Settings.GALLERY_HASH_FILE).open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        return {"__error__": str(e)}


def load_all_gdrive_hashes(cache_dir: Path) -> Dict[str, Dict]:
    """Aggregate GDrive hashes across all categories, tagging with the source folder."""
    all_hashes: Dict[str, Dict] = {}
    for cat in Settings.kategorien():
        path = cache_dir / cat["key"] / Settings.GDRIVE_HASH_FILE
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            for name, entry in data.items():
                if isinstance(entry, dict):
                    entry_with_folder = entry.copy()
                    entry_with_folder["source_folder"] = cat["key"]
                    all_hashes[name] = entry_with_folder
        except Exception:
            continue
    return all_hashes


async def fetch_files_in_folder(
        service,
        cached_folder_files,
        folder_id,
        folder_name,
        local_md5=None,
        filename=None):
    if folder_id in cached_folder_files:
        files = cached_folder_files[folder_id]
    else:
        files = None

    if not files:
        page_token = None
        while True:
            try:
                await update_detail_status(f"Lese {folder_name} ...")
                response = service.files().list(
                    q=f"'{folder_id}' in parents and trashed=false",
                    fields="nextPageToken, files(id, name, md5Checksum, parents)",
                    pageSize=Settings.PAGESIZE,
                    pageToken=page_token
                ).execute()

                if files:
                    files.extend(response.get('files', []))
                else:
                    files = response.get('files', [])

                await update_detail_status(f"Gelesen {folder_name} : {len(files)}")

                # Hole nächste Seite oder beende
                page_token = response.get('nextPageToken')
                if not page_token:
                    cached_folder_files[folder_id] = files
                    break

            except Exception as e:
                logger.error(f"Fehler beim Durchsuchen von Ordner {folder_name}: {e}")
                break

    for file in files:
        if ((local_md5 and file.get('md5Checksum') == local_md5) or
                (filename and file.get('name') == filename)):
            await update_detail_status(f"Gefunden {filename} in {folder_name}")
            return {
                "id": file['id'],
                "md5": file.get('md5Checksum'),
                "source_folder": folder_name
            }

    return None  # Nichts gefunden


async def process_hash_entry(
        service,
        filename: str,
        local_md5: str,
        all_gdrive_hashes: Dict[str, Dict],
        folder_name: str,
        folder_path: Path,
        gdrive_folder_names: Set[str],
        cached_folder_files
) -> Tuple[bool, bool]:
    entry = None
    for cat in Settings.kategorien():
        # Fetch the file details
        folder_name_cat = cat["key"]
        folder_id_cat = folder_id_by_name(folder_name_cat)
        entry = await fetch_files_in_folder(
            service,
            cached_folder_files,
            folder_id_cat,
            folder_name_cat,
            local_md5,
            filename)
        if entry:
            break

    entryhash = all_gdrive_hashes.get(filename)
    if entryhash:
        if entry:
            if entryhash.get("source_folder") != entry.get("source_folder"):
                gdrive_folder_names.add(entryhash.get("source_folder"))
                gdrive_folder_names.add(entry.get("source_folder"))
        else:
            gdrive_folder_names.add(entryhash.get("source_folder"))

    # Wenn Datei existiert, MD5 übereinstimmt UND im richtigen Ordner ist -> nichts tun
    if entry and entry.get("md5") == local_md5 and entry.get("source_folder") == folder_name:
        return False, False

    # Move existing if same MD5 but wrong folder
    if entry and entry.get("md5") == local_md5 and entry.get("source_folder") != folder_name:
        target_id = folder_id_by_name(folder_name)
        if target_id:
            moved = move_file_to_folder(
                service, entry["id"], target_id, gdrive_folder_names
            )
            return bool(moved), False
        return False, False

    # Upload new if not present
    file_path = folder_path / filename
    if file_path.exists():
        target_id = folder_id_by_name(folder_name)
        if target_id:
            await upload_file_to_gdrive(service, "image/*", file_path, target_id)
            return False, True
    return False, False


async def move_gdrive_files_by_local(service, folder_name: str):
    """Main sync: move or upload local files to GDrive, then sync back moved files."""
    # Special handling for textfiles folder
    if Settings.TEXTFILES_FOLDERNAME == folder_name:
        await gdrive_textfiles_files_by_local(service, folder_name)
        return

    try:
        # Initialize progress
        await init_progress_state()
        await update_progress_auto(f"🔄 Sync GDrive für Ordner: {folder_name}")

        cache = Path(Settings.IMAGE_FILE_CACHE_DIR)
        folder_path = cache / folder_name
        local_hashes = load_local_hashes(folder_path)
        if "__error__" in local_hashes:
            await update_progress_auto(
                f"⚠️ Fehler beim Lesen lokaler Hashes: {local_hashes['__error__']}"
            )
            return

        gdrive_hashes = load_all_gdrive_hashes(cache)
        total = len(local_hashes)
        moved = uploaded = 0

        await update_progress_auto(f"🔍 Gefunden local: {total} Dateien")
        await start_detail_progress(f"🔍 Gefunden lokal: {total} Dateien")
        gdrive_folders: Set[str] = set()

        cached_folder_files = {}

        for idx, (filename, md5) in enumerate(local_hashes.items()):
            status_prefix = "✓"  # Standard-Status

            moved_flag, uploaded_flag = await process_hash_entry(
                service,
                filename,
                md5,
                gdrive_hashes,
                folder_name,
                folder_path,
                gdrive_folders,
                cached_folder_files
            )

            # Status-Emoji basierend auf der Aktion
            if moved_flag:
                status_prefix = "→"  # Verschoben
            elif uploaded_flag:
                status_prefix = "⬆️"  # Hochgeladen

            moved += moved_flag
            uploaded += uploaded_flag
            progress = calc_detail_progress(idx, total)

            await update_detail_progress(
                detail_status=f"{status_prefix} {filename} ({idx + 1}/{total})",
                detail_progress=progress,
            )

        del cached_folder_files

        await update_progress_auto(f"🔍 Verarbeitet: {total} Dateien")
        await stop_detail_progress(f"🔍 Verarbeitet: {total} Dateien")

        await finalize_sync(service, folder_name, gdrive_folders, moved, uploaded)

        moved_back = await check_and_move_gdrive_files(
            service, folder_name, cache, Settings.IMAGE_EXTENSIONS
        )
        if moved_back:
            await update_progress_text(f"✅ {moved_back} GDrive Dateien verschoben")
    finally:
        await stop_progress()


async def finalize_sync(
        service,
        folder_name: str,
        gdrive_folders: Set[str],
        moved: int,
        uploaded: int,
):
    """Handle post-sync hash updates and progress messages."""
    if moved or uploaded:
        await update_progress_text(f"🔄 Aktualisiere Hashes für {folder_name}")
        await delete_duplicates_in_gdrive_folder(service, folder_name)
        for f in gdrive_folders:
            await update_gdrive_hashes(
                service,
                f,
                Settings.IMAGE_EXTENSIONS,
                Path(Settings.IMAGE_FILE_CACHE_DIR),
            )
    await update_progress_text(
        f"✅ Sync abgeschlossen. {moved} verschoben, {uploaded} hochgeladen"
    )


def p7():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))

    asyncio.run(move_gdrive_files_by_local(service, "ki"))


if __name__ == "__main__":
    p7()
