import io
import os
from pathlib import Path
from typing import Set
from googleapiclient.http import MediaIoBaseDownload
from tqdm.auto import tqdm

from app.config import Settings
from app.config_gdrive import calculate_md5, SettingsGdrive, folder_id_by_name
from app.routes.auth import load_drive_service_token
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


def find_and_download_missing_images(service, recheck_folder_id: str) -> None:
    """
    Findet alle Bilddateien in Google Drive und lädt sie herunter, falls sie noch nicht im
    IMAGE_FILE_CACHE_DIR/recheck Ordner oder dessen Unterordnern existieren.

    Args:
        service: Google Drive Service Objekt
        recheck_folder_id: ID des "recheck" Ordners in Google Drive
    """
    cache_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
    existing_hashes: Set[str] = set()

    # Alle Bilddateien in Google Drive finden
    files_to_process = []
    page_token = None
    with tqdm(desc="📁 Durchsuche Google Drive", unit="Seite") as pbar:
        while True:
            query = f"mimeType contains 'image/' and trashed = false"
            response = service.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name, md5Checksum)',
                pageSize=Settings.PAGESIZE,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()

            current_files = [f for f in response.get('files', [])
                             if any(f.get('name', '').lower().endswith(ext)
                                    for ext in Settings.IMAGE_EXTENSIONS)]
            files_to_process.extend(current_files)
            pbar.update(1)
            pbar.set_postfix({"Gefunden": len(files_to_process)})

            page_token = response.get('nextPageToken')
            if not page_token:
                break

    # Sammle MD5-Hashes aller existierenden Bilder in allen Unterordnern
    local_files = list(cache_dir.rglob("*"))
    with tqdm(local_files, desc="🔍 Sammle lokale Hashes", unit="Datei") as pbar:
        for img_path in pbar:
            if img_path.is_file() and img_path.suffix.lower() in Settings.IMAGE_EXTENSIONS:
                try:
                    hash_md5 = calculate_md5(img_path)
                    existing_hashes.add(hash_md5)
                    pbar.set_postfix({"Hash": hash_md5[:8]})
                except Exception as e:
                    tqdm.write(f"❌ Fehler beim Hashen von {img_path}: {e}")

    # Zielordner erstellen
    target_dir = cache_dir / "recheck"
    target_dir.mkdir(parents=True, exist_ok=True)

    # Download der fehlenden Dateien
    download_stats = {"neu": 0, "übersprungen": 0}
    with tqdm(files_to_process, desc="⬇️ Lade Dateien herunter", unit="Datei") as pbar:
        for file in pbar:
            file_name = file.get('name', '')
            file_hash = file.get('md5Checksum')
            pbar.set_description(f"⬇️ {file_name[:30]}...")

            if file_hash and file_hash not in existing_hashes:
                try:
                    request = service.files().get_media(fileId=file['id'])
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False

                    while not done:
                        status, done = downloader.next_chunk()
                        if status:
                            pbar.set_postfix({"Progress": f"{int(status.progress() * 100)}%"})

                    target_path = target_dir / file_name
                    counter = 1
                    while target_path.exists():
                        name_parts = file_name.rsplit('.', 1)
                        new_name = f"{name_parts[0]}_{counter}.{name_parts[1]}"
                        target_path = target_dir / new_name
                        counter += 1

                    with open(target_path, "wb") as f:
                        f.write(fh.getvalue())

                    download_stats["neu"] += 1
                    tqdm.write(f"✅ Heruntergeladen: {target_path}")

                except Exception as e:
                    tqdm.write(f"❌ Fehler beim Download von {file_name}: {e}")
            else:
                download_stats["übersprungen"] += 1

            pbar.set_postfix(download_stats)

    # Abschlussbericht
    logger.info("\n📊 Zusammenfassung:")
    logger.info(f"🔍 Lokale Dateien geprüft: {len(local_files)}")
    logger.info(f"📁 Drive-Dateien gefunden: {len(files_to_process)}")
    logger.info(f"⬇️ Neue Dateien heruntergeladen: {download_stats['neu']}")
    logger.info(f"⏭️ Übersprungene Dateien: {download_stats['übersprungen']}")


import io
from pathlib import Path
from typing import Set
from googleapiclient.http import MediaIoBaseDownload
from tqdm.auto import tqdm

from app.config import Settings
from app.config_gdrive import calculate_md5
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


def find_and_download_missing_text_files(service, textfiles_folder_id: str) -> None:
    """
    Findet alle .txt Dateien im "textfiles" Ordner von Google Drive und lädt sie herunter,
    falls sie noch nicht im TEXT_FILE_CACHE_DIR existieren oder verändert wurden.

    Args:
        service: Google Drive Service Objekt
        textfiles_folder_id: ID des "textfiles" Ordners in Google Drive
    """
    cache_dir = Path(Settings.TEXT_FILE_CACHE_DIR)
    cache_dir.mkdir(parents=True, exist_ok=True)
    existing_hashes: Set[str] = set()

    # Sammle MD5-Hashes aller existierenden Textdateien
    local_files = list(cache_dir.glob("*.txt"))
    with tqdm(local_files, desc="🔍 Sammle lokale Hashes", unit="Datei") as pbar:
        for txt_path in pbar:
            if txt_path.is_file():
                try:
                    hash_md5 = calculate_md5(txt_path)
                    existing_hashes.add(hash_md5)
                    pbar.set_postfix({"Hash": hash_md5[:8]})
                except Exception as e:
                    tqdm.write(f"❌ Fehler beim Hashen von {txt_path}: {e}")

    # Alle .txt Dateien in Google Drive textfiles Ordner finden
    files_to_process = []
    page_token = None
    with tqdm(desc="📁 Durchsuche Google Drive textfiles", unit="Seite") as pbar:
        while True:
            query = f"'{textfiles_folder_id}' in parents and mimeType='text/plain' and trashed = false"
            response = service.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name, md5Checksum)',
                pageSize=Settings.PAGESIZE,
                pageToken=page_token
            ).execute()

            current_files = [f for f in response.get('files', [])
                             if f.get('name', '').lower().endswith('.txt')]
            files_to_process.extend(current_files)
            pbar.update(1)
            pbar.set_postfix({"Gefunden": len(files_to_process)})

            page_token = response.get('nextPageToken')
            if not page_token:
                break

    # Download der fehlenden Dateien
    download_stats = {"neu": 0, "übersprungen": 0}
    with tqdm(files_to_process, desc="⬇️ Lade Dateien herunter", unit="Datei") as pbar:
        for file in pbar:
            file_name = file.get('name', '')
            file_hash = file.get('md5Checksum')
            pbar.set_description(f"⬇️ {file_name[:30]}...")

            if file_hash and file_hash not in existing_hashes:
                try:
                    request = service.files().get_media(fileId=file['id'])
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False

                    while not done:
                        status, done = downloader.next_chunk()
                        if status:
                            pbar.set_postfix({"Progress": f"{int(status.progress() * 100)}%"})

                    target_path = cache_dir / file_name
                    with open(target_path, "wb") as f:
                        f.write(fh.getvalue())

                    download_stats["neu"] += 1
                    tqdm.write(f"✅ Heruntergeladen: {target_path}")

                except Exception as e:
                    tqdm.write(f"❌ Fehler beim Download von {file_name}: {e}")
            else:
                download_stats["übersprungen"] += 1

            pbar.set_postfix(download_stats)

    # Abschlussbericht
    logger.info("\n📊 Zusammenfassung:")
    logger.info(f"🔍 Lokale Dateien geprüft: {len(local_files)}")
    logger.info(f"📁 Drive-Dateien gefunden: {len(files_to_process)}")
    logger.info(f"⬇️ Neue Dateien heruntergeladen: {download_stats['neu']}")
    logger.info(f"⏭️ Übersprungene Dateien: {download_stats['übersprungen']}")


def p5():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))
    # find_and_download_missing_images(service, "recheck")
    find_and_download_missing_text_files(service, folder_id_by_name(Settings.TEXTFILES_FOLDERNAME))


if __name__ == "__main__":
    p5()
