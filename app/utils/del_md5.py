import json
from pathlib import Path

from app.config import Settings
from app.config_gdrive import SettingsGdrive
from app.services.image_processing import clean
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


def cleanmd5(md5: str, image_name: str):
    try:
        # Lokale Hashes bereinigen
        for folder in Path(Settings.IMAGE_FILE_CACHE_DIR).iterdir():
            if folder.is_dir():
                hash_file = folder / Settings.GALLERY_HASH_FILE
                if hash_file.exists():
                    try:
                        with open(hash_file, 'r') as f:
                            hashes = json.load(f)

                        # Entferne den Hash (wenn vorhanden)
                        modified = False
                        for filename, file_hash in list(hashes.items()):
                            if file_hash == md5:
                                del hashes[filename]
                                modified = True
                                logger.info(f"Hash {md5} für {filename} aus {hash_file} entfernt")

                        # Speichere aktualisierte Hashes
                        if modified:
                            with open(hash_file, 'w') as f:
                                json.dump(hashes, f, indent=2)
                    except Exception as e:
                        logger.error(f"Fehler beim Bereinigen der lokalen Hashes in {hash_file}: {e}")

        # Google Drive Cache bereinigen
        gdrive_hash_file = Path(Settings.IMAGE_FILE_CACHE_DIR) / Settings.GDRIVE_HASH_FILE
        if gdrive_hash_file.exists():
            try:
                with open(gdrive_hash_file, 'r') as f:
                    gdrive_hashes = json.load(f)

                # Entferne den Hash (wenn vorhanden)
                modified = False
                for filename, file_hash in list(gdrive_hashes.items()):
                    if file_hash == md5:
                        del gdrive_hashes[filename]
                        modified = True
                        logger.info(f"Hash {md5} für {filename} aus Google Drive Cache entfernt")

                # Speichere aktualisierte GDrive Hashes
                if modified:
                    with open(gdrive_hash_file, 'w') as f:
                        json.dump(gdrive_hashes, f, indent=2)
            except Exception as e:
                logger.error(f"Fehler beim Bereinigen des Google Drive Caches: {e}")

    except Exception as e:
        logger.error(f"Allgemeiner Fehler beim Bereinigen des MD5 Hashes {md5}: {e}")

    clean("img_2583.pn")


def p5():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    cleanmd5("9eb31c70f7aa1e1d85cbaa4acfa6e04a", "img_2583.pn")


if __name__ == "__main__":
    p5()
