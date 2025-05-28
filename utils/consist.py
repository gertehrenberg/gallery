import json
import logging
from pathlib import Path

from app.config import Settings
from app.database import load_folder_status_from_db_by_name, save_folder_status_to_db
from app.tools import readimages
from app.utils.hash_builder import save_simple_hashes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def local():
    global service
    Settings.IMAGE_FILE_CACHE_DIR = "../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../cache/textfiles"
    Settings.DB_PATH = '../gallery_local.db'


if __name__ == "__main__":
    local()

    for eintrag in Settings.kategorien:
        folder_key = eintrag["key"]
        if folder_key == "real":
            continue

        local_files = {}

        readimages(Settings.IMAGE_FILE_CACHE_DIR + "/" + folder_key, local_files)

        local_path = Path(Settings.IMAGE_FILE_CACHE_DIR + "/" + folder_key) / Settings.GALLERY_HASH_FILE

        try:
            with local_path.open("r", encoding="utf-8") as f:
                dir_cache = json.load(f)
                dir_cache = dir_cache if isinstance(dir_cache, dict) else {}
        except:
            dir_cache = {}

        # Schlüssel in dir_cache, aber nicht in local_files
        missing_in_local_files = dir_cache.keys() - local_files.keys()
        for key in missing_in_local_files:
            logging.info(f"{folder_key} [compare] 🔑 Nur in dir_cache: {key} → {dir_cache[key]}")
            del dir_cache[key]

        # Schlüssel in local_files, aber nicht in dir_cache
        missing_in_local_data = local_files.keys() - dir_cache.keys()
        for key in missing_in_local_data:
            logging.info(f"{folder_key} [compare] 🔑 Nur in local_files: {key} → {local_files[key]}")
            dir_cache[key] = folder_key

        save_simple_hashes(dir_cache, local_path)

        db_cache = {}
        rows = load_folder_status_from_db_by_name(Settings.DB_PATH, folder_key)
        logging.info(f"Anzahl DB: {folder_key}: {len(rows)}")
        for image_id, folder_key in rows:
            istda = False
            for image_name, entry in local_files.items():
                if entry.get("image_id") == image_id:
                    istda = True
                    db_cache[image_name] = {
                        "image_id": image_id,
                        "folder": folder_key
                    }
                    break
            if not istda:
                logging.info(f"{folder_key} [compare] 🔑 Nur in DB: {image_id}")

        # Schlüssel in local_files, aber nicht in dir_cache
        missing_in_local_data = local_files.keys() - db_cache.keys()
        for key in missing_in_local_data:
            save_folder_status_to_db(Settings.DB_PATH, local_files[key]["image_id"], folder_key)
            logging.info(f"{folder_key} [compare] 🔑 Nur in local_files: {key} → {local_files[key]}")
