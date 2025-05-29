import json
import logging
import os
from pathlib import Path
from typing import Dict

from tqdm import tqdm

from app.config import Settings
from app.config_gdrive import calculate_md5
from app.database import save_folder_status_to_db
from app.tools import fill_pair_cache

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def save_simple_hashes(hashes: Dict[str, str], hashfile_path: Path):
    hashfile_path.parent.mkdir(parents=True, exist_ok=True)
    with hashfile_path.open("w", encoding="utf-8") as f:
        json.dump(hashes, f, indent=2)
    os.chmod(hashfile_path, 0o644)


def _prepare_folder(folder_path: Path) -> bool:
    if not folder_path.exists():
        try:
            folder_path.mkdir(parents=True, exist_ok=True)
            logging.info(f"[fill_folder_cache] 📁 Ordner automatisch erstellt: {folder_path}")
        except Exception as e:
            logging.warning(f"[fill_folder_cache] ⚠️ Ordner konnte nicht erstellt werden: {folder_path} → {e}")
            return False
    return True


def _process_image_files(image_files, folder_name, file_parents_cache, db_path):
    for index, image_file in enumerate(image_files):
        if not image_file.is_file() or image_file.suffix.lower() not in Settings.IMAGE_EXTENSIONS:
            continue
        image_name = image_file.name.lower()
        pair = Settings.CACHE["pair_cache"].get(image_name)
        if not pair:
            logging.warning(f"[fill_folder_cache] ⚠️ Kein Eintrag im pair_cache für: {image_name}")
            continue
        logging.info(f"[fill_folder_cache] ✅️ Eintrag im pair_cache für: {folder_name} / {image_name}")
        image_id = pair["image_id"]
        file_parents_cache[folder_name].append(image_id)
        save_folder_status_to_db(db_path, image_id, folder_name)


def fill_file_parents_cache_by_name(db_path: str, folder_key: str):
    file_parents_cache = Settings.CACHE["file_parents_cache"]
    if folder_key not in file_parents_cache:
        file_parents_cache[folder_key] = []

    folder_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_key
    if not _prepare_folder(folder_path):
        return

    image_files = list(folder_path.iterdir())
    _process_image_files(image_files, folder_key, file_parents_cache, db_path)
    logging.info(f"[fill_folder_cache] ✅ Einzelner Ordner verarbeitet: {folder_key}")


def fillcache_local(pair_cache_path_local: str, image_file_cache_dir: str):
    pair_cache = Settings.CACHE["pair_cache"]
    pair_cache.clear()

    logging.info(f"[fillcache_local] 📂 Lesen: {pair_cache_path_local}")

    if os.path.exists(pair_cache_path_local):
        try:
            with open(pair_cache_path_local, 'r') as f:
                pair_cache.update(json.load(f))
                logging.info(f"[fillcache_local] Pair-Cache geladen: {len(pair_cache)} Paare")
                return
        except Exception as e:
            logging.warning(f"[fillcache_local] Fehler beim Laden von pair_cache.json: {e}")

    fill_pair_cache(image_file_cache_dir, pair_cache, pair_cache_path_local)
