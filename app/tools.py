import hashlib
import json
import logging
import os
from pathlib import Path

from app.config import Settings  # Importiere die Settings-Klasse

logger = logging.getLogger(__name__)


def find_image_id_by_name(image_name: str):
    logging.info(f"[find_image_id_by_name] 🔎 Suche ID für Bild: {image_name}")
    pair_cache = Settings.CACHE.get("pair_cache")
    pair = pair_cache.get(image_name)
    if pair:
        logging.info(f"[find_image_id_by_name] ✅ Gefunden: {pair.get('image_id')}")
        return pair.get("image_id")
    logging.warning(f"[find_image_id_by_name] ❌ Kein Eintrag gefunden für: {image_name}")
    return None


def find_image_name_by_id(image_id: str):
    logging.info(f"[find_image_name_by_id] 🔍 Suche Bildname für ID: {image_id}")
    pair_cache = Settings.CACHE.get("pair_cache")
    for image_name, pair in pair_cache.items():
        if pair.get("image_id") == image_id:
            logging.info(f"[find_image_name_by_id] ✅ Gefunden: {image_name}")
            return image_name
    logging.warning(f"[find_image_name_by_id] ❌ Kein Bildname gefunden für ID: {image_id}")
    return None


def fill_pair_cache(image_file_cache_dir, pair_cache, pair_cache_path_local):
    pair_cache.clear()
    for name in os.listdir(image_file_cache_dir):
        full_path = os.path.join(image_file_cache_dir, name)
        if os.path.isdir(full_path):
            if any(full_path.lower().endswith(key) for key in Settings.CHECKBOX_CATEGORIES):
                readimages(full_path, pair_cache)
        elif os.path.isdir(full_path):
            for subname in os.listdir(image_file_cache_dir):
                subpath = os.path.join(full_path, subname)
                if os.path.isfile(subpath):
                    if any(subpath.lower().endswith(key) for key in Settings.CHECKBOX_CATEGORIES):
                        readimages(full_path, pair_cache)
    save_pair_cache(pair_cache, pair_cache_path_local)


def save_pair_cache(pair_cache, pair_cache_path_local):
    try:
        with open(pair_cache_path_local, 'w') as f:
            json.dump(pair_cache, f)
        logging.info(f"[fillcache_local] Pair-Cache gespeichert: {len(pair_cache)} Paare")
    except Exception as e:
        logging.warning(f"[fillcache_local] Fehler beim Speichern von pair_cache.json: {e}")
    logging.info(
        f"[fillcache_local] Cache vollständig aktualisiert: "
        f"{len(pair_cache)} Bilder"
    )


def readimages(folder_path: str, pair_cache: dict):
    folder = Path(folder_path)
    for file_path in folder.iterdir():
        if file_path.is_file() and file_path.suffix.lower() in Settings.IMAGE_EXTENSIONS:
            image_name = file_path.name.lower()
            md5_hash = hashlib.md5(image_name.encode()).hexdigest()
            pair_cache[image_name] = {
                "image_id": md5_hash,
                "folder": str(file_path.parent.name)
            }
