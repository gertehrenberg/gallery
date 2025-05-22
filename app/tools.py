import logging

from app.config_new import Settings  # Importiere die Settings-Klasse

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
