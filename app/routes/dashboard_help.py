from pathlib import Path

from app.config import Settings
from app.utils.db_utils import save_folder_status_to_db
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


def _prepare_folder(folder_path: Path) -> bool:
    if not folder_path.exists():
        try:
            folder_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"[fill_folder_cache] 📁 Ordner automatisch erstellt: {folder_path}")
        except Exception as e:
            logger.warning(f"[fill_folder_cache] ⚠️ Ordner konnte nicht erstellt werden: {folder_path} → {e}")
            return False
    return True


def _process_image_files(image_files, folder_name, file_parents_cache, db_path):
    for index, image_file in enumerate(image_files):
        if not image_file.is_file() or image_file.suffix.lower() not in Settings.IMAGE_EXTENSIONS:
            continue
        image_name = image_file.name.lower()
        pair = Settings.CACHE["pair_cache"].get(image_name)
        if not pair:
            logger.warning(f"[fill_folder_cache] ⚠️ Kein Eintrag im pair_cache für: {image_name}")
            continue
        logger.info(f"[fill_folder_cache] ✅️ Eintrag im pair_cache für: {folder_name} / {image_name}")
        image_id = pair["image_id"]
        file_parents_cache[folder_name].append(image_id)
        save_folder_status_to_db(db_path, image_id, folder_name)

