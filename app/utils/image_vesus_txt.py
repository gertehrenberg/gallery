from pathlib import Path
from typing import Dict
from typing import List

from tqdm import tqdm

from ..config import Settings
from ..config_gdrive import SettingsGdrive
from ..utils.logger_config import setup_logger

logger = setup_logger(__name__)


def analyze_image_text_relationships() -> Dict[str, List[str]]:
    """
    Analysiert die Beziehungen zwischen Bilddateien und zugehörigen Textdateien.

    Returns:
        Dict[str, List[str]]: Dictionary mit Bildnamen als Schlüssel und Liste zugehöriger Textdateien als Werte
    """
    image_text_map: Dict[str, List[str]] = {}

    image_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
    text_dir = Path(Settings.TEXT_FILE_CACHE_DIR)

    # Sammle alle Bilddateien
    image_files = []
    for ext in Settings.IMAGE_EXTENSIONS:
        image_files.extend(image_dir.rglob(f"*{ext}"))

    logger.info(f"[analyze] 🖼️ Gefundene Bilddateien: {len(image_files)}")

    with tqdm(image_files, desc="Analysiere Bild-Text-Beziehungen", unit="Bild") as pbar:
        for image_path in pbar:
            # Use full name including extension
            image_name_with_ext = image_path.name

            # Suche nach zugehörigen Textdateien
            matching_text_files = list(text_dir.glob(f"{image_name_with_ext}*"))

            if matching_text_files:
                relative_paths = [str(text_file.relative_to(text_dir)) for text_file in matching_text_files]
                image_text_map[image_name_with_ext] = relative_paths

                if len(matching_text_files) > 1:
                    logger.debug(f"[analyze] ℹ️ {image_name_with_ext}: {len(matching_text_files)} Textdateien gefunden")
            else:
                image_text_map[image_name_with_ext] = []
                logger.debug(f"[analyze] ⚠️ Keine Textdatei für {image_name_with_ext} gefunden")

    # Statistiken erstellen und loggen
    total_images = len(image_text_map)
    images_with_text = sum(1 for texts in image_text_map.values() if texts)
    total_text_files = sum(len(texts) for texts in image_text_map.values())

    logger.info("\n[analyze] 📊 Zusammenfassung:")
    logger.info(f"[analyze] 📸 Gesamtanzahl Bilder: {total_images}")
    logger.info(f"[analyze] 📝 Bilder mit Textdateien: {images_with_text}")
    logger.info(f"[analyze] 📄 Gesamtanzahl Textdateien: {total_text_files}")
    if total_images > 0:
        logger.info(f"[analyze] 📊 Durchschnitt Textdateien pro Bild: {total_text_files / total_images:.2f}")
    else:
        logger.info("[analyze] 📊 Durchschnitt Textdateien pro Bild: 0.00")

    return image_text_map


def p5():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    results = analyze_image_text_relationships()

    # Detaillierte Ausgabe für Bilder mit mehreren Textdateien
    logger.info("\n[analyze] 🔍 Bilder mit mehreren Textdateien:")
    for image_name, text_files in results.items():
        if len(text_files) > 1:
            logger.info(f"[analyze] 📸 {image_name}:")
            for text_file in text_files:
                logger.info(f"[analyze]   └─ 📄 {text_file}")


if __name__ == "__main__":
    p5()
