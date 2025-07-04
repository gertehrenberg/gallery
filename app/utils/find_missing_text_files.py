import asyncio
import json
import shutil
from pathlib import Path
from typing import List, Dict, Any

from app.config import Settings
from app.config_gdrive import sanitize_filename, SettingsGdrive
from app.utils.logger_config import setup_logger
from app.utils.progress import update_progress_auto
from app.utils.progress_detail import start_detail_progress, update_detail_progress, stop_detail_progress, \
    calc_detail_progress

logger = setup_logger(__name__)


async def load_hash_files(image_cache_dir: Path) -> List[Path]:
    """Return all hash JSON files in the image cache directory."""
    await update_progress_auto("Lade Hash-Dateien")
    return list(image_cache_dir.rglob("gallery202505_hashes.json"))


def read_hashfile(hashfile: Path) -> Dict:
    """Read and parse a JSON hashfile. Returns empty dict on error."""
    try:
        return json.loads(hashfile.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error("Failed to read %s: %s", hashfile, e)
        return {}


async def collect_missing_entries(hash_files, text_dir) -> list[Any]:
    total_files = len(hash_files)
    missing = []

    await update_progress_auto("Prüfe auf fehlende .txt-Dateien")
    await start_detail_progress("Prüfe auf fehlende .txt-Dateien")

    ignore = [Settings.RECHECK, "delete", "gemini"]

    for index, hashfile in enumerate(hash_files, 1):

        # Fortschritt berechnen
        progress = calc_detail_progress(index, total_files)

        # Status aktualisieren
        await update_detail_progress(
            detail_status=f"Verarbeite Datei {index}/{total_files}",
            detail_progress=progress
        )

        # Korrektur: hashfile.parent.name statt hash_files.paren.name
        if hashfile.parent.name in ignore:
            continue

        folder = hashfile.parent
        data = read_hashfile(hashfile)

        for name in data:
            if not (text_dir / (sanitize_filename(name) + ".txt")).exists():
                missing.append((name, folder / name))

    # Fortschritt abschließen
    await stop_detail_progress("Suche abgeschlossen")

    return missing


async def move_missing_files(missing, recheck_dir):
    total_files = len(missing)

    await update_progress_auto("Verschiebe fehlende Dateien")
    await start_detail_progress("Verschiebe fehlende Dateien")

    for index, (_, img_path) in enumerate(missing, 1):
        # Fortschritt berechnen
        progress = calc_detail_progress(index, total_files)

        # Status aktualisieren
        await update_detail_progress(
            detail_status=f"Verschiebe Datei {index}/{total_files}",
            detail_progress=progress
        )

        try:
            if img_path.exists():
                # Verwendung der vorhandenen _move_file Funktion
                _move_file(img_path, recheck_dir)
        except Exception as e:
            logger.error(f"Fehler beim Verschieben von {img_path}: {e}")

    await stop_detail_progress("Verschieben abgeschlossen")


def _move_file(src: Path, dest_dir: Path) -> None:
    """Helper to move a single file if not already present at destination."""
    target = dest_dir / src.name
    if target.exists():
        logger.info("Skipped existing: %s", target)
        return
    try:
        shutil.move(str(src), str(target))
        logger.info("Moved: %s → %s", src, target)
    except Exception as e:
        logger.error("Error moving %s: %s", src, e)


def calculate_size_threshold(txt_files: List[Path], percentile: float = 0.2, max_bytes: int = 100) -> int:
    """Calculate size the threshold based on percentile, capped by max_bytes."""
    sizes = sorted(f.stat().st_size for f in txt_files)
    if not sizes:
        return 0
    idx = int(len(sizes) * percentile)
    return min(sizes[idx], max_bytes)


def handle_text_file(txt_file: Path, threshold: int, image_cache_dir: Path,
                     recheck_dir: Path, temp_dir: Path) -> None:
    """Process a single text file: move if too small or orphaned."""
    size = txt_file.stat().st_size
    name = txt_file.stem
    sanitized = sanitize_filename(name)
    images = list(Path(image_cache_dir).glob(f"**/{sanitized}"))
    orphan = not any(img.suffix.lower() in Settings.IMAGE_EXTENSIONS for img in images)

    if size < threshold or orphan:
        reason = "no image" if orphan else f"size {size} bytes"
        logger.info("Handling %s: %s", txt_file.name, reason)
        for img in images:
            if img.suffix.lower() in Settings.IMAGE_EXTENSIONS:
                _move_file(img, recheck_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)
        _move_file(txt_file, temp_dir)


def detect_and_handle_small_texts(text_dir: Path, image_cache_dir: Path,
                                  recheck_dir: Path, temp_dir: Path) -> None:
    """Identify and handle very small or orphan text files."""
    txt_files = list(text_dir.glob("*.txt"))
    threshold = calculate_size_threshold(txt_files)
    if threshold <= 0:
        return
    for txt in txt_files:
        handle_text_file(txt, threshold, image_cache_dir, recheck_dir, temp_dir)


async def move_images_without_textfile_2_recheck(image_cache_dir: Path, text_dir: Path) -> None:
    """Main entry point: find and handle missing text and image files."""
    hash_files = await load_hash_files(image_cache_dir)
    missing_entries = await collect_missing_entries(hash_files, text_dir)
    recheck_dir = Path(Settings.IMAGE_FILE_CACHE_DIR) / Settings.RECHECK
    await move_missing_files(missing_entries, recheck_dir)

    temp_dir = Path(Settings.TEMP_DIR_PATH)
    # detect_and_handle_small_texts(text_dir, image_cache_dir, recheck_dir, temp_dir)


def p5():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    asyncio.run(
        move_images_without_textfile_2_recheck(Path(Settings.IMAGE_FILE_CACHE_DIR), Path(Settings.TEXT_FILE_CACHE_DIR)))


if __name__ == "__main__":
    p5()
