import json
import shutil
from pathlib import Path
from typing import List, Tuple, Dict

from tqdm import tqdm

from app.config_gdrive import sanitize_filename
from app.utils.logger_config import setup_logger
from config import IMAGE_EXTENSIONS, TEMP_FILE_DIR, IMAGE_FILE_CACHE_DIR, TEXT_FILE_CACHE_DIR

logger = setup_logger(__name__)


def load_hash_files(image_cache_dir: Path) -> List[Path]:
    """Return all hash JSON files in the image cache directory."""
    return list(image_cache_dir.rglob("gallery202505_hashes.json"))


def read_hashfile(hashfile: Path) -> Dict:
    """Read and parse a JSON hashfile. Returns empty dict on error."""
    try:
        return json.loads(hashfile.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error("Failed to read %s: %s", hashfile, e)
        return {}


def collect_missing_entries(hash_files: List[Path], text_dir: Path) -> List[Tuple[str, Path]]:
    """Collect names and image paths for entries missing a corresponding .txt file."""
    missing = []
    for hashfile in tqdm(hash_files, desc="Prüfe auf fehlende .txt-Dateien", unit="Ordner"):
        folder = hashfile.parent
        data = read_hashfile(hashfile)
        for name in data:
            if not (text_dir / (sanitize_filename(name) + ".txt")).exists():
                missing.append((name, folder / name))
    return missing


def move_missing_images(missing: List[Tuple[str, Path]], recheck_dir: Path) -> None:
    """Move missing image files to the recheck directory."""
    recheck_dir.mkdir(parents=True, exist_ok=True)
    for _, img_path in missing:
        if img_path.exists():
            _move_file(img_path, recheck_dir)


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
    orphan = not any(img.suffix.lower() in IMAGE_EXTENSIONS for img in images)

    if size < threshold or orphan:
        reason = "no image" if orphan else f"size {size} bytes"
        logger.info("Handling %s: %s", txt_file.name, reason)
        for img in images:
            if img.suffix.lower() in IMAGE_EXTENSIONS:
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


def find_missing_text_files(image_cache_dir: Path, text_dir: Path) -> None:
    """Main entry point: find and handle missing text and image files."""
    hash_files = load_hash_files(image_cache_dir)
    missing_entries = collect_missing_entries(hash_files, text_dir)

    recheck_dir = Path(IMAGE_FILE_CACHE_DIR) / "recheck"
    temp_dir = Path(TEMP_FILE_DIR)

    move_missing_images(missing_entries, recheck_dir)
    detect_and_handle_small_texts(text_dir, image_cache_dir, recheck_dir, temp_dir)


if __name__ == "__main__":
    find_missing_text_files(Path(IMAGE_FILE_CACHE_DIR), Path(TEXT_FILE_CACHE_DIR))
