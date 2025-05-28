import json
import logging
import os
from pathlib import Path

from app.config import Settings
from app.database import save_folder_status_to_db, clear_folder_status_db, load_folder_status_from_db
from app.tools import fill_pair_cache
from app.utils.progress import update_progress

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def _load_file_parents_cache_from_db(db_path: str, file_parents_cache: dict) -> bool:
    rows = load_folder_status_from_db(db_path)
    if not rows:
        return False
    logging.info("[fill_folder_cache] 📦 Lade file_parents_cache aus der Datenbank...")
    for image_id, folder_id in rows:
        if folder_id not in file_parents_cache:
            Settings.folders_loaded += 1
            file_parents_cache[folder_id] = []
            logging.info(
                f"[fill_folder_cache] ✅ Cache aus DB geladen: {Settings.folders_loaded}/{Settings.folders_total} {folder_id}")
        file_parents_cache[folder_id].append(image_id)
    if Settings.folders_loaded != Settings.folders_total:
        Settings.folders_loaded = Settings.folders_total
        logging.info(
            f"[fill_folder_cache] ✅ Cache aus DB geladen: {Settings.folders_loaded}/{Settings.folders_total}")
    return True


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


async def _process_image_files_progress(image_files, folder_name, file_parents_cache, db_path):
    total = len(image_files)
    for index, image_file in enumerate(image_files):
        await update_progress(f"Kategorie: {folder_name} : {total} Dateien ({image_file})",
                              int(index / total * 100), 0.02)
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


def fill_file_parents_cache(db_path: str):
    file_parents_cache = Settings.CACHE["file_parents_cache"]
    file_parents_cache.clear()

    if _load_file_parents_cache_from_db(db_path, file_parents_cache):
        return

    logging.info("[fill_folder_cache] 🚀 Keine Cache-Daten vorhanden, lade von lokal...")
    clear_folder_status_db(db_path)

    for kat in Settings.kategorien:
        folder_name = kat["key"]
        file_parents_cache[folder_name] = []
        folder_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name
        if not _prepare_folder(folder_path):
            continue
        logging.info(f"[fill_folder_cache] 📂 Lese Bilder aus: {folder_name}")
        image_files = list(folder_path.iterdir())
        _process_image_files(image_files, folder_name, file_parents_cache, db_path)
        Settings.folders_loaded += 1
        logging.info(
            f"[fill_folder_cache] ✅ {Settings.folders_loaded}/{Settings.folders_total} Ordner geladen: {folder_name}")


async def fill_file_parents_cache_progress(db_path: str):
    file_parents_cache = Settings.CACHE["file_parents_cache"]
    file_parents_cache.clear()

    if _load_file_parents_cache_from_db(db_path, file_parents_cache):
        return

    logging.info("[fill_folder_cache] 🚀 Keine Cache-Daten vorhanden, lade von lokal...")
    clear_folder_status_db(db_path)

    for kat in Settings.kategorien:
        folder_name = kat["key"]
        file_parents_cache[folder_name] = []
        folder_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name
        if not _prepare_folder(folder_path):
            continue
        logging.info(f"[fill_folder_cache] 📂 Lese Bilder aus: {folder_name}")
        image_files = list(folder_path.iterdir())
        await update_progress(f"Kategorie: {folder_name} : {len(image_files)} Dateien", 0)
        await _process_image_files_progress(image_files, folder_name, file_parents_cache, db_path)
        Settings.folders_loaded += 1
        logging.info(
            f"[fill_folder_cache] ✅ {Settings.folders_loaded}/{Settings.folders_total} Ordner geladen: {folder_name}")


def load_rendered_html_file(path: Path) -> str:
    if path.exists():
        try:
            return path.read_text(encoding='utf-8')
        except Exception as e:
            logging.warning(f"[rendered_html] ⚠️ Fehler beim Lesen: {path} → {e}")
    return ""


def save_rendered_html_file(path: Path, content: str):
    try:
        path.write_text(content, encoding='utf-8')
        logging.info(f"[rendered_html] 💾 Gespeichert: {path}")
    except Exception as e:
        logging.warning(f"[rendered_html] ⚠️ Fehler beim Speichern: {path} → {e}")


def delete_rendered_html_file(path: Path):
    try:
        if path.exists():
            path.unlink()
            logging.info(f"[rendered_html] ❌ Gelöscht: {path}")
    except Exception as e:
        logging.warning(f"[rendered_html] ⚠️ Fehler beim Löschen: {path} → {e}")


def fill_file_parents_cache_by_name(db_path: str, folder_name: str):
    file_parents_cache = Settings.CACHE["file_parents_cache"]
    if folder_name not in file_parents_cache:
        file_parents_cache[folder_name] = []

    folder_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name
    if not _prepare_folder(folder_path):
        return

    image_files = list(folder_path.iterdir())
    _process_image_files(image_files, folder_name, file_parents_cache, db_path)
    logging.info(f"[fill_folder_cache] ✅ Einzelner Ordner verarbeitet: {folder_name}")
