import hashlib
import json
import logging
import os
from pathlib import Path

from app.config_new import Settings  # Importiere die Settings-Klasse
from app.database import save_folder_status_to_db, clear_folder_status_db, load_folder_status_from_db


def fillcache_local(pair_cache_path_local: str, image_file_cache_dir: str):
    """Füllt den Cache für Bild- und Textdateien aus dem lokalen Dateisystem."""
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

    image_paths = []
    for name in os.listdir(image_file_cache_dir):
        full_path = os.path.join(image_file_cache_dir, name)
        if os.path.isfile(full_path) and name.lower().endswith(tuple(Settings.IMAGE_EXTENSIONS)):
            image_paths.append(full_path)
        elif os.path.isdir(full_path):
            for subname in os.listdir(image_file_cache_dir):
                subpath = os.path.join(full_path, subname)
                if os.path.isfile(subpath) and subname.lower().endswith(tuple(Settings.IMAGE_EXTENSIONS)):
                    image_paths.append(subpath)

    image_paths.sort(key=lambda p: os.path.getctime(p), reverse=True)
    image_name_cache = {os.path.basename(p).lower(): (p, "") for p in image_paths}

    logging.info(f"[image_name_cache] 📂 Gelesen Bilder aus: {len(image_name_cache)}")

    for image_name in list(image_name_cache.keys()):
        image_path, _ = image_name_cache[image_name]
        if not os.path.exists(image_path):
            logging.warning(f"[fillcache_local] Bild fehlt und wird aus dem Cache entfernt: {image_name}")
            continue
        md5_hash = hashlib.md5(image_name.encode()).hexdigest()
        pair_cache[image_name] = {
            "image_id": md5_hash,
            "text_id": "",
            "web_link": ""
        }

    try:
        with open(pair_cache_path_local, 'w') as f:
            json.dump(pair_cache, f)
        logging.info(f"[fillcache_local] Pair-Cache gespeichert: {len(pair_cache)} Paare")
    except Exception as e:
        logging.warning(f"[fillcache_local] Fehler beim Speichern von pair_cache.json: {e}")

    logging.info(
        f"[fillcache_local] Cache vollständig aktualisiert: "
        f"{len(image_name_cache)} Bilder, "
        f"{len(pair_cache)} Paare ")


def fill_folder_cache(db_path: str):
    """Füllt den Cache für die Zuordnung von Bildern zu Ordnern."""
    file_parents_cache = Settings.CACHE["file_parents_cache"]
    file_parents_cache.clear()

    rows = load_folder_status_from_db(db_path)
    if rows:
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
        return

    logging.info("[fill_folder_cache] 🛰️ Keine Cache-Daten vorhanden, lade von lokal...")
    clear_folder_status_db(db_path)

    for kat in Settings.kategorien:
        folder_name = kat["key"]
        current_loading_folder = kat["label"]
        file_parents_cache[folder_name] = []
        folder_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name
        if not folder_path.exists():
            try:
                folder_path.mkdir(parents=True, exist_ok=True)
                logging.info(f"[fill_folder_cache] 📁 Ordner automatisch erstellt: {folder_path}")
            except Exception as e:
                logging.warning(f"[fill_folder_cache] ⚠️ Ordner konnte nicht erstellt werden: {folder_path} → {e}")
                continue
        logging.info(f"[fill_folder_cache] 📂 Lese Bilder aus: {folder_name}")
        for image_file in folder_path.iterdir():
            if not image_file.is_file():
                continue
            image_name = image_file.name.lower()
            pair = Settings.CACHE["pair_cache"].get(image_name)
            if not pair:
                logging.warning(f"[fill_folder_cache] ⚠️ Kein Eintrag im pair_cache für: {image_name}")
                continue
            image_id = pair["image_id"]
            file_parents_cache[folder_name].append(image_id)
            save_folder_status_to_db(db_path, image_id, folder_name)
        Settings.folders_loaded += 1
        logging.info(
            f"[fill_folder_cache] ✅ {Settings.folders_loaded}/{Settings.folders_total} Ordner geladen: {folder_name}")


def clear_folder_parents_cache(folder_id: str):
    file_parents_cache = Settings.CACHE["file_parents_cache"]
    if folder_id in file_parents_cache:
        del file_parents_cache[folder_id]


def load_rendered_html_file(file_dir: Path, file_name: str) -> str | None:
    file_path = file_dir / (file_name + ".j2")
    if file_path.is_file():
        try:
            logging.info(f"[load_rendered_html_file] ✅ {file_path}")
            return file_path.read_text(encoding='utf-8')
        except Exception as e:
            logging.error(f"Fehler beim Laden der Datei {file_path}: {e}")
            return None
    else:
        logging.info(f"[load_rendered_html_file] ⚠️ {file_path}")
        return None


def save_rendered_html_file(file_dir: Path, file_name: str, content: str) -> bool:
    file_path = file_dir / (file_name + ".j2")
    try:
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding="utf-8")
        return True
    except Exception as e:
        logging.error(f"Fehler beim Speichern der Datei {file_path}: {e}")
        return False


def delete_rendered_html_file(file_dir: Path, file_name: str) -> bool:
    file_path = file_dir / (file_name + ".j2")
    if file_path.is_file():
        try:
            file_path.unlink()
            return True
        except Exception as e:
            logging.error(f"Fehler beim Löschen der Datei {file_path}: {e}")
            return False
    return False
