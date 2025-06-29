import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any

from PIL import Image

from app.config import Settings
from app.config_gdrive import calculate_md5, SettingsGdrive
from app.utils.logger_config import setup_logger
from app.utils.progress import update_progress_text
from app.utils.progress_detail import update_detail_progress

logger = setup_logger(__name__)


class ImageCacheError(Exception):
    """Basisklasse für Exceptions im Image-Cache-Kontext."""
    pass


def fillcache_local(pair_cache_path_local: str, image_file_cache_dir: str):
    pair_cache = Settings.CACHE["pair_cache"]
    pair_cache.clear()

    logger.info(f"[fillcache_local] 📂 Lesen: {pair_cache_path_local}")

    if os.path.exists(pair_cache_path_local):
        try:
            with open(pair_cache_path_local, 'r') as f:
                pair_cache.update(json.load(f))
                logger.info(f"[fillcache_local] Pair-Cache geladen: {len(pair_cache)} Paare")
                return
        except Exception as e:
            logger.warning(f"[fillcache_local] Fehler beim Laden von pair_cache.json: {e}")

    fill_pair_cache(image_file_cache_dir, pair_cache, pair_cache_path_local)


def find_image_id_by_name(image_name: str) -> Optional[str]:
    """
    Findet die Image-ID für einen gegebenen Bildnamen im Cache.
    Versucht den Cache neu zu laden falls keine ID gefunden wird.

    Args:
        image_name: Name des Bildes (case-insensitive)

    Returns:
        Optional[str]: Image-ID wenn gefunden, sonst None
    """
    logger.info(f"🔎 Suche ID für Bild: {image_name}")

    # Normalisiere den Bildnamen (lowercase)
    image_name = image_name.lower()

    # Erste Suche im Cache
    pair_cache = Settings.CACHE.get("pair_cache", {})
    if pair := pair_cache.get(image_name):
        image_id = pair.get('image_id')
        if image_id:
            logger.info(f"✅ Gefunden: {image_id}")
            return image_id

    # Cache neu laden wenn nichts gefunden wurde
    logger.info("🔄 Keine ID im Cache gefunden, lade Cache neu...")
    try:
        fillcache_local(
            str(Settings.PAIR_CACHE_PATH),
            Settings.IMAGE_FILE_CACHE_DIR
        )

        # Erneute Suche nach der Cache-Aktualisierung
        pair_cache = Settings.CACHE.get("pair_cache", {})
        if pair := pair_cache.get(image_name):
            image_id = pair.get('image_id')
            if image_id:
                logger.info(f"✅ Nach Cache-Aktualisierung gefunden: {image_id}")
                return image_id

    except Exception as e:
        logger.error(f"❌ Fehler beim Neuladen des Cache: {e}")

    logger.warning(f"❌ Keine ID gefunden für Bild: {image_name}")
    return None


def find_image_name_by_id(image_id: str) -> Optional[str]:
    """
    Findet den Bildnamen für eine gegebene Image-ID im Cache.
    Versucht den Cache neu zu laden falls keine ID gefunden wird.

    Args:
        image_id: ID des Bildes im Google Drive

    Returns:
        Optional[str]: Name der Bilddatei wenn gefunden, sonst None
    """
    logger.info(f"🔎 Suche Bildname für ID: {image_id}")

    # Erster Versuch im existierenden Cache
    pair_cache = Settings.CACHE.get("pair_cache", {})

    for image_name, pair in pair_cache.items():
        if pair.get("image_id") == image_id:
            logger.info(f"✅ Gefunden: {image_name}")
            return image_name

    # Cache neu laden wenn nichts gefunden wurde
    logger.info(f"🔄 Cache-Miss für ID {image_id}, lade Cache neu...")
    try:
        fillcache_local(
            str(Settings.PAIR_CACHE_PATH),
            Settings.IMAGE_FILE_CACHE_DIR
        )

        # Erneute Suche im aktualisierten Cache
        pair_cache = Settings.CACHE.get("pair_cache", {})
        for image_name, pair in pair_cache.items():
            if pair.get("image_id") == image_id:
                logger.info(f"✅ Nach Cache-Aktualisierung gefunden: {image_name}")
                return image_name

    except Exception as e:
        logger.error(f"❌ Fehler beim Neuladen des Cache: {e}")

    logger.warning(f"❌ Kein Bildname gefunden für ID: {image_id}")
    return None


def process_directory(directory: Path) -> bool:
    """
    Prüft, ob ein Verzeichnis verarbeitet werden soll.

    Args:
        directory: Pfad zum Verzeichnis

    Returns:
        bool: True wenn das Verzeichnis verarbeitet werden soll
    """
    return any(str(directory).lower().endswith(key) for key in Settings.CHECKBOX_CATEGORIES)


def fill_pair_cache(image_file_cache_dir: str, pair_cache: Dict[str, Any], pair_cache_path_local: str) -> None:
    """
    Füllt den Pair-Cache mit Bildinformationen aus dem Verzeichnis.

    Args:
        image_file_cache_dir: Pfad zum Bildverzeichnis
        pair_cache: Cache-Dictionary
        pair_cache_path_local: Pfad zur lokalen Cache-Datei
    """
    try:
        pair_cache.clear()
        root_path = Path(image_file_cache_dir)

        # Verarbeite alle Verzeichnisse
        for item in root_path.iterdir():
            if item.is_dir() and process_directory(item):
                asyncio.run(readimages(str(item), pair_cache))
                # Verarbeite Unterverzeichnisse
                for subitem in item.iterdir():
                    if subitem.is_dir() and process_directory(subitem):
                        asyncio.run(readimages(str(subitem), pair_cache))

        save_pair_cache(pair_cache, pair_cache_path_local)

    except Exception as e:
        logger.error(f"Fehler beim Füllen des Caches: {e}")
        raise ImageCacheError(f"Cache-Aktualisierung fehlgeschlagen: {e}")


def save_pair_cache(pair_cache: Dict[str, Any], pair_cache_path_local: str) -> None:
    """
    Speichert den Pair-Cache in eine JSON-Datei.

    Args:
        pair_cache: Cache-Dictionary
        pair_cache_path_local: Pfad zur lokalen Cache-Datei
    """
    try:
        cache_path = Path(pair_cache_path_local)
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        with cache_path.open('w', encoding='utf-8') as f:
            json.dump(pair_cache, f, ensure_ascii=False, indent=2)

        logger.info(f"📝 Pair-Cache gespeichert: {len(pair_cache)} Paare")

    except Exception as e:
        logger.error(f"Fehler beim Speichern von pair_cache.json: {e}")
        raise ImageCacheError(f"Cache-Speicherung fehlgeschlagen: {e}")


def get_image_date(file_path: Path) -> Tuple[datetime, str]:
    """
    Ermittelt das Aufnahmedatum eines Bildes aus EXIF-Daten oder Dateisystem.

    Args:
        file_path: Pfad zum Bild

    Returns:
        Tuple[datetime, str]: (datetime-Objekt, formatiertes deutsches Datum)
    """
    aufnahmedatum = None

    try:
        with Image.open(file_path) as img:
            # Bessere Methode um EXIF-Daten zu lesen
            if hasattr(img, 'getexif'):
                exif = img.getexif()
            else:
                # Fallback für ältere Pillow Versionen
                exif = img._getexif() if hasattr(img, '_getexif') else None

            if exif:
                # 36867 ist der StandardTag für DateTimeOriginal
                # Alternative: TAGS.get('DateTimeOriginal')
                if 36867 in exif:
                    datum_str = exif[36867]
                    aufnahmedatum = datetime.strptime(datum_str, '%Y:%m:%d %H:%M:%S')

    except Exception as e:
        logger.warning(f"EXIF-Daten konnten nicht gelesen werden für {file_path}: {e}")

    # Fallback auf Datei-Änderungsdatum
    if not aufnahmedatum:
        aufnahmedatum = datetime.fromtimestamp(file_path.stat().st_mtime)

    return aufnahmedatum, aufnahmedatum.strftime('%d.%m.%Y %H:%M:%S')


def update_date_in_txt_file(txt_file_path: Path, german_date: str) -> None:
    """
    Aktualisiert oder fügt das Aufnahmedatum in der txt-Datei hinzu.

    Args:
        txt_file_path: Pfad zur txt-Datei
        german_date: Formatiertes deutsches Datum
    """
    if not txt_file_path.exists():
        return

    try:
        # Datei lesen
        lines = txt_file_path.read_text(encoding='utf-8').splitlines()

        # Prüfen ob Datum aktualisiert werden muss
        soll_ueberschreiben = (not lines or
                               not lines[0].startswith("Aufgenommen") or
                               lines[0].strip() == "Aufgenommen: None")

        if soll_ueberschreiben:
            neue_zeile = f"Aufgenommen: {german_date}"
            if lines:
                lines[0] = neue_zeile
            else:
                lines.insert(0, neue_zeile)

            txt_file_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')

    except Exception as e:
        logger.error(f"Fehler beim Aktualisieren der Datei {txt_file_path}: {e}")


async def readimages(folder_path: str, pair_cache: Dict[str, Any]) -> None:
    """
    Liest Bilder aus einem Verzeichnis und aktualisiert den Cache.

    Args:
        folder_path: Pfad zum Bildverzeichnis
        pair_cache: Cache-Dictionary
    """
    folder = Path(folder_path)
    bilder_daten: List[Dict[str, Any]] = []

    # Zuerst die Gesamtzahl der Dateien ermitteln
    total_files = sum(1 for file_path in folder.iterdir()
                     if file_path.is_file() and file_path.suffix.lower() in Settings.IMAGE_EXTENSIONS)

    if total_files == 0:
        await update_detail_progress(
            detail_status="⚠️ Keine Bilddateien gefunden",
            detail_progress=1000
        )
        return

    counter = 0
    errors = 0

    await update_detail_progress(
        detail_status=f"🔍 Gefunden: {total_files} Bilder",
        detail_progress=0
    )

    # Iteriere über alle Dateien im Ordner
    for file_path in folder.iterdir():
        if not (file_path.is_file() and file_path.suffix.lower() in Settings.IMAGE_EXTENSIONS):
            continue

        image_name = file_path.name.lower()
        counter += 1

        try:
            # Berechne Fortschritt in Prozent
            progress = int((counter / total_files) * 100)

            # Update Fortschrittsanzeige
            await update_detail_progress(
                detail_status=f"🖼️ [{counter}/{total_files}] Verarbeite {image_name}",
                detail_progress=progress
            )

            # Hauptverarbeitung
            md5_hash = calculate_md5(file_path)
            endung = file_path.suffix.lower()[1:]

            # Datum ermitteln und txt-Datei aktualisieren
            aufnahmedatum, german_date = get_image_date(file_path)
            txt_file_path = Path(Settings.TEXT_FILE_CACHE_DIR) / f"{md5_hash}.{endung}.txt"
            update_date_in_txt_file(txt_file_path, german_date)

            # Bilddaten sammeln
            bilder_daten.append({
                'name': image_name,
                'data': {
                    "image_id": md5_hash,
                    "folder": str(file_path.parent.name),
                    "aufnahmedatum": aufnahmedatum.isoformat()
                }
            })

        except Exception as e:
            errors += 1
            logger.error(f"Fehler bei {image_name}: {str(e)}")
            continue

    # Abschlussmeldung
    status = f"✅ Verarbeitet: {counter} Bilder"
    if errors > 0:
        status += f" | ⚠️ {errors} Fehler"

    # Nach Datum sortieren und Cache aktualisieren
    bilder_daten.sort(key=lambda x: x['data']['aufnahmedatum'], reverse=True)
    for bild in bilder_daten:
        pair_cache[bild['name']] = bild['data']

    await update_detail_progress(
        detail_status=status,
        detail_progress=1000
    )

def p5():
    Settings.DB_PATH = '../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../cache/gdrive_folders.pkl")

    find_image_name_by_id("ad85c7d9b978ef73332b017063d15d29")


if __name__ == "__main__":
    p5()
