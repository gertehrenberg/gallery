import hashlib
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Tuple, Any
from PIL import Image
from PIL.ExifTags import TAGS

from app.config import Settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class ImageCacheError(Exception):
    """Basisklasse für Exceptions im Image-Cache-Kontext."""
    pass


def find_image_id_by_name(image_name: str) -> Optional[str]:
    """
    Findet die Image-ID für einen gegebenen Bildnamen im Cache.

    Args:
        image_name: Name des Bildes

    Returns:
        Optional[str]: Image-ID wenn gefunden, sonst None
    """
    logger.info(f"🔎 Suche ID für Bild: {image_name}")
    pair_cache = Settings.CACHE.get("pair_cache", {})
    pair = pair_cache.get(image_name)

    if pair:
        logger.info(f"✅ Gefunden: {pair.get('image_id')}")
        return pair.get('image_id')

    logger.warning(f"❌ Kein Eintrag gefunden für: {image_name}")
    return None


def find_image_name_by_id(image_id: str) -> Optional[str]:
    """
    Findet den Bildnamen für eine gegebene Image-ID im Cache.

    Args:
        image_id: ID des Bildes

    Returns:
        Optional[str]: Bildname wenn gefunden, sonst None
    """
    logger.info(f"🔍 Suche Bildname für ID: {image_id}")
    pair_cache = Settings.CACHE.get("pair_cache", {})

    for image_name, pair in pair_cache.items():
        if pair.get("image_id") == image_id:
            logger.info(f"✅ Gefunden: {image_name}")
            return image_name

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
                readimages(str(item), pair_cache)

                # Verarbeite Unterverzeichnisse
                for subitem in item.iterdir():
                    if subitem.is_dir() and process_directory(subitem):
                        readimages(str(subitem), pair_cache)

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


def readimages(folder_path: str, pair_cache: Dict[str, Any]) -> None:
    """
    Liest Bilder aus einem Verzeichnis und aktualisiert den Cache.

    Args:
        folder_path: Pfad zum Bildverzeichnis
        pair_cache: Cache-Dictionary
    """
    folder = Path(folder_path)
    bilder_daten: List[Dict[str, Any]] = []

    for file_path in folder.iterdir():
        if not (file_path.is_file() and file_path.suffix.lower() in Settings.IMAGE_EXTENSIONS):
            continue

        image_name = file_path.name.lower()
        md5_hash = hashlib.md5(image_name.encode()).hexdigest()
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

    # Nach Datum sortieren und Cache aktualisieren
    bilder_daten.sort(key=lambda x: x['data']['aufnahmedatum'], reverse=True)
    for bild in bilder_daten:
        pair_cache[bild['name']] = bild['data']
