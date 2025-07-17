import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Any

from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
from geopy.geocoders import Nominatim
from starlette.responses import JSONResponse

from app.config import Settings  # Importiere die Settings-Klasse
from app.scores.faces import load_faces
from app.scores.nsfw import load_nsfw
from app.scores.quality import load_quality
from app.services.thumbnail import get_thumbnail_path, generate_thumbnail, thumbnail
from app.tools import find_image_id_by_name, newpaircache
from app.utils.db_utils import delete_checkbox_status
from app.utils.logger_config import setup_logger
from app.utils.score_utils import delete_scores

logger = setup_logger(__name__)


def get_exif_data(full_image_path: Path):
    logger.info(f"📥 Starte get_exif_data() für: {full_image_path}")
    try:
        image = Image.open(full_image_path)
        exif_data = image._getexif()
        if not exif_data:
            logger.warning(f"[get_exif_data] Keine EXIF-Daten vorhanden: {full_image_path}")
            return None, None

        exif = {}
        gps_info = {}
        for tag, value in exif_data.items():
            decoded = TAGS.get(tag, tag)
            if decoded == "GPSInfo":
                for t in value:
                    sub_decoded = GPSTAGS.get(t, t)
                    gps_info[sub_decoded] = value[t]
            else:
                exif[decoded] = value

        date_taken = exif.get("DateTimeOriginal", None)
        gps_coords = get_coordinates(gps_info) if gps_info else None
        logger.info(f"[get_exif_data] ✅ Erfolgreich gelesen: {date_taken}, {gps_coords}")
        return date_taken, gps_coords
    except Exception as e:
        logger.error(f"[get_exif_data] ❌ Fehler beim Lesen der EXIF-Daten: {e}")
        return None, None


def get_coordinates(gps_info: dict) -> tuple | None:
    def convert_to_degrees(value):
        try:
            d, m, s = value
            return float(d) + float(m) / 60 + float(s) / 3600
        except Exception as e:
            logger.warning(f"[get_coordinates] ❌ Ungültige GPS-Daten: {value} -> {e}")
            return None

    lat = convert_to_degrees(gps_info.get("GPSLatitude", ()))
    if lat is not None and gps_info.get("GPSLatitudeRef") != "N":
        lat = -lat

    lon = convert_to_degrees(gps_info.get("GPSLongitude", ()))
    if lon is not None and gps_info.get("GPSLongitudeRef") != "E":
        lon = -lon

    return (lat, lon) if lat is not None and lon is not None else None


def reverse_geocode(coords: tuple) -> str | None:
    logger.info(f"📍 Starte reverse_geocode() für Koordinaten: {coords}")
    key = f"{coords[0]:.6f},{coords[1]:.6f}"
    if key in Settings.CACHE["geo_cache"]:
        logger.info(f"[reverse_geocode] 🔁 Aus Cache geladen: {key}")
        return Settings.CACHE["geo_cache"][key]

    geolocator = Nominatim(user_agent="photo_exif_locator")
    try:
        location = geolocator.reverse(coords, exactly_one=True, language='de', timeout=10)
        address = location.address if location else None
        Settings.CACHE["geo_cache"][key] = address
        logger.info(f"[reverse_geocode] ✅ Adresse gefunden: {address}")
        return address
    except Exception as e:
        logger.error(f"[reverse_geocode] ❌ Geocoding-Fehler: {e}")
        return None


def find_png_file(filename: str, use_cache: bool = True) -> List[Path]:
    """
    Search for a specific PNG file in all subdirectories of IMAGE_FILE_CACHE_DIR.
    Results are cached for better performance.

    Args:
        filename: Name of the PNG file to find (e.g., "xxx.png")
        use_cache: Whether to use cached results if available

    Returns:
        List of Path objects where the file was found
    """
    logger.info(f"🔍 Searching for file: {filename}")

    # Initialize cache if it doesn't exist
    if 'file_cache' not in Settings.CACHE:
        Settings.CACHE['file_cache'] = {}

    # Check cache first if enabled
    if use_cache and filename in Settings.CACHE['file_cache']:
        cached_paths = Settings.CACHE['file_cache'][filename]
        # Verify cached paths still exist
        valid_paths = [Path(p) for p in cached_paths if Path(p).exists()]
        if valid_paths:
            logger.info(f"✅ Found {len(valid_paths)} cached results for {filename}")
            return valid_paths

    # Search in all subdirectories
    root_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
    results = list(root_dir.rglob(filename))

    # Update cache with new results
    if results:
        Settings.CACHE['file_cache'][filename] = [str(p) for p in results]
        logger.info(f"✅ Found {len(results)} new results for {filename}")
    else:
        logger.warning(f"❌ No files found matching: {filename}")

    return results


def download_text_file(folder_name: str, image_name: str, cache_dir: str) -> str | None:
    logger.info(f"📥 Starte download_text_file() für {folder_name}/{image_name}")
    german_date = None
    full_image_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name / image_name
    date_str, gps = get_exif_data(full_image_path)
    dt = None
    if date_str:
        try:
            dt = datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S")
            german_date = dt.strftime("%d.%m.%Y %H:%M")
        except Exception as e:
            logger.warning(f"[download_text_file] ❌ Ungültiges Datum in {image_name}: {e}")

    location_name = reverse_geocode(gps) if gps else None
    full_txt_path = Path(cache_dir, f"{image_name}.txt")
    try:
        lines = full_txt_path.read_text(encoding="utf-8").splitlines()
        aufnahme_info = f"Aufgenommen: {german_date}" + (f" @ {location_name}" if location_name else "")
        if lines and lines[0].startswith("Aufgenommen:"):
            lines[0] = aufnahme_info
        else:
            lines.insert(0, aufnahme_info)
        full_txt_path.write_text("\n".join(lines), encoding="utf-8")
        if dt:
            os.utime(full_txt_path, (dt.timestamp(), dt.timestamp()))
            os.utime(full_image_path, (dt.timestamp(), dt.timestamp()))
        logger.info(f"[download_text_file] ✅ Textdatei aktualisiert: {full_txt_path}")
        return full_txt_path.read_text(encoding='utf-8')
    except FileNotFoundError:
        logger.warning(f"[download_text_file] ❌ Textdatei nicht gefunden: {full_txt_path}")
        return None
    except Exception as e:
        logger.error(f"[download_text_file] ❌ Fehler beim Lesen/Schreiben: {e}")
        return None


def find_file_by_name(root_dir: Path, image_name: str):
    return list(root_dir.rglob(image_name))


def download_and_save_image(folder_name: str, image_name: str, image_id: str) -> Path | None:
    logger.info(f"📥 Starte download_and_save_image() für {folder_name}/{image_name}")
    image_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name / image_name

    if not os.path.exists(image_path):
        treffer = find_file_by_name(Path(Settings.IMAGE_FILE_CACHE_DIR), image_name)
        for path in treffer:
            try:
                shutil.move(path, image_path)
                break
            except Exception as e:
                logger.warning(f"[download_and_save_image] ❌ Fehler beim Verschieben von {path} -> {image_path}: {e}")
                return None

    if not os.path.exists(image_path):
        logger.warning(f"[download_and_save_image] ❌ Originalbild nicht gefunden: {image_path}")
        return None

    thumbnail_path = get_thumbnail_path(image_id)
    if not os.path.exists(thumbnail_path):
        if not generate_thumbnail(image_path, thumbnail_path, image_id):
            return None

    return thumbnail_path


def prepare_image_data(count: int, folder_name: str, image_name: str):
    logger.info(f"📦 Starte prepare_image_data() für {image_name}")
    image_name = image_name.lower()
    pair_cache = newpaircache(folder_name)
    pair = pair_cache[image_name]
    image_id = pair['image_id']

    try:
        if image_name not in Settings.CACHE["text_cache"]:
            content = download_text_file(folder_name, image_name, Settings.TEXT_FILE_CACHE_DIR)
            Settings.CACHE["text_cache"][image_name] = content
    except Exception as e:
        logger.error(f"[prepare_image_data] ❌ Fehler beim Laden von Textdatei: {e}")
        Settings.CACHE["text_cache"][image_name] = f"Fehler beim Laden: {e}"

    thumbnail_src = thumbnail(count, folder_name, image_id, image_name)

    quality_scores = load_quality(Settings.DB_PATH, Settings.IMAGE_FILE_CACHE_DIR, folder_name, image_name)
    nsfw_scores = load_nsfw(Settings.DB_PATH, folder_name, image_name)
    extra_thumbnails1 = add_gif_thumbnail(image_name)
    extra_thumbnails2 = load_faces(Settings.DB_PATH, folder_name, image_name, image_id)
    extra_thumbnails = extra_thumbnails1 + extra_thumbnails2

    return {
        "thumbnail_src": thumbnail_src,
        "image_id": image_id,
        "quality_scores": quality_scores,
        "nsfw_scores": nsfw_scores,
        "extra_thumbnails": extra_thumbnails
    }


def add_gif_thumbnail(image_name: str) -> list[Any] | list[dict[str, str]]:
    gif_file = Settings.GIF_FILE_CACHE_PATH / f"{image_name}.gif"
    gif_file.parent.mkdir(parents=True, exist_ok=True)

    if not gif_file.exists():
        return []

    logger.info(f"[Gallery] Gif-Datei gefunden: {gif_file}")

    # Verwende relative Pfade für die URLs
    relative_path = gif_file.relative_to(Settings.DATA_DIR)

    extra_thumbnails = [{
        "src": f"/gallery/static/{relative_path}",
        "link": f"/gallery/static/{relative_path}",
        "image_name": f"/gallery/static/{Settings.GIF_FILE_CACHE_PATH.name}/{gif_file.name}"
    }]

    return extra_thumbnails


def delete_rendered_html_file(file_dir: Path, image_id: str) -> bool:
    try:
        success = False
        for file_path in file_dir.glob(f"*{image_id}*"):
            try:
                file_path.unlink()
                logger.info(f"[delete_rendered_html_file] ✅ gelöscht: {file_path}")
                success = True
            except Exception as e:
                logger.error(f"[delete_rendered_html_file] ❌ Fehler beim Löschen von {file_path}: {e}")
        return success
    except Exception as e:
        logger.error(f"[delete_rendered_html_file] ❌ Fehler beim Durchsuchen des Verzeichnisses: {e}")
        return False


def clean(image_name: str, image_id : str = None) -> JSONResponse | None:
    logger.info(f"🧹 Starte clean() für: {image_name}")
    text_cache = Settings.CACHE.get("text_cache")

    if image_name in text_cache:
        text_cache.pop(image_name, None)
        logger.info(f"[clean] ✅ text_cache gelöscht: {image_name}")

    if not image_id:
        image_id = find_image_id_by_name(image_name)

    delete_checkbox_status(image_name)
    delete_scores(image_name)
    delete_scores(image_id)

    if delete_rendered_html_file(Settings.RENDERED_HTML_DIR, image_id):
        logger.info(f"[clean] ✅ gerendertes HTML gelöscht: {image_id}")

    thumbnail_path = get_thumbnail_path(image_id)
    if os.path.exists(thumbnail_path):
        os.remove(thumbnail_path)
        logger.info(f"[clean] ✅ Thumbnail gelöscht: {thumbnail_path}")

    face_dir = Path(Settings.GESICHTER_FILE_CACHE_DIR)
    for file in face_dir.glob(f"{image_id}_*.jpg"):
        try:
            file.unlink()
            logger.info(f"[clean] ✅ Gesicht gelöscht: {file}")
        except Exception as e:
            logger.error(f"[clean] ❌ Fehler beim Löschen von {file}: {e}")

    return JSONResponse(content={"status": "ok", "image_name": image_name})
