from datetime import datetime
import logging
import os
from pathlib import Path

from PIL import Image, ImageOps
from PIL.ExifTags import TAGS, GPSTAGS
from geopy.geocoders import Nominatim

from app.config_new import Settings  # Importiere die Settings-Klasse
from app.scores.nsfw import load_nsfw
from app.scores.quality import load_quality


def get_exif_data(full_image_path: Path):
    """Liest EXIF-Daten aus einer Bilddatei."""
    try:
        image = Image.open(full_image_path)
        exif_data = image._getexif()
        if not exif_data:
            logging.warning(f"[get_exif_data] Keine Daten: {full_image_path}")
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
        return date_taken, gps_coords
    except Exception as e:
        logging.error(f"[get_exif_data] Fehler beim Lesen der EXIF-Daten: {e}")
        return None, None


def get_coordinates(gps_info: dict) -> tuple | None:
    """Extrahiert die GPS-Koordinaten aus den EXIF-Daten."""

    def convert_to_degrees(value):
        try:
            d, m, s = value
            return float(d) + float(m) / 60 + float(s) / 3600
        except Exception as e:
            logging.warning(f"[Warnung] Ungültige GPS-Daten: {value} -> {e}")
            return None

    lat = convert_to_degrees(gps_info.get("GPSLatitude", ()))
    if lat is not None and gps_info.get("GPSLatitudeRef") != "N":
        lat = -lat

    lon = convert_to_degrees(gps_info.get("GPSLongitude", ()))
    if lon is not None and gps_info.get("GPSLongitudeRef") != "E":
        lon = -lon

    return (lat, lon) if lat is not None and lon is not None else None


def reverse_geocode(coords: tuple) -> str | None:
    """Führt eine Reverse-Geocoding durch, um den Ort zu ermitteln."""
    key = f"{coords[0]:.6f},{coords[1]:.6f}"
    if key in Settings.CACHE["geo_cache"]:
        return Settings.CACHE["geo_cache"][key]

    geolocator = Nominatim(user_agent="photo_exif_locator")
    try:
        location = geolocator.reverse(coords, exactly_one=True, language='de', timeout=10)
        address = location.address if location else None
        Settings.CACHE["geo_cache"][key] = address  # Speichern im Cache
        return address
    except Exception as e:
        logging.error(f"Geocoding-Fehler: {e}")
        return None


def download_text_file(folder_name: str, image_name: str, cache_dir: str) -> str | None:
    """Lädt die Textdatei zum Bild und fügt EXIF-Daten ein."""
    german_date = None
    full_image_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name / image_name
    date_str, gps = get_exif_data(full_image_path)
    dt = None
    if date_str:
        try:
            dt = datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S")
            german_date = dt.strftime("%d.%m.%Y %H:%M")
        except Exception as e:
            logging.warning(f"[Warnung] Ungültiges Datum in {image_name}: {e}")

    location_name = reverse_geocode(gps) if gps else None

    full_txt_path = Path(cache_dir, f"{image_name}.txt")
    try:
        lines = full_txt_path.read_text(encoding="utf-8").splitlines()
        aufnahme_info = f"Aufgenommen: {german_date}" + (f" @ {location_name}" if location_name else "")
        if lines and lines[0].startswith("Aufgenommen:"):
            lines[0] = aufnahme_info
        else:
            lines.insert(0, aufnahme_info)
        full_txt_path.write_text("\n".join(lines), encoding="utf-8")  # Schreibt die Datei neu mit aufnahme info
        if dt:
            os.utime(full_txt_path, (dt.timestamp(), dt.timestamp()))
            os.utime(full_image_path, (dt.timestamp(), dt.timestamp()))

        return full_txt_path.read_text(encoding='utf-8')
    except FileNotFoundError:
        logging.warning(f"[download_text_file] Textdatei nicht gefunden: {full_txt_path}")
        return None
    except Exception as e:
        logging.error(f"[download_text_file] Fehler beim Lesen/Schreiben der Textdatei: {e}")
        return None


def download_and_save_image(folder_name: str, image_name: str) -> str | None:
    """Erzeugt ein Thumbnail aus einer lokalen Originalbilddatei."""
    image_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name / image_name
    thumbnail_path = os.path.join(Settings.THUMBNAIL_CACHE_DIR_300, image_name)

    if not os.path.exists(image_path):
        # TODO: Sollte die Funktion find_file_by_name aus dem globalen Kontext importiert werden?
        # Das würde Abhängigkeiten klarer machen
        # Hier ist die Funktion auch nicht definiert.
        # treffer = find_file_by_name(Path(IMAGE_FILE_CACHE_DIR), image_name)
        # for path in treffer:
        #    try:
        #        shutil.move(path, image_path)
        #    except Exception as e:
        #        logging.warning(f"[download_and_save_image] Originalbild nicht gefunden: {image_path}")
        #        return None
        #    break
        logging.warning(f"[download_and_save_image] Originalbild nicht gefunden: {image_path}")
        return None

    if not os.path.exists(thumbnail_path):
        if not generate_thumbnail(image_path, thumbnail_path, image_name):
            return None
    return thumbnail_path


def generate_thumbnail(image_path: Path, thumbnail_path: str, image_name: str) -> bool:
    """Erzeugt ein Thumbnail und speichert es."""
    try:
        logging.info(f"[generate_thumbnail] Erzeuge Thumbnail für {image_name}")
        img = Image.open(image_path)
        img = ImageOps.exif_transpose(img)
        img.thumbnail((300, 300), Image.Resampling.LANCZOS)
        os.makedirs(os.path.dirname(thumbnail_path), exist_ok=True)
        img.convert("RGB").save(thumbnail_path, format="JPEG")
        logging.info(f"[generate_thumbnail] ✅ Thumbnail gespeichert: {thumbnail_path}")
        return True
    except Exception as e:
        logging.error(f"[generate_thumbnail] ❌ Fehler beim Erzeugen von Thumbnail {image_name}: {e}")
        return False


def get_extra_thumbnails(folder_name: str, image_name: str) -> list[dict]:
    """Erzeugt zusätzliche Thumbnails für Gesichter und liefert deren Pfade."""
    full_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name / image_name
    # TODO: gen_faces auch importieren
    # gen_faces(folder_name, image_name)  # Gesichter extrahieren
    stem = full_path.stem
    face_dir = Path(Settings.GESICHTER_FILE_CACHE_DIR)
    base_url = "/static/facefiles"
    return [
        {
            "src": f"/gallery{base_url}/{thumb.name}",
            "link": f"/gallery{base_url}/{thumb.name}",
            "image_name": f"{thumb.name}"
        }
        for thumb in sorted(face_dir.glob(f"{stem}_*.jpg"))
    ]


def prepare_image_data(count: int, folder_name: str, image_name: str):
    """Bereitet alle Variablen für ein einzelnes Bild vor, inkl. Qualität."""
    image_name = image_name.lower()
    pair = Settings.CACHE["pair_cache"][image_name]
    image_id = pair['image_id']

    try:
        if image_name not in Settings.CACHE["text_cache"]:
            content = download_text_file(folder_name, image_name, Settings.TEXT_FILE_CACHE_DIR)
            Settings.CACHE["text_cache"][image_name] = content
    except Exception as e:
        Settings.CACHE["text_cache"][image_name] = f"Fehler beim Laden: {e}"

    local_thumbnail_path = download_and_save_image(folder_name, image_name)

    if local_thumbnail_path and os.path.exists(local_thumbnail_path):
        if count != 1:
            thumbnail_src = f"/gallery/static/thumbnails/{image_name}"
        else:
            thumbnail_src = f"/gallery/static/imagefiles/{folder_name}/{image_name}"
    else:
        thumbnail_src = "https://via.placeholder.com/150?text=Kein+Bild"

    quality_scores = load_quality(Settings.DB_PATH, Settings.IMAGE_FILE_CACHE_DIR, folder_name, image_name)
    nsfw_scores = load_nsfw(Settings.DB_PATH, folder_name, image_name)

    extra_thumbnails = get_extra_thumbnails(folder_name, image_name)

    return {
        "thumbnail_src": thumbnail_src,
        "image_id": image_id,
        "quality_scores": quality_scores,
        "nsfw_scores": nsfw_scores,
        "extra_thumbnails": extra_thumbnails
    }
