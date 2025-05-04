import json
import os
import sys
from datetime import datetime
from pathlib import Path

from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
import geopy
from geopy.geocoders import Nominatim
from tqdm import tqdm

script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

from config import IMAGE_EXTENSIONS, IMAGE_FILE_CACHE_DIR, CACHE_DATEI_PATH

# Cache laden oder leeres Dictionary erstellen
if CACHE_DATEI_PATH.exists():
    with open(CACHE_DATEI_PATH, "r", encoding="utf-8") as f:
        geo_cache = json.load(f)
else:
    geo_cache = {}

def get_exif_data(image_path):
    image = Image.open(image_path)
    exif_data = image._getexif()
    if not exif_data:
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
    if gps_info:
        gps_coords = get_coordinates(gps_info)
    else:
        gps_coords = None

    return date_taken, gps_coords


def get_coordinates(gps_info):
    def convert_to_degrees(value):
        try:
            d, m, s = value
            return float(d) + float(m) / 60 + float(s) / 3600
        except Exception as e:
            print(f"[Warnung] Ungültige GPS-Daten: {value} → {e}")
            return None

    lat = convert_to_degrees(gps_info.get("GPSLatitude", ()))
    if lat is not None and gps_info.get("GPSLatitudeRef") != "N":
        lat = -lat

    lon = convert_to_degrees(gps_info.get("GPSLongitude", ()))
    if lon is not None and gps_info.get("GPSLongitudeRef") != "E":
        lon = -lon

    if lat is not None and lon is not None:
        return lat, lon
    return None


def reverse_geocode(coords):
    key = f"{coords[0]:.6f},{coords[1]:.6f}"
    if key in geo_cache:
        return geo_cache[key]

    geolocator = Nominatim(user_agent="photo_exif_locator")
    try:
        location = geolocator.reverse(coords, exactly_one=True, language='de', timeout=10)
        address = location.address if location else None
        geo_cache[key] = address
        return address
    except Exception as e:
        print(f"Geocoding-Fehler: {e}")
        return None


def process_images(folder_path):
    image_folder = Path(folder_path)
    image_files = [f for f in image_folder.iterdir() if f.suffix.lower() in IMAGE_EXTENSIONS]
    total = len(image_files)
    aktualisiert = 0

    aktualisiert = proccess_image(aktualisiert, image_files)

    # Cache speichern
    with open(CACHE_DATEI_PATH, "w", encoding="utf-8") as f:
        json.dump(geo_cache, f, ensure_ascii=False, indent=2)

    print(f"\n[Abschluss] {aktualisiert} TXT-Dateien aktualisiert. Cache enthält {len(geo_cache)} Einträge.")


def proccess_image(aktualisiert, image_files):
    for idx, img_path in enumerate(tqdm(image_files, desc="Verarbeite Bilder"), 1):
        date_str, gps = get_exif_data(img_path)
        if date_str:
            try:
                dt = datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S")
                german_date = dt.strftime("%d.%m.%Y %H:%M")
            except Exception as e:
                print(f"[Warnung] Ungültiges Datum in {img_path.name}: {e}")
                continue
        else:
            continue

        location_name = reverse_geocode(gps) if gps else None

        txt_path = img_path.with_name(img_path.name + ".txt")
        if txt_path.exists():
            lines = txt_path.read_text(encoding="utf-8").splitlines()
            aufnahme_info = f"Aufgenommen: {german_date}" + (f" @ {location_name}" if location_name else "")
            if lines and lines[0].startswith("Aufgenommen:"):
                lines[0] = aufnahme_info
            else:
                lines.insert(0, aufnahme_info)
            # txt_path.write_text("\n".join(lines), encoding="utf-8")
            os.utime(txt_path, (dt.timestamp(), dt.timestamp()))
            os.utime(img_path, (dt.timestamp(), dt.timestamp()))
            aktualisiert += 1
            if aktualisiert > 0:
                print(f"→ TXT aktualisiert: {txt_path.name}")
    return aktualisiert


def main():
    process_images(IMAGE_FILE_CACHE_DIR)


if __name__ == "__main__":
    main()
