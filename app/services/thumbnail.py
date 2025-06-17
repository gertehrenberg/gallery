import logging
import os
from PIL import Image, ImageOps
from pathlib import Path

from app.config import Settings  # Importiere die Settings-Klasse


def get_thumbnail_path(image_id) -> Path:
    return Path(Settings.THUMBNAIL_CACHE_DIR_300) / f"{image_id}.png"


def generate_thumbnail(image_path: Path, thumbnail_path: Path, image_id: str) -> bool:
    try:
        logging.info(f"[generate_thumbnail] 🖼️ Erzeuge Thumbnail für {image_id}")
        img = Image.open(image_path)
        img = ImageOps.exif_transpose(img)
        img.thumbnail((300, 300), Image.Resampling.LANCZOS)
        os.makedirs(os.path.dirname(thumbnail_path), exist_ok=True)
        img.convert("RGB").save(thumbnail_path, format="JPEG")
        logging.info(f"[generate_thumbnail] ✅ Thumbnail gespeichert: {thumbnail_path}")
        return True
    except Exception as e:
        logging.error(f"[generate_thumbnail] ❌ Fehler beim Erzeugen von Thumbnail {image_id}: {e}")
        return False


def thumbnail(count, folder_name, image_id, image_name):
    from app.services.image_processing import download_and_save_image
    local_thumbnail_path = download_and_save_image(folder_name, image_name, image_id)

    if local_thumbnail_path and os.path.exists(local_thumbnail_path):
        if count != 1:
            thumbnail_src = f"/gallery/static/thumbnails/{image_id}.png"
        else:
            thumbnail_src = f"/gallery/static/imagefiles/{folder_name}/{image_name}"
    else:
        thumbnail_src = "https://via.placeholder.com/150?text=Kein+Bild"
    return thumbnail_src
