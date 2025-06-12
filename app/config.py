from pathlib import Path
from typing import List


score_type_map = {
    "drawings": 10,
    "hentai": 11,
    "neutral": 12,
    "porn": 13,
    "sexy": 14,
    "nsfw_score": 15
}
reverse_score_type_map = {v: k for k, v in score_type_map.items()}


class Settings:
    """
    Konfigurationsklasse für die FastAPI-Anwendung.

    Diese Klasse definiert alle Konstanten und Einstellungen, die für die Anwendung benötigt werden.
    """
    DATA_DIR = Path('/data')

    DB_PATH = 'gallery_local.db'

    PAIR_CACHE_PATH = DATA_DIR / 'pair_cache_local.json'
    RENDERED_HTML_DIR = DATA_DIR / "rendered_html"
    THUMBNAIL_CACHE_DIR_300 = DATA_DIR / 'thumbnailfiles300'
    GESICHTER_FILE_CACHE_DIR = '/data/facefiles'
    CACHE_DATEI_NAME = DATA_DIR / "geo_cache.json"

    IMAGE_EXTENSIONS = (".bmp", ".gif", ".jpg", ".jpeg", ".png")
    IMAGE_FILE_CACHE_DIR = '/data/imagefiles'

    TEXT_EXTENSIONS = {".txt"}
    TEXT_FILE_CACHE_DIR = DATA_DIR / 'textfiles'

    GDRIVE_HASH_FILE = "hashes.json"
    GALLERY_HASH_FILE = "gallery202505_hashes.json"

    SAVE_LOG_FILE = "/data/from_save_"

    REDIRECT_URI = "http://localhost/gallery/auth/callback"  # Sollte konfigurierbar sein

    WORKFLOW_DIR = '/data/workflows'

    COSTS_FILE_DIR = '/data/costs'

    # Kategorien für die Bildergalerie
    kategorien: List[dict] = [
        {"key": "real", "label": "Alle Bilder", "icon": "💾", "folderid": "1fyE_ZYoVoGZ7ehjuWrS9Kd6WW4w2UZWy"},
        {"key": "top", "label": "Fast Perfekt", "icon": "💎", "folderid": "1oKNY7jB8hEFMEn7amA6Osrbo8K9z5jAX"},
        {"key": "delete", "label": "Löschen", "icon": "❌", "folderid": "1wjUj6NHZ_ZHwlahQuJUbCTf_HplqePVw"},
        {"key": "recheck", "label": "Neu", "icon": "🔄", "folderid": "1EyrM6LLv_nEyB8s6zzGDGzf-hcPC76dg"},
        {"key": "bad", "label": "Schlecht", "icon": "⛔", "folderid": "1EkX7TxoRJlYUyeNA10T3Gzdt5Nd7yRRf"},
        {"key": "sex", "label": "Anzüglich", "icon": "🔞", "folderid": "1XCOjgEi0m0YGu11oPo3IZJizUf3p3tZg"},
        {"key": "ki", "label": "KI", "icon": "🤖", "folderid": "1LWF_V26zvX-W9vRNwscmeQ6U7YeJxOuL"},
        {"key": "comfyui", "label": "ComfyUI", "icon": "🛠️", "folderid": "1UjmQV-dO3y8uhqmWjSIzU1t7w6-rQEqG"},
        {"key": "document", "label": "Dokumente", "icon": "📄", "folderid": "1oKNY7jB8hEFMEn6amA6Osrbo8K9z5jAW"},
        {"key": "double", "label": "Doppelt?", "icon": "👯", "folderid": "1oKNY7jB8hEFMEn6amA6Osrbo8K9z5jAX"},
    ]
    CHECKBOX_CATEGORIES = [k["key"] for k in kategorien]

    # Standardordnername
    FOLDER_NAME = next((k["key"] for k in kategorien if k["key"] == "real"), None)

    # Sonstige Konstanten
    KEIN_TEXT_GEFUNDEN = "Kein Text gefunden"
    CONTAINER = "gallery"
    DB_PATH_IN_CONTAINER = "/app/gallery_local.db"
    DUMP_FILE = "dump.sql"
    LOCAL_DB = "gallery_local.db"
    DEFAULT_PORT = 8000  # Standard Port für die Anwendung
    CACHE_DIR = "cache"

    # Caches (Dictionary-Struktur beibehalten, aber zentralisiert)
    CACHE = {
        "image_cache": {},  # file_id -> { 'thumbnail': url }
        "text_cache": {},  # lowercase text filename -> content
        "pair_cache": {},  # lowercase image filename -> { image_id, text_id, web_link }
        "file_parents_cache": {},
        "geo_cache": {},
        "score_filter_result": {}
    }

    folders_total = len(kategorien)
    current_loading_folder = ""
    folders_loaded = 0
