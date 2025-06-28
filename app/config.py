from pathlib import Path
from typing import List

score_type_map = {
    "q1": 1,
    "q2": 2,
    "faces": 5,
    "text": 9,
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
    app_ready = False
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

    TEMP_DIR_PATH = Path('/data/temp')

    # Kategorien für die Bildergalerie
    kategorien: List[dict] = [
        {"key": "real", "label": "Alle Bilder", "icon": "💾"},
        {"key": "top", "label": "Fast Perfekt", "icon": "💎"},
        {"key": "delete", "label": "Löschen", "icon": "❌"},
        {"key": "recheck", "label": "Neu", "icon": "🔄"},
        {"key": "bad", "label": "Schlecht", "icon": "⛔"},
        {"key": "sex", "label": "Anzüglich", "icon": "🔞"},
        {"key": "ki", "label": "KI", "icon": "🤖"},
        {"key": "comfyui", "label": "ComfyUI", "icon": "🛠️"},
        {"key": "document", "label": "Dokumente", "icon": "📄"},
        {"key": "double", "label": "Doppelt?", "icon": "👯"},
        {"key": "gemini", "label": "n8n Scan", "icon": "🤖"}
    ]

    CHECKBOX_CATEGORIES = [k["key"] for k in kategorien]

    PAGESIZE = 1000

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
        "geo_cache": {},
        "score_filter_result": {}
    }

    folders_total = len(kategorien)
    current_loading_folder = ""
    folders_loaded = 0

    RECOLL_CONFIG_DIR = "/data/recoll_config"
