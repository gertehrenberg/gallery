from enum import Enum
from pathlib import Path
from typing import List
from typing import Tuple

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

# Basis-Kategorien für die Bildergalerie
_base_kategorien: List[dict] = [
    {"key": "real", "label": "Alle Bilder", "icon": "🖼️"},
    {"key": "top", "label": "Perfekt", "icon": "😉"},
    {"key": "delete", "label": "Löschen", "icon": "❌"},
    {"key": "recheck", "label": "Neu", "icon": "🔄"},
    {"key": "bad", "label": "Schlecht", "icon": "⛔"},
    {"key": "sex", "label": "Anzüglich", "icon": "🔞"},
    {"key": "ki", "label": "KI", "icon": "🤖"},
    {"key": "comfyui", "label": "ComfyUI", "icon": "🛠️"},
    {"key": "document", "label": "Dokumente", "icon": "📄"},
    {"key": "double", "label": "Doppelt?", "icon": "👯"},
    {"key": "gemini", "label": "Analyse", "icon": "📊"}
]


class UserType(Enum):
    ADMIN = "admin"
    GUEST = "guest"


class Settings:
    _user_type = UserType.GUEST  # Default is Guest

    @classmethod
    def set_user_type(cls, user_type: UserType) -> None:
        if not isinstance(user_type, UserType):
            raise ValueError(f"user_type must be UserType enum, not {type(user_type)}")
        cls._user_type = user_type
        cls._kategorien = None  # Reset cache when user type changes
        cls._checkbox_categories = None  # Reset checkbox categories cache

    @classmethod
    def get_user_type(cls) -> UserType:
        return cls._user_type

    @classmethod
    def is_admin(cls) -> bool:
        return cls._user_type == UserType.ADMIN

    _kategorien = None  # Private class variable for caching
    _checkbox_categories = None  # Private class variable for checkbox categories cache

    @classmethod
    def kategorien(cls) -> List[dict]:
        if cls._kategorien is None:
            if cls._user_type == UserType.GUEST:
                cls._kategorien = [k for k in _base_kategorien if k["key"] != "sex"]
            else:
                cls._kategorien = _base_kategorien
        return cls._kategorien

    @classmethod
    def checkbox_categories(cls) -> List[str]:
        if cls._checkbox_categories is None:
            cls._checkbox_categories = [k["key"] for k in cls.kategorien()]
        return cls._checkbox_categories

    TEXTFILES_FOLDERNAME = "textfiles"

    app_ready = False
    DATA_DIR = Path('/data')

    DB_PATH = 'gallery_local.db'

    PAIR_CACHE_PATH = DATA_DIR / 'pair_cache_local.json'
    RENDERED_HTML_DIR = DATA_DIR / "rendered_html"
    THUMBNAIL_CACHE_DIR_300 = DATA_DIR / 'thumbnailfiles300'
    GESICHTER_FILE_CACHE_DIR = '/data/facefiles'
    CACHE_DATEI_NAME = DATA_DIR / "geo_cache.json"

    IMAGE_EXTENSIONS: Tuple[str, ...] = (".bmp", ".gif", ".jpg", ".jpeg", ".png")
    IMAGE_FILE_CACHE_DIR = '/data/imagefiles'

    GIF_FILE_CACHE_PATH = Path(DATA_DIR) / 'comfyui_gif'

    TEXT_EXTENSIONS = {".txt"}
    TEXT_FILE_CACHE_DIR = DATA_DIR / TEXTFILES_FOLDERNAME

    GDRIVE_HASH_FILE = "hashes.json"
    GALLERY_HASH_FILE = "gallery202505_hashes.json"

    SAVE_LOG_FILE = "/data/from_save_"

    REDIRECT_URI = "http://localhost/gallery/auth/callback"  # Sollte konfigurierbar sein

    WORKFLOW_DIR = '/data/workflows'

    COSTS_FILE_DIR = '/data/costs'

    TEMP_DIR_PATH = Path('/data/temp')

    PAGESIZE = 1000

    RECHECK = "recheck"
    COMFYUI = "comfyui"

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

    current_loading_folder = ""
    folders_loaded = 0

    RECOLL_CONFIG_DIR = "/data/recoll_config"
