from pathlib import Path

# Basisverzeichnisse
CACHE_DATEI_NAME = Path("geocache.json")
PFAD_WORT = Path(r"woerter")

PFAD_HASHES = Path("~/gallery/cache/hash.txt").expanduser()
PFAD_REAL = Path("~/gallery/cache/imagefiles").expanduser()

# Konfiguration
IMAGE_EXTENSIONS = (".bmp", ".gif", ".jpg", ".jpeg", ".png")
MIN_TXT_SIZE_BYTES = 100
MAX_IMAGES_PER_PAGE = 3 * 40
