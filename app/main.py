import logging
import os
import threading

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from starlette.middleware.sessions import SessionMiddleware  # Korrigierter Import

# Importiere die Konfiguration aus app/config_new.py
from app.config import Settings
# Importiere die Datenbankfunktionen aus app/database.py
from app.database import init_db
# Importiere die Routen
from app.routes import auth, gallery, static, admin, login, dashboard
from app.scores.nsfw import log_missing_scores_from_cache
# Importiere die Cache-Funktionen aus app/services/cache_management.py
from app.services.cache_management import fillcache_local, fill_file_parents_cache
# Importiere die Google Drive Funktionen aus app/services/google_drive.py
from app.services.google_drive import verify_folders_exist

from app.routes.auth import SCOPES, TOKEN_FILE

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)  # Verwende einen benannten Logger

# FastAPI application setup
app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="**idefix**")  # Secret Key aus Config laden!
# Templates setup
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))  # Adjusted path

geo_cache = {}  # Muss initialisiert werden.  Vielleicht als Dependency?


@app.on_event("startup")
def init_service():
    """Initialisiert Dienste beim Start der Anwendung."""
    os.environ.pop("HTTPS_PROXY", None)
    os.environ.pop("HTTP_PROXY", None)

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        logging.warning("Kein Token gefunden. Bitte besuche 'http://localhost/gallery/auth'.")
        return

    try:
        service = build('drive', 'v3', credentials=creds)
        # Rufe verify_folders_exist mit den Kategorien aus der Konfiguration auf
        verify_folders_exist(service, Settings.kategorien)
    except Exception as e:
        logger.error(f"Fehler beim Initialisieren des Google Drive Service: {e}")
        raise  # Stops app startup

    threading.Thread(target=slow_start, daemon=True).start()


def slow_start():
    """Langsame Initialisierung im Hintergrund."""
    logger.info("🏁 Starte langsames Initialisieren...")
    try:
        init_db(Settings.DB_PATH)
        fillcache_local(Settings.PAIR_CACHE_PATH, Settings.IMAGE_FILE_CACHE_DIR)
        fill_file_parents_cache(Settings.DB_PATH)
    except Exception as e:
        logger.error(f"Fehler bei der Initialisierung: {e}")
        # Anwendungsstart abbrechen
        os._exit(1)
    Settings.app_ready = True
    logger.info("🚀 Anwendung bereit!")


# Include Routers
app.include_router(auth.router)
app.include_router(login.router)
app.include_router(gallery.router)
app.include_router(static.router)
app.include_router(admin.router)
app.include_router(dashboard.router)

def local():
    import uvicorn

    """Initialisiert Dienste beim Start der Anwendung."""
    os.environ.pop("HTTPS_PROXY", None)
    os.environ.pop("HTTP_PROXY", None)

    Settings.CRED_FILE = '../secrets/credentials.json'
    Settings.TOKEN_FILE = '../secrets/token.json'
    Settings.DB_PATH = '../gallery_local.db'
    Settings.PAIR_CACHE_PATH = "../cache/pair_cache_local.json"
    Settings.IMAGE_FILE_CACHE_DIR = "../cache/imagefiles"
    Settings.RENDERED_HTML_DIR = "../cache/rendered_html"

    uvicorn.run(app, host="0.0.0.0", port=Settings.DEFAULT_PORT)


if __name__ == "__main__":

    Settings.PAIR_CACHE_PATH = "../cache/pair_cache_local.json"
    Settings.IMAGE_FILE_CACHE_DIR = "../cache/imagefiles"
    fillcache_local(Settings.PAIR_CACHE_PATH, Settings.IMAGE_FILE_CACHE_DIR)



