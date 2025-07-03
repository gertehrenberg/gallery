import asyncio
import subprocess
from pathlib import Path
from typing import Any

from app.config import Settings
from app.config_gdrive import SettingsGdrive
from app.utils.find_missing_text_files import move_images_without_textfile_2_recheck

# Type-Hint für IDE und Fallback-Import
try:
    from recoll import recoll
except ImportError:
    # Dummy-Klasse für IDE, wird nie zur Laufzeit verwendet
    class recoll:  # type: ignore
        @staticmethod
        def connect(*args: Any, **kwargs: Any) -> Any: ...

from app.utils.logger_config import setup_logger
from app.utils.progress import init_progress_state, stop_progress

logger = setup_logger(__name__)


async def reload_texte():
    await init_progress_state()
    try:
        await move_images_without_textfile_2_recheck(Path(Settings.IMAGE_FILE_CACHE_DIR), Path(Settings.TEXT_FILE_CACHE_DIR))
    finally:
        await stop_progress()


async def search_recoll(query: str) -> list:
    config_dir: str = Settings.RECOLL_CONFIG_DIR

    try:
        cmd = ["recollq", "-c", config_dir, query]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            logger.info(f"Fehler bei der Suche: {result.stderr}")
            return []

        # Ergebnisse nach Zeilen aufteilen und leere Zeilen entfernen
        results = [line.strip() for line in result.stdout.split('\n') if line.strip()]

        # Die erste Zeile (Anzahl der Ergebnisse) überspringen
        results = results[1:]

        # Extrahiere nur die Dateinamen aus den Pfaden
        cleaned_results = []
        for line in results:
            # Suche nach dem Muster [file:///path/filename]
            if ']' in line and '[' in line:
                # Extrahiere den Teil zwischen den letzten eckigen Klammern
                filename = line.split('[')[-1].split(']')[0]
                # Entferne .txt Endung
                if filename.endswith('.txt'):
                    base_name = filename[:-4]
                    cleaned_results.append(base_name)

        return cleaned_results

    except Exception as e:
        logger.info(f"Fehler bei der Ausführung der Suche: {e}")
        return []


def p4():
    """Konfiguration und Start des Gemini-Prozesses"""
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")
    Settings.RENDERED_HTML_DIR = "../../cache/rendered_html"
    Settings.RECOLL_CONFIG_DIR = "../../cache/recoll_config"

    # Fix: Properly run the async function and store its result
    erg = asyncio.run(search_recoll("keine Verbesserung"))
    logger.info(erg)


if __name__ == "__main__":
    p4()
