import asyncio
from pathlib import Path
import heapq
from typing import Set, List
import datetime

from app.config import Settings
from app.config_gdrive import SettingsGdrive
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


def format_timestamp(timestamp: float) -> str:
    """Convert timestamp to human readable format"""
    return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


async def get_latest_unprocessed_files(directory: str, n: int, processed_files: Set[str], extensions: Set[str]) -> List[str]:
    """
    Gibt die n neuesten unverarbeiteten Text-Dateien asynchron zurück
    """
    files = []
    for file in Path(directory).glob('*'):
        if file.is_file() and file.suffix.lower() in extensions:
            abs_path = str(file.absolute())
            if abs_path not in processed_files:
                stat = await asyncio.to_thread(file.stat)
                files.append((stat.st_mtime, abs_path))
                logger.debug(f"Gefundene Datei: {file.name}, Datum: {format_timestamp(stat.st_mtime)}")

    newest_files = heapq.nlargest(n, files) if files else []
    return [f[1] for f in newest_files]


async def process_file(file_path: str):
    """
    Verarbeitet eine einzelne Text-Datei asynchron
    """
    try:
        file_stat = await asyncio.to_thread(Path(file_path).stat)
        modified_time = format_timestamp(file_stat.st_mtime)
        logger.info(f"Verarbeite Text-Datei: {file_path} (Zuletzt geändert: {modified_time})")

        content = await asyncio.to_thread(lambda: Path(file_path).read_text())
        logger.info(f"Datei gelesen: {file_path} ({len(content)} Zeichen)")
        return True
    except Exception as e:
        logger.error(f"Fehler bei der Verarbeitung von {file_path}: {e}")
        return False


async def monitor_files(directory: str, batch_size: int = 5, extensions: Set[str] = None, sleep: int = 15):
    """
    Überwacht ein Verzeichnis auf neue Text-Dateien
    """
    if extensions is None:
        extensions = Settings.TEXT_EXTENSIONS

    processed_files: Set[str] = set()
    start_time = format_timestamp(datetime.datetime.now().timestamp())

    logger.info(f"Starte Überwachung von Text-Dateien in: {directory}")
    logger.info(f"Start Zeit: {start_time}")
    logger.info(f"Überwachte Endungen: {extensions}")

    while True:
        try:
            current_time = format_timestamp(datetime.datetime.now().timestamp())
            new_files = await get_latest_unprocessed_files(directory, batch_size, processed_files, extensions)

            if new_files:
                tasks = [process_file(file) for file in new_files]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for file, result in zip(new_files, results):
                    if result is True:
                        processed_files.add(file)
                        logger.info(f"Erfolgreich verarbeitet: {file}")

                logger.info(f"Verarbeitete Dateien insgesamt: {len(processed_files)}")

            logger.info(f"[{current_time}] Warte {sleep} Sekunden...")
            await asyncio.sleep(sleep)

        except Exception as e:
            logger.error(f"Fehler im Hauptloop: {e}")
            await asyncio.sleep(sleep)


async def main():
    try:
        watch_dirs = [
            (Settings.TEXT_FILE_CACHE_DIR, 5, Settings.TEXT_EXTENSIONS, 10),
        ]
        tasks = [monitor_files(dir_path, batch_size=batch_size, extensions=extensions, sleep=sleep_time)
                 for dir_path, batch_size, extensions, sleep_time in watch_dirs]
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Beende Überwachung...")


if __name__ == "__main__":
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")
    asyncio.run(main())