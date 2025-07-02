import os
from pathlib import Path

from app.config import Settings
from app.config_gdrive import collect_all_folders, save_dict, SettingsGdrive, folder_id_by_name, folder_name_by_id
from app.routes.auth import load_drive_service_token
from logger_config import setup_logger

logger = setup_logger(__name__)


def update_folder_cache(service):
    """
    Aktualisiert den Cache aller Google Drive Ordner aus 'Meine Ablage' und speichert diesen in GDRIVE_FOLDERS_PKL.
    """
    logger.info("Aktualisiere Folder-Cache...")

    name_to_id = {}
    id_to_name = {}

    # 'root' ist die ID für "Meine Ablage"
    collect_all_folders(service, "root", name_to_id, id_to_name)

    # Cache-Dictionary erstellen
    folder_dict = {
        "name_to_id": name_to_id,
        "id_to_name": id_to_name
    }

    # In Datei speichern
    save_dict(folder_dict, SettingsGdrive.GDRIVE_FOLDERS_PKL)

    # Globalen Cache aktualisieren
    global _cached_folder_dict
    _cached_folder_dict = folder_dict

    logger.info(f"[✓] {len(name_to_id)} Ordner im Cache gespeichert")


def log_folder_ids():
    """Logge die Folder-IDs für alle Kategorien"""
    logger.info("Start Logging Folder-IDs für alle Kategorien")
    for kategorie in Settings.kategorien():
        folder_id = folder_id_by_name(kategorie["key"])
        logger.info(
            f"Kategorie: {kategorie['key']} ({kategorie['label']}) {kategorie['icon']} → Folder-ID: {folder_id}")
    logger.info("Ende Logging Folder-IDs")


if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../../secrets/innate-setup-454010-i9-f92b1b6a1c44.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")
    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))

    update_folder_cache(service)
    folder_id = folder_id_by_name('real')
    folder_name = folder_name_by_id(folder_id)
    logger.info(f"{folder_name} : {folder_id}")

    log_folder_ids()
