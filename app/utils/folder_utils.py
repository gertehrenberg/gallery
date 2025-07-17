import sqlite3

from ..utils.logger_config import setup_logger

logger = setup_logger(__name__)


def count_folder_entries(db_path: str, folder_key: str) -> int:
    """
    Zählt die Anzahl der Einträge für einen bestimmten Ordner in der Datenbank.

    Args:
        db_path: Pfad zur SQLite-Datenbank
        folder_key: Schlüssel/ID des Ordners

    Returns:
        int: Anzahl der Einträge für den angegebenen Ordner
    """
    logger.info(f"[count_folder_entries] Start – db_path={db_path}, folder_key={folder_key}")
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM image_folder_status WHERE folder_id = ?",
            (folder_key,)
        ).fetchone()
        return row[0] if row else 0
