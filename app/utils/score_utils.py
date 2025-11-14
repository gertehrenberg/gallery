import sqlite3

from ..config import Settings
from ..utils.logger_config import setup_logger

logger = setup_logger(__name__)


def delete_scores(image_name: str):
    """Löscht alle Scores für ein bestimmtes Bild"""
    logger.info(f"[delete_scores] Start – image_name={image_name}")
    with sqlite3.connect(Settings.DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       DELETE
                       FROM image_quality_scores
                       WHERE LOWER(image_name) = LOWER(?)
                       """, (image_name,))


def delete_scores_by_type(score_type: int):
    """Löscht alle Scores eines bestimmten Typs"""
    logger.info(f"[delete_scores_by_type] Start – score_type={score_type}")
    with sqlite3.connect(Settings.DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       DELETE
                       FROM image_quality_scores
                       WHERE score_type = ?
                       """, (score_type,))
