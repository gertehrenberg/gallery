import sqlite3

from ..config import Settings
from ..tools import dict2md5
from ..utils.logger_config import setup_logger

logger = setup_logger(__name__)


def save_folder_status_to_db(db_path: str, image_id: str, folder_key: str):
    logger.info(f"[save_folder_status_to_db] Start – db_path={db_path}, image_id={image_id}, folder_key={folder_key}")
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO image_folder_status (image_id, folder_id)
                VALUES (?, ?)
            """, (image_id, folder_key))
            conn.commit()
    except Exception as e:
        logger.warning(f"[fill_folder_cache] Fehler beim Speichern von {image_id} → {folder_key}: {e}")


def load_folder_status_from_db(db_path: str):
    logger.info(f"[load_folder_status_from_db] Start – db_path={db_path}")
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM image_folder_status").fetchone()
        if row and row[0] > 0:
            return conn.execute("SELECT image_id, folder_id FROM image_folder_status").fetchall()
        return []


def load_face_from_db(db_path: str, image_name: str):
    logger.info(f"[load_face_from_db] Start – db_path={db_path}, image_name={image_name}")
    with sqlite3.connect(db_path) as conn:
        return conn.execute("""
                            SELECT score_type, score
                            FROM image_quality_scores
                            WHERE LOWER(image_name) = LOWER(?)
                              AND score_type BETWEEN 5 AND 5
                            """, (image_name,)).fetchall()


def save_quality_scores(db_path: str, image_name: str, quality_scores: dict[str, int], mapping):
    logger.info(
        f"[save_quality_scores] Start – db_path={db_path}, image_name={image_name}, quality_scores={quality_scores}")
    with sqlite3.connect(db_path) as conn:
        for label, value in quality_scores.items():
            type_id = mapping.get(label)
            if type_id:
                conn.execute("""
                    INSERT OR REPLACE INTO image_quality_scores (image_name, score_type, score)
                    VALUES (?, ?, ?)
                """, (image_name, type_id, value))


def load_nsfw_from_db(db_path: str, image_name: str):
    logger.info(f"[load_nsfw_from_db] Start – db_path={db_path}, image_name={image_name}")
    with sqlite3.connect(db_path) as conn:
        return conn.execute("""
                            SELECT score_type, score
                            FROM image_quality_scores
                            WHERE LOWER(image_name) = LOWER(?)
                              AND score_type BETWEEN 10 AND 15
                            """, (image_name,)).fetchall()


def save_nsfw_scores(db_path: str, image_name: str, nsfw_scores: dict[str, int], mapping):
    logger.info(f"[save_nsfw_scores] Start – db_path={db_path}, image_name={image_name}, nsfw_scores={nsfw_scores}")
    with sqlite3.connect(db_path) as conn:
        for label, value in nsfw_scores.items():
            type_id = mapping.get(label)
            if type_id:
                conn.execute("""
                    INSERT OR REPLACE INTO image_quality_scores (image_name, score_type, score)
                    VALUES (?, ?, ?)
                """, (image_name, type_id, value))


def load_all_nsfw_scores(db_path: str):
    logger.info(f"[load_all_nsfw_scores] Start – db_path={db_path}")
    with sqlite3.connect(db_path) as conn:
        return conn.execute("""
                            SELECT image_name, score_type, score
                            FROM image_quality_scores
                            WHERE score_type BETWEEN 10 AND 15
                            """).fetchall()


def load_quality_from_db(db_path: str, image_name: str):
    logger.info(f"[load_quality_from_db] Start – db_path={db_path}, image_name={image_name}")
    with sqlite3.connect(db_path) as conn:
        return conn.execute("""
                            SELECT score_type, score
                            FROM image_quality_scores
                            WHERE LOWER(image_name) = LOWER(?)
                              AND score_type BETWEEN 1 AND 2
                            """, (image_name,)).fetchall()


def delete_checkbox_status(image_name: str):
    logger.info(f"[delete_checkbox_status] Start – image_name={image_name}")
    with sqlite3.connect(Settings.DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       DELETE
                       FROM checkbox_status
                       WHERE LOWER(image_name) = LOWER(?)
                       """, (image_name,))


def delete_all_checkbox_status():
    """
    Löscht alle Einträge aus der checkbox_status Tabelle.
    """
    logger.info("[delete_all_checkbox_status] Start – Lösche alle Checkbox-Status Einträge")
    with sqlite3.connect(Settings.DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM checkbox_status")
        deleted_count = cursor.rowcount
        logger.info(f"[delete_all_checkbox_status] ✅ {deleted_count} Einträge gelöscht")


def delete_all_external_tasks():
    """
    Löscht alle Einträge aus der checkbox_status Tabelle.
    """
    logger.info("[delete_all_checkbox_status] Start – Lösche alle External Tasks-Status Einträge")
    with sqlite3.connect(Settings.DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM external_tasks")
        deleted_count = cursor.rowcount
        logger.info(f"[delete_all_external_tasks] ✅ {deleted_count} Einträge gelöscht")


def load_scores_from_db(db_path, image_name):
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("""
                            SELECT score_type, score
                            FROM image_quality_scores
                            WHERE LOWER(image_name) = LOWER(?)
                              AND score_type BETWEEN 10 AND 15
                            """, (image_name,)).fetchall()

    # Ergebnis in dict umwandeln:
    score_map = {}
    for score_type, score in rows:
        idx = score_type - 9  # 10 → score1, 11 → score2, ..., 15 → score6
        score_map[f"score{idx}"] = score

    return score_map


def load_comfyui_count(db_path, image_id: str) -> int:
    image_id = dict2md5(image_id)

    """
    Lädt den gespeicherten Wert für 'comfyui_count' eines Bildes aus der Tabelle 'text_status'.
    Gibt 0 zurück, wenn kein Eintrag gefunden wurde.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            key = "comfyui_count"
            cursor = conn.execute(
                """
                SELECT value
                FROM text_status
                WHERE image_name = ?
                  AND field = ?
                """,
                (image_id, key,)
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                try:
                    logger.info(f"[load_comfyui_count] ✅ Textfeld '{key}' für {image_id} geladen. Wert: {int(row[0])}")
                    return int(row[0])
                except ValueError:
                    logger.warning(
                        f"Wert für comfyui_count bei {image_id} ist kein Integer: {row[0]!r}. Verwende 0 als Fallback."
                    )
            return 0
    except sqlite3.Error as e:
        logger.error(f"Fehler beim Laden des comfyui_count für {image_id}: {e}")
        return 0
