import sqlite3

from app.config import Settings
from app.tools import find_image_name_by_id
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


def set_status(image_name: str, key: str, checked: int = 1):
    logger.info(f"[set_status] 📝 Setze Status für {image_name}, Checkbox: {key}, Wert: {checked}")
    if key is None:
        return
    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            conn.execute(
                """
                INSERT INTO checkbox_status (image_name, checkbox, checked)
                VALUES (?, ?, ?) ON CONFLICT(image_name, checkbox)
                DO
                UPDATE SET checked = excluded.checked
                """,
                (image_name, key, checked)
            )
            conn.commit()
        logger.info(f"[set_status] ✅ Status gesetzt für {image_name} ({key}={checked})")
    except sqlite3.Error as e:
        logger.error(f"[set_status] ❌ Fehler beim Setzen des Status für {image_name}: {e}")
        raise


def load_status(image_name: str):
    logger.info(f"[load_status] 📥 Lade Status für: {image_name}")
    status = {}
    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            rows = conn.execute("""
                                SELECT checkbox, checked
                                FROM checkbox_status
                                WHERE image_name = ?
                                """, (image_name,))
            for row in rows:
                status[row[0]] = bool(row[1])

            rows = conn.execute("""
                                SELECT field, value
                                FROM text_status
                                WHERE image_name = ?
                                """, (image_name,))
            for row in rows:
                status[row[0]] = row[1]
        logger.info(f"[load_status] ✅ Status geladen: {status}")
    except sqlite3.Error as e:
        logger.error(f"[load_status] ❌ Fehler beim Laden des Status für {image_name}: {e}")
        raise
    return status


def save_status(image_id: str, data: dict):
    logger.info(f"[save_status] 💾 Speichere Status für ID: {image_id}, Daten: {data}")
    image_name = find_image_name_by_id(image_id)
    logger.info(f"[save_status] Speichern des Status für {image_name}. Eingabedaten: {data}")

    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            for key, value in data.items():
                if key in Settings.checkbox_categories():
                    checked = 1 if str(value).lower() in ["1", "true", "on"] else 0
                    conn.execute("""
                        INSERT OR REPLACE INTO checkbox_status (image_name, checkbox, checked)
                        VALUES (?, ?, ?)
                    """, (image_name, key, checked))
                    logger.info(f"[save_status] ✅ Checkbox '{key}' für {image_name} gespeichert. Wert: {checked}")
                else:
                    conn.execute("""
                        INSERT OR REPLACE INTO text_status (image_name, field, value)
                        VALUES (?, ?, ?)
                    """, (image_name, key, value))
                    logger.info(f"[save_status] ✅ Textfeld '{key}' für {image_name} gespeichert. Wert: {value}")
            conn.commit()
    except sqlite3.Error as e:
        logger.error(f"[save_status] ❌ Fehler beim Speichern des Status für {image_name}: {e}")
        raise