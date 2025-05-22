import logging
import shutil
import sqlite3
import time
from pathlib import Path

from app.config_new import Settings  # Importiere die Settings-Klasse
from app.tools import find_image_name_by_id

logger = logging.getLogger(__name__)


def init_db(db_path):
    logging.info(f"[init_db] 🛠️ Initialisiere Datenbank: {db_path}")
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                         CREATE TABLE IF NOT EXISTS checkbox_status
                         (
                             image_name
                             TEXT,
                             checkbox
                             TEXT,
                             checked
                             INTEGER,
                             PRIMARY
                             KEY
                         (
                             image_name,
                             checkbox
                         )
                             )
                         """)
            conn.execute("""
                         CREATE TABLE IF NOT EXISTS text_status
                         (
                             image_name
                             TEXT,
                             field
                             TEXT,
                             value
                             TEXT,
                             PRIMARY
                             KEY
                         (
                             image_name,
                             field
                         )
                             )
                         """)

            conn.execute("""
                         CREATE TABLE IF NOT EXISTS image_folder_status
                         (
                             image_id
                             TEXT
                             PRIMARY
                             KEY,
                             folder_id
                             TEXT
                         )
                         """)

            image_quality_scores(conn)
        logging.info(f"[init_db] ✅ Datenbank initialisiert")
    except sqlite3.Error as e:
        logging.error(f"[init_db] ❌ Fehler beim Initialisieren: {e}")


def image_quality(conn):
    logging.info(f"[image_quality] 🧮 Erstelle Tabelle image_quality")
    try:
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS image_quality
                     (
                         image_name
                         TEXT
                         PRIMARY
                         KEY,
                         scoreq1
                         INTEGER,
                         scoreq2
                         INTEGER
                     )
                     """)
        logging.info(f"[image_quality] ✅ Tabelle erstellt")
    except sqlite3.Error as e:
        logging.error(f"[image_quality] ❌ Fehler beim Erstellen der Tabelle: {e}")


def image_quality_scores(conn):
    logging.info(f"[image_quality_scores] 📊 Erstelle Tabelle image_quality_scores")
    try:
        conn.execute("""
                     CREATE TABLE IF NOT EXISTS image_quality_scores
                     (
                         image_name
                         TEXT,
                         score_type
                         INTEGER,
                         score
                         INTEGER,
                         PRIMARY
                         KEY
                     (
                         image_name,
                         score_type
                     )
                         )
                     """)
        logging.info(f"[image_quality_scores] ✅ Tabelle erstellt")
    except sqlite3.Error as e:
        logging.error(f"[image_quality_scores] ❌ Fehler beim Erstellen der Tabelle: {e}")


def migrate_score():
    logging.info(f"[migrate_score] 🔄 Starte Migration der Scores")
    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            image_quality_scores(conn)
            conn.execute("DELETE FROM image_quality_scores")
            rows = conn.execute("SELECT image_name, scoreq1, scoreq2 FROM image_quality").fetchall()
            for image_name, scoreq1, scoreq2 in rows:
                conn.execute("""
                    INSERT OR REPLACE INTO image_quality_scores (image_name, score_type, score)
                    VALUES (?, ?, ?)
                """, (image_name, 1, scoreq1))
                conn.execute("""
                    INSERT OR REPLACE INTO image_quality_scores (image_name, score_type, score)
                    VALUES (?, ?, ?)
                """, (image_name, 2, scoreq2))
            conn.commit()
        logging.info(f"[migrate_score] ✅ {len(rows)} Einträge migriert.")

        with sqlite3.connect(Settings.DB_PATH) as conn:
            conn.execute("DROP TABLE IF EXISTS image_quality")
            conn.commit()
        logging.info("[migrate_score] 🗑️ Alte Tabelle image_quality gelöscht.")
    except sqlite3.Error as e:
        logging.error(f"[migrate_score] ❌ Fehler bei der Migration der Scores: {e}")
        raise


def set_status(image_name: str, key: str, checked: int = 1):
    logging.info(f"[set_status] 📝 Setze Status für {image_name}, Checkbox: {key}, Wert: {checked}")
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
        logging.info(f"[set_status] ✅ Status gesetzt für {image_name} ({key}={checked})")
    except sqlite3.Error as e:
        logging.error(f"[set_status] ❌ Fehler beim Setzen des Status für {image_name}: {e}")
        raise


def save_status(image_id: str, data: dict):
    logging.info(f"[save_status] 💾 Speichere Status für ID: {image_id}, Daten: {data}")
    image_name = find_image_name_by_id(image_id)
    logging.info(f"[save_status] Speichern des Status für {image_name}. Eingabedaten: {data}")

    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            for key, value in data.items():
                if key in Settings.CHECKBOX_CATEGORIES:
                    checked = 1 if str(value).lower() in ["1", "true", "on"] else 0
                    conn.execute("""
                        INSERT OR REPLACE INTO checkbox_status (image_name, checkbox, checked)
                        VALUES (?, ?, ?)
                    """, (image_name, key, checked))
                    logging.info(f"[save_status] ✅ Checkbox '{key}' für {image_name} gespeichert. Wert: {checked}")
                else:
                    conn.execute("""
                        INSERT OR REPLACE INTO text_status (image_name, field, value)
                        VALUES (?, ?, ?)
                    """, (image_name, key, value))
                    logging.info(f"[save_status] ✅ Textfeld '{key}' für {image_name} gespeichert. Wert: {value}")
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"[save_status] ❌ Fehler beim Speichern des Status für {image_name}: {e}")
        raise


def load_status(image_name: str):
    logging.info(f"[load_status] 📥 Lade Status für: {image_name}")
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
        logging.info(f"[load_status] ✅ Status geladen: {status}")
    except sqlite3.Error as e:
        logging.error(f"[load_status] ❌ Fehler beim Laden des Status für {image_name}: {e}")
        raise
    return status


def move_file_db(conn: sqlite3.Connection, image_name: str, old_folder_id: str, new_folder_id: str,
                 retries: int = 5) -> bool:
    logging.info(f"[move_file_db] 🔁 move_file_db({image_name}, {old_folder_id} → {new_folder_id})")
    image_name = image_name.lower()
    pair_cache = Settings.CACHE.get("pair_cache")
    pair = pair_cache.get(image_name)
    if not pair:
        logging.warning(f"[move_file_db] ⚠️ Kein Eintrag im pair_cache für: {image_name}")
        return False
    image_id = pair["image_id"]
    file_parents_cache = Settings.CACHE.get("file_parents_cache")

    for attempt in range(retries):
        try:
            conn.execute("""
                         UPDATE image_folder_status
                         SET folder_id = ?
                         WHERE image_id = ?
                           AND folder_id = ?
                         """, (new_folder_id, image_id, old_folder_id))

            if old_folder_id in file_parents_cache:
                try:
                    file_parents_cache[old_folder_id].remove(image_id)
                except ValueError:
                    logging.warning(
                        f"[move_file_db] Datei {image_name} war nicht im Cache von {old_folder_id} vorhanden.")

            if new_folder_id not in file_parents_cache:
                file_parents_cache[new_folder_id] = []

            if image_id not in file_parents_cache[new_folder_id]:
                file_parents_cache[new_folder_id].append(image_id)

            conn.commit()
            logging.info(f"[move_file_db] ✅ Erfolgreich verschoben (nur DB): {image_id}")
            return True

        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                logging.warning(f"[move_file_db] Datenbank gesperrt, Versuch {attempt + 1}/{retries}")
                time.sleep(0.3 * (attempt + 1))
            else:
                logging.error(f"[move_file_db] ❌ Unerwarteter Fehler bei {image_id}: {e}")
                return False
        except Exception as e:
            logging.error(f"[move_file_db] ❌ Fehler beim Verschieben von {image_name}: {e}")
            return False

    logging.error(f"[move_file_db] ❌ Max. Versuche erreicht für {image_id}: Datenbank bleibt gesperrt")
    return False


def delete_checkbox_status(image_name: str):
    with sqlite3.connect(Settings.DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       DELETE
                       FROM checkbox_status
                       WHERE LOWER(image_name) = LOWER(?)
                       """, (image_name,))


def delete_quality_scores(image_name: str):
    with sqlite3.connect(Settings.DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       DELETE
                       FROM image_quality_scores
                       WHERE LOWER(image_name) = LOWER(?)
                       """, (image_name,))


def move_marked_images_by_checkbox(current_folder: str, new_folder: str) -> int:
    logger.info(f"📦 Starte move_marked_images_by_checkbox() von '{current_folder}' nach '{new_folder}'")

    with sqlite3.connect(Settings.DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       SELECT image_name
                       FROM checkbox_status
                       WHERE checked = 1
                         AND checkbox = ?
                       """, (new_folder,))
        rows = cursor.fetchall()

        logger.info(f"🔍 {len(rows)} markierte Bilder gefunden für '{new_folder}'")

        anzahl_verschoben = 0

        for (image_name,) in rows:
            if not image_name:
                logger.warning("⚠️  Leerer image_name – überspringe.")
                continue

            logger.info(f"➡️  Verarbeite Bild: {image_name}")
            success = move_file_db(conn, image_name, current_folder, new_folder)
            if success:
                try:
                    conn.execute("""
                                 DELETE
                                 FROM checkbox_status
                                 WHERE image_name = ?
                                   AND checkbox = ?
                                 """, (image_name, new_folder))

                    src = Path(Settings.IMAGE_FILE_CACHE_DIR) / current_folder / image_name
                    dst = Path(Settings.IMAGE_FILE_CACHE_DIR) / new_folder / image_name
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(src, dst)

                    logger.info(f"✅ Verschoben: {image_name} → {new_folder}")
                    anzahl_verschoben += 1
                except Exception as e:
                    logger.error(f"❌ Fehler beim Verschieben/Löschen von {image_name}: {e}")
            else:
                logger.warning(f"⚠️  move_file_db fehlgeschlagen für {image_name} – kein Verschieben")

        conn.commit()
        logger.info(f"📊 Insgesamt verschoben: {anzahl_verschoben} Dateien")

    return anzahl_verschoben
