import logging
import sqlite3
import time

from app.config_new import Settings  # Importiere die Settings-Klasse


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
    global pair_cache
    pair = pair_cache.get(image_name)
    if not pair:
        logging.warning(f"[move_file_db] ⚠️ Kein Eintrag im pair_cache für: {image_name}")
        return False
    image_id = pair["image_id"]
    global file_parents_cache

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


def find_image_id_by_name(image_name: str):
    logging.info(f"[find_image_id_by_name] 🔎 Suche ID für Bild: {image_name}")
    global pair_cache
    pair = pair_cache.get(image_name)
    if pair:
        logging.info(f"[find_image_id_by_name] ✅ Gefunden: {pair.get('image_id')}")
        return pair.get("image_id")
    logging.warning(f"[find_image_id_by_name] ❌ Kein Eintrag gefunden für: {image_name}")
    return None


def find_image_name_by_id(image_id: str):
    logging.info(f"[find_image_name_by_id] 🔍 Suche Bildname für ID: {image_id}")
    pair_cache = Settings.CACHE.get("pair_cache")
    for image_name, pair in pair_cache.items():
        if pair.get("image_id") == image_id:
            logging.info(f"[find_image_name_by_id] ✅ Gefunden: {image_name}")
            return image_name
    logging.warning(f"[find_image_name_by_id] ❌ Kein Bildname gefunden für ID: {image_id}")
    return None
