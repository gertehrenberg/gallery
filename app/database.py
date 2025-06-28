import shutil
import sqlite3
import time
from pathlib import Path

from app.config import Settings, reverse_score_type_map, score_type_map  # Importiere die Settings-Klasse
from app.tools import find_image_name_by_id
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


def init_db(db_path):
    logger.info(f"[init_db] 🛠️ Initialisiere Datenbank: {db_path}")
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
        logger.info(f"[init_db] ✅ Datenbank initialisiert")
    except sqlite3.Error as e:
        logger.error(f"[init_db] ❌ Fehler beim Initialisieren: {e}")


def image_quality(conn):
    logger.info(f"[image_quality] 🧮 Erstelle Tabelle image_quality")
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
        logger.info(f"[image_quality] ✅ Tabelle erstellt")
    except sqlite3.Error as e:
        logger.error(f"[image_quality] ❌ Fehler beim Erstellen der Tabelle: {e}")


def image_quality_scores(conn):
    logger.info(f"[image_quality_scores] 📊 Erstelle Tabelle image_quality_scores")
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
        logger.info(f"[image_quality_scores] ✅ Tabelle erstellt")
    except sqlite3.Error as e:
        logger.error(f"[image_quality_scores] ❌ Fehler beim Erstellen der Tabelle: {e}")


def migrate_score():
    logger.info(f"[migrate_score] 🔄 Starte Migration der Scores")
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
        logger.info(f"[migrate_score] ✅ {len(rows)} Einträge migriert.")

        with sqlite3.connect(Settings.DB_PATH) as conn:
            conn.execute("DROP TABLE IF EXISTS image_quality")
            conn.commit()
        logger.info("[migrate_score] 🗑️ Alte Tabelle image_quality gelöscht.")
    except sqlite3.Error as e:
        logger.error(f"[migrate_score] ❌ Fehler bei der Migration der Scores: {e}")
        raise


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


def save_status(image_id: str, data: dict):
    logger.info(f"[save_status] 💾 Speichere Status für ID: {image_id}, Daten: {data}")
    image_name = find_image_name_by_id(image_id)
    logger.info(f"[save_status] Speichern des Status für {image_name}. Eingabedaten: {data}")

    try:
        with sqlite3.connect(Settings.DB_PATH) as conn:
            for key, value in data.items():
                if key in Settings.CHECKBOX_CATEGORIES:
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


def move_file_db(conn: sqlite3.Connection, image_name: str, old_folder_id: str, new_folder_id: str,
                 retries: int = 5) -> bool:
    logger.info(f"[move_file_db] 🔁 move_file_db({image_name}, {old_folder_id} → {new_folder_id})")
    image_name = image_name.lower()
    pair_cache = Settings.CACHE.get("pair_cache")
    pair = pair_cache.get(image_name)
    if not pair:
        logger.warning(f"[move_file_db] ⚠️ Kein Eintrag im pair_cache für: {image_name}")
        return False
    image_id = pair["image_id"]

    for attempt in range(retries):
        try:
            conn.execute("""
                         UPDATE image_folder_status
                         SET folder_id = ?
                         WHERE image_id = ?
                           AND folder_id = ?
                         """, (new_folder_id, image_id, old_folder_id))

            conn.commit()
            logger.info(f"[move_file_db] ✅ Erfolgreich verschoben (nur DB): {image_id}")
            return True

        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                logger.warning(f"[move_file_db] Datenbank gesperrt, Versuch {attempt + 1}/{retries}")
                time.sleep(0.3 * (attempt + 1))
            else:
                logger.error(f"[move_file_db] ❌ Unerwarteter Fehler bei {image_id}: {e}")
                return False
        except Exception as e:
            logger.error(f"[move_file_db] ❌ Fehler beim Verschieben von {image_name}: {e}")
            return False

    logger.error(f"[move_file_db] ❌ Max. Versuche erreicht für {image_id}: Datenbank bleibt gesperrt")
    return False


def delete_checkbox_status(image_name: str):
    logger.info(f"[delete_checkbox_status] Start – image_name={image_name}")
    with sqlite3.connect(Settings.DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       DELETE
                       FROM checkbox_status
                       WHERE LOWER(image_name) = LOWER(?)
                       """, (image_name,))


def delete_scores(image_name: str):
    logger.info(f"[delete_scores] Start – image_name={image_name}")
    with sqlite3.connect(Settings.DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       DELETE
                       FROM image_quality_scores
                       WHERE LOWER(image_name) = LOWER(?)
                       """, (image_name,))


def delete_scores_by_type(score_type: int):
    logger.info(f"[delete_scores_by_type] Start – score_type={score_type}")
    with sqlite3.connect(Settings.DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       DELETE
                       FROM image_quality_scores
                       WHERE score_type = ?
                       """, (score_type,))


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


def get_checkbox_count(checkbox: str):
    logger.info(f"[get_checkbox_count] Start – checkbox={checkbox}")
    if checkbox not in Settings.CHECKBOX_CATEGORIES:
        logger.warning("⚠️ Ungültige Checkbox-Kategorie")
        return {"count": 0}
    with sqlite3.connect(Settings.DB_PATH) as conn:
        count = conn.execute("""
                             SELECT COUNT(*)
                             FROM checkbox_status
                             WHERE checked = 1
                               AND checkbox = ?
                             """, (checkbox,)).fetchone()[0]
    logger.info(f"🔢 Anzahl markierter Bilder in '{checkbox}': {count}")
    return {"count": count}


def load_face_from_db(db_path: str, image_name: str):
    logger.info(f"[load_face_from_db] Start – db_path={db_path}, image_name={image_name}")
    with sqlite3.connect(db_path) as conn:
        return conn.execute("""
                            SELECT score_type, score
                            FROM image_quality_scores
                            WHERE LOWER(image_name) = LOWER(?)
                              AND score_type BETWEEN 5 AND 5
                            """, (image_name,)).fetchall()


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


def load_folder_status_from_db(db_path: str):
    logger.info(f"[load_folder_status_from_db] Start – db_path={db_path}")
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM image_folder_status").fetchone()
        if row and row[0] > 0:
            return conn.execute("SELECT image_id, folder_id FROM image_folder_status").fetchall()
        return []


def load_folder_status_from_db_by_name(db_path: str, folder_key: str):
    logger.info(f"[load_folder_status_from_db_by_name] Start – db_path={db_path}, folder_key={folder_key}")
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM image_folder_status WHERE folder_id = ?",
            (folder_key,)
        ).fetchone()

        if row and row[0] > 0:
            return conn.execute(
                "SELECT image_id, folder_id FROM image_folder_status WHERE folder_id = ?",
                (folder_key,)
            ).fetchall()

        return []


def count_folder_entries(db_path: str, folder_key: str) -> int:
    logger.info(f"[count_folder_entries] Start – db_path={db_path}, folder_key={folder_key}")
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM image_folder_status WHERE folder_id = ?",
            (folder_key,)
        ).fetchone()
        return row[0] if row else 0


def clear_folder_status_db(db_path: str):
    logger.info(f"[clear_folder_status_db] Start – db_path={db_path}")
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM image_folder_status")
        conn.commit()


def clear_folder_status_db_by_name(db_path: str, folder_key: str) -> None:
    logger.info(f"[clear_folder_status_db_by_name] Start – db_path={db_path}, folder_key={folder_key}")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM image_folder_status WHERE folder_id = ?",
            (folder_key,))
        conn.commit()


def check_folder_status_in_db(db_path, image_id, folder_key):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute("""
                       SELECT 1
                       FROM image_folder_status
                       WHERE image_id = ?
                         AND folder_id = ? LIMIT 1
                       """, (image_id, folder_key))
        exists = cursor.fetchone() is not None
        return exists
    finally:
        conn.close()


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


def get_scores_filtered_by_expr(db_path, expr):
    import sqlite3

    def extract_used_score_types(expr):
        types = set()
        # Klartextnamen wie 'porn' oder 'nsfw_score'
        for key in score_type_map:
            if key in expr:
                types.add(score_type_map[key])
        return sorted(types)

    used_score_types = extract_used_score_types(expr)
    if not used_score_types:
        return {}

    placeholders = ",".join("?" for _ in used_score_types)
    query = f"""
        SELECT LOWER(image_name), score_type, score
        FROM image_quality_scores
        WHERE score_type IN ({placeholders})
    """

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query, used_score_types).fetchall()

    result = {}
    for image_name, score_type, score in rows:
        score_key = reverse_score_type_map.get(score_type)
        if score_key:
            result.setdefault(image_name, {})[score_key] = score

    return result


def load_all_nsfw_images(db_path: str, score_type: int, score: int) -> set[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("""
                            SELECT DISTINCT LOWER(image_name)
                            FROM image_quality_scores
                            WHERE score_type = ?
                              AND score > ?
                            """, (score_type, score)).fetchall()
        return {row[0] for row in rows}
