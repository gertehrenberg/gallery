import sqlite3

from app.config import Settings, reverse_score_type_map, score_type_map
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
