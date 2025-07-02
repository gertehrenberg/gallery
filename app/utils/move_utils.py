import shutil
import sqlite3
import time
from pathlib import Path
from typing import Optional

from app.config import Settings
from app.config_gdrive import calculate_md5
from app.routes.dashboard import fill_pair_cache_folder
from app.routes.hashes import update_local_hash
from app.tools import newpaircache
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


async def move_single_image(image_name: str, current_folder: str, new_folder: str) -> bool:
    """
    Verschiebt ein einzelnes Bild von current_folder nach new_folder und löscht dessen checkbox_status.

    Args:
        image_name: Name der Bilddatei
        current_folder: Aktueller Ordner
        new_folder: Zielordner

    Returns:
        bool: True wenn erfolgreich verschoben, False sonst
    """
    if not image_name:
        logger.warning("⚠️ Leerer image_name – überspringe.")
        return False

    logger.info(f"📦 Verschiebe Bild '{image_name}' von '{current_folder}' nach '{new_folder}'")

    with sqlite3.connect(Settings.DB_PATH) as conn:
        try:
            # Versuche die Datei in der DB zu verschieben
            success = await move_file_db(conn, image_name, current_folder, new_folder)
            if success:
                # Lösche checkbox_status für dieses Bild
                conn.execute(
                    "DELETE FROM checkbox_status WHERE image_name = ? AND checkbox = ?",
                    (image_name, new_folder)
                )
                conn.commit()
                return True
            else:
                logger.warning(f"⚠️ move_file_db fehlgeschlagen für {image_name}")
                return False

        except Exception as e:
            logger.error(f"❌ Fehler beim Verschieben von {image_name}: {e}")
            return False


async def move_marked_images_by_checkbox(current_folder: str, new_folder: str) -> int:
    """Verschiebt alle markierten Bilder zwischen Ordnern."""
    logger.info(f"📦 Starte Massenverschiebung von '{current_folder}' nach '{new_folder}'")

    with sqlite3.connect(Settings.DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT image_name FROM checkbox_status WHERE checked = 1 AND checkbox = ?",
            (new_folder,)
        )
        rows = cursor.fetchall()
        logger.info(f"🔍 {len(rows)} markierte Bilder gefunden für '{new_folder}'")

        anzahl_verschoben = 0
        for (image_name,) in rows:
            if await move_single_image(image_name, current_folder, new_folder):
                anzahl_verschoben += 1

        logger.info(f"📊 Insgesamt verschoben: {anzahl_verschoben} Dateien")
        return anzahl_verschoben

def get_checkbox_count(checkbox: str):
    logger.info(f"[get_checkbox_count] Start – checkbox={checkbox}")
    if checkbox not in Settings.checkbox_categories():
        logger.warning("⚠️ Ungültige Checkbox-Kategorie")
        return {"count": 0}
    with sqlite3.connect(Settings.DB_PATH) as conn:
        count = conn.execute(
            """
            SELECT COUNT(*)
            FROM checkbox_status
            WHERE checked = 1
              AND checkbox = ?
            """,
            (checkbox,),
        ).fetchone()[0]
    logger.info(f"🔢 Anzahl markierter Bilder in '{checkbox}': {count}")
    return {"count": count}


async def move_file_db(
        conn: sqlite3.Connection,
        image_name: str,
        old_folder_id: str,
        new_folder_id: str,
        retries: int = 5
) -> bool:
    """
    Verschiebt eine Bilddatei inklusive Datenbankeintrag, Cache- und Hash-Aktualisierung.
    """
    logger.info(f"[move_file_db] 🔁 Verschiebe {image_name} von {old_folder_id} → {new_folder_id}")

    image_name = image_name.lower()
    image_id = _get_image_id(image_name)
    if not image_id:
        logger.error(f"⚠️ Kein Cache-Eintrag für: {image_name}")
        return False

    if not _update_db_with_retries(conn, image_id, old_folder_id, new_folder_id, retries):
        return False

    await _refresh_pair_caches(old_folder_id, new_folder_id)
    return await _move_file_and_update_hash(old_folder_id, new_folder_id, image_name)


def _get_image_id(image_name: str) -> Optional[int]:
    """Liest den Image-ID-Wert aus den konfigurierten Kategorien aus dem Cache."""
    try:
        for kategorie in Settings.kategorien():
            key = kategorie["key"]
            logger.info(f"✅ Cache-Aktualisierung: {key}")
            pair_cache = newpaircache(key)
            Settings.CACHE["pair_cache"].update(pair_cache)

            pair = pair_cache.get(image_name)
            if isinstance(pair, dict) and pair.get("image_id"):
                return pair["image_id"]
    except Exception as e:
        logger.error(f"❌ Fehler beim Lesen des Hash: {e}")
    return None


def _update_db_with_retries(
        conn: sqlite3.Connection,
        image_id: int,
        old_folder_id: str,
        new_folder_id: str,
        retries: int
) -> bool:
    """Versucht das DB-Update mit Wiederholungen bei Locked-Errors."""
    sql = (
        "UPDATE image_folder_status "
        "SET folder_id = ? "
        "WHERE image_id = ? AND folder_id = ?"
    )
    for attempt in range(1, retries + 1):
        try:
            conn.execute(sql, (new_folder_id, image_id, old_folder_id))
            conn.commit()
            logger.info(f"[move_file_db] ✅ DB-Update erfolgreich für image_id={image_id}")
            return True
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                wait = 0.3 * attempt
                logger.warning(f"🔒 DB gesperrt, Versuch {attempt}/{retries}, warte {wait:.1f}s")
                time.sleep(wait)
                continue
            logger.error(f"❌ DB-Fehler: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unerwarteter Fehler: {e}")
            return False
    logger.error(f"❌ Maximum von {retries} DB-Versuchen erreicht")
    return False


async def _refresh_pair_caches(old_folder_id: str, new_folder_id: str) -> None:
    """Aktualisiert den Pair-Cache für die betroffenen Ordner."""
    await fill_pair_cache_folder(
        old_folder_id, Settings.IMAGE_FILE_CACHE_DIR,
        Settings.CACHE["pair_cache"], Settings.PAIR_CACHE_PATH
    )
    await fill_pair_cache_folder(
        new_folder_id, Settings.IMAGE_FILE_CACHE_DIR,
        Settings.CACHE["pair_cache"], Settings.PAIR_CACHE_PATH
    )


async def _move_file_and_update_hash(
        old_folder_id: str,
        new_folder_id: str,
        image_name: str
) -> bool:
    """Verschiebt die Datei physisch und aktualisiert die lokalen Hash-Dateien."""
    old_dir = Path(Settings.IMAGE_FILE_CACHE_DIR) / old_folder_id
    new_dir = Path(Settings.IMAGE_FILE_CACHE_DIR) / new_folder_id
    old_file = old_dir / image_name

    if not old_file.exists():
        logger.error(f"⚠️ Quelldatei nicht gefunden: {old_file}")
        return False

    try:
        file_md5 = calculate_md5(old_file)
        await update_local_hash(old_dir, image_name, file_md5, False)
        new_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(old_file), str(new_dir / image_name))
        await update_local_hash(new_dir, image_name, file_md5, True)
        logger.info(f"[move_file_db] ✅ Datei und Hashes aktualisiert für: {image_name}")
        return True
    except Exception as e:
        logger.error(f"⚠️ Fehler beim Verschieben/Aktualisieren: {e}")
        return False
