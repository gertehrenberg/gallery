import shutil
import sqlite3
from datetime import time
from pathlib import Path

from app.config import Settings
from app.config_gdrive import calculate_md5
from app.routes.dashboard import fill_pair_cache_folder
from app.routes.hashes import update_local_hash
from app.tools import fillcache_local
from app.utils.logger_config import setup_logger

logger = setup_logger(__name__)


async def move_marked_images_by_checkbox(current_folder: str, new_folder: str) -> int:
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
            success = await move_file_db(conn, image_name, current_folder, new_folder)
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


async def move_file_db(conn: sqlite3.Connection, image_name: str, old_folder_id: str, new_folder_id: str,
                       retries: int = 5) -> bool:
    """
    Verschiebt ein Bild in der Datenbank von einem Ordner in einen anderen und aktualisiert die Caches.

    Args:
        conn: Datenbankverbindung
        image_name: Name des Bildes
        old_folder_id: Quell-Ordner ID
        new_folder_id: Ziel-Ordner ID
        retries: Anzahl der Wiederholversuche bei gesperrter DB

    Returns:
        bool: True wenn erfolgreich, False sonst
    """
    logger.info(f"[move_file_db] 🔁 Verschiebe {image_name} von {old_folder_id} → {new_folder_id}")

    # Normalisiere Bildnamen
    image_name = image_name.lower()

    # Hole und validiere Cache-Eintrag
    pair_cache = Settings.CACHE.get("pair_cache", {})
    pair = pair_cache.get(image_name)

    # Wenn kein Cache-Eintrag gefunden wurde, versuche Cache neu zu laden
    if not pair:
        logger.warning(f"[move_file_db] ⚠️ Kein Cache-Eintrag für: {image_name}")
        try:
            logger.info("[move_file_db] 🔄 Lade Cache neu...")
            fillcache_local(
                str(Settings.PAIR_CACHE_PATH),
                Settings.IMAGE_FILE_CACHE_DIR
            )

            # Prüfe erneut nach Cache-Aktualisierung
            pair_cache = Settings.CACHE.get("pair_cache", {})
            pair = pair_cache.get(image_name)

            if not pair:
                logger.error(f"[move_file_db] ❌ Bild nicht gefunden nach Cache-Reload: {image_name}")
                return False

            logger.info(f"[move_file_db] ✅ Bild nach Cache-Reload gefunden: {image_name}")

        except Exception as e:
            logger.error(f"[move_file_db] ❌ Cache-Reload fehlgeschlagen: {str(e)}")
            return False

    # Extrahiere image_id
    image_id = pair["image_id"]

    # Versuche DB-Update mit Wiederholungen
    for attempt in range(retries):
        try:
            # Führe DB-Update durch
            conn.execute("""
                         UPDATE image_folder_status
                         SET folder_id = ?
                         WHERE image_id = ?
                           AND folder_id = ?
                         """, (new_folder_id, image_id, old_folder_id))

            conn.commit()
            logger.info(f"[move_file_db] ✅ DB-Update erfolgreich für: {image_name}")

            try:
                # 1. Aktualisiere pair_cache für beide Ordner
                await fill_pair_cache_folder(old_folder_id, Settings.IMAGE_FILE_CACHE_DIR,
                                             Settings.CACHE["pair_cache"], Settings.PAIR_CACHE_PATH)
                await fill_pair_cache_folder(new_folder_id, Settings.IMAGE_FILE_CACHE_DIR,
                                             Settings.CACHE["pair_cache"], Settings.PAIR_CACHE_PATH)

                # 2. Aktualisiere Hash-Dateien
                old_dir = Path(Settings.IMAGE_FILE_CACHE_DIR) / old_folder_id
                new_dir = Path(Settings.IMAGE_FILE_CACHE_DIR) / new_folder_id

                # Berechne MD5 der Datei vom alten Pfad
                old_file_path = old_dir / image_name
                if old_file_path.exists():
                    file_md5 = calculate_md5(old_file_path)

                    # Zuerst Hash aus altem Verzeichnis entfernen
                    await update_local_hash(old_dir, image_name, file_md5, False)

                    # Dann physische Datei verschieben
                    new_dir.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(old_file_path), str(new_dir / image_name))

                    # Zuletzt Hash im neuen Verzeichnis hinzufügen
                    await update_local_hash(new_dir, image_name, file_md5, True)

                    logger.info(f"[move_file_db] ✅ Alle Caches und Hashes aktualisiert für: {image_name}")
                else:
                    logger.error(f"[move_file_db] ⚠️ Quelldatei nicht gefunden: {old_file_path}")

            except Exception as cache_error:
                logger.error(f"[move_file_db] ⚠️ Cache/Hash-Aktualisierung fehlgeschlagen: {str(cache_error)}")

            return True

        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                wait_time = 0.3 * (attempt + 1)
                logger.warning(f"[move_file_db] 🔒 DB gesperrt, Versuch {attempt + 1}/{retries}, "
                               f"warte {wait_time:.1f}s")
                time.sleep(wait_time)
            else:
                logger.error(f"[move_file_db] ❌ DB-Fehler für {image_name}: {str(e)}")
                return False
        except Exception as e:
            logger.error(f"[move_file_db] ❌ Unerwarteter Fehler für {image_name}: {str(e)}")
            return False

    logger.error(f"[move_file_db] ❌ Maximum von {retries} Versuchen erreicht für: {image_name}")
    return False
