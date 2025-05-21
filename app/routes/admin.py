import logging
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path

from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import HTMLResponse

from app.config_new import Settings  # Importiere die Settings-Klasse
from app.database import move_file_db  # Funktion aus database.py importieren
from app.dependencies import require_login

router = APIRouter()
logger = logging.getLogger(__name__)


def run(cmd, **kwargs):
    """Führt einen Befehl in der Shell aus."""
    logging.info("⚙️  %s", " ".join(cmd))
    subprocess.run(cmd, check=True, **kwargs)


def dump_from_container():
    logger.info("📤 Starte dump_from_container()")
    try:
        with open(Settings.DUMP_FILE, "w") as out:
            run(["docker", "exec", Settings.CONTAINER, "sqlite3", Settings.DB_PATH_IN_CONTAINER, ".dump"],
                stdout=out)
        logger.info("✅ Dump erfolgreich erstellt: %s", Settings.DUMP_FILE)
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Fehler bei dump_from_container(): {e}")
        raise


def restore_to_local():
    logger.info("📥 Starte restore_to_local()")
    if os.path.exists(Settings.LOCAL_DB):
        try:
            os.remove(Settings.LOCAL_DB)
            logger.info("🗑️  Alte lokale DB gelöscht: %s", Settings.LOCAL_DB)
        except OSError as e:
            logger.error(f"❌ Fehler beim Löschen der lokalen DB: {e}")
            raise
    try:
        with open(Settings.DUMP_FILE, "rb") as f:
            run(["sqlite3", Settings.LOCAL_DB], stdin=f)
        logger.info("✅ Lokale DB wiederhergestellt: %s", Settings.LOCAL_DB)
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Fehler bei restore_to_local(): {e}")
        raise


def remove_db_in_container():
    logger.info("🧹 Starte remove_db_in_container()")
    try:
        run(["docker", "exec", Settings.CONTAINER, "rm", "-f", Settings.DB_PATH_IN_CONTAINER])
        logger.info("✅ Container-DB entfernt")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Fehler bei remove_db_in_container(): {e}")
        raise


def restore_to_container():
    logger.info("📥 Starte restore_to_container()")
    try:
        with open(Settings.DUMP_FILE, "rb") as f:
            run(["docker", "exec", "-i", Settings.CONTAINER, "sqlite3", Settings.DB_PATH_IN_CONTAINER], stdin=f)
        logger.info("✅ Dump erfolgreich in Container eingespielt")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Fehler bei restore_to_container(): {e}")
        raise


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
