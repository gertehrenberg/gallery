import logging
import os
import subprocess

from fastapi import APIRouter

from app.config_new import Settings  # Importiere die Settings-Klasse

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
