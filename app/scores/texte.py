from app.utils.logger_config import setup_logger
from app.utils.progress import init_progress_state, progress_state, stop_progress

logger = setup_logger(__name__)

async def reload_texte():
    await init_progress_state()
    progress_state["running"] = True

    logger.info("➡️  Texte-Score wird gelöscht...")

    await stop_progress()
