import asyncio

from app.config import Settings
from app.database import clear_folder_status_db
from app.utils.progress import init_progress_state, progress_state, update_progress, stop_progress


async def reloadcache_progress():
    init_progress_state()
    progress_state["running"] = True

    await update_progress(f"clear_folder_status_db ...", 0)
    clear_folder_status_db(Settings.DB_PATH)
    await update_progress(f"clear_folder_status_db fertig", 100)
    await asyncio.sleep(1.0)

    from app.services.cache_management import fillcache_local
    await update_progress(f"fillcache_local ...", 33)
    fillcache_local(Settings.PAIR_CACHE_PATH, Settings.IMAGE_FILE_CACHE_DIR)
    await update_progress(f"fillcache_local fertig", 100)
    await asyncio.sleep(1.0)

    from app.services.cache_management import fill_file_parents_cache_progress
    await fill_file_parents_cache_progress(Settings.DB_PATH)

    await stop_progress()
