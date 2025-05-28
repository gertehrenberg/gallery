import asyncio

from app.config import Settings
from app.database import clear_folder_status_db
from app.tools import readimages, save_pair_cache
from app.utils.progress import init_progress_state, progress_state, update_progress, stop_progress


async def reloadcache_progress(folder_key):
    await init_progress_state()
    progress_state["running"] = True

    Settings.folders_loaded = 0

    if folder_key in Settings.CHECKBOX_CATEGORIES:
        pair_cache = Settings.CACHE.get("pair_cache")
        pair_cache_path_local = Settings.PAIR_CACHE_PATH
        folder_name = next((k["label"] for k in Settings.kategorien if k["key"] == folder_key), None)

        from app.services.cache_management import fillcache_local
        await update_progress(f"{folder_name}: fillcache_local ...", 33)
        to_delete = [key for key, value in pair_cache.items()
                     if value.get("folder", "") == folder_key]
        for key in to_delete:
            del pair_cache[key]
        readimages(Settings.IMAGE_FILE_CACHE_DIR + "/" + folder_key, pair_cache)
        save_pair_cache(pair_cache, pair_cache_path_local)
        await update_progress(f"{folder_name}: fillcache_local fertig", 100)
        await asyncio.sleep(1.0)

        from app.services.cache_management import fill_file_parents_cache_progress
        await fill_file_parents_cache_progress(Settings.DB_PATH, folder_key)
    else:
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
        await fill_file_parents_cache_progress(Settings.DB_PATH, None)

    await stop_progress()
