import asyncio
import logging
import os

from PIL import Image

from app.config import Settings
from app.tools import readimages
from app.utils.progress import init_progress_state, progress_state, update_progress, stop_progress

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


async def reload_comfyui():
    await init_progress_state()
    progress_state["running"] = True

    for eintrag in Settings.kategorien:
        if eintrag["key"] != "ki":
            continue
        folder_key = eintrag["key"]

        local_files = {}

        readimages(Settings.IMAGE_FILE_CACHE_DIR + "/" + folder_key, local_files)

        all_files = []

        for image_name, entry in local_files.items():
            entry["image_name"] = image_name
            all_files.append(entry)

        count = 0
        label = next((k["label"] for k in Settings.kategorien if k["key"] == folder_key), folder_key)
        await update_progress(f"Bilder in \"{label}\"", 0)
        for i, file_info in enumerate(all_files, 1):
            image = os.path.join(Settings.IMAGE_FILE_CACHE_DIR, folder_key, file_info["image_name"])
            percent = int(i / len(all_files) * 100)
            await update_progress(f"Bilder in \"{label}\": {i}/{len(all_files)} (erzeugt: {count})", percent)
            im = Image.open(image)
            meta = im.info

            if 'workflow' in meta:
                count += 1
                # print("ComfyUI Workflow gefunden:", meta['workflow'])
            elif 'parameters' in meta:
                count += 1
                # print("Möglicher Workflow:", meta['parameters'])

    await stop_progress()


def localp1():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.RENDERED_HTML_DIR = "../../cache/rendered_html"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"


def p1():
    localp1()

    asyncio.run(reload_comfyui())

if __name__ == "__main__":
    p1()

