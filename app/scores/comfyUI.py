import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional
from PIL import Image

from app.config import Settings
from app.tools import readimages
from app.utils.progress import init_progress_state, progress_state, update_progress, stop_progress

# Constants
COMFYUI_CATEGORY = "ki"
WORKFLOW_KEY = 'workflow'
PARAMETERS_KEY = 'parameters'

logger = logging.getLogger(__name__)


class ComfyUIScanner:
    def __init__(self, image_dir: str):
        self.image_dir = image_dir
        self.local_files: Dict[str, dict] = {}
        self.workflow_dir = Path(image_dir) / Settings.WORKFLOW_DIR
        self._ensure_workflow_dir()

    def _ensure_workflow_dir(self) -> None:
        """Create workflow directory if it doesn't exist."""
        self.workflow_dir.mkdir(parents=True, exist_ok=True)

    def _save_workflow(self, image_name: str, workflow_data: dict) -> Path:
        """
        Save workflow data to a JSON file.

        Args:
            image_name: Name of the original image
            workflow_data: Workflow data to save

        Returns:
            Path: Path to the saved workflow file
        """
        json_name = Path(image_name).stem + '.json'
        workflow_path = self.workflow_dir / json_name

        try:
            with workflow_path.open('w', encoding='utf-8') as f:
                json.dump(workflow_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved workflow to {workflow_path}")
            return workflow_path
        except Exception as e:
            logger.error(f"Error saving workflow for {image_name}: {e}")
            return None

    async def process_image(self, file_info: dict, label: str, total_files: int, current_index: int) -> bool:
        """
        Process a single image and check for ComfyUI metadata.

        Args:
            file_info: Dictionary containing image information
            label: Category label for progress display
            total_files: Total number of files to process
            current_index: Current file index

        Returns:
            bool: True if workflow metadata was found
        """
        image_path = Path(self.image_dir) / COMFYUI_CATEGORY / file_info["image_name"]
        percent = int(current_index / total_files * 100)

        try:
            with Image.open(image_path) as im:
                meta = im.info
                has_workflow = False

                if WORKFLOW_KEY in meta:
                    workflow_data = meta[WORKFLOW_KEY]
                    logger.info(f"ComfyUI Workflow found in {file_info['image_name']}")
                    workflow_path = self._save_workflow(file_info["image_name"], workflow_data)
                    has_workflow = True
                elif PARAMETERS_KEY in meta:
                    workflow_data = meta[PARAMETERS_KEY]
                    logger.info(f"Possible workflow found in {file_info['image_name']}")
                    workflow_path = self._save_workflow(file_info["image_name"], workflow_data)
                    has_workflow = True

                await update_progress(
                    f'Images in "{label}": {current_index}/{total_files} (Workflows: {current_index})',
                    percent
                )
                return has_workflow

        except Exception as e:
            logger.error(f"Error processing image {image_path}: {e}")
            return False

    def get_category_info(self) -> Optional[dict]:
        """Get ComfyUI category information from settings."""
        return next(
            (cat for cat in Settings.kategorien if cat["key"] == COMFYUI_CATEGORY),
            None
        )

    async def scan_images(self) -> int:
        """
        Scan images for ComfyUI workflow metadata.

        Returns:
            int: Number of images with workflow metadata
        """
        await init_progress_state()
        progress_state["running"] = True

        category = self.get_category_info()
        if not category:
            logger.warning(f"Category {COMFYUI_CATEGORY} not found in settings")
            await stop_progress()
            return 0

        readimages(os.path.join(self.image_dir, COMFYUI_CATEGORY), self.local_files)

        all_files = [
            {**entry, "image_name": image_name}
            for image_name, entry in self.local_files.items()
        ]

        workflow_count = 0
        label = category.get("label", COMFYUI_CATEGORY)

        await update_progress(f'Images in "{label}"', 0)

        for i, file_info in enumerate(all_files, 1):
            if await self.process_image(file_info, label, len(all_files), i):
                workflow_count += 1

        logger.info(f"Found and saved {workflow_count} workflows")
        await stop_progress()
        return workflow_count


async def reload_comfyui() -> int:
    """
    Main function to reload and scan ComfyUI images.

    Returns:
        int: Number of images with workflow metadata
    """
    scanner = ComfyUIScanner(Settings.IMAGE_FILE_CACHE_DIR)
    return await scanner.scan_images()


def configure_local_paths():
    """Configure paths for local development."""
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.RENDERED_HTML_DIR = "../../cache/rendered_html"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.WORKFLOW_DIR = "../../cache/workflows"


def main():
    """Main entry point of the script."""
    configure_local_paths()
    asyncio.run(reload_comfyui())


if __name__ == "__main__":
    main()
