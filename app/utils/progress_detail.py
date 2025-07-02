import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Zustand für Detail-Fortschritt
detail_state = {
    "status": "",
    "progress": 0,
    "range": {
        "min": 0,
        "max": 1000
    }
}

def calc_detail_progress(current_progress: int, total_progress: int) -> Optional[int]:
    if current_progress == 0 or total_progress <= 1:
        return 0
    return int((current_progress * 100) / total_progress)

async def start_detail_progress(detail_status: str):
    await update_detail_progress(detail_status, 0)

async def stop_detail_progress(detail_status: str):
    await update_detail_progress(detail_status, 1000)

async def update_detail_progress(
        detail_status: Optional[str] = None,
        detail_progress: Optional[int] = None,
        ctime: float = 0.01):
    """
    Aktualisiert den Detail-Fortschritt.

    Args:
        detail_status: Detail-Status-Text
        detail_progress: Detail-Fortschritt (0-1000)
        ctime: Wartezeit zwischen Updates
    """
    if detail_status is not None:
        detail_state["status"] = detail_status
    if detail_progress is not None:
        detail_state["progress"] = detail_progress

    logger.info(f"Detail: {detail_status} ({detail_progress})")
    await asyncio.sleep(ctime)


async def update_detail_status(detail_status: str, ctime: float = 0.01):
    """
    Aktualisiert nur den Detail-Status-Text.

    Args:
        detail_status: Detail-Status-Text
        ctime: Wartezeit zwischen Updates
    """
    detail_state["status"] = detail_status
    logger.info(f"Detail: {detail_status}")
    await asyncio.sleep(ctime)
