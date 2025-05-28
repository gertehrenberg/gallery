import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

progress_state = {
    "progress": 0,
    "status": "Warte auf Start...",
    "running": False
}


async def update_progress(status: str, progress: int, ctime=0.1):
    if isinstance(status, str) and len(status) > 0:
        progress_state["status"] = status
    progress_state["progress"] = progress
    logging.info(f"{status} : {progress}")
    await asyncio.sleep(ctime)  # <<< Damit der Balken Zeit zur Anzeige bekommt


def init_progress_state():
    progress_state["running"] = False
    update_progress("Warte auf Start...", 0)


init_progress_state()


async def stop_progress():
    progress_state["running"] = False
    await update_progress("Abgeschlossen.", 100)
