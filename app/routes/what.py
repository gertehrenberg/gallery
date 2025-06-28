import logging
import os
import shutil
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import JSONResponse

from app.config import Settings
from app.utils.score_utils import delete_scores_by_type

router = APIRouter()

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


@router.get("/dashboard/what", response_class=HTMLResponse)
async def what(request: Request):
    checkboxes = [
        ("thumbnail", "Thumbnail"),
        ("rendered", "Rendered"),
        ("faces", "Gesichter")
    ]
    return templates.TemplateResponse("what.j2", {"request": request, "checkboxes": checkboxes})


@router.post("/dashboard/what/confirm", response_class=HTMLResponse)
async def confirm_what(request: Request):
    form = await request.form()
    selected = form.getlist("option")

    logger.info(f"✅ Ausgewählt für Reload: {selected}")
    messages = []

    if "thumbnail" in selected:
        await remove_items(Path(Settings.THUMBNAIL_CACHE_DIR_300), "thumbnail")
        if "rendered" not in selected:
            selected.append("rendered")
        messages.append("Thumbnails gelöscht")

    if "faces" in selected:
        logger.info("➡️  Gesichter werden gelöscht...")
        await remove_items(Path(Settings.GESICHTER_FILE_CACHE_DIR), "faces")
        if "rendered" not in selected:
            selected.append("rendered")
        messages.append("Gesichter gelöscht")

    if "rendered" in selected:
        logger.info("➡️  Gerenderte HTML-Seiten werden gelöscht...")
        await remove_items(Path(Settings.RENDERED_HTML_DIR), "rendered")
        messages.append("Gerenderte HTML-Seiten gelöscht")

    return JSONResponse({
        "status": "ok",
        "message": " | ".join(messages),
        "selected": selected
    })


async def remove_items(dir, name):
    if name == "faces":
        delete_scores_by_type(3)

    if dir.exists() and dir.is_dir():
        for item in dir.iterdir():
            try:
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
            except Exception as e:
                logger.warning(f"❌ [{name}] Fehler beim Löschen von {item}: {e}")
