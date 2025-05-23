import logging
import os

from fastapi import APIRouter
from fastapi.templating import Jinja2Templates

from app.config import Settings

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))
logger = logging.getLogger(__name__)

from fastapi import Request
from fastapi.responses import HTMLResponse


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    gdrive_stats = compare_hashfile_counts_dash(Settings.IMAGE_FILE_CACHE_DIR, subfolders=True)
    return templates.TemplateResponse("dashboard.j2", {
        "request": request,
        "gdrive_stats": gdrive_stats
    })


import json
from pathlib import Path


def compare_hashfile_counts_dash(file_folder_dir, subfolders: bool = True):
    root = Path(file_folder_dir)
    all_dirs = [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]
    result = []

    for subdir in sorted(all_dirs):
        gdrive_path = subdir / "hashes.json"
        local_path = subdir / "gallery202505_hashes.json"

        try:
            with gdrive_path.open("r", encoding="utf-8") as f:
                gdrive_data = json.load(f)
                gdrive_data = gdrive_data if isinstance(gdrive_data, dict) else {}
        except:
            gdrive_data = {}

        try:
            with local_path.open("r", encoding="utf-8") as f:
                local_data = json.load(f)
                local_data = local_data if isinstance(local_data, dict) else {}
        except:
            local_data = {}

        result.append({
            "ordner": subdir.name,
            "gdrive_count": len(gdrive_data),
            "local_count": len(local_data)
        })
    return result


def compare_hashfile_counts(file_folder_dir, subfolders: bool = True):
    results = compare_hashfile_counts_dash(file_folder_dir, subfolders=subfolders)

    header = f"{'Ordner':<15}{'GDrive-Hashes':>15}{'Lokal-Hashes':>15}"
    print(header)
    print("-" * len(header))

    for entry in results:
        print(f"{entry['ordner']:<15}{entry['gdrive_count']:>15}{entry['local_count']:>15}")
