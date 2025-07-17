from pathlib import Path

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import FileResponse

from ..config import Settings  # Importiere die Settings-Klasse
from ..dependencies import require_login

router = APIRouter()


@router.get("/thumbnails/{file_path:path}")
@router.get("/static/thumbnails/{file_path:path}")
async def _thumbnails(file_path: str, request: Request):
    file = Path("/app/thumbnails") / file_path
    print("🔍 Angeforderte Datei:", file)
    print("📁 Existiert:", file.exists())
    if file.exists() and file.is_file():
        return FileResponse(file)
    raise HTTPException(status_code=404)


@router.get("/imagefiles/{file_path:path}")
@router.get("/static/imagefiles/{file_path:path}")
async def _imagefiles(file_path: str, request: Request, user: str = Depends(require_login)):
    """Liefert eine Bilddatei. Wenn nicht vorhanden, wird anhand der Kategorien gesucht."""
    base_path = Path("/app/imagefiles")
    file = base_path / file_path

    if file.exists() and file.is_file():
        return FileResponse(file)

    # Versuche alternative Pfade durch Ersetzen des Präfix
    for eintrag in Settings.kategorien():
        alt_key = eintrag["key"]
        if file_path.startswith(alt_key + "/"):
            rest = file_path[len(alt_key) + 1:]  # Restlicher Pfad ohne Präfix
            for ersatz in [k["key"] for k in Settings.kategorien() if k["key"] != alt_key]:
                alt_file = base_path / ersatz / rest
                if alt_file.exists() and alt_file.is_file():
                    return FileResponse(alt_file)

    raise HTTPException(status_code=404)


@router.get("/facefiles/{file_path:path}")
@router.get("/static/facefiles/{file_path:path}")
async def _facefiles(file_path: str, request: Request, user: str = Depends(require_login)):
    """Liefert eine Datei mit Gesichtsausschnitten."""
    # decoded_file_path = unquote(file_path) # Entfernt
    file = Path("/app/facefiles") / file_path  # Zurück zum Originalpfad
    if file.exists() and file.is_file():
        return FileResponse(file)
    raise HTTPException(status_code=404)


@router.get("/comfyui_gif/{file_path:path}")
@router.get("/static/comfyui_gif/{file_path:path}")
async def _comfyui_gif(file_path: str, request: Request, user: str = Depends(require_login)):
    """Liefert eine Datei mit Gesichtsausschnitten."""
    # decoded_file_path = unquote(file_path) # Entfernt
    file = Path("/app/comfyui_gif") / file_path  # Zurück zum Originalpfad
    if file.exists() and file.is_file():
        return FileResponse(file)
    raise HTTPException(status_code=404)
