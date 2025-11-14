import json
import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

from fastapi import APIRouter
from fastapi import FastAPI
from fastapi import Query
from fastapi import Request
from pydantic import BaseModel

from .auth import load_drive_service
from ..config import Settings
from ..config_gdrive import folder_id_by_name
from ..config_gdrive import sanitize_filename
from ..utils.logger_config import setup_logger

logger = setup_logger(__name__)

router = APIRouter()

# Konfigurierbare Parameter über ENV
LOCK_FILE = Path(os.getenv("LOCK_FILE", "/data/workflow.lock"))
LOCK_TTL = int(os.getenv("LOCK_TTL", "3600"))  # in Sekunden

app = FastAPI(
    title="Lock-Management Service",
    version="1.0",
    description="Service zum Setzen, Prüfen und Entfernen eines einfachen Dateilocks mit TTL."
)


class FileNameResponse(BaseModel):
    id: str
    is_valid: bool
    txtFileName: str


@router.post("/filename", response_model=FileNameResponse)
async def _filename(request: Request):
    body = await request.json()

    file_id = None
    try:
        file_id = body['id']
    except Exception as e:
        logger.error(f"Keine id lesbar: {str(e)}")
        return FileNameResponse(id='', is_valid=False, txtFileName='')

    file = None
    try:
        service = load_drive_service()
        if service:
            file = service.files().get(
                fileId=file_id,
                fields="id, name, mimeType, trashed, md5Checksum"
            ).execute()
            logger.info(f"Datei gefunden: {file.get('name')} (Typ: {file.get('mimeType')})")
    except Exception as e:
        logger.error(f"Fehler beim Laden der Datei {file_id}: {str(e)}")
        return FileNameResponse(id=file_id, is_valid=False, txtFileName='')

    if not file or file.get('trashed', False):
        logger.warning(f"Datei {file_id} existiert nicht oder ist im Papierkorb")
        return FileNameResponse(id=file_id, is_valid=False, txtFileName='')

    md5 = sanitize_filename(file.get('md5Checksum'))
    original_filename = file.get('name')
    sanitized_filename = sanitize_filename(original_filename)

    logger.info(f"Verarbeite Datei: {original_filename} (MD5: {md5})")

    # Prüfe ob Dateiname sanitized werden muss
    if original_filename != sanitized_filename:
        logger.info(f"Dateiname muss bereinigt werden: {original_filename} → {sanitized_filename}")
        try:
            service.files().update(
                fileId=file_id,
                body={"name": sanitized_filename},
                fields="id, name"
            ).execute()
            logger.info(f"Dateiname erfolgreich bereinigt zu: {sanitized_filename}")
            original_filename = sanitized_filename
        except Exception as e:
            logger.error(f"Fehler beim Bereinigen des Dateinamens: {e}")
            return FileNameResponse(id=file_id, is_valid=False, txtFileName='')

    if not any(sanitized_filename.endswith(ext.lower()) for ext in Settings.IMAGE_EXTENSIONS):
        logger.warning(f"Ungültige Dateiendung für {sanitized_filename}")
        try:
            logger.info(f"Verschiebe {sanitized_filename} in temp Ordner")
            service.files().update(
                fileId=file_id,
                addParents=folder_id_by_name("temp"),
                removeParents='root'
            ).execute()
            logger.info(f"Datei {sanitized_filename} erfolgreich in temp Ordner verschoben")
        except Exception as e:
            logger.error(f"Fehler beim Verschieben der Datei in temp: {e}")
        return FileNameResponse(id=file_id, is_valid=False, txtFileName='')

    if original_filename.startswith(f"{md5}_"):
        logger.info(f"Datei {original_filename} hat bereits korrektes MD5-Prefix")
        return FileNameResponse(id=file_id, is_valid=True, txtFileName=original_filename + ".txt")

    if not original_filename.startswith(f"img_"):
        logger.info(f"Datei {original_filename} beginnt nicht mit 'img_', keine Umbenennung nötig")
        return FileNameResponse(id=file_id, is_valid=True, txtFileName=original_filename + ".txt")

    try:
        new_name = f"{md5}_{original_filename}"
        logger.info(f"Benenne Datei um: {original_filename} → {new_name}")
        service.files().update(
            fileId=file_id,
            body={"name": new_name},
            fields="id, name"
        ).execute()
        logger.info(f"Erfolgreich umbenannt zu: {new_name}")
        return FileNameResponse(id=file_id, is_valid=True, txtFileName=new_name + ".txt")
    except Exception as e:
        logger.error(f"Fehler beim Umbenennen der Datei {original_filename}: {e}")
        return FileNameResponse(id=file_id, is_valid=False, txtFileName='')


class LockResponse(BaseModel):
    locked: bool


def _is_stale(timestamp: datetime) -> bool:
    return datetime.now(timezone.utc) - timestamp > timedelta(seconds=LOCK_TTL)


@router.post("/lock", response_model=LockResponse)
def check_lock():
    """Prüft, ob der Lock existiert und noch gültig ist."""
    if LOCK_FILE.exists():
        try:
            data = json.loads(LOCK_FILE.read_text())
            ts = datetime.fromisoformat(data.get("timestamp"))
            # Falls der Timestamp naive ist, betrachten wir ihn als UTC
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except Exception:
            # Ungültige Datei-Inhalte => Lock entfernen
            LOCK_FILE.unlink(missing_ok=True)
            return LockResponse(locked=False)
        if _is_stale(ts):
            LOCK_FILE.unlink(missing_ok=True)
            return LockResponse(locked=False)
        return LockResponse(locked=True)
    return LockResponse(locked=False)


@router.get("/lock")
def set_or_remove_lock(value: bool = Query(..., description="True=Setzen, False=Entfernen")):
    """Setzt oder löscht den Lock basierend auf dem Query-Parameter 'value'."""
    if value:
        # Lock setzen
        LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).isoformat()
        payload = {"timestamp": timestamp}
        LOCK_FILE.write_text(json.dumps(payload))
        return {"status": "lock set", "timestamp": timestamp}
    else:
        # Lock löschen
        LOCK_FILE.unlink(missing_ok=True)
        return {"status": "lock removed"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("n8nlock:app", host="0.0.0.0", port=8001)
