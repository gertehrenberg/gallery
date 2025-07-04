import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

from fastapi import FastAPI, Query, APIRouter
from pydantic import BaseModel

router = APIRouter()

# Konfigurierbare Parameter über ENV
LOCK_FILE = Path(os.getenv("LOCK_FILE", "/data/workflow.lock"))
LOCK_TTL = int(os.getenv("LOCK_TTL", "3600"))  # in Sekunden

app = FastAPI(
    title="Lock-Management Service",
    version="1.0",
    description="Service zum Setzen, Prüfen und Entfernen eines einfachen Dateilocks mit TTL."
)


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
