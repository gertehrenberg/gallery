import json
import os
import random
import string
from typing import List

from googleapiclient.http import MediaInMemoryUpload
from .auth import load_drive_service
from ..config import Settings
from ..config_gdrive import folder_id_by_name
from ..utils.logger_config import setup_logger

logger = setup_logger(__name__)

# ID-Store speichert nur FILE-IDs
ID_STORE_PATH = Settings.DATA_DIR / "drive_testdata_ids.json"

_DRIVE = None


def get_drive():
    """Load Google Drive API service once."""
    global _DRIVE
    if _DRIVE is None:
        _DRIVE = load_drive_service()
    return _DRIVE


# -------------------------------------------------------------------
# Hilfsfunktionen
# -------------------------------------------------------------------

def random_filename(ext="jpg"):
    name = "".join(random.choices(string.ascii_lowercase, k=8))
    return f"{name}.{ext}"


def make_random_bytes(size_kb: int) -> bytes:
    return os.urandom(size_kb * 1024)


# -------------------------------------------------------------------
# Google Drive Upload-Funktionen
# -------------------------------------------------------------------

def upload_random_file(folder_id: str, filename: str, size_kb: int) -> str:
    """Erstellt eine Datei mit Zufallsbytes in Google Drive."""
    file_metadata = {
        "name": filename,
        "parents": [folder_id]
    }
    media = MediaInMemoryUpload(
        make_random_bytes(size_kb),
        mimetype="image/jpeg"
    )

    file = get_drive().files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    return file["id"]


def upload_duplicate_files(folder_id: str, filename1: str, filename2: str, size_kb: int):
    """Erstellt zwei Dateien mit identischem Content (Duplikate)."""
    content = make_random_bytes(size_kb)

    def upload(name, data):
        metadata = {"name": name, "parents": [folder_id]}
        media = MediaInMemoryUpload(data, mimetype="image/jpeg")
        file = get_drive().files().create(
            body=metadata,
            media_body=media,
            fields="id"
        ).execute()
        return file["id"]

    return upload(filename1, content), upload(filename2, content)


# -------------------------------------------------------------------
# ID-Speicher — NUR FILES
# -------------------------------------------------------------------

def store_ids(file_ids: List[str]):
    """Speichert ausschließlich File-IDs."""
    data = {"files": file_ids}

    with open(ID_STORE_PATH, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"💾 FILE-IDs gespeichert in {ID_STORE_PATH}")


def load_ids() -> List[str]:
    """Liest ausschließlich File-IDs."""
    if not ID_STORE_PATH.exists():
        return []

    data = json.load(open(ID_STORE_PATH))
    return data.get("files", [])


# -------------------------------------------------------------------
# Testdaten generieren (KEINE neuen Ordner!)
# -------------------------------------------------------------------

def generate_gdrive_test_data(parent_id):
    """
    Erzeugt Testdaten im Google Drive.
    - KEINE neuen Ordner werden angelegt!
    - Ordner werden NUR per Name gesucht.
    - ID-Store speichert nur File-IDs.
    """

    logger.info("🧪 Erstelle Testdaten im Google Drive…")
    logger.info(f"📁 Parent-ID (unbenutzt): {parent_id}")

    all_cats = Settings.kategorien()
    all_keys = [c["key"] for c in all_cats if c["key"] != "real"]

    folders = random.sample(all_keys, 3)
    logger.info(f"🎯 Benutzte Test-Ordner (müssen existieren): {folders}")

    file_ids = []

    for folder in folders:

        # Folder muss existieren!
        folder_id = folder_id_by_name(folder)
        if not folder_id:
            raise Exception(f"❌ Ordner '{folder}' existiert NICHT in Drive!")

        logger.info(f"📂 Kategorie-Ordner gefunden: {folder} → {folder_id}")

        # 1) 3 normale Dateien
        for _ in range(3):
            name = random_filename()
            fid = upload_random_file(folder_id, name, 20)
            file_ids.append(fid)

        # 2) 2 UPPERCASE-Dateien
        for _ in range(2):
            name = random_filename().upper()
            fid = upload_random_file(folder_id, name, 10)
            file_ids.append(fid)

        # 3) Duplikat-Paar
        for _ in range(2):
            low = random_filename()
            up = low.upper()
            fid1, fid2 = upload_duplicate_files(folder_id, low, up, 30)
            file_ids += [fid1, fid2]

        # 4) große Datei
        big_name = random_filename()
        size_kb = random.randint(1500, 3000)
        fid = upload_random_file(folder_id, big_name, size_kb)
        file_ids.append(fid)

        logger.info(f"✔ Fertig mit Ordner {folder}")

    # Nur FILES speichern
    store_ids(file_ids)
    logger.info("🎉 Google-Drive-Testdaten vollständig erzeugt!")


# -------------------------------------------------------------------
# Löschen: NUR FILES + ID-Store löschen
# -------------------------------------------------------------------

def delete_gdrive_testdata_files_from_store():
    """
    Löscht NUR die Dateien, die in drive_testdata_ids.json stehen.
    - Fehlende Dateien werden ignoriert.
    - Ordner werden nicht gelöscht.
    - Am Ende wird die ID_STORE_PATH Datei gelöscht.
    """

    if not ID_STORE_PATH.exists():
        logger.info("⚠ Keine Testdaten-ID-Datei vorhanden – nichts zu löschen.")
        return {"deleted_files": [], "errors": []}

    service = get_drive()
    file_ids = load_ids()

    deleted_files = []
    errors = []

    logger.info("🗑️ Lösche Testdateien aus ID-Store...")

    for file_id in file_ids:
        try:
            service.files().delete(fileId=file_id).execute()
            deleted_files.append(file_id)
            logger.info(f"✔ Datei gelöscht: {file_id}")

        except Exception as e:
            # Wenn Datei bereits gelöscht → ignorieren
            if "File not found" in str(e) or "notFound" in str(e):
                logger.info(f"⚠ Datei existiert nicht mehr: {file_id}")
                continue

            logger.warning(f"✘ Fehler beim Löschen {file_id}: {e}")
            errors.append({"file_id": file_id, "error": str(e)})

    # -----------------------------------------
    # 🔥 ID-Store Datei löschen
    # -----------------------------------------
    try:
        os.remove(ID_STORE_PATH)
        logger.info(f"🧼 ID-Datei gelöscht: {ID_STORE_PATH}")
    except Exception as e:
        logger.warning(f"⚠ Konnte ID-Datei nicht löschen: {e}")
        errors.append({"id_store_delete": str(e)})

    return {
        "deleted_files": deleted_files,
        "errors": errors
    }