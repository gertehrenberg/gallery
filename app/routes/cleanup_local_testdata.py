from ..config import Settings
from ..utils.logger_config import setup_logger

logger = setup_logger(__name__)

import random
import string
from pathlib import Path

LOCAL_BASE = Settings.IMAGE_FILE_CACHE_DIR


# ---------------------------------------------------------
# Hilfsfunktionen
# find . -type f -newermt "today 00:00" -delete
# ---------------------------------------------------------

def random_filename(ext="jpg"):
    name = "".join(random.choices(string.ascii_lowercase, k=8))
    return f"{name}.{ext}"


def make_random_bytes(size_kb: int):
    return os.urandom(size_kb * 1024)


def write_file(path: Path, size_kb: int):
    with open(path, "wb") as f:
        f.write(make_random_bytes(size_kb))


def write_duplicate(path1: Path, path2: Path, size_kb: int):
    """Erstellt 2 identische Dateien (= gleicher MD5)."""
    content = make_random_bytes(size_kb * 1024)
    with open(path1, "wb") as f:
        f.write(content)
    with open(path2, "wb") as f:
        f.write(content)


def ensure_dir(folder: str):
    p = Path(LOCAL_BASE) / folder
    p.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# Erzeuge Testdaten für 3 zufällige Kategorien
# ---------------------------------------------------------

def generate_local_test_data():
    logger.info("🧪 Erstelle *kleine* Testdaten für 3 Kategorien…")

    # --- Kategorien aus Settings ---
    all_cats = Settings.kategorien()
    all_keys = [c["key"] for c in all_cats if c["key"] != "real"]

    # --- 3 zufällige Ordner ---
    folders = random.sample(all_keys, 3)
    logger.info(f"🎯 Ausgewählte Test-Ordner: {folders}")

    for folder in folders:
        ensure_dir(folder)
        folder_path = Path(LOCAL_BASE) / folder
        logger.info(f"📁 Testdaten für Ordner: {folder}")

        # 1) 3 normale Dateien
        for _ in range(3):
            name = random_filename()
            write_file(folder_path / name, size_kb=20)

        # 2) 2 weird-case Dateien (UPPERCASE)
        for _ in range(2):
            name = random_filename().upper()
            write_file(folder_path / name, size_kb=10)

        # 3) 2 Duplikat-Paare (lower + UPPER)
        for _ in range(2):
            low = random_filename()
            up = low.upper()
            write_duplicate(folder_path / low, folder_path / up, size_kb=30)

        # 4) 1 große Datei (1.5–3 MB)
        big_name = random_filename()
        write_file(folder_path / big_name, size_kb=random.randint(1500, 3000))

        logger.info(f"✔ Fertig: {folder}")

    logger.info("🎉 Kleine Testdaten vollständig erzeugt!")


import os
from typing import List


def delete_imagefile_caps(base_path: str = Settings.IMAGE_FILE_CACHE_DIR) -> List[str]:
    """
    Löscht alle bekannten Großbuchstaben-JPG-Dateien aus dem imagefiles-Verzeichnis.

    :param base_path: Root-Ordner der Bilder (/data/imagefiles)
    :return: Liste der erfolgreich gelöschten Dateien
    """

    files_to_delete = [
        "bad/mehykwrl.jpg"
    ]

    deleted = []

    for rel_path in files_to_delete:
        abs_path = os.path.join(base_path, rel_path)

        if os.path.isfile(abs_path):
            try:
                os.remove(abs_path)
                deleted.append(abs_path)
                logger.info(f"✔ gelöscht: {abs_path}")
            except Exception as e:
                logger.info(f"✘ Fehler bei {abs_path}: {e}")
        else:
            logger.info(f"⚠ Datei nicht gefunden: {abs_path}")

    return deleted
