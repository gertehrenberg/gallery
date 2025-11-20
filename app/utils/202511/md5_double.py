import os
import hashlib
import sqlite3
from collections import defaultdict

from gallery.app.config import Settings


# ----------------------------------------------------
# MD5 einer Datei berechnen
# ----------------------------------------------------
def md5_of_file(filepath, blocksize=65536):
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            md5.update(block)
    return md5.hexdigest()


# ----------------------------------------------------
# Alle Dateinamen im Ordner in lowercase umbenennen
# ----------------------------------------------------
def normalize_filenames_to_lowercase(folder):
    """Benannt alle Bilddateien im Ordner in lowercase um (keine Unterordner)."""
    renamed = []

    for filename in os.listdir(folder):
        old_path = os.path.join(folder, filename)

        # Nur Dateien, keine Ordner
        if not os.path.isfile(old_path):
            continue

        # Nur Bilddateien
        if not filename.lower().endswith(Settings.IMAGE_EXTENSIONS):
            continue

        new_filename = filename.lower()
        new_path = os.path.join(folder, new_filename)

        # Schon klein? → weiter
        if filename == new_filename:
            continue

        # Ziel existiert bereits → überspringen
        if os.path.exists(new_path):
            print(f"⚠️ Datei existiert schon, nicht überschrieben: {new_path}")
            continue

        try:
            os.rename(old_path, new_path)
            renamed.append((old_path, new_path))
            print(f"Umbenannt: {filename}  →  {new_filename}")
        except Exception as e:
            print(f"Fehler beim Umbenennen {old_path}: {e}")

    print(f"\n{len(renamed)} Dateien wurden auf lowercase normalisiert.")
    return renamed


# ----------------------------------------------------
# Duplikate + fehlende MD5 in DB finden (keine Unterordner)
# ----------------------------------------------------
def find_duplicates(folder, ids: set):
    hashes = defaultdict(list)
    image_count = 0
    missing = []

    for filename in os.listdir(folder):
        path = os.path.join(folder, filename)

        # Nur Dateien
        if not os.path.isfile(path):
            continue

        # Nur Bilddateien
        if not filename.lower().endswith(Settings.IMAGE_EXTENSIONS):
            continue

        image_count += 1

        try:
            file_hash = md5_of_file(path)

            # MD5 nicht in DB?
            if file_hash not in ids:
                missing.append(path)

            # Für Duplikate sammeln
            hashes[file_hash].append(path)

        except Exception as e:
            print(f"Fehler bei {path}: {e}")

    # Nur MD5 mit mehr als einem Pfad sind echte Duplikate
    duplicates = {h: p for h, p in hashes.items() if len(p) > 1}

    return duplicates, image_count, missing


# ----------------------------------------------------
# Case-Duplikate löschen (löscht NUR Dateien mit Großbuchstaben!)
# ----------------------------------------------------
LOGFILE = "deleted_case_duplicates.csv"


def delete_case_duplicates(duplicates):
    deleted_entries = []

    for file_hash, paths in duplicates.items():

        # Basenames normalized
        names_lower = {os.path.basename(p).lower() for p in paths}

        # Case-only duplicates → nur wenn ALLE names_lower identisch
        if len(names_lower) != 1:
            continue

        # Eine lowercase-Datei behalten
        keep = None
        for p in paths:
            if os.path.basename(p) == os.path.basename(p).lower():
                keep = p
                break

        # Wenn keine lowercase-Datei existiert → nichts löschen
        if not keep:
            continue

        # Alle anderen löschen (nur wenn uppercase!)
        for p in paths:
            if p == keep:
                continue

            base = os.path.basename(p)
            if base != base.lower():
                try:
                    os.remove(p)
                    print(f"Gelöscht (Case-Duplikat): {p}")
                    deleted_entries.append((file_hash, p, keep))
                except Exception as e:
                    print(f"Fehler beim Löschen von {p}: {e}")

    # Logfile schreiben
    if deleted_entries:
        with open(LOGFILE, "w", encoding="utf-8") as f:
            f.write("md5,deleted_file,kept_file\n")
            for md5, deleted, kept in deleted_entries:
                f.write(f"{md5},{deleted},{kept}\n")

        print(f"\nLogfile geschrieben: {LOGFILE}")
    else:
        print("\nKeine Case-Duplikate gefunden.")


# ----------------------------------------------------
# Prozess für eine Kategorie
# ----------------------------------------------------
def xx(folder_key):
    Settings.DB_PATH = '/home/ubuntu/gallery/gallery_local.db'
    folder = os.path.expanduser("~/gallery/cache/imagefiles/" + folder_key)

    print(f"\n=== {folder_key} ===")

    # 1) Dateinamen vorher normalisieren → lowercase
    normalize_filenames_to_lowercase(folder)

    # 2) MD5-Liste aus der DB lesen
    ids = set()
    with sqlite3.connect(Settings.DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT image_id FROM image_folder_status WHERE folder_id = ?",
            (folder_key,)
        ).fetchall()

        for row in rows:
            ids.add(row["image_id"])

    # 3) Duplikate & fehlende MD5 finden
    duplicates, image_count, missing = find_duplicates(folder, ids)

    # 4) Übersicht
    print(f"Bilddateien im Ordner      : {image_count}")
    print(f"Datensätze in der DB       : {len(ids)}")
    print(f"Fehlende (nicht in DB)     : {len(missing)}")

    if missing:
        print("\nFehlende Dateien:")
        for path in missing:
            print("  -", path)

    if duplicates:
        print("\nDuplicate MD5:")
        for h, paths in duplicates.items():
            print("\nMD5:", h)
            for p in paths:
                print("  -", p)
    else:
        print("\nKeine Duplikate gefunden.")

    # 5) Case-Duplikate automatisch löschen
    delete_case_duplicates(duplicates)


# ----------------------------------------------------
# MAIN PROGRAMM
# ----------------------------------------------------
if __name__ == "__main__":

    #xx("gemini")
    for kategorie in Settings.kategorien():
        xx(kategorie["key"])
