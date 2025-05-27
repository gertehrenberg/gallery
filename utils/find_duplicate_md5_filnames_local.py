import json
import os
import shutil
from collections import defaultdict
from pathlib import Path

from tqdm import tqdm

from app.config import Settings
from app.routes.auth import load_drive_service, load_drive_service_token

TEMP_FILE_DIR = "../temp"

def rename_file(service, file_id: str, new_name: str):
    service.files().update(
        fileId=file_id,
        body={"name": new_name},
        fields="id, name"
    ).execute()


def find_duplicate_md5_filenames(cache_dir: Path):
    md5_to_names = defaultdict(set)
    md5_to_entries = defaultdict(list)
    hashfiles = list(cache_dir.rglob("*hashes.json"))
    with tqdm(total=len(hashfiles), desc="Durchsuche Hash-Dateien", unit="Datei") as bar:
        for hashfile in hashfiles:
            try:
                with hashfile.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    for name, entry in data.items():
                        if isinstance(entry, dict) and 'md5' in entry:
                            md5 = entry['md5']
                            md5_to_names[md5].add(name)
                            md5_to_entries[md5].append((hashfile, name, entry))
                        elif isinstance(entry, str):
                            md5 = entry
                            md5_to_names[md5].add(name)
                            md5_to_entries[md5].append((hashfile, name, {"md5": md5}))
            except Exception as e:
                print(f"[Fehler] {hashfile}: {e}")
            bar.update(1)

    print("\n[Duplikate basierend auf MD5-Hash]")
    service = load_drive_service_token(Path("../secrets/") /"token.json")
    temp_dir = Path(TEMP_FILE_DIR)
    temp_dir.mkdir(parents=True, exist_ok=True)

    duplicate_items = [(md5, names) for md5, names in md5_to_names.items() if len(names) > 1]
    for md5, names in tqdm(duplicate_items, desc="Verarbeite Duplikate", unit="Hash"):
        sorted_names = sorted(names, key=lambda n: (len(n), n))
        canonical_name = sorted_names[0]
        print(f"{md5}: {sorted(names)} → {canonical_name}")
        for hashfile, name, entry in md5_to_entries[md5]:
            if name == canonical_name:
                continue
            print(f"  [⮕] Ändere in {hashfile} → {name} → {canonical_name}")
            try:
                with hashfile.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                data.pop(name, None)
                if canonical_name not in data:
                    data[canonical_name] = entry
                with hashfile.open("w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                print(f"  [✓] Lokale Umbenennung erfolgreich")
            except Exception as e:
                print(f"  [Fehler beim lokalen Umbenennen] {name}: {e}")

            # Lokale Datei verschieben
            local_path = hashfile.parent / name
            if local_path.exists():
                try:
                    shutil.move(str(local_path), str(temp_dir / name))
                    print(f"  [→] Verschoben nach TEMP: {name}")
                except Exception as e:
                    print(f"  [Fehler beim Verschieben nach TEMP] {name}: {e}")

            # Google Drive Datei ggf. löschen
            file_id = entry.get("id") if isinstance(entry, dict) else None
            if file_id:
                try:
                    service.files().delete(fileId=file_id).execute()
                    print(f"  [🗑] GDrive-Datei gelöscht: {file_id}")
                except Exception as e:
                    print(f"  [Fehler beim Löschen auf GDrive] {name}: {e}")


def local():
    Settings.RENDERED_HTML_DIR = "../cache/rendered_html"
    Settings.PAIR_CACHE_PATH = "../cache/pair_cache_local.json"
    Settings.IMAGE_FILE_CACHE_DIR = "../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../cache/textfiles"


if __name__ == "__main__":
    local()
    find_duplicate_md5_filenames(Path(Settings.IMAGE_FILE_CACHE_DIR))
