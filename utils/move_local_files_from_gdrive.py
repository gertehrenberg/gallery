import json
import shutil
from pathlib import Path

from googleapiclient.http import MediaIoBaseDownload
from tqdm import tqdm

from app.config import Settings
from app.routes.auth import load_drive_service


def download_file(service, file_id, local_path):
    request = service.files().get_media(fileId=file_id)
    with open(local_path, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()


def map_gdrive_to_local():
    base_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
    base_dir.mkdir(parents=True, exist_ok=True)
    service = load_drive_service()
    gallery_hashes = {}
    processed_files = set()

    all_local_folders = [p for p in base_dir.iterdir() if p.is_dir() and p.name != "real"]

    for folder_path in sorted(all_local_folders):
        folder_name = folder_path.name
        hash_file_path = folder_path / Settings.GDRIVE_HASH_FILE

        if not hash_file_path.exists():
            print(f"[SKIP] Keine hashes.json in {folder_name}")
            continue

        try:
            with open(hash_file_path, "r", encoding="utf-8") as f:
                entries = json.load(f)
        except Exception as e:
            print(f"[Fehler beim Lesen von {hash_file_path}]: {e}")
            continue

        for name, entry in tqdm(entries.items(), desc=f"{folder_name}", unit="Datei"):
            if name in processed_files:
                continue

            md5 = None
            file_id = None
            if isinstance(entry, dict):
                md5 = entry.get("md5")
                file_id = entry.get("id")
            elif isinstance(entry, str):
                md5 = entry

            if not md5:
                continue

            local_target = folder_path / name
            all_matches = list(base_dir.rglob(name))
            best_match = None
            for match in all_matches:
                if match.resolve() != local_target.resolve() and match.parent != local_target.parent:
                    best_match = match
                    break

            if local_target.exists():
                processed_files.add(name)
                gallery_hashes[name] = {"md5": md5, "id": file_id} if file_id else md5
            elif best_match:
                try:
                    shutil.move(str(best_match), str(local_target))
                    if local_target.exists():
                        print(f"[MOVE] {name} → {folder_name}")
                        processed_files.add(name)
                        gallery_hashes[name] = {"md5": md5, "id": file_id} if file_id else md5
                    else:
                        raise RuntimeError("Ziel existiert nach Move nicht")
                except Exception as e:
                    print(f"[MOVE-FEHLER] {name}: {e}")
                    raise SystemExit(f"Abbruch: Datei konnte nicht verschoben werden: {best_match} → {local_target}")
            elif file_id:
                try:
                    download_file(service, file_id, local_target)
                    if local_target.exists():
                        print(f"[DL] {name} ↓ {folder_name}")
                        processed_files.add(name)
                        gallery_hashes[name] = {"md5": md5, "id": file_id} if file_id else md5
                except Exception as e:
                    print(f"[Fehler beim Herunterladen] {name}: {e}")
            elif best_match:
                print(f"\033[94m[FEHLT] {name} → kein Download möglich, aber lokal gefunden\033[0m")

    gallery_hash_path = base_dir / Settings.GALLERY_HASH_FILE
    with open(gallery_hash_path, "w", encoding="utf-8") as f:
        json.dump(gallery_hashes, f, indent=2)


if __name__ == "__main__":
    map_gdrive_to_local()
