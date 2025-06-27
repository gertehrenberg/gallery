import json
from pathlib import Path
from typing import Dict

from googleapiclient.http import MediaFileUpload
from tqdm import tqdm

from app.config import Settings
from app.routes.auth import load_drive_service


def build_folder_id_map(service) -> Dict[str, str]:
    folder_map = {}
    page_token = None
    with tqdm(desc="Lese GDrive-Ordnernamen", unit="Seite") as bar:
        while True:
            response = service.files().list(
                q="mimeType='application/vnd.google-apps.folder' and trashed=false",
                spaces="drive",
                fields="nextPageToken, files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=Settings.PAGESIZE,
                pageToken=page_token
            ).execute()
            for file in response.get("files", []):
                folder_map[file["name"]] = file["id"]
            page_token = response.get("nextPageToken")
            bar.update(1)
            if not page_token:
                break
    return folder_map


def move_gdrive_files_from_local(service, cache_dir: Path):
    global_gdrive_hashes = load_all_gdrive_hashes(cache_dir)
    folder_id_map = build_folder_id_map(service)
    hashfiles = list(cache_dir.rglob("gallery202505_hashes.json"))

    with tqdm(total=len(hashfiles), desc="Vergleiche lokale mit GDrive-Hashes", unit="Ordner") as bar:
        for gallery_hashfile in hashfiles:
            folder_path = gallery_hashfile.parent
            folder = folder_path.name
            gdrive_hashfile = folder_path / Settings.GDRIVE_HASH_FILE

            try:
                with gallery_hashfile.open("r", encoding="utf-8") as f:
                    local_hashes = json.load(f)
            except Exception as e:
                tqdm.write(f"[Fehler] {gallery_hashfile}: {e}")
                bar.update(1)
                continue

            try:
                with gdrive_hashfile.open("r", encoding="utf-8") as f:
                    gdrive_hashes = json.load(f)
            except Exception:
                gdrive_hashes = {}

            updated = False

            with tqdm(total=len(local_hashes), desc=f"{folder}", unit="Datei") as file_bar:
                for name, md5 in local_hashes.items():
                    existing = gdrive_hashes.get(name)
                    current_md5 = existing.get("md5") if isinstance(existing, dict) else existing
                    if name not in gdrive_hashes or current_md5 != md5:
                        file_info = global_gdrive_hashes.get(md5)
                        if file_info:
                            tqdm.write(f"[✓] {name} fehlt in {folder}, aber global vorhanden als: {file_info['name']}")
                            file_id = file_info.get("id")
                            if file_id:
                                target_folder_id = folder_id_map.get(folder)
                                if not target_folder_id:
                                    tqdm.write(f"[!] Keine Ordner-ID für {folder} gefunden")
                                    file_bar.update(1)
                                    continue
                                try:
                                    move_file_to_folder(service, file_id, target_folder_id)
                                    gdrive_hashes[name] = {
                                        "md5": file_info["md5"],
                                        "id": file_id
                                    }
                                    updated = True
                                except Exception as e:
                                    tqdm.write(f"[Fehler beim Verschieben] {name}: {e}")
                        else:
                            local_file = folder_path / name
                            if local_file.exists():
                                target_folder_id = folder_id_map.get(folder)
                                if target_folder_id:
                                    try:
                                        file_metadata = {"name": name, "parents": [target_folder_id]}
                                        media = MediaFileUpload(str(local_file), resumable=True)
                                        uploaded = service.files().create(
                                            body=file_metadata,
                                            media_body=media,
                                            fields="id"
                                        ).execute()
                                        gdrive_hashes[name] = {
                                            "md5": md5,
                                            "id": uploaded["id"]
                                        }
                                        updated = True
                                        tqdm.write(f"[↑] {name} hochgeladen in {folder}")
                                    except Exception as e:
                                        tqdm.write(f"[Fehler beim Hochladen] {name}: {e}")
                                else:
                                    tqdm.write(f"[!] Keine Zielordner-ID für {folder} gefunden")
                            else:
                                tqdm.write(f"[!] {name} fehlt in {folder} und global nicht gefunden")
                    file_bar.update(1)

            if updated:
                with gdrive_hashfile.open("w", encoding="utf-8") as f:
                    json.dump(gdrive_hashes, f, indent=2)
                tqdm.write(f"[↑] {Settings.GDRIVE_HASH_FILE} aktualisiert für Ordner {folder}")

            bar.update(1)


def load_all_gdrive_hashes(cache_dir: Path) -> Dict[str, Dict[str, str]]:
    global_hashes = {}
    hashfiles = list(cache_dir.rglob(Settings.GDRIVE_HASH_FILE))
    with tqdm(total=len(hashfiles), desc="Lese alle GDrive-Hashes", unit="Datei") as bar:
        for hashfile in hashfiles:
            try:
                with hashfile.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    for name, entry in data.items():
                        if isinstance(entry, dict) and 'md5' in entry and 'id' in entry:
                            global_hashes[entry['md5']] = {
                                "name": name,
                                "id": entry['id'],
                                "md5": entry['md5']
                            }
            except Exception as e:
                tqdm.write(f"[Fehler] {hashfile}: {e}")
            bar.update(1)
    return global_hashes


def build_folder_id_map(service) -> Dict[str, str]:
    folder_map = {}
    page_token = None
    with tqdm(desc="Lese GDrive-Ordnernamen", unit="Seite") as bar:
        while True:
            response = service.files().list(
                q="mimeType='application/vnd.google-apps.folder' and trashed=false",
                spaces="drive",
                fields="nextPageToken, files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=Settings.PAGESIZE,
                pageToken=page_token
            ).execute()
            for file in response.get("files", []):
                folder_map[file["name"]] = file["id"]
            page_token = response.get("nextPageToken")
            bar.update(1)
            if not page_token:
                break
    return folder_map


def move_file_to_folder(service, file_id: str, target_folder_id: str):
    file = service.files().get(fileId=file_id, fields="parents").execute()
    previous_parents = ",".join(file.get("parents", []))
    service.files().update(
        fileId=file_id,
        addParents=target_folder_id,
        removeParents=previous_parents,
        fields="id, parents"
    ).execute()


if __name__ == "__main__":
    service = load_drive_service()
    move_gdrive_files_from_local(service, Path(Settings.IMAGE_FILE_CACHE_DIR))
    compare_hashfile_counts(Settings.IMAGE_FILE_CACHE_DIR)


