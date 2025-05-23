import json
import os
from pathlib import Path
from typing import Dict, List

from tqdm import tqdm

from app.config import Settings
from app.config_gdrive import calculate_md5, delete_all_hashfiles, load_drive_service, sanitize_filename, \
    get_all_subfolders
from app.routes.dashboard import compare_hashfile_counts
from gdrive_folder_dict import folder_id_by_name


def save_structured_hashes(hashes: Dict[str, Dict[str, str]], hashfile_path: Path):
    hashfile_path.parent.mkdir(parents=True, exist_ok=True)
    with hashfile_path.open("w", encoding="utf-8") as f:
        json.dump(hashes, f, indent=2)
    os.chmod(hashfile_path, 0o644)


def save_simple_hashes(hashes: Dict[str, str], hashfile_path: Path):
    hashfile_path.parent.mkdir(parents=True, exist_ok=True)
    with hashfile_path.open("w", encoding="utf-8") as f:
        json.dump(hashes, f, indent=2)
    os.chmod(hashfile_path, 0o644)


def process_image_folders(service, extensions, file_folder_dir, folder_ids: List[str], subfolders: bool = True):
    folder_names: Dict[str, str] = {}

    for root_id in folder_ids:
        all_ids = get_all_subfolders(service, root_id) if subfolders else [root_id]
        for folder_id in all_ids:
            files = []
            page_token = None
            query = f"'{folder_id}' in parents and trashed = false"

            with tqdm(desc=f"Lade aus {folder_id[:6]}...", unit="Seite") as pbar:
                while True:
                    response = service.files().list(
                        q=query,
                        spaces='drive',
                        fields="nextPageToken, files(id, name, size, parents, md5Checksum)",
                        pageSize=1000,
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                        pageToken=page_token
                    ).execute()
                    batch = response.get('files', [])
                    files.extend([
                        f for f in batch
                        if isinstance(f.get('name'), str) and f['name'].lower().endswith(tuple(extensions))
                    ])
                    page_token = response.get('nextPageToken')
                    pbar.update(1)
                    if not page_token:
                        break

            if not files:
                continue

            folder = service.files().get(fileId=folder_id, fields="name").execute()
            folder_name = folder.get("name", "real")
            folder_names[folder_id] = folder_name

            gdrive_hashes: Dict[str, Dict[str, str]] = {}
            for file in files:
                try:
                    name = sanitize_filename(file['name'])
                    md5_drive = file.get("md5Checksum")
                    if md5_drive:
                        gdrive_hashes[name] = {
                            "md5": md5_drive,
                            "id": file['id']
                        }
                except Exception as e:
                    print(f"[Fehler] {file['name']}: {e}")

            if subfolders:
                local_dir = Path(file_folder_dir) / folder_name
            else:
                local_dir = Path(file_folder_dir)
            save_structured_hashes(gdrive_hashes, local_dir / "hashes.json")
            print(f"[✓] Gespeichert: {local_dir}/hashes.json")


def write_local_hashes(extensions, file_folder_dir, subfolders: bool = True):
    root = Path(file_folder_dir)
    all_dirs = [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]
    with tqdm(total=len(all_dirs), desc="Erzeuge lokale Hashes", unit="Ordner") as pbar:
        for subdir in all_dirs:
            local_hashes: Dict[str, str] = {}
            image_files = [f for f in subdir.iterdir() if f.is_file() and f.suffix.lower() in extensions]
            with tqdm(total=len(image_files), desc=f"{subdir.name}", unit="Bild", leave=False) as inner:
                for file in image_files:
                    try:
                        md5_local = calculate_md5(file)
                        local_hashes[file.name] = md5_local
                    except Exception as e:
                        tqdm.write(f"[Fehler] {file.name}: {e}")
                    inner.update(1)
            hashfile_name = "gallery202505_hashes.json"
            save_simple_hashes(local_hashes, subdir / hashfile_name)
            tqdm.write(f"[✓] Lokale Hashes gespeichert: {subdir / hashfile_name}")
            pbar.update(1)


def images(service):
    delete_all_hashfiles(Settings.IMAGE_FILE_CACHE_DIR)
    process_image_folders(service, Settings.IMAGE_EXTENSIONS, Settings.IMAGE_FILE_CACHE_DIR,
                          [folder_id_by_name("imagefiles")])
    write_local_hashes(Settings.IMAGE_EXTENSIONS, Settings.IMAGE_FILE_CACHE_DIR)


def text(service):
    delete_all_hashfiles(Settings.TEXT_FILE_CACHE_DIR, False)
    process_image_folders(service, Settings.TEXT_EXTENSIONS, Settings.TEXT_FILE_CACHE_DIR,
                          [folder_id_by_name("textfiles")], False)
    write_local_hashes(Settings.TEXT_EXTENSIONS, Settings.TEXT_FILE_CACHE_DIR, False)


def local():
    global service
    Settings.IMAGE_FILE_CACHE_DIR = "../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../cache/textfiles"
    SECRET_PATH = "../secrets"
    return load_drive_service(os.path.abspath(os.path.join(SECRET_PATH, "token.json")))


if __name__ == "__main__":
    service = local()

    images(service)
    text(service)

    compare_hashfile_counts(Settings.IMAGE_FILE_CACHE_DIR)
    compare_hashfile_counts(Settings.TEXT_FILE_CACHE_DIR, False)
