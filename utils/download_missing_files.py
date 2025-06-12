import io
import json
import shutil
from pathlib import Path
from typing import Dict, List

from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from tqdm import tqdm

from app.config import Settings
from app.config_gdrive import get_all_subfolders, calculate_md5
from app.routes.auth import load_drive_service


def upload_to_drive(service, file_path: Path, parent_id: str):
    existing = service.files().list(
        q=f"'{parent_id}' in parents and name = '{file_path.name}' and trashed = false",
        spaces='drive',
        fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute().get("files", [])
    for f in existing:
        service.files().delete(fileId=f['id']).execute()

    file_metadata = {
        'name': file_path.name,
        'parents': [parent_id]
    }
    media = MediaFileUpload(str(file_path), resumable=True)
    service.files().create(body=file_metadata, media_body=media, fields='id').execute()


def download_missing_files(service, extensions, file_folder_dir, folder_ids: List[str], recurse: bool = True):
    for id in tqdm(folder_ids, desc="Verarbeite Startordner", unit="Ordner", position=0):
        all_ids = get_all_subfolders(service, id) if recurse else [id]
        for folder_id in tqdm(all_ids, desc="Scanne Unterordner", unit="Ordner", leave=False, position=1):
            folder = service.files().get(fileId=folder_id, fields="name").execute()
            folder_name = folder.get("name", "real")
            if recurse:
                local_dir = Path(file_folder_dir) / folder_name
            else:
                local_dir = Path(file_folder_dir)

            local_dir.mkdir(parents=True, exist_ok=True)

            # GDrive Hashes lesen
            gdrive_hash_path = local_dir / Settings.GDRIVE_HASH_FILE
            if not gdrive_hash_path.exists():
                tqdm.write(f"[Übersprungen] Keine Hash-Datei vorhanden: {gdrive_hash_path}")
                continue
            with gdrive_hash_path.open("r", encoding="utf-8") as f:
                raw_gdrive_hashes = json.load(f)
                gdrive_hashes = {k: v["md5"] for k, v in raw_gdrive_hashes.items() if
                                 isinstance(v, dict) and "md5" in v}

            # Lokale Hashes aus Datei lesen
            local_hashfile = local_dir / "gallery202505_hashes.json"
            if local_hashfile.exists():
                with open(local_hashfile, "r", encoding="utf-8") as f:
                    local_hashes: Dict[str, str] = json.load(f)
            else:
                tqdm.write(f"[Warnung] Lokale Hash-Datei nicht gefunden: {local_hashfile}")
                local_hashes = {}

            # Testausgabe zur Validierung
            g_total = len(gdrive_hashes)
            l_total = len(local_hashes)
            exact_match = sum(1 for name, md5 in gdrive_hashes.items()
                              if name in local_hashes and local_hashes[name] == md5)

            tqdm.write(f"[Test] {folder_name}: GDrive={g_total}, Lokal={l_total}, Übereinstimmend={exact_match}")

            # Nur fehlende oder veränderte Dateien herunterladen
            to_download = [name for name, md5 in gdrive_hashes.items()
                           if name not in local_hashes or local_hashes[name] != md5]

            tqdm.write(f"[Test] {folder_name}: ToDownload={len(to_download)}")

            if not to_download:
                tqdm.write(f"[✓] {folder_name}: Alle Dateien aktuell")
                continue

            tqdm.write(f"[↓] {folder_name}: {len(to_download)} Dateien fehlen oder sind geändert")
            with tqdm(total=len(to_download), desc=f"Lade {folder_name}", unit="Datei", leave=False, position=3) as bar:
                for name in to_download:
                    response = service.files().list(
                        q=f"'{folder_id}' in parents and name='{name}' and trashed = false",
                        spaces='drive',
                        fields="files(id)",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True
                    ).execute()
                    files = response.get("files", [])
                    if not files:
                        bar.update(1)
                        continue
                    file_id = files[0]['id']
                    request = service.files().get_media(fileId=file_id)
                    fh = io.BytesIO()
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    short_name = name if len(name) <= 40 else f"{name[:18]}...{name[-18:]}"
                    with tqdm(total=100, desc="Download", unit="%", leave=False, position=2) as dl_bar:
                        dl_bar.set_description_str(short_name)
                        while not done:
                            status, done = downloader.next_chunk()
                            if status:
                                dl_bar.n = int(status.progress() * 100)
                                dl_bar.refresh()
                    with open(local_dir / name, "wb") as f:
                        f.write(fh.getvalue())
                    bar.update(1)


def vorsicht_move_all_duplicate_images(extensions, file_folder_dir, temp_file_dir):
    hash_to_files = {}
    temp_dir = Path(temp_file_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    all_files = list(Path(file_folder_dir).rglob("*"))
    with tqdm(total=len(all_files), desc="Analysiere Dateien", unit="Datei") as bar1:
        for file in all_files:
            if file.is_file() and file.suffix.lower() in extensions:
                try:
                    md5 = calculate_md5(file)
                    hash_to_files.setdefault(md5, []).append(file)
                except Exception as e:
                    print(f"[Fehler] {file}: {e}")
            bar1.update(1)

    with tqdm(total=len(hash_to_files), desc="Verarbeite Duplikatgruppen", unit="Hash") as bar2:
        for md5, files in hash_to_files.items():
            if not files:
                bar2.update(1)
                continue

            # Kürzester Dateiname, bei Gleichstand alphabetisch erste
            chosen = sorted(files, key=lambda f: (len(f.name), f.name))[0]

            try:
                shutil.move(str(chosen), str(temp_dir / chosen.name))
            except Exception as e:
                print(f"[Fehler beim Verschieben] {chosen}: {e}")

            for file in files:
                if file != chosen:
                    try:
                        file.unlink()
                    except Exception as e:
                        print(f"[Fehler beim Löschen] {file}: {e}")
            bar2.update(1)

    print(f"[✓] Pro Duplikatgruppe wurde die kürzeste Datei nach {temp_file_dir} verschoben – alle anderen gelöscht.")


if __name__ == "__main__":
    service = load_drive_service()

    # download_missing_files(service, IMAGE_EXTENSIONS, IMAGE_FILE_CACHE_DIR, [IMAGE_FOLDER_ID])
    # compare_hashfile_counts(IMAGE_FILE_CACHE_DIR)

    download_missing_files(service, Settings.TEXT_EXTENSIONS, Settings.TEXT_FILE_CACHE_DIR, [Settings.TEXT_FOLDER_ID], False)
    compare_hashfile_counts(Settings.TEXT_FILE_CACHE_DIR)
