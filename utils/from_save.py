import hashlib
from io import BytesIO
from pathlib import Path

from googleapiclient.http import MediaIoBaseDownload
from tqdm import tqdm

from app.config_gdrive import TEXT_FILE_CACHE_DIR, IMAGE_FILE_CACHE_DIR
from app.config_gdrive import load_drive_service, compare_hashfile_counts
from app.config_gdrive import sanitize_filename
from gdrive_folder_dict import folder_id_by_name

TEXT_FILE_CACHE_DIR = Path(TEXT_FILE_CACHE_DIR)


def move_file_to_folder(service, file_id, old_parents, new_parent):
    service.files().update(
        fileId=file_id,
        addParents=new_parent,
        removeParents=",".join(old_parents),
        fields='id, parents'
    ).execute()


def delete_file(service, file_id):
    service.files().delete(fileId=file_id).execute()


def download_file(service, file_id, destination_path):
    request = service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    new_content = fh.getvalue()

    existing_bytes = destination_path.read_bytes() if destination_path.exists() else b""
    if not existing_bytes or len(new_content) > len(existing_bytes):
        with open(destination_path, 'wb') as f:
            f.write(new_content)
        new_md5 = hashlib.md5(new_content).hexdigest()
        written_md5 = md5_of_path(destination_path)
        if new_md5 != written_md5:
            print(f"❌ MD5 stimmt nach dem Schreiben nicht für: {destination_path.name}")
            raise RuntimeError("MD5-Integritätsprüfung fehlgeschlagen")


def md5_of_path(path):
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def list_image_files(service, folder_id):
    files = []
    page_token = None
    with tqdm(desc=f"Lade Bilddateien aus {folder_id}", unit="Seite") as pbar:
        while True:
            response = service.files().list(
                q=f"'{folder_id}' in parents and mimeType != 'text/plain' and trashed=false",
                fields="nextPageToken, files(id, name, size, md5Checksum, parents)",
                pageToken=page_token,
                pageSize=1000
            ).execute()
            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            pbar.update(1)
            if not page_token:
                break
    return files


def sync_image_files(service, from_folder_name, to_folder_name):
    from_folder_id = folder_id_by_name(from_folder_name)

    from_files = list_image_files(service, from_folder_id)
    if len(from_files) == 0:
        return

    to_folder_id = folder_id_by_name(to_folder_name)

    from_files = list_image_files(service, from_folder_id)
    to_files = list_image_files(service, to_folder_id)
    existing_hashes = {f['md5Checksum'] for f in to_files if 'md5Checksum' in f}
    downloaded = perform_local_sync(service, from_files, Path(IMAGE_FILE_CACHE_DIR) / to_folder_name, existing_hashes)
    moved, deleted = perform_gdrive_sync(service, from_files, to_files, existing_hashes, to_folder_id, from_folder_id)

    print("Zusammenfassung:")
    print(f"🔢 Zu verarbeiten: {len(from_files)}")
    print(f"📥 Heruntergeladen lokal: {downloaded}")
    print(f"📦 Verschoben nach GDrive: {moved}")
    print(f"🗑️  Gelöscht auf GDrive: {deleted}")


def list_txt_files(service, folder_id):
    files = []
    page_token = None
    with tqdm(desc=f"Lade Dateien aus {folder_id}", unit="Seite") as pbar:
        while True:
            response = service.files().list(
                q=f"'{folder_id}' in parents and mimeType='text/plain' and trashed=false",
                fields="nextPageToken, files(id, name, size, md5Checksum, parents)",
                pageToken=page_token,
                pageSize=1000
            ).execute()
            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            pbar.update(1)
            if not page_token:
                break
    return files


def sync_txt_files(service, from_folder_name, to_folder_name):
    from_folder_id = folder_id_by_name(from_folder_name)

    from_files = list_txt_files(service, from_folder_id)
    if len(from_files) == 0:
        return

    to_folder_id = folder_id_by_name(to_folder_name)
    to_files = list_txt_files(service, to_folder_id)
    existing_hashes = {f['md5Checksum'] for f in to_files if 'md5Checksum' in f}

    downloaded = perform_local_sync(service, from_files, TEXT_FILE_CACHE_DIR, existing_hashes)
    moved, deleted = perform_gdrive_sync(service, from_files, to_files, existing_hashes, to_folder_id, from_folder_id)

    print("Zusammenfassung:")
    print(f"🔢 Zu verarbeiten: {len(from_files)}")
    print(f"📥 Heruntergeladen lokal: {downloaded}")
    print(f"📦 Verschoben nach GDrive: {moved}")
    print(f"🗑️  Gelöscht auf GDrive: {deleted}")


def perform_local_sync(service, save_files, local_file_dir, existing_hashes):
    downloaded = 0
    for file in tqdm(save_files, desc="Lokaler Abgleich", unit="Datei", leave=True):
        original_name = file['name']
        sanitized_name = sanitize_filename(original_name)
        file_id = file['id']
        remote_md5 = file.get('md5Checksum')
        local_path = local_file_dir / sanitized_name

        status = []
        if remote_md5 in existing_hashes:
            status.append("✅ bereits vorhanden (MD5 match)")
        elif local_path.exists():
            local_md5 = md5_of_path(local_path)
            if remote_md5 == local_md5:
                status.append("✅ lokal identisch")
            else:
                download_file(service, file_id, local_path)
                downloaded += 1
                status.append("🔁 lokal aktualisiert")
        else:
            download_file(service, file_id, local_path)
            downloaded += 1
            status.append("⬇️ heruntergeladen")
        tqdm.write(f"{original_name}: " + ", ".join(status))
    return downloaded


def perform_gdrive_sync(service, save_files, _files, existing_hashes, to_folder_id, from_folder_id):
    moved = 0
    deleted = 0
    for file in tqdm(save_files, desc="GDrive-Abgleich", unit="Datei", leave=True):
        original_name = file['name']
        file_id = file['id']
        remote_md5 = file.get('md5Checksum')

        status = []
        if remote_md5 in existing_hashes:
            status.append("✅ bereits vorhanden (MD5 match)")
            remote_size = int(file.get("size", 0))
            target_file = next(
                (f for f in _files if f.get("md5Checksum") == remote_md5 and f.get("name") == file.get("name")),
                None)
            target_size = int(target_file.get("size", 0)) if target_file else 0
            if remote_size > target_size:
                move_file_to_folder(service, file_id, file['parents'], to_folder_id)
                moved += 1
                status.append("📦 verschoben (größer)")
            else:
                delete_file(service, file_id)
                deleted += 1
                status.append("🗑️ gelöscht (nicht größer oder gleichnamig)")
        else:
            move_file_to_folder(service, file_id, file['parents'], to_folder_id)
            moved += 1
            status.append("📦 verschoben (neuer Hash)")

        tqdm.write(f"{original_name}: " + ", ".join(status))

    remaining = list_txt_files(service, from_folder_id)
    print(f"📂 Verbleibend im Ursprungsordner: {len(remaining)}")
    return moved, deleted


def normalize_filenames_in_drive_folder(service, folder_id_name, conflict_folder_name):
    folder_id = folder_id_by_name(folder_id_name)
    conflict_folder_id = folder_id_by_name(conflict_folder_name)

    print(f"\n🔁 Normalisiere: {conflict_folder_name}")

    files = list_txt_files(service, folder_id)
    md5_to_sanitized = {}
    renamed = 0
    moved = 0
    for file in tqdm(files, desc=f"Normalisiere Namen in {folder_id}", unit="Datei"):
        file_id = file['id']
        original_name = file['name']
        md5 = file.get("md5Checksum")
        sanitized = sanitize_filename(original_name)

        if sanitized != original_name:
            if md5 not in md5_to_sanitized:
                md5_to_sanitized[md5] = sanitized
                # Umbenennen
                service.files().update(fileId=file_id, body={"name": sanitized}).execute()
                tqdm.write(f"✏️  Umbenannt: {original_name} -> {sanitized}")
                renamed += 1
            else:
                # Gleicher MD5 aber doppelter Name: verschieben
                move_file_to_folder(service, file_id, file['parents'], conflict_folder_id)
                tqdm.write(f"📦 Verschoben wegen doppeltem MD5: {original_name}")
                moved += 1
    print(f"🔁 Umbenannt: {renamed}, 📦 Verschoben: {moved}")


if __name__ == "__main__":
    service = load_drive_service()

    # Erst normalize beide Ordner
    # normalize_filenames_in_drive_folder(service, "textfiles", "temp")
    # normalize_filenames_in_drive_folder(service, "real", "temp")
    # normalize_filenames_in_drive_folder(service, "save", "temp")

    sync_txt_files(service, "save", "textfiles")
    sync_image_files(service, "save", "real")

