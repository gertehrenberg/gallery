import hashlib
from io import BytesIO
from pathlib import Path

from googleapiclient.http import MediaIoBaseDownload
from tqdm import tqdm

from config import REAL_FOLDER_ID
from config import TEXT_FOLDER_ID, TEXT_FILE_CACHE_DIR
from config import load_drive_service, compare_hashfile_counts
from config import sanitize_filename

TEXT_FILE_CACHE_DIR = Path(TEXT_FILE_CACHE_DIR)


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


def sync_txt_files(service, from_folder_id, to_folder_id):
    TEXT_FILE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    save_files = list_txt_files(service, from_folder_id)
    text_files = list_txt_files(service, to_folder_id)
    existing_hashes = {f['md5Checksum'] for f in text_files if 'md5Checksum' in f}

    downloaded = perform_local_sync(service, save_files, existing_hashes)
    moved, deleted = perform_gdrive_sync(service, save_files, text_files, existing_hashes, to_folder_id, from_folder_id)

    print("Zusammenfassung:")
    print(f"🔢 Zu verarbeiten: {len(save_files)}")
    print(f"📥 Heruntergeladen lokal: {downloaded}")
    print(f"📦 Verschoben nach GDrive: {moved}")
    print(f"🗑️  Gelöscht auf GDrive: {deleted}")


def perform_local_sync(service, save_files, existing_hashes):
    downloaded = 0
    for file in tqdm(save_files, desc="Lokaler Abgleich", unit="Datei", leave=True):
        original_name = file['name']
        sanitized_name = sanitize_filename(original_name)
        file_id = file['id']
        remote_md5 = file.get('md5Checksum')
        local_path = TEXT_FILE_CACHE_DIR / sanitized_name

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


def perform_gdrive_sync(service, save_files, text_files, existing_hashes, to_folder_id, from_folder_id):
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
            target_file = next((f for f in text_files if f.get("md5Checksum") == remote_md5 and sanitize_filename(f.get("name", "")) == sanitize_filename(file.get("name", ""))), None)
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


if __name__ == "__main__":
    sync_txt_files(load_drive_service(), REAL_FOLDER_ID, TEXT_FOLDER_ID)
    compare_hashfile_counts(TEXT_FILE_CACHE_DIR, False)
