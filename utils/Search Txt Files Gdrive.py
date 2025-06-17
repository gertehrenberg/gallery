import io
from pathlib import Path
from typing import List

from googleapiclient.http import MediaIoBaseDownload
from tqdm import tqdm

from config import IMAGE_FILE_CACHE_DIR, TEXT_FILE_CACHE_DIR, load_drive_service, sanitize_filename

# Basisordner (lokal) vorbereiten
TEXT_FILE_CACHE_DIR = Path(TEXT_FILE_CACHE_DIR)
TEXT_FILE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# GDrive "My Drive" Root-ID ist "root"
GDRIVE_ROOT_ID = "root"


def list_txt_files_recursive(service, parent_id: str) -> List[dict]:
    """Gibt alle .txt-Dateien unterhalb eines GDrive-Ordners rekursiv zurück."""
    files = []
    query = f"'{parent_id}' in parents and trashed = false"
    page_token = None

    with tqdm(desc="Durchsuche GDrive", unit="Anfrage") as outer_bar:
        while True:
            response = service.files().list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType, parents)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageSize=1000,
                pageToken=page_token
            ).execute()

            for file in response.get("files", []):
                if file["mimeType"] == "application/vnd.google-apps.folder":
                    files.extend(list_txt_files_recursive(service, file["id"]))
                elif file["name"].lower().endswith(".txt"):
                    file["name"] = sanitize_filename(file["name"])
                    files.append(file)

            page_token = response.get("nextPageToken")
            outer_bar.update(1)
            if not page_token:
                break

    return files


def main():
    # Alle Bilddateien in allen Unterordnern von IMAGE_FILE_CACHE_DIR durchsuchen
    image_files = list(Path(IMAGE_FILE_CACHE_DIR).rglob("*"))
    matched_txts = 0
    skipped_existing = 0
    missing_txt_keys = []

    with tqdm(total=len(image_files), desc="Prüfe lokale .txt-Dateien", unit="Bild") as bar:
        for img in image_files:
            if not img.is_file():
                bar.update(1)
                continue
            base_name = sanitize_filename(img.stem)

            existing_txts = list(TEXT_FILE_CACHE_DIR.glob(base_name + "_*.txt"))
            if existing_txts:
                skipped_existing += 1
            else:
                missing_txt_keys.append(base_name)
            bar.update(1)

    tqdm.write(f"{skipped_existing} bereits lokal vorhanden, {len(missing_txt_keys)} fehlen → Suche auf GDrive ...")

    if not missing_txt_keys:
        return

    service = load_drive_service()
    gdrive_txt_files = list_txt_files_recursive(service, GDRIVE_ROOT_ID)
    tqdm.write(f"{len(gdrive_txt_files)} .txt-Dateien auf GDrive gefunden.")

    with tqdm(total=len(missing_txt_keys), desc="Suche & lade fehlende .txt-Dateien", unit="Datei") as bar:
        for base_name in missing_txt_keys:
            matching_txt = next(
                (f for f in gdrive_txt_files if f["name"].startswith(base_name + "_") and f["name"].endswith(".txt")),
                None)
            if matching_txt:
                request = service.files().get_media(fileId=matching_txt["id"])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                with tqdm(total=100, desc=f"Lade {matching_txt['name'][:30]}...", unit="%", leave=False) as dl_bar:
                    while not done:
                        status, done = downloader.next_chunk()
                        if status:
                            dl_bar.n = int(status.progress() * 100)
                            dl_bar.refresh()

                sanitized_name = sanitize_filename(matching_txt["name"])
                output_path = TEXT_FILE_CACHE_DIR / sanitized_name
                with open(output_path, "wb") as f:
                    f.write(fh.getvalue())

                tqdm.write(f"[↓] {sanitized_name} heruntergeladen")
                matched_txts += 1
            bar.update(1)

    tqdm.write(f"Fertig. {matched_txts} neue .txt-Dateien heruntergeladen.")


if __name__ == "__main__":
    main()
