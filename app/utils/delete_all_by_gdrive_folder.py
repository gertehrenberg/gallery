import asyncio
import os
from pathlib import Path

from app.config import Settings
from app.config_gdrive import SettingsGdrive, sanitize_filename, folder_id_by_name
from app.routes.auth import load_drive_service_token
from app.routes.hashes import update_gdrive_hashes, update_local_hashes_text, update_local_hashes
from app.utils.progress import update_progress, stop_progress, \
    update_progress_text


async def delete_files_in_folder(service, folder_id: str = "1q1b1DpBAVDfkvAOMBQ-G8aJ-xJuqf_B6"):
    affected_folders = set()
    try:
        await update_progress_text(f"🔍 Suche Dateien im Ordner...")

        # Google Drive Dateien auflisten
        files = []
        page_token = None
        while True:
            response = service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="nextPageToken, files(id, name, md5Checksum, parents)",
                pageSize=Settings.PAGESIZE,
                pageToken=page_token
            ).execute()

            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken')
            if not page_token:
                break

        total_files = len(files)
        await update_progress_text(f"🗑️ Lösche {total_files} Dateien...")

        # Dateien in Google Drive löschen
        deleted_gdrive = 0
        deleted_local = 0

        for idx, file in enumerate(files):
            try:
                # Google Drive Datei löschen
                service.files().delete(fileId=file['id']).execute()
                deleted_gdrive += 1

                # Suche und lösche entsprechende lokale Datei
                for cache_dir in [Settings.IMAGE_FILE_CACHE_DIR]:
                    for path in Path(cache_dir).rglob(sanitize_filename(file['name'])):
                        try:
                            # Ordner merken für Cache-Rebuild
                            affected_folders.add(path.parent.name)
                            path.unlink()
                            deleted_local += 1

                            # Suche und lösche in anderen Google Drive Ordnern
                            other_folder_id = folder_id_by_name(path.parent.name)
                            if other_folder_id and other_folder_id != folder_id:
                                try:
                                    response = service.files().list(
                                        q=f"name = '{file['name']}' and '{other_folder_id}' in parents and trashed=false",
                                        fields="files(id, name)"
                                    ).execute()
                                    for other_file in response.get('files', []):
                                        service.files().delete(fileId=other_file['id']).execute()
                                        deleted_gdrive += 1
                                except Exception as e:
                                    await update_progress_text(
                                        f"Fehler beim Löschen aus anderem Ordner {path.parent.name}: {e}")

                        except Exception as e:
                            await update_progress_text(f"Fehler beim Löschen der lokalen Datei {path}: {e}")

                for cache_dir in [Settings.TEXT_FILE_CACHE_DIR]:
                    for path in Path(cache_dir).rglob(sanitize_filename(file['name']) + ".txt"):
                        try:
                            # Ordner merken für Cache-Rebuild
                            affected_folders.add(path.parent.name)
                            path.unlink()
                            deleted_local += 1

                            # Suche und lösche in anderen Google Drive Ordnern
                            other_folder_id = folder_id_by_name(path.parent.name)
                            if other_folder_id and other_folder_id != folder_id:
                                try:
                                    response = service.files().list(
                                        q=f"name = '{file['name']}.txt' and '{other_folder_id}' in parents and trashed=false",
                                        fields="files(id, name)"
                                    ).execute()
                                    for other_file in response.get('files', []):
                                        service.files().delete(fileId=other_file['id']).execute()
                                        deleted_gdrive += 1
                                except Exception as e:
                                    await update_progress_text(
                                        f"Fehler beim Löschen aus anderem Ordner {path.parent.name}: {e}")

                        except Exception as e:
                            await update_progress_text(f"Fehler beim Löschen der lokalen Datei {path}: {e}")

                # Fortschritt aktualisieren
                progress = int((idx + 1) / total_files * 100)
                await update_progress(
                    f"🗑️ Lösche Dateien ({idx + 1}/{total_files})",
                    progress
                )

            except Exception as e:
                await update_progress_text(f"Fehler beim Löschen von {file['name']}: {e}")
                continue

    finally:
        await stop_progress()

    for folder_key in affected_folders:
        if folder_key == Settings.TEXTFILES_FOLDERNAME:
            await update_progress_text("🗃️ Modus: Textverarbeitung")
            await update_gdrive_hashes(
                service,
                Settings.TEXTFILES_FOLDERNAME,
                Settings.TEXT_EXTENSIONS,
                Path(Settings.TEXT_FILE_CACHE_DIR).parent)
            await update_local_hashes_text()
            continue
        await update_progress_text(
            f"🔄 Starte Cache-Rebuild für Ordner: {folder_key}"
        )
        await update_gdrive_hashes(service, folder_key, Settings.IMAGE_EXTENSIONS, Path(Settings.IMAGE_FILE_CACHE_DIR))
        await update_local_hashes(folder_key)

    return affected_folders


def p7():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))

    asyncio.run(delete_files_in_folder(service))


if __name__ == "__main__":
    p7()
