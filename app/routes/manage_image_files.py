import json
from pathlib import Path
from typing import Set

from app.config import Settings
from app.config_gdrive import folder_id_by_name
from app.routes.hashes import update_gdrive_hashes, delete_duplicates_in_gdrive_folder, upload_file_to_gdrive
from app.routes.manage_text_files import gdrive_textfiles_files_by_local, check_and_move_gdrive_files, \
    move_file_to_folder
from app.utils.progress import init_progress_state, progress_state, update_progress_text, \
    update_progress_auto
from app.utils.progress_detail import update_detail_status, update_detail_progress, start_detail_progress, \
    calc_detail_progress


# await update_progress_text("🗑️ Lösche alte Hash-Dateien...")
# await delete_all_hashfiles_async(Settings.IMAGE_FILE_CACHE_DIR)
# await update_all_local_hashes()
# await update_all_gdrive_hashes(service)
async def move_gdrive_files_by_local(service, folder_name: str):
    if Settings.TEXTFILES_FOLDERNAME == folder_name:
        await gdrive_textfiles_files_by_local(service, folder_name)
        return

    await init_progress_state()
    progress_state["running"] = True

    await update_progress_auto(f"🔄 Starte GDrive Synchronisation für Ordner: {folder_name}")

    # Lade alle lokalen Hash-Dateien
    cache_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
    folder_path = cache_dir / folder_name

    # Lade lokale Hashes des aktuellen Ordners
    try:
        with (folder_path / Settings.GALLERY_HASH_FILE).open("r", encoding="utf-8") as f:
            local_hashes = json.load(f)
    except Exception as e:
        await update_progress_auto(f"⚠️ Fehler beim Lesen lokaler Hashes für {folder_name}: {e}")
        return

    # Lade alle GDrive Hashes (aus allen Ordnern) mit Ordnerzuordnung
    all_gdrive_hashes = {}
    for kategorie in Settings.kategorien:
        gdrive_hash_file = cache_dir / kategorie["key"] / Settings.GDRIVE_HASH_FILE
        try:
            with gdrive_hash_file.open("r", encoding="utf-8") as f:
                folder_hashes = json.load(f)
                # Füge den Ordnernamen zu jedem Eintrag hinzu
                for filename, entry in folder_hashes.items():
                    if isinstance(entry, dict):
                        entry_with_folder = entry.copy()
                        entry_with_folder['source_folder'] = kategorie["key"]
                        all_gdrive_hashes[filename] = entry_with_folder
        except Exception as e:
            await update_progress_auto(f"ℹ️ Keine GDrive Hashes für {kategorie['key']}: {e}")

    moved = 0
    uploaded = 0
    total_files = len(local_hashes)

    await start_detail_progress(f"🔍 Gefunden: {total_files} Dateien")

    gdrive_folder_names: Set[str] = set()

    # Verarbeite jede lokale Datei
    for idx, (filename, local_md5) in enumerate(local_hashes.items()):
        progress = await calc_detail_progress(idx, total_files)
        was_moved, was_uploaded = await process_single_file(
            service,
            "image/*",
            filename,
            local_md5,
            all_gdrive_hashes,
            folder_name,
            folder_path,
            gdrive_folder_names
        )
        if was_moved:
            moved += 1
        if was_uploaded:
            uploaded += 1

        await update_detail_progress(
            detail_status=f"💾 Speichere DB Eintrag {idx + 1}/{total_files}",
            detail_progress=progress
        )

    # Update GDrive hashes wenn Änderungen vorgenommen wurden
    if moved > 0 or uploaded > 0:
        await update_progress_text(f"🔄 Aktualisiere GDrive Hashes für {folder_name}...")
        await delete_duplicates_in_gdrive_folder(service, folder_name)
        for gdrive_folder_name in gdrive_folder_names:
            await update_gdrive_hashes(service, gdrive_folder_name)

    await update_progress_text(
        f"✅ Synchronisation abgeschlossen. {moved} Dateien verschoben, {uploaded} Dateien hochgeladen")

    # Prüfe und verschiebe GDrive Dateien basierend auf lokalen Hashes
    moved_gdrive = await check_and_move_gdrive_files(service, folder_name, cache_dir, Settings.IMAGE_EXTENSIONS)
    if moved_gdrive > 0:
        await update_progress_text(
            f"✅ Zusätzlich wurden {moved_gdrive} GDrive Dateien in ihre korrekten Ordner verschoben")


async def process_single_file(
        service,
        mimetype,
        filename: str,
        local_md5: str,
        all_gdrive_hashes: dict,
        folder_name: str,
        folder_path: Path,
        gdrive_folder_names: Set[str]) -> tuple[bool, bool]:
    """
    Verarbeitet eine einzelne Datei im Synchronisationsprozess.

    Returns:
        Tuple[bool, bool]: (wurde_verschoben, wurde_hochgeladen)
    """
    found = False
    moved = False
    uploaded = False

    # Suche nach der Datei in allen GDrive Hashes
    for gdrive_name, entry in all_gdrive_hashes.items():
        if isinstance(entry, dict) and entry.get('md5') == local_md5:
            found = True
            # Wenn Datei in falschem Ordner ist, verschieben
            if entry.get('source_folder') != folder_name:
                try:
                    target_folder_id = folder_id_by_name(folder_name)
                    if target_folder_id:
                        await update_detail_status(f"📦 Verschiebe {filename} nach {folder_name}")
                        moved = move_file_to_folder(service, entry['id'], target_folder_id, gdrive_folder_names)
                        await update_detail_status(f"✅ {filename} wurde verschoben")
                        gdrive_folder_names.add(folder_name)
                except Exception as e:
                    await update_detail_status(f"❌ Fehler beim Verschieben von {filename}: {e}")
            break

    # Wenn Datei nirgends im GDrive gefunden wurde, hochladen
    if not found:
        try:
            file_path = folder_path / filename
            if file_path.exists():
                await update_detail_status(f"⬆️ Lade {filename} nach {folder_name} hoch")
                target_folder_id = folder_id_by_name(folder_name)
                if target_folder_id:
                    await upload_file_to_gdrive(service, mimetype, file_path, target_folder_id)
                    uploaded = True
                    await update_detail_status(f"✅ {filename} wurde hochgeladen")
        except Exception as e:
            await update_detail_status(f"❌ Fehler beim Hochladen von {filename}: {e}")

    return moved, uploaded
