import json
from pathlib import Path
from typing import Optional
from typing import Set, Dict, Any, Tuple, List

from app.config import Settings
from app.config_gdrive import folder_id_by_name
from app.routes.hashes import upload_file_to_gdrive, update_gdrive_hashes, delete_duplicates_in_gdrive_folder
from app.utils.progress import (
    init_progress_state,
    progress_state,
    update_progress,
    update_progress_text, update_progress_auto, stop_progress,
)
from app.utils.progress_detail import stop_detail_progress, \
    start_detail_progress
from app.utils.progress_detail import update_detail_status, update_detail_progress, calc_detail_progress


async def gdrive_textfiles_files_by_local(service, folder_name: str) -> None:
    """
    Synchronisiert Textdateien zwischen lokalem Speicher und Google Drive.

    Args:
        service: Google Drive Service-Objekt
        folder_name: Name des zu synchronisierenden Ordners
    """
    await update_progress_text(f"🔄 Starte GDrive Synchronisation für Ordner: {folder_name}")
    await init_progress_state()

    try:
        # Lade lokale Textdateien
        cache_dir = Path(Settings.TEXT_FILE_CACHE_DIR)
        text_files = [
            f for f in cache_dir.iterdir()
            if f.is_file() and f.suffix.lower() in Settings.TEXT_EXTENSIONS
        ]

        if not text_files:
            await stop_detail_progress("⚠️ Keine Textdateien gefunden")
            return

        # Lade GDrive Hashes
        all_gdrive_hashes = await load_gdrive_hashes(cache_dir, folder_name)

        # Verarbeite Dateien
        moved, uploaded = await process_files(
            service=service,
            text_files=text_files,
            all_gdrive_hashes=all_gdrive_hashes,
            folder_name=folder_name,
            cache_dir=cache_dir
        )

        # Update GDrive Hashes wenn nötig
        if moved > 0 or uploaded > 0:
            await update_gdrive_hashes_after_changes(
                service,
                folder_name,
                Path(Settings.TEXT_FILE_CACHE_DIR),
                Settings.TEXT_EXTENSIONS)

        await update_progress_text(
            f"✅ Synchronisation abgeschlossen. {moved} Dateien verschoben, {uploaded} Dateien hochgeladen"
        )

        # Prüfe GDrive Dateien
        moved_gdrive = await check_and_move_gdrive_files(service, folder_name, cache_dir, Settings.TEXT_EXTENSIONS)
        if moved_gdrive > 0:
            await update_progress_text(
                f"✅ Zusätzlich wurden {moved_gdrive} GDrive Dateien in ihre korrekten Ordner verschoben"
            )

    except Exception as e:
        error_msg = f"❌ Fehler bei der Synchronisation: {str(e)}"
        await update_progress_text(error_msg)
        await update_detail_status(error_msg)
    finally:
        await stop_progress()


async def load_gdrive_hashes(cache_dir: Path, folder_name: str) -> Dict[str, Any]:
    """
    Lädt die Google Drive Hashes aus der Hash-Datei.

    Args:
        cache_dir: Pfad zum Cache-Verzeichnis
        folder_name: Name des Ordners

    Returns:
        Dict mit den geladenen Hashes
    """
    all_gdrive_hashes: Dict[str, Any] = {}
    gdrive_hash_file = cache_dir / Settings.GDRIVE_HASH_FILE

    try:
        if gdrive_hash_file.exists():
            with gdrive_hash_file.open("r", encoding="utf-8") as f:
                folder_hashes = json.load(f)
                for filename, entry in folder_hashes.items():
                    if isinstance(entry, dict):
                        entry_with_folder = entry.copy()
                        entry_with_folder['source_folder'] = folder_name
                        all_gdrive_hashes[filename] = entry_with_folder
    except Exception as e:
        await update_progress_text(f"⚠️ Fehler beim Laden der GDrive Hashes: {e}")

    return all_gdrive_hashes


async def process_files(
        service,
        text_files: List[Path],
        all_gdrive_hashes: Dict[str, Any],
        folder_name: str,
        cache_dir: Path
) -> Tuple[int, int]:
    """Verarbeitet die Textdateien und gibt die Anzahl der verschobenen und hochgeladenen Dateien zurück."""
    moved = uploaded = 0
    total_files = len(text_files)
    gdrive_folder_names: Set[str] = set()

    await start_detail_progress(f"🔍 Gefunden: {total_files} Textdateien")

    for idx, file in enumerate(text_files):
        progress = calc_detail_progress(idx, total_files)
        was_moved, was_uploaded = await process_text_file(
            service=service,
            filename=file.name,
            all_gdrive_hashes=all_gdrive_hashes,
            folder_name=folder_name,
            folder_path=cache_dir,
            gdrive_folder_names=gdrive_folder_names
        )

        moved += int(was_moved)
        uploaded += int(was_uploaded)

        await update_detail_progress(f"🔄 Verarbeite Dateien ({idx + 1}/{total_files})", progress)

    await stop_detail_progress(f"✅ {total_files} Einträge gespeichert")
    return moved, uploaded


async def process_text_file(
        service,
        filename: str,
        all_gdrive_hashes: dict,
        folder_name: str,
        folder_path: Path,
        gdrive_folder_names: Set[str]
) -> Tuple[bool, bool]:
    file_path = folder_path / filename

    # Validiere Datei
    if not file_path.is_file() or file_path.suffix.lower() not in Settings.TEXT_EXTENSIONS:
        await update_detail_status(f"⚠️ Ungültige Textdatei: {filename}")
        return False, False

    # Suche nach der Datei in allen GDrive Hashes
    gdrive_entry = all_gdrive_hashes.get(filename)
    if gdrive_entry:
        return await handle_existing_file(
            service,
            filename,
            gdrive_entry,
            folder_name,
            gdrive_folder_names
        )
    else:
        return await handle_new_file(
            service,
            "text/plain",
            filename,
            file_path,
            folder_name
        )


async def handle_existing_file(
        service,
        filename: str,
        gdrive_entry: Dict[str, Any],
        folder_name: str,
        gdrive_folder_names: Set[str]
) -> Tuple[bool, bool]:
    """Behandelt eine bereits in Google Drive existierende Datei."""
    if gdrive_entry.get('source_folder') != folder_name:
        return await move_file(
            service, filename, gdrive_entry, folder_name, gdrive_folder_names
        )
    return False, False


async def handle_new_file(
        service,
        mimetype,
        filename: str,
        file_path: Path,
        folder_name: str
) -> Tuple[bool, bool]:
    """Behandelt eine neue Datei, die noch nicht in Google Drive existiert."""
    if not file_path.exists():
        return False, False

    target_folder_id = folder_id_by_name(folder_name)
    if not target_folder_id:
        await update_detail_status(f"❌ Zielordner {folder_name} nicht gefunden")
        return False, False

    try:
        await update_detail_status(f"⬆️ Lade {filename} nach {folder_name} hoch")
        uploaded = await upload_file_to_gdrive(service, mimetype, file_path, target_folder_id)

        if uploaded:
            await update_detail_status(f"✅ {filename} wurde hochgeladen")
            return False, True
        else:
            await update_detail_status(f"⚠️ Hochladen von {filename} fehlgeschlagen")
            return False, False

    except Exception as e:
        await update_detail_status(f"❌ Fehler beim Hochladen von {filename}: {str(e)}")
        return False, False


async def move_file(
        service,
        filename: str,
        gdrive_entry: Dict[str, Any],
        folder_name: str,
        gdrive_folder_names: Set[str]
) -> Tuple[bool, bool]:
    """Verschiebt eine Datei in Google Drive."""
    try:
        target_folder_id = folder_id_by_name(folder_name)
        if not target_folder_id:
            await update_detail_status(f"❌ Zielordner {folder_name} nicht gefunden")
            return False, False

        await update_detail_status(f"📦 Verschiebe {filename} nach {folder_name}")
        moved = move_file_to_folder(
            service,
            gdrive_entry['id'],
            target_folder_id,
            gdrive_folder_names
        )

        if moved:
            await update_detail_status(f"✅ {filename} wurde verschoben")
            gdrive_folder_names.add(folder_name)
            return True, False
        else:
            await update_detail_status(f"⚠️ Verschieben von {filename} fehlgeschlagen")
            return False, False

    except Exception as e:
        await update_detail_status(f"❌ Fehler beim Verschieben von {filename}: {str(e)}")
        return False, False


async def update_gdrive_hashes_after_changes(service, folder_name: str, base_dir: Path,
                                             extension: Tuple[str, ...]) -> None:
    await update_progress_auto(f"🔄 Aktualisiere GDrive Hashes für {folder_name}...")
    await delete_duplicates_in_gdrive_folder(service, folder_name)
    await update_gdrive_hashes(service, folder_name, extension, base_dir)


async def check_and_move_gdrive_files(
        service,
        folder_name: str,
        cache_dir: Path,
        extension
) -> int:
    try:
        gdrive_hashes = load_folder_hashes(folder_name, cache_dir)
        if not gdrive_hashes:
            return 0

        return await process_gdrive_files(service, folder_name, gdrive_hashes, cache_dir, extension)

    except Exception as e:
        error_msg = f"ℹ️ Keine GDrive Hashes für {folder_name}: {e}"
        await update_progress_text(error_msg)
        await update_detail_status(f"❌ {error_msg}")
        return 0


def load_folder_hashes(folder_name: str, cache_dir: Path) -> Dict[str, Any]:
    """Lädt die Hash-Datei für einen bestimmten Ordner."""
    gdrive_hash_file = cache_dir / folder_name / Settings.GDRIVE_HASH_FILE
    if not gdrive_hash_file.exists():
        gdrive_hash_file = cache_dir.parent / Settings.GDRIVE_HASH_FILE

    try:
        with gdrive_hash_file.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


async def process_gdrive_files(
        service,
        folder_name: str,
        all_gdrive_hashes: Dict[str, Any],
        cache_dir: Path,
        extension
) -> int:
    """Verarbeitet die Google Drive Dateien und verschiebt sie bei Bedarf."""
    moved = 0
    total_files = len(all_gdrive_hashes)
    processed = 0

    await update_progress_text(
        f"🔄 Prüfe {total_files} GDrive Dateien auf korrekte Ordnerzuordnung..."
    )
    await start_detail_progress("Starte Überprüfung...")

    for filename, gdrive_entry in all_gdrive_hashes.items():
        progress = calc_detail_progress(processed, total_files)
        if isinstance(gdrive_entry, dict):
            moved += await process_gdrive_file(
                service, filename, gdrive_entry, folder_name, cache_dir
            )

        processed += 1
        await update_progress("🔄 Prüfe GDrive Dateien", progress)
        await update_detail_progress(detail_progress=progress)

    if moved > 0:
        await update_gdrive_hashes_after_changes(service, folder_name, cache_dir, extension)

    await update_progress_text(
        f"✅ GDrive Dateiüberprüfung abgeschlossen. {moved} Dateien verschoben"
    )
    await stop_detail_progress("✅ Fertig")

    return moved


async def process_gdrive_file(
        service,
        filename: str,
        gdrive_entry: Dict[str, Any],
        folder_name: str,
        cache_dir: Path
) -> int:
    try:
        gdrive_md5 = gdrive_entry.get('md5')
        if not gdrive_md5:
            return 0

        await update_detail_status(f"🔍 Suche nach {filename} in lokalen Ordnern")
        found_in_folder = await find_file_in_local_folders(
            filename, gdrive_md5, cache_dir
        )

        if found_in_folder and found_in_folder != folder_name:
            return await move_gdrive_file(
                service, filename, gdrive_entry, found_in_folder
            )

        return 0

    except Exception as e:
        await update_detail_status(f"❌ Fehler bei der Verarbeitung von {filename}: {str(e)}")
        return 0


async def find_file_in_local_folders(filename: str, gdrive_md5: str, cache_dir: Path) -> Optional[str]:
    await update_detail_status(f"🔍 Suche nach {filename}")

    # Zähle Kategorien für Fortschrittsberechnung
    total_categories = len(Settings.kategorien())

    # Durchsuche jeden Kategorie-Ordner
    for idx, kategorie in enumerate(Settings.kategorien()):
        folder_name = kategorie["key"]
        hash_file = cache_dir / folder_name / Settings.GALLERY_HASH_FILE

        progress = calc_detail_progress(idx, total_categories)
        await update_detail_progress(f"🔍 Durchsuche Ordner {folder_name}", progress)

        try:
            if not hash_file.exists():
                continue

            # Lade die Hash-Datei des Ordners
            with hash_file.open("r", encoding="utf-8") as f:
                folder_hashes = json.load(f)

            # Suche nach übereinstimmendem MD5
            for local_name, local_md5 in folder_hashes.items():
                if local_md5 == gdrive_md5:
                    await update_detail_status(f"✅ Gefunden: {filename} in {folder_name}")
                    return folder_name

        except Exception as e:
            await update_detail_status(f"⚠️ Fehler in {folder_name}: {e}")
            continue

    await update_detail_status(f"❌ Nicht gefunden: {filename}")
    return None


async def move_gdrive_file(
        service,
        filename: str,
        gdrive_entry: Dict[str, Any],
        target_folder: str
) -> int:
    try:
        # Hole die File-ID
        file_id = gdrive_entry.get('id')
        if not file_id:
            await update_detail_status(f"⚠️ Keine File-ID für {filename}")
            return 0

        # Hole die Zielordner-ID
        target_folder_id = folder_id_by_name(target_folder)
        if not target_folder_id:
            await update_detail_status(f"⚠️ Zielordner {target_folder} nicht gefunden")
            return 0

        # Hole aktuelle Eltern-Ordner
        file_metadata = service.files().get(
            fileId=file_id,
            fields='parents'
        ).execute()
        previous_parents = ','.join(file_metadata.get('parents', []))

        # Verschiebe die Datei
        await update_detail_status(f"📦 Verschiebe {filename} nach {target_folder}")

        # Update the file's parent folder
        service.files().update(
            fileId=file_id,
            addParents=target_folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()

        await update_detail_status(f"✅ {filename} wurde erfolgreich verschoben")
        return 1

    except Exception as e:
        await update_detail_status(f"❌ Fehler beim Verschieben von {filename}: {str(e)}")
        return 0


def move_file_to_folder(
        service, file_id: str,
        target_folder_id: str,
        gdrive_folder_names: Set[str] | None) -> bool:
    try:
        # Get current parents
        file = service.files().get(fileId=file_id, fields="parents").execute()
        previous_parents = ",".join(file.get("parents", []))

        # Move file
        result = service.files().update(
            fileId=file_id,
            addParents=target_folder_id,
            removeParents=previous_parents,
            fields="id, parents"
        ).execute()

        # Check if the target folder is in new parents
        new_parents = result.get('parents', [])
        gdrive_folder_names.add(previous_parents)
        return target_folder_id in new_parents

    except Exception:
        return False
