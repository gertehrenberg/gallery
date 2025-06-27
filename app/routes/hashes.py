import asyncio
import hashlib
import json
import os
from pathlib import Path
from typing import Dict, List
from typing import Optional

from app.config import Settings
from app.config_gdrive import sanitize_filename, folder_id_by_name, SettingsGdrive, collect_all_folders
from app.database import clear_folder_status_db_by_name, save_folder_status_to_db, clear_folder_status_db, \
    load_folder_status_from_db
from app.routes.auth import load_drive_service_token
from app.routes.dashboard_help import _prepare_folder, _process_image_files
from app.routes.gdrive_from_lokal import save_structured_hashes
from app.tools import readimages, save_pair_cache
from app.utils.progress import init_progress_state, progress_state, update_progress, update_progress_text, \
    save_simple_hashes, hold_progress, stop_progress, write_local_hashes_progress


def is_valid_image(filename: str) -> bool:
    """Prüft, ob eine Datei eine gültige Bilddatei ist."""
    return any(filename.lower().endswith(ext) for ext in Settings.IMAGE_EXTENSIONS)


async def delete_all_hashfiles_async(file_folder_dir: str, subfolders: bool = True) -> int:
    """Löscht alle Hash-Dateien in einem Verzeichnis und optional seinen Unterverzeichnissen."""
    await init_progress_state()
    await update_progress_text("🔄 Starte Löschvorgang der Hash-Dateien...")

    root = Path(file_folder_dir)
    all_dirs = [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]
    deleted = 0
    total_dirs = len(all_dirs)

    for idx, subdir in enumerate(all_dirs, 1):
        await update_progress(f"Verarbeite Verzeichnis {subdir.name}", int((idx / total_dirs) * 100))

        for file in subdir.glob(f"*{Settings.GDRIVE_HASH_FILE}"):
            try:
                file.unlink()
                await update_progress_text(f"🗑️ Gelöscht: {file}")
                deleted += 1
            except Exception as e:
                await update_progress_text(f"❌ Fehler beim Löschen von {file}: {e}")

    await update_progress_text(f"✅ Hash-Dateien gelöscht: {deleted}")
    await hold_progress()
    return deleted


async def update_all_local_hashes():
    """Aktualisiert die Hash-Dateien für alle Kategorien."""
    await init_progress_state()
    progress_state["running"] = True

    try:
        total_kategorien = len(Settings.kategorien)
        local_cache = {}

        for idx, kategorie in enumerate(Settings.kategorien, 1):
            folder_name = kategorie["key"]
            folder_path = os.path.join(Settings.IMAGE_FILE_CACHE_DIR, folder_name)

            await update_progress_text(f"📁 Verarbeite Kategorie: {folder_name}")
            await update_progress(f"Kategorie {folder_name}", int((idx / total_kategorien) * 100))

            local_cache.clear()
            await readimages(folder_path, local_cache)

            hash_file = Path(folder_path) / Settings.GALLERY_HASH_FILE
            local_hashes = {name: data.get('image_id', '') for name, data in local_cache.items()}

            try:
                await save_simple_hashes(local_hashes, hash_file)
                await update_progress_text(f"✅ {len(local_hashes)} Hashes gespeichert für {folder_name}")
            except Exception as e:
                await update_progress_text(f"❌ Fehler beim Speichern der Hashes für {folder_name}: {e}")

        await update_progress_text("✅ Hash-Aktualisierung abgeschlossen")

    except Exception as e:
        await update_progress_text(f"❌ Fehler bei der Hash-Aktualisierung: {e}")
    finally:
        await hold_progress()


async def update_all_gdrive_hashes(service) -> None:
    """Aktualisiert die Hashes für alle Google Drive Ordner."""
    await update_progress_text("🔄 Starte Google Drive Hash-Aktualisierung...")

    total_kategorien = len(Settings.kategorien)
    for idx, kategorie in enumerate(Settings.kategorien, 1):
        folder_name = kategorie["key"]
        folder_id = folder_id_by_name(folder_name)

        if not folder_id:
            await update_progress_text(f"⚠️ Keine Folder-ID gefunden für: {folder_name}")
            continue

        await update_progress(f"Verarbeite {folder_name}", int((idx / total_kategorien) * 100))
        await update_gdrive_hashes(service, folder_name)


async def update_gdrive_hashes(service, folder_name: Optional[str] = None):
    """Liest Dateien aus Google Drive und aktualisiert die hashes.json Dateien."""
    await update_progress_text(f"🔄 Aktualisiere GDrive Hashes{' für ' + folder_name if folder_name else ''}")
    await init_progress_state()
    progress_state["running"] = True

    try:
        base_dir = Path(Settings.IMAGE_FILE_CACHE_DIR)
        folders_to_process = [base_dir / folder_name] if folder_name else [p for p in base_dir.iterdir() if p.is_dir()]
        total_folders = len(folders_to_process)

        for folder_idx, folder_path in enumerate(sorted(folders_to_process), 1):
            current_folder = folder_path.name
            folder_id = folder_id_by_name(current_folder)

            if not folder_id:
                await update_progress_text(f"⚠️ Keine Folder-ID gefunden für: {current_folder}")
                continue

            await update_progress_text(f"📂 Verarbeite {current_folder} ({folder_idx}/{total_folders})")

            gdrive_hashes: Dict[str, Dict[str, str]] = {}
            page_token = None
            files_processed = 0

            while True:
                try:
                    results = service.files().list(
                        q=f"'{folder_id}' in parents and trashed=false",
                        spaces='drive',
                        fields="nextPageToken, files(id, name, md5Checksum)",
                        pageSize=Settings.PAGESIZE,
                        pageToken=page_token
                    ).execute()

                    for item in results.get('files', []):
                        name = sanitize_filename(item.get('name', ''))
                        if name and is_valid_image(name):
                            md5_checksum = item.get('md5Checksum')
                            file_id = item.get('id')
                            if md5_checksum and file_id:
                                gdrive_hashes[name] = {
                                    "md5": md5_checksum,
                                    "id": file_id
                                }
                                files_processed += 1

                        if files_processed % 100 == 0:
                            await update_progress(
                                f"⚡ Dateien in {current_folder}: {files_processed}",
                                int((files_processed / (files_processed + 1)) * 100)
                            )

                    page_token = results.get('nextPageToken')
                    if not page_token:
                        break

                except Exception as e:
                    await update_progress_text(f"❌ Fehler beim Lesen von {current_folder}: {e}")
                    break

            if not gdrive_hashes:
                gdrive_hashes = {}
            try:
                hash_file_path = folder_path / Settings.GDRIVE_HASH_FILE
                await save_structured_hashes(gdrive_hashes, hash_file_path)
                await update_progress_text(f"✅ {current_folder}: {len(gdrive_hashes)} Einträge gespeichert")
            except Exception as e:
                await update_progress_text(f"❌ Fehler beim Speichern für {current_folder}: {e}")

        await update_progress_text("✅ GDrive Hash-Aktualisierung abgeschlossen")

    except Exception as e:
        await update_progress_text(f"❌ Fehler bei Hash-Aktualisierung: {e}")
    finally:
        await hold_progress()


async def reloadcache_progress(service, folder_key: Optional[str] = None):
    """
    Reloads the cache for folders based on the folder_key parameter.

    Args:
        folder_key: Optional folder key.
                   If None, processes all categories.
                   If in CHECKBOX_CATEGORIES, processes only that folder.
                   If "textfiles", processes text files.
    """
    try:
        await init_progress_state()
        progress_state["running"] = True
        await update_progress_text(f"🔄 Starte reloadcache_progress für Ordner: {folder_key}")
        Settings.folders_loaded = 0

        if folder_key == "textfiles":
            await update_progress_text("🗃️ Modus: Textverarbeitung")
            await process_text_files()

        elif folder_key in Settings.CHECKBOX_CATEGORIES:
            await update_progress_text(f"📂 Modus: Einzelne Kategorie ({folder_key})")
            folder_name = next(
                (k["label"] for k in Settings.kategorien if k["key"] == folder_key),
                folder_key
            )
            await process_category(folder_key, folder_name)
            Settings.folders_loaded += 1

        else:
            await update_progress_text("📂 Modus: Alle Kategorien")
            pair_cache = Settings.CACHE.get("pair_cache")
            pair_cache.clear()

            for kategorie in Settings.kategorien:
                await process_category(kategorie["key"], kategorie["label"])
                Settings.folders_loaded += 1

            await update_progress_text("🗃️ Modus: Textverarbeitung")
            await process_text_files()

            await process_image_folders_gdrive_progress(service, folder_key)
            await update_progress_text(f"[✓] Hash-Datei aktualisiert für {folder_key}")

    except Exception as e:
        await update_progress_text(f"❌ Fehler beim Reload-Cache: {e}")
        raise
    finally:
        await update_progress_text("✅ reloadcache_progress abgeschlossen")
        await stop_progress()


async def process_text_files():
    """Processes text files in the text directory."""
    await update_progress_text("🧮 Schreibe lokale Hashes (Texte)")
    await write_local_hashes_progress(
        Settings.TEXT_EXTENSIONS,
        Settings.TEXT_FILE_CACHE_DIR,
        False
    )


async def process_category(folder_key: str, folder_name: str):
    """
    Processes a single category folder.

    Args:
        folder_key: The key of the folder to process
        folder_name: The display name of the folder
    """
    await update_progress_text(f"📂 Verarbeite Kategorie: {folder_key} ({folder_name})")

    pair_cache = Settings.CACHE.get("pair_cache")

    # Update progress
    await update_progress(f"{folder_name}: fillcache_local ...", 33)

    # Clear existing entries
    to_delete = [
        key for key, value in pair_cache.items()
        if value.get("folder", "") == folder_key
    ]

    await update_progress_text(f"🧹 Entferne {len(to_delete)} bestehende Einträge aus pair_cache für {folder_key}")
    for key in to_delete:
        del pair_cache[key]

    # Process images
    image_dir = f"{Settings.IMAGE_FILE_CACHE_DIR}/{folder_key}"
    await update_progress_text(f"📸 Lese Bilder aus {image_dir}")
    await readimages(image_dir, pair_cache)

    # Save cache
    save_pair_cache(pair_cache, Settings.PAIR_CACHE_PATH)
    await update_progress_text(f"💾 pair_cache gespeichert: {Settings.PAIR_CACHE_PATH}")

    await update_progress(f"{folder_name}: fillcache_local fertig", 100)
    await asyncio.sleep(1.0)

    # Update database
    await update_progress_text("🔄 Aktualisiere Elternpfade in DB")
    await fill_file_parents_cache_progress(Settings.DB_PATH, folder_key)

    # Write hashes
    await update_progress_text("🧮 Schreibe lokale Hashes (Bilder)")
    await write_local_hashes_progress(Settings.IMAGE_EXTENSIONS, image_dir, False)


async def process_image_folders_gdrive_progress(service, folder_name: str):
    folder_id = folder_id_by_name(folder_name)
    if not folder_id:
        raise ValueError(f"Keine Folder-ID gefunden für: {folder_name}")

    await update_progress_text(f"Lade Dateien aus {folder_name}...")
    files = []
    page_token = None

    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            spaces='drive',
            fields="nextPageToken, files(id, name, md5Checksum)",
            pageSize=Settings.PAGESIZE,
            pageToken=page_token
        ).execute()

        batch = [f for f in response.get('files', [])
                 if is_valid_image(f.get('name', ''))]
        files.extend(batch)
        await update_progress_text(f"Geladen: {len(files)} Dateien...")

        page_token = response.get('nextPageToken')
        if not page_token:
            break

    gdrive_hashes = {}
    for file in files:
        try:
            name = sanitize_filename(file['name'])
            if is_valid_image(name):
                md5_drive = file.get("md5Checksum")
                if md5_drive:
                    gdrive_hashes[name] = {
                        "md5": md5_drive,
                        "id": file['id']
                    }
        except Exception as e:
            await update_progress_text(f"[Fehler] {file['name']}: {e}")

    hashfile_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name / Settings.GDRIVE_HASH_FILE
    hashfile_path.parent.mkdir(parents=True, exist_ok=True)
    with hashfile_path.open("w", encoding="utf-8") as f:
        json.dump(gdrive_hashes, f, indent=2)
    await update_progress_text(f"Hash-Datei gespeichert für {folder_name}")


async def fill_file_parents_cache_progress(db_path: str, folder_key: str | None):
    if folder_key:

        file_parents_cache = Settings.CACHE["file_parents_cache"]
        if folder_key in file_parents_cache:
            del file_parents_cache[folder_key]

        folder_name = next((k["label"] for k in Settings.kategorien if k["key"] == folder_key), None)

        clear_folder_status_db_by_name(db_path, folder_key)

        await update_progress_text(
            "[fill_file_parents_cache_progress] 🚀 Keine Cache-Daten vorhanden, lade von lokal...")

        file_parents_cache[folder_key] = []
        folder_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_key
        if not _prepare_folder(folder_path):
            return
        image_files = list(folder_path.iterdir())
        await update_progress(f"{folder_name}: Kategorie: {folder_key} : {len(image_files)} Dateien", 0)
        await _process_image_files_progress(image_files, folder_key, file_parents_cache, db_path)
        Settings.folders_loaded += 1
        await update_progress_text(
            f"[fill_file_parents_cache_progress] ✅ {Settings.folders_loaded}/{Settings.folders_total} Ordner geladen: {folder_key}")
    else:
        file_parents_cache = Settings.CACHE["file_parents_cache"]
        file_parents_cache.clear()

        if await _load_file_parents_cache_from_db(db_path, file_parents_cache):
            return

        await update_progress_text(
            "[fillfill_file_parents_cache_progress] 🚀 Keine Cache-Daten vorhanden, lade von lokal...")
        clear_folder_status_db(db_path)

        for kat in Settings.kategorien:
            if folder_key and kat != folder_key:
                continue
            folder_key = kat["key"]
            file_parents_cache[folder_key] = []
            folder_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_key
            if not _prepare_folder(folder_path):
                continue
            await update_progress_text(f"[fill_file_parents_cache_progress] 📂 Lese Bilder aus: {folder_key}")
            image_files = list(folder_path.iterdir())
            await update_progress(f"Kategorie: {folder_key} : {len(image_files)} Dateien", 0)
            await _process_image_files_progress(image_files, folder_key, file_parents_cache, db_path)
            Settings.folders_loaded += 1
            await update_progress_text(
                f"[fill_file_parents_cache_progress] ✅ {Settings.folders_loaded}/{Settings.folders_total} Ordner geladen: {folder_key}")


async def _process_image_files_progress(image_files, folder_key, file_parents_cache, db_path):
    folder_name = label = next((k["label"] for k in Settings.kategorien if k["key"] == folder_key), None)
    total = len(image_files)
    for index, image_file in enumerate(image_files):
        await update_progress(f"Kategorie: {folder_name} : {total} Dateien ({image_file})",
                              int(index / total * 100), 0.02)
        if not image_file.is_file() or image_file.suffix.lower() not in Settings.IMAGE_EXTENSIONS:
            continue
        image_name = image_file.name.lower()
        pair = Settings.CACHE["pair_cache"].get(image_name)
        if not pair:
            await update_progress_text(
                f"[_process_image_files_progress] ⚠️ Kein Eintrag im pair_cache für: {image_name}")
            continue
        await update_progress_text(
            f"[_process_image_files_progress] ✅️ Eintrag im pair_cache für: {folder_key} / {image_name}")
        image_id = pair["image_id"]
        file_parents_cache[folder_key].append(image_id)

        save_folder_status_to_db(db_path, image_id, folder_key)


async def _load_file_parents_cache_from_db(db_path: str, file_parents_cache: dict) -> bool:
    rows = load_folder_status_from_db(db_path)
    if not rows:
        return False
    await update_progress_text("[fill_folder_cache] 📦 Lade file_parents_cache aus der Datenbank...")
    for image_id, folder_key in rows:
        if folder_key not in file_parents_cache:
            Settings.folders_loaded += 1
            file_parents_cache[folder_key] = []
            await update_progress_text(
                f"[fill_folder_cache] ✅ Cache aus DB geladen: {Settings.folders_loaded}/{Settings.folders_total} {folder_key}")
        file_parents_cache[folder_key].append(image_id)
    if Settings.folders_loaded != Settings.folders_total:
        Settings.folders_loaded = Settings.folders_total
        await update_progress_text(
            f"[fill_folder_cache] ✅ Cache aus DB geladen: {Settings.folders_loaded}/{Settings.folders_total}")
    return True


async def fill_file_parents_cache(db_path: str):
    file_parents_cache = Settings.CACHE["file_parents_cache"]
    file_parents_cache.clear()

    if await _load_file_parents_cache_from_db(db_path, file_parents_cache):
        return

    await update_progress_text("[fill_folder_cache] 🚀 Keine Cache-Daten vorhanden, lade von lokal...")
    clear_folder_status_db(db_path)

    for kat in Settings.kategorien:
        folder_name = kat["key"]
        file_parents_cache[folder_name] = []
        folder_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name
        if not _prepare_folder(folder_path):
            continue
        await update_progress_text(f"[fill_folder_cache] 📂 Lese Bilder aus: {folder_name}")
        image_files = list(folder_path.iterdir())
        _process_image_files(image_files, folder_name, file_parents_cache, db_path)
        Settings.folders_loaded += 1
        await update_progress_text(
            f"[fill_folder_cache] ✅ {Settings.folders_loaded}/{Settings.folders_total} Ordner geladen: {folder_name}")


async def verify_file_cache_consistency(base_dir, folder_name: Optional[str] = None) -> dict:
    """
    Überprüft die Konsistenz zwischen dem lokalen Cache und den tatsächlichen Dateien im Ordner.

    Args:
        folder_name: Optional - Spezifischer Ordner zum Überprüfen. Wenn None, werden alle Ordner überprüft.

    Returns:
        Dict mit Statistiken und Inkonsistenzen
    """
    await init_progress_state()
    await update_progress_text("🔍 Starte lokale Cache-Konsistenzprüfung...")

    result = {
        "statistics": {
            "total_files": 0,
            "cached_files": 0,
            "missing_in_cache": [],
            "missing_in_folder": [],
            "processed_folders": 0
        },
        "inconsistencies": {}
    }

    try:
        folders_to_check = ([base_dir / folder_name] if folder_name
                            else [p for p in base_dir.iterdir() if p.is_dir()])

        total_folders = len(folders_to_check)

        for folder_idx, folder_path in enumerate(folders_to_check, 1):
            current_folder = folder_path.name
            await update_progress_text(f"📁 Prüfe Ordner: {folder_path} ({folder_idx}/{total_folders})")

            # Sammle alle Dateien im Ordner
            folder_files = set()
            try:
                folder_files = {
                    f.name.lower() for f in folder_path.iterdir()
                    if f.is_file() and f.suffix.lower() in Settings.IMAGE_EXTENSIONS
                }
            except Exception as e:
                await update_progress_text(f"⚠️ Fehler beim Lesen von {current_folder}: {e}")
                continue

            # Sammle alle Dateien im lokalen Hash-Cache
            hash_cache_path = folder_path / Settings.GALLERY_HASH_FILE
            cached_files = set()

            try:
                if hash_cache_path.exists():
                    with hash_cache_path.open('r') as f:
                        cache_data = json.load(f)
                        cached_files = {name.lower() for name in cache_data.keys()}
            except Exception as e:
                await update_progress_text(f"⚠️ Fehler beim Lesen der Cache-Datei für {current_folder}: {e}")
                continue

            # Finde Unterschiede
            missing_in_cache = folder_files - cached_files
            missing_in_folder = cached_files - folder_files

            # Aktualisiere Statistiken
            result["statistics"]["total_files"] += len(folder_files)
            result["statistics"]["cached_files"] += len(cached_files)
            result["statistics"]["processed_folders"] += 1

            if missing_in_cache or missing_in_folder:
                result["inconsistencies"][current_folder] = {
                    "missing_in_cache": list(missing_in_cache),
                    "missing_in_folder": list(missing_in_folder)
                }

                if missing_in_cache:
                    await update_progress_text(
                        f"⚠️ {len(missing_in_cache)} Dateien in {current_folder} nicht im Cache"
                    )
                if missing_in_folder:
                    await update_progress_text(
                        f"⚠️ {len(missing_in_folder)} Cache-Einträge in {current_folder} nicht im Ordner"
                    )

            result["statistics"]["missing_in_cache"].extend(
                f"{current_folder}/{f}" for f in missing_in_cache
            )
            result["statistics"]["missing_in_folder"].extend(
                f"{current_folder}/{f}" for f in missing_in_folder
            )

            # Update progress
            await update_progress(
                f"Ordner verarbeitet: {folder_idx}/{total_folders}",
                int((folder_idx / total_folders) * 100)
            )

        # Abschlussbericht
        total_inconsistencies = len(result["statistics"]["missing_in_cache"]) + \
                                len(result["statistics"]["missing_in_folder"])

        if total_inconsistencies == 0:
            await update_progress_text("✅ Lokaler Cache ist vollständig konsistent!")
        else:
            await update_progress_text(
                f"⚠️ {total_inconsistencies} Inkonsistenzen gefunden:\n"
                f"   • {len(result['statistics']['missing_in_cache'])} Dateien nicht im Cache\n"
                f"   • {len(result['statistics']['missing_in_folder'])} Cache-Einträge nicht im Ordner"
            )

    except Exception as e:
        await update_progress_text(f"❌ Fehler bei der Cache-Überprüfung: {e}")
    finally:
        await stop_progress()

    return result


async def delete_duplicates_in_gdrive_folder(service, folder_id: str) -> None:
    """
    Löscht doppelte Dateien (basierend auf MD5 Hash) in einem Google Drive Ordner.

    Args:
        service: Google Drive Service Objekt
        folder_id: ID des zu prüfenden Ordners
    """
    try:
        await update_progress_text("🔍 Suche nach Dateien im Ordner...")

        # Alle Dateien im Ordner abrufen
        files = []
        page_token = None
        while True:
            response = service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                spaces='drive',
                fields='nextPageToken, files(id, name, md5Checksum)',
                pageSize=Settings.PAGESIZE,
                pageToken=page_token
            ).execute()

            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken')
            if not page_token:
                break

        # MD5 Hashes gruppieren
        md5_groups = {}
        for file in files:
            md5 = file.get('md5Checksum')
            if md5:
                if md5 not in md5_groups:
                    md5_groups[md5] = []
                md5_groups[md5].append(file)

        # Duplikate finden und löschen
        deleted_count = 0
        for md5, file_group in md5_groups.items():
            if len(file_group) > 1:
                # Behalte die erste Datei, lösche den Rest
                original = file_group[0]
                duplicates = file_group[1:]

                await update_progress_text(f"🔍 Gefunden: {original['name']} hat {len(duplicates)} Duplikate")

                for dup in duplicates:
                    try:
                        service.files().delete(fileId=dup['id']).execute()
                        deleted_count += 1
                        await update_progress_text(f"🗑️ Gelöscht: {dup['name']}")
                    except Exception as e:
                        await update_progress_text(f"❌ Fehler beim Löschen von {dup['name']}: {str(e)}")

        await update_progress_text(f"✅ Abgeschlossen: {deleted_count} Duplikate gelöscht")

    except Exception as e:
        await update_progress_text(f"❌ Fehler: {str(e)}")


async def move_duplicates_in_folder(folder_path: str) -> None:
    """
    Verschiebt doppelte Bilddateien in einem lokalen Ordner und seinen Unterordnern
    in den TEMP_DIR_PATH Ordner.
    """
    try:
        base_folder = Path(folder_path)
        folder_name = base_folder.name
        await update_progress_text(f"[{folder_name}] 🔍 Initialisiere...")

        # Temp-Ordner vorbereiten
        Settings.TEMP_DIR_PATH.mkdir(parents=True, exist_ok=True)

        # Sammle alle Unterordner und Dateien vorab für bessere Fortschrittsanzeige
        folders: Dict[str, Path] = {folder_name: base_folder}
        total_files = 0
        files_to_process = []

        await update_progress_text(f"[{folder_name}] 📂 Sammle Dateien...", ctime=0.01)

        # Sammle erst alle Ordner und zähle Dateien
        for subdir in base_folder.rglob("*"):
            if subdir.is_dir():
                folders[subdir.name] = subdir
                for file_path in subdir.iterdir():
                    if file_path.is_file() and any(
                            file_path.name.lower().endswith(ext) for ext in Settings.IMAGE_EXTENSIONS):
                        total_files += 1
                        files_to_process.append(file_path)

        # Hauptordner auch durchsuchen
        for file_path in base_folder.iterdir():
            if file_path.is_file() and any(file_path.name.lower().endswith(ext) for ext in Settings.IMAGE_EXTENSIONS):
                total_files += 1
                files_to_process.append(file_path)

        await update_progress_text(f"[{folder_name}] 📂 {len(folders)} Ordner, {total_files} Dateien gefunden")

        # Verarbeite Dateien
        files_by_md5: Dict[str, List[Path]] = {}
        processed_files = 0
        last_progress = 0

        for file_path in files_to_process:
            try:
                # Dateinamen überprüfen und ggf. umbenennen
                original_name = file_path.name
                sanitized_name = sanitize_filename(original_name)

                if original_name != sanitized_name:
                    try:
                        new_path = file_path.parent / sanitized_name
                        file_path.rename(new_path)
                        file_path = new_path
                    except Exception as e:
                        await update_progress_text(
                            f"[{folder_name}] ❌ Fehler beim Umbenennen von {original_name}: {str(e)}", ctime=0.01)
                        continue

                # MD5 Hash berechnen
                with open(file_path, 'rb') as f:
                    md5 = hashlib.md5(f.read()).hexdigest()
                    if md5 not in files_by_md5:
                        files_by_md5[md5] = []
                    files_by_md5[md5].append(file_path)

                processed_files += 1
                current_progress = int((processed_files / total_files) * 100)

                # Update nur bei signifikanter Änderung
                if current_progress - last_progress >= 5:
                    await update_progress_text(
                        f"[{folder_name}] 🔍 Analysiere Dateien: {processed_files}/{total_files} ({current_progress}%)",
                        ctime=0.01)
                    last_progress = current_progress

            except Exception as e:
                await update_progress_text(
                    f"[{folder_name}] ❌ Fehler bei {file_path.name}: {str(e)}", ctime=0.01)

        # Duplikate in temp Ordner verschieben
        duplicates_found = sum(len(files) - 1 for files in files_by_md5.values() if len(files) > 1)
        await update_progress_text(f"[{folder_name}] 🎯 {duplicates_found} Duplikate gefunden")

        moved_count = 0
        for md5, file_list in files_by_md5.items():
            if len(file_list) > 1:
                original = file_list[0]
                duplicates = file_list[1:]

                for dup in duplicates:
                    try:
                        target_path = Settings.TEMP_DIR_PATH / dup.name
                        if target_path.exists():
                            base_name = target_path.stem
                            suffix = target_path.suffix
                            counter = 1
                            while target_path.exists():
                                target_path = Settings.TEMP_DIR_PATH / f"{base_name}_{counter}{suffix}"
                                counter += 1

                        dup.rename(target_path)
                        moved_count += 1

                        # Update nur bei jedem 5. Move oder bei Vielfachen von 10
                        if moved_count % 5 == 0 or moved_count % 10 == 0:
                            await update_progress_text(
                                f"[{folder_name}] 📦 {moved_count}/{duplicates_found} Duplikate verschoben",
                                ctime=0.01)
                    except Exception as e:
                        await update_progress_text(
                            f"[{folder_name}] ❌ Fehler beim Verschieben von {dup.name}: {str(e)}",
                            ctime=0.01)

        await update_progress_text(
            f"[{folder_name}] ✅ Abgeschlossen: {moved_count} von {duplicates_found} Duplikaten verschoben")

    except Exception as e:
        await update_progress_text(f"[{folder_name}] ❌ Fehler: {str(e)}")


async def move_duplicates_in_gdrive_folder(service, folder_id: str) -> None:
    """
    Verschiebt doppelte Bilddateien in einem Google Drive Ordner und seinen Unterordnern
    in einen 'temp' Ordner.
    """
    try:
        folder_name = service.files().get(fileId=folder_id, fields="name").execute().get("name", "Unbekannt")
        await update_progress_text(f"[{folder_name}] 🔍 Initialisiere Suche nach Duplikaten...")

        # Hole temp Ordner ID
        temp_folder_id = folder_id_by_name("temp")
        await update_progress_text(f"[{folder_name}] 📁 Temp Ordner ID: {temp_folder_id}")

        # Sammle erst alle Ordner
        name_to_id = {}
        id_to_name = {}
        collect_all_folders(service, folder_id, name_to_id, id_to_name)

        # Alle Ordner-IDs sammeln (inkl. Ursprungsordner)
        folder_ids = list(id_to_name.keys())
        folder_ids.append(folder_id)

        files = []
        for current_folder_id in folder_ids:
            current_folder_name = id_to_name.get(current_folder_id, "Ursprungsordner")
            try:
                await update_progress_text(f"[{folder_name}/{current_folder_name}] 📂 Verarbeite Ordner...")

                # Query für aktuellen Ordner
                query = f"'{current_folder_id}' in parents and trashed=false"

                # Dateien für aktuellen Ordner abrufen
                page_token = None
                while True:
                    response = service.files().list(
                        q=query,
                        spaces='drive',
                        fields='nextPageToken, files(id, name, md5Checksum, parents)',
                        pageSize=Settings.PAGESIZE,
                        pageToken=page_token
                    ).execute()

                    batch = response.get('files', [])
                    files.extend(batch)
                    await update_progress_text(
                        f"[{folder_name}/{current_folder_name}] 📂 {len(files)} Dateien gefunden...")

                    page_token = response.get('nextPageToken')
                    if not page_token:
                        break
            except Exception as e:
                await update_progress_text(f"[{folder_name}/{current_folder_name}] ❌ Fehler beim Verarbeiten: {str(e)}")

        # Zuerst alle Dateinamen überprüfen und ggf. umbenennen
        for file in files:
            if not any(file.get('name', '').lower().endswith(ext) for ext in Settings.IMAGE_EXTENSIONS):
                continue

            original_name = file.get('name', '')
            sanitized_name = sanitize_filename(original_name)

            if original_name != sanitized_name:
                try:
                    service.files().update(
                        fileId=file['id'],
                        body={'name': sanitized_name}
                    ).execute()
                    await update_progress_text(f"[{folder_name}] ✏️ Umbenannt: {original_name} → {sanitized_name}")
                    file['name'] = sanitized_name
                except Exception as e:
                    await update_progress_text(
                        f"[{folder_name}] ❌ Fehler beim Umbenennen von {original_name}: {str(e)}")

        # Duplikate nach MD5 gruppieren
        md5_groups = {}
        for file in files:
            if not any(file.get('name', '').lower().endswith(ext) for ext in Settings.IMAGE_EXTENSIONS):
                continue

            md5 = file.get('md5Checksum')
            if md5:
                if md5 not in md5_groups:
                    md5_groups[md5] = []
                md5_groups[md5].append(file)

        # Duplikate in temp Ordner verschieben
        moved_count = 0
        for md5, file_group in md5_groups.items():
            if len(file_group) > 1:
                original = file_group[0]
                duplicates = file_group[1:]

                for dup in duplicates:
                    try:
                        service.files().update(
                            fileId=dup['id'],
                            addParents=temp_folder_id,
                            removeParents=dup['parents'][0]
                        ).execute()
                        moved_count += 1
                        await update_progress_text(f"[{folder_name}] 📦 Verschoben: {dup['name']}")
                    except Exception as e:
                        await update_progress_text(
                            f"[{folder_name}] ❌ Fehler beim Verschieben von {dup['name']}: {str(e)}")
                        continue

        await update_progress_text(
            f"[{folder_name}] ✅ Abgeschlossen: {moved_count} Duplikate in temp Ordner verschoben")

    except Exception as e:
        await update_progress_text(f"[{folder_name}] ❌ Fehler: {str(e)}")


def p5():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    for idx, kat in enumerate(Settings.kategorien, 1):
        folder_name = kat["key"]
        result = asyncio.run(verify_file_cache_consistency(Path(Settings.IMAGE_FILE_CACHE_DIR), folder_name))
        if len(result['inconsistencies']) > 0:
            # Die Ergebnisse auswerten
            print(f"Geprüfte Dateien: {result['statistics']['total_files']}")
            print(f"Cache-Einträge: {result['statistics']['cached_files']}")
            print(f"Inkonsistente Ordner: {len(result['inconsistencies'])}")

    result = asyncio.run(verify_file_cache_consistency(Path(Settings.TEXT_FILE_CACHE_DIR).parent, "textfiles"))
    if len(result['inconsistencies']) > 0:
        # Die Ergebnisse auswerten
        print(f"Geprüfte Dateien: {result['statistics']['total_files']}")
        print(f"Cache-Einträge: {result['statistics']['cached_files']}")
        print(f"Inkonsistente Ordner: {len(result['inconsistencies'])}")


def p6():
    Settings.DB_PATH = '../../gallery_local.db'
    Settings.TEMP_DIR_PATH = Path("../../cache/temp")
    Settings.IMAGE_FILE_CACHE_DIR = "../../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../../cache/textfiles"
    Settings.PAIR_CACHE_PATH = "../../cache/pair_cache_local.json"
    SettingsGdrive.GDRIVE_FOLDERS_PKL = Path("../../cache/gdrive_folders.pkl")

    service = load_drive_service_token(os.path.abspath(os.path.join("../../secrets", "token.json")))

    asyncio.run(move_duplicates_in_gdrive_folder(service, folder_id_by_name("imagefiles")))
    asyncio.run(update_all_gdrive_hashes(service))

    asyncio.run(move_duplicates_in_folder(Settings.IMAGE_FILE_CACHE_DIR))
    asyncio.run(update_all_local_hashes())


if __name__ == "__main__":
    p6()
