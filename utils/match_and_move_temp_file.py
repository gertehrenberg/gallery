import json
import shutil
from pathlib import Path

from tqdm import tqdm

from config import IMAGE_EXTENSIONS, IMAGE_FILE_CACHE_DIR, TEMP_FILE_DIR
from config import calculate_md5, compare_hashfile_counts


def match_and_move_temp_files(temp_file_dir: Path, image_cache_dir: Path):
    temp_file_dir = Path(temp_file_dir)
    image_cache_dir = Path(image_cache_dir)

    temp_files = [f for f in temp_file_dir.iterdir() if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS]
    moved = 0
    total = len(temp_files)

    with tqdm(total=total, desc="Vergleiche & verschiebe Temp-Dateien", unit="Datei") as bar:
        for temp_file in temp_files:
            temp_hash = calculate_md5(temp_file)
            found = False

            for hashfile in image_cache_dir.rglob("hashes.json"):
                try:
                    with hashfile.open("r", encoding="utf-8") as f:
                        hashes = json.load(f)
                    folder = hashfile.parent
                    if temp_file.name in hashes and hashes[temp_file.name] == temp_hash:
                        shutil.move(str(temp_file), str(folder / temp_file.name))
                        moved += 1
                        tqdm.write(f"[✓] {temp_file.name} → {folder.name}")
                        found = True
                        break
                except Exception as e:
                    tqdm.write(f"[Fehler beim Lesen von {hashfile}]: {e}")

            if not found:
                tqdm.write(f"[!] Kein Zielordner für {temp_file.name} gefunden")

            bar.update(1)

    print(f"[✓] {moved} von {total} Dateien wurden zugeordnet und verschoben.")


if __name__ == "__main__":
    match_and_move_temp_files(Path(TEMP_FILE_DIR), Path(IMAGE_FILE_CACHE_DIR))
    compare_hashfile_counts(IMAGE_FILE_CACHE_DIR)
