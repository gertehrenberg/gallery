import shutil
from pathlib import Path

from tqdm import tqdm

from config import IMAGE_EXTENSIONS, IMAGE_FILE_CACHE_DIR
from config import TEXT_FILE_CACHE_DIR
from config import compare_hashfile_counts, sanitize_filename


def import_local_files():
    source = Path("/mnt/d/e-save/Bilder_mit_Text")
    if not source.exists():
        print(f"[Fehler] Quellverzeichnis nicht gefunden: {source}")
        return

    txt_target = Path(TEXT_FILE_CACHE_DIR)
    img_target = Path(IMAGE_FILE_CACHE_DIR) / "real"
    txt_target.mkdir(parents=True, exist_ok=True)
    img_target.mkdir(parents=True, exist_ok=True)

    files = list(source.rglob("*"))
    with tqdm(total=len(files), desc="Importiere lokale Dateien", unit="Datei") as bar:
        for file in files:
            if not file.is_file():
                bar.update(1)
                continue
            clean_name = sanitize_filename(file.name)
            try:
                if file.suffix.lower() == ".txt":
                    shutil.copy(file, txt_target / clean_name)
                elif file.suffix.lower() in IMAGE_EXTENSIONS:
                    shutil.copy(file, img_target / clean_name)
            except Exception as e:
                print(f"[Fehler] {file}: {e}")
            bar.update(1)

    print(f"[✓] Import abgeschlossen: {source}")


if __name__ == "__main__":
    import_local_files()
    compare_hashfile_counts(IMAGE_FILE_CACHE_DIR)
