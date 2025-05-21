import json
import shutil
from pathlib import Path

from tqdm import tqdm

from config import IMAGE_FILE_CACHE_DIR, TEXT_FILE_CACHE_DIR
from config import sanitize_filename, IMAGE_EXTENSIONS, TEMP_FILE_DIR

def find_missing_text_files(image_cache_dir: Path, text_dir: Path):
    missing = []
    hashfiles = list(image_cache_dir.rglob("gallery202505_hashes.json"))

    with tqdm(total=len(hashfiles), desc="Prüfe auf fehlende .txt-Dateien", unit="Ordner") as bar:
        for hashfile in hashfiles:
            folder = hashfile.parent
            try:
                with hashfile.open("r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"[Fehler] {hashfile}: {e}")
                bar.update(1)
                continue

            for name in data:
                txt_name = sanitize_filename(name) + ".txt"
                txt_path = text_dir / txt_name
                if not txt_path.exists():
                    missing.append((name, folder / name))

            bar.update(1)

    recheck_dir = Path(IMAGE_FILE_CACHE_DIR) / "recheck"
    recheck_dir.mkdir(parents=True, exist_ok=True)

    for name, img_path in missing:
        if img_path.exists():
            target_path = recheck_dir / img_path.name
            if not target_path.exists():  # Nur verschieben, wenn Zieldatei nicht bereits existiert
                try:
                    shutil.move(str(img_path), str(target_path))
                    print(f"[→] Verschoben: {img_path} → {target_path}")
                except Exception as e:
                    print(f"[Fehler beim Verschieben] {img_path}: {e}")
            else:
                print(f"[Übersprungen] Zieldatei existiert bereits: {target_path}")

    # Zusatzfunktion: sehr kleine .txt-Dateien erkennen und Bild verschieben
    txt_files = list(text_dir.glob("*.txt"))
    sizes = sorted([f.stat().st_size for f in txt_files])
    if not sizes:
        return
    threshold_index = int(len(sizes) * 0.20)
    threshold = sizes[threshold_index]
    threshold = min(threshold, 100)  # Maximal 100 Bytes Schwelle

    temp_dir = Path(TEMP_FILE_DIR)
    temp_dir.mkdir(parents=True, exist_ok=True)

    for txt_file in txt_files:
        size = txt_file.stat().st_size
        original_name = txt_file.name[:-4]  # ".txt" entfernen
        sanitized_name = sanitize_filename(original_name)
        possible_images = list(Path(IMAGE_FILE_CACHE_DIR).glob(f"**/{sanitized_name}"))
        has_image = any(img_file.suffix.lower() in IMAGE_EXTENSIONS for img_file in possible_images)

        if size < threshold or not has_image:
            if not has_image:
                print(f"[→] Keine Bilddatei gefunden zu: {txt_file.name}, wird verschoben")
            else:
                print(f"[→] .txt-Datei zu klein: {txt_file.name}, wird verschoben")
            for img_file in possible_images:
                if img_file.suffix.lower() in IMAGE_EXTENSIONS:
                    target_img = recheck_dir / img_file.name
                    if not target_img.exists():
                        try:
                            shutil.move(str(img_file), str(target_img))
                            print(f"[→] Bild verschoben: {img_file} → {target_img}")
                        except Exception as e:
                            print(f"[Fehler beim Verschieben] {img_file}: {e}")
            try:
                temp_txt = temp_dir / txt_file.name
                shutil.move(str(txt_file), str(temp_txt))
                print(f"[→] .txt-Datei verschoben: {txt_file} → {temp_txt}")
            except Exception as e:
                print(f"[Fehler beim Verschieben der .txt-Datei] {txt_file}: {e}")

if __name__ == "__main__":
    find_missing_text_files(Path(IMAGE_FILE_CACHE_DIR), Path(TEXT_FILE_CACHE_DIR))
