import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import List, Tuple

import piexif
from PIL import Image
from tqdm import tqdm

script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

from config import PFAD_SAVE, PFAD_TEMP, PFAD_WORT, IMAGE_EXTENSIONS, MIN_TXT_SIZE_BYTES, MAX_IMAGES_PER_PAGE
from bildinfo import proccess_image


def lade_woerter_allgemein() -> dict[str, dict[str, list[str]]]:
    woerter_map: dict[str, dict[str, list[str]]] = {}
    for pfad in PFAD_WORT.glob("*.json"):
        kategorie = pfad.stem.lower()
        try:
            daten = json.loads(pfad.read_text(encoding="utf-8"))
            woerter_map[kategorie] = {
                "WHITE": sorted([w.lower() for w in daten.get("WHITE_DE", []) + daten.get("WHITE_EN", [])]),
                "BLACK": sorted([w.lower() for w in daten.get("BLACK_DE", []) + daten.get("BLACK_EN", [])])
            }
        except Exception as e:
            print(f"[Fehler] Konnte Datei nicht laden: {pfad} → {e}")
    return woerter_map


WOERTER_KATEGORIEN = lade_woerter_allgemein()


def clean_small_txt_files(source_dir, min_size):
    deleted = 0
    for file in os.listdir(source_dir):
        path = Path(source_dir) / file
        if path.suffix.lower() == ".txt" and path.is_file():
            size = os.path.getsize(path)
            if size < min_size:
                try:
                    os.remove(path)
                    deleted += 1
                    print(f"Gelöscht (zu klein): {file} ({size} Bytes)")
                except Exception as e:
                    print(f"Fehler beim Löschen von {file}: {e}")
    if deleted:
        print(f"\n{deleted} .txt-Datei(en) unter {min_size} Bytes gelöscht.\n")


def exif_dateiname(bildpfad: Path) -> str:
    stem = bildpfad.stem  # z. B. "urlaub_2024"
    suffix = bildpfad.suffix.lower()  # z. B. ".jpg"
    return f"Exif_{stem}{suffix}"


def move_image_text_pairs(source_dir, target_dir, use_exif):
    if not os.path.isdir(source_dir):
        print(f"Quellverzeichnis '{source_dir}' existiert nicht.")
        return

    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    files = os.listdir(source_dir)
    image_files = [f for f in files if Path(f).suffix.lower() in IMAGE_EXTENSIONS]

    print(f"[Info] Verarbeite {len(image_files)} Bilddateien...")
    if use_exif:
        for image in tqdm(image_files, desc="EXIF prüfen und ggf. speichern"):
            image_path = Path(source_dir) / image
            if image_path.suffix.lower() == ".jpg":
                try:
                    img = Image.open(image_path)
                    exif_bytes = img.info.get('exif', None)
                    exif_dict = piexif.load(exif_bytes) if exif_bytes else {
                        "0th": {}, "Exif": {}, "GPS": {}, "1st": {}, "thumbnail": None
                    }

                    existing_comment = exif_dict["Exif"].get(piexif.ExifIFD.UserComment, b"")
                    if existing_comment and existing_comment.strip() != b"":
                        decoded = existing_comment.decode("utf-8", errors="ignore").replace("\x00", "")
                        if "UNICODE" not in decoded:
                            encoded_comment = decoded.encode("utf-8")
                            exif_dict["Exif"][piexif.ExifIFD.UserComment] = b"ASCII\x00\x00\x00" + encoded_comment
                            exif_bytes = piexif.dump(exif_dict)
                            img.save(
                                target_dir / exif_dateiname(image_path),
                                "jpeg",
                                exif=exif_bytes,
                                quality=95,
                                optimize=True,
                                progressive=False,
                                subsampling="keep"
                            )
                            continue
                except Exception as e:
                    print(f"[Fehler] Problem bei EXIF-Verarbeitung von {image_path.name}: {e}")

    valid_pairs = [f for f in image_files if (Path(source_dir) / (f + ".txt")).exists()]
    total_pairs = len(valid_pairs)
    moved = 0

    bild_liste = []

    print(f"[Info] Verschiebe {total_pairs} gültige Bild/Text-Paare...")
    for filename in tqdm(valid_pairs, desc="Dateien verschieben"):
        image_path = Path(source_dir) / filename
        text_path = Path(source_dir) / (filename + ".txt")
        target_image_path = Path(target_dir) / filename
        target_text_path = Path(target_dir) / (filename + ".txt")

        try:
            shutil.move(image_path, target_image_path)
            shutil.move(text_path, target_text_path)
            bild_liste.append(target_image_path)
            moved += 1
        except Exception as e:
            print(f"[Fehler] beim Verschieben von {filename} und {filename}.txt: {e}")

    proccess_image(-1, bild_liste)
    print(f"\nFertig: {moved} von {total_pairs} Bild/Text-Paaren verarbeitet und verschoben.")


def finde_bild_text_paare_exif_or_paare(source_dir: Path) -> List[Tuple[str, str]]:
    datei_text_map = []

    # .txt-Dateien nach Änderungsdatum sortiert
    txt_dateien = sorted(source_dir.glob("*.txt"), key=lambda f: f.stat().st_mtime, reverse=True)

    # Alle Bilddateien im Verzeichnis
    image_files = sorted(
        [f for f in source_dir.iterdir() if f.suffix.lower() in IMAGE_EXTENSIONS],
        key=lambda f: f.stat().st_mtime,
        reverse=True
    )

    total = len(image_files)
    print(f"[Info] Durchsuche {total} Bilddateien nach Text- oder Exif-Kommentaren...\n")

    for idx, image_path in enumerate(image_files, 1):
        print(f"  [{idx}/{total}] {image_path.name}", end="")

        # Suche nach zugehöriger .txt-Datei mit gleichem Namen + .txt
        passende_txt = list(filter(lambda f: image_path.name in f.name, txt_dateien))

        if len(passende_txt) == 1:
            try:
                inhalt = passende_txt[0].read_text(encoding="utf-8")
                datei_text_map.append((image_path.name, inhalt))
                print("  ✅ TXT gefunden")
            except Exception as e:
                print(f"  ⚠️ Fehler beim Lesen von {passende_txt[0].name}: {e}")
            continue

        # Wenn keine oder mehrere passende .txt → prüfe EXIF
        try:
            img = Image.open(image_path)
            exif_bytes = img.info.get("exif", None)
            exif_dict = piexif.load(exif_bytes) if exif_bytes else {
                "0th": {}, "Exif": {}, "GPS": {}, "1st": {}, "thumbnail": None
            }

            user_comment = exif_dict["Exif"].get(piexif.ExifIFD.UserComment, b"")
            if user_comment and user_comment.strip() != b"":
                decoded = user_comment.decode("utf-8", errors="ignore").replace("\x00", "")
                if decoded.strip():
                    datei_text_map.append((image_path.name, decoded))
                    print("  ✅ EXIF-Kommentar gefunden")
                    continue
        except Exception as e:
            print(f"  ⚠️ Fehler beim Lesen von EXIF: {e}")
            continue

        print("  ⚠️ Kein passender Text gefunden")

    print(f"\n[Abschluss] {len(datei_text_map)} Bild/Text-Paare gefunden.\n")
    return datei_text_map


def schreibe_html_exif(paare: list[tuple[str, str]], verzeichnis: Path, suchbegriffe: list[str]) -> Path:
    for html_file in verzeichnis.glob("galerie_seite_*.html"):
        try:
            html_file.unlink()
        except Exception as e:
            print(f"[Warnung] Konnte {html_file.name} nicht löschen: {e}")

    seiten = [paare[i:i + MAX_IMAGES_PER_PAGE] for i in range(0, len(paare), MAX_IMAGES_PER_PAGE)]
    html_dateien = []
    gesamt = len(paare)

    for seite_index, seite in enumerate(seiten, 1):
        html_datei = verzeichnis / f"galerie_seite_{seite_index}.html"
        html_dateien.append(html_datei)

        with open(html_datei, "w", encoding="utf-8") as f:
            f.write(f"""<!DOCTYPE html>
<html lang='de'>
<head>
  <meta charset='UTF-8'>
  <title>Galerie – Seite {seite_index}</title>
  <style>
    body {{
      font-family: sans-serif;
      margin: 0;
      background: #f7f7f7;
    }}
    .sticky-nav {{
      position: sticky;
      top: 0;
      background: #fff;
      padding: 10px;
      border-bottom: 1px solid #ccc;
      z-index: 1000;
      text-align: center;
    }}
    .sticky-nav a {{
      margin: 0 8px;
      text-decoration: none;
      font-weight: bold;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(6, 1fr);
      gap: 20px;
      padding: 20px;
    }}
    .eintrag {{
      grid-column: span 2;
      background: white;
      border: 1px solid #ddd;
      border-radius: 10px;
      padding: 10px;
      box-shadow: 2px 2px 6px rgba(0,0,0,0.1);
      display: flex;
      flex-direction: column;
      gap: 10px;
    }}
    .bild-links {{
      text-align: center;
      font-size: 0.8em;
      margin-bottom: 8px;
    }}
    .bild-links a {{
      margin: 0 5px;
      text-decoration: none;
      color: #0077cc;
    }}
    .bild {{
      display: block;
      margin: 0 auto;
      max-width: 400px;
      height: auto;
      border-radius: 5px;
      cursor: zoom-in;
      transition: transform 0.2s ease;
    }}
    .bild:hover {{
      transform: scale(1.03);
    }}
    .bildname {{
      text-align: center;
      font-weight: bold;
      margin-top: 8px;
    }}
    .text {{
      white-space: pre-wrap;
    }}
    .highlight {{
      background: yellow;
      color: red;
      font-weight: bold;
    }}
    .highlight-green {{
      background: #ccffcc;
      color: green;
      font-weight: bold;
    }}
    .lightbox {{
      display: flex;
      align-items: center;
      justify-content: center;
      position: fixed;
      z-index: 9999;
      left: 0;
      top: 0;
      width: 100%;
      height: 100%;
      background-color: rgba(0,0,0,0.9);
    }}
    .lightbox img {{
      max-width: 90%;
      max-height: 90%;
      box-shadow: 0 0 20px rgba(255,255,255,0.3);
    }}
    .checkbox-container {{
      display: flex;
      justify-content: center;
      gap: 10px;
      margin-top: 10px;
    }}
  </style>
</head>
<body>
<div class='sticky-nav'>
  <a href='galerie_seite_1.html'>⏮ Anfang</a>
  <a href='galerie_seite_{max(1, seite_index - 10)}.html'>⏪ -10</a>
  <a href='galerie_seite_{max(1, seite_index - 1)}.html'>⬅ Zurück</a>
  <span style='margin: 0 15px; font-weight: bold;'>Seite {seite_index}</span>
  <a href='galerie_seite_{min(len(seiten), seite_index + 1)}.html'>Weiter ➡</a>
  <a href='galerie_seite_{min(len(seiten), seite_index + 10)}.html'>⏩ +10</a>
  <a href='galerie_seite_{len(seiten)}.html'>⏭ Ende</a>
</div>
<div class='grid'>
""")
            for idx, (bild, text) in enumerate(seite, 1):
                bildnummer = (seite_index - 1) * MAX_IMAGES_PER_PAGE + idx
                inhalt = re.sub(r"^ASCIIMD5:[a-fA-F0-9]{32}\s+", "", text.strip())

                for kategorie in WOERTER_KATEGORIEN:
                    for wort in WOERTER_KATEGORIEN[kategorie]["WHITE"]:
                        pattern = re.escape(wort)
                        inhalt = re.sub(pattern, lambda m: f'<span class="highlight-green">{m.group(0)}</span>', inhalt,
                                        flags=re.IGNORECASE)
                for wort in suchbegriffe:
                    pattern = re.escape(wort)
                    inhalt = re.sub(pattern, lambda m: f'<span class="highlight">{m.group(0)}</span>', inhalt,
                                    flags=re.IGNORECASE)

                f.write(f"""<div class='eintrag'>
  <img src='{bild}' alt='{bild}' class='bild' onclick='openLightbox(this.src)'>
  <div class='bildname'>{bild} ({bildnummer}/{gesamt})</div>
  <form class='checkbox-container'>
    <label><input type='checkbox' name='{bild}_delete'> Löschen</label>
    <label><input type='checkbox' name='{bild}_recheck'> Neu beurteilen</label>
    <label><input type='checkbox' name='{bild}_bad'> Schlecht</label>
    <label><input type='checkbox' name='{bild}_sex'> Sex</label>
    <label><input type='checkbox' name='{bild}_animal'> Tiere</label>
  </form>
  <div class='text'>{inhalt}</div>
</div>
""")

            f.write("""
</div>
<script>
function openLightbox(src) {
  const overlay = document.createElement('div');
  overlay.className = 'lightbox';
  overlay.innerHTML = `<img src="${src}" alt="">`;
  overlay.addEventListener('click', () => overlay.remove());
  document.body.appendChild(overlay);
}
</script>
</body>
</html>
""")

    return html_dateien[-1] if html_dateien else None


def genhtlm(pfad: Path, woerter_black_global: list[str]) -> None:
    verzeichnis = Path(pfad)
    if not verzeichnis.exists() or not verzeichnis.is_dir():
        print(f"Fehler: Das Verzeichnis '{verzeichnis}' existiert nicht oder ist kein Verzeichnis.")
        sys.exit(1)
    datei_text_map = finde_bild_text_paare_exif_or_paare(verzeichnis)
    if not datei_text_map:
        print("Keine passenden Bild-/Textpaare gefunden.")
    html_datei = schreibe_html_exif(datei_text_map, verzeichnis, woerter_black_global)
    print(f"HTML-Datei wurde erstellt: {html_datei.resolve()}")


def move_save2temp_reload(source_dir: Path, target_dir: Path) -> None:
    paare = []

    # Zielordner vorbereiten
    target_dir.mkdir(parents=True, exist_ok=True)

    # Alle Bilddateien im Quellverzeichnis prüfen
    for bild_datei in source_dir.iterdir():
        if bild_datei.suffix.lower() in IMAGE_EXTENSIONS:
            txt_datei = source_dir / (bild_datei.name + ".txt")
            if txt_datei.exists():
                paare.append((bild_datei.name, txt_datei.name))

                # Zielpfade
                target_bild = target_dir / bild_datei.name
                target_txt = target_dir / txt_datei.name

                # Verschieben
                shutil.move(bild_datei, target_bild)
                shutil.move(txt_datei, target_txt)

    print(f"\nFertig: {len(paare)} Bild/Text-Paare verschoben.")


def finde_leere_usercomment_ascii(pfad: Path, suchorte: list[Path]) -> None:
    jpg_bilder = list(pfad.glob("*.jpg"))
    print(f"[Info] Prüfe {len(jpg_bilder)} JPG-Bilder auf leere UserComment-Einträge...")

    for bild in tqdm(jpg_bilder, desc="Prüfe EXIF-Kommentare"):
        try:
            img = Image.open(bild)
            originalname = bild.name.replace("Exif_", "")
            txtname = Path(originalname + ".txt")
            neuer_comment = ""
            for suchpfad in suchorte:
                txtpfad = suchpfad / txtname
                if txtpfad.exists():
                    print(f" → Gefundene Textdatei: {txtpfad}")
                    inhalt = txtpfad.read_text(encoding="utf-8")
                    neuer_comment = inhalt.encode("utf-8")
                    break
            if neuer_comment == "":
                print(" → Keine passende Textdatei gefunden.")
                continue

            exif_bytes = img.info.get("exif", None)
            if not exif_bytes:
                continue

            exif = piexif.load(exif_bytes)
            comment = exif["Exif"].get(piexif.ExifIFD.UserComment, b"")

            if comment == b"ASCII\x00\x00\x00":
                print(f"[Hinweis] Nur ASCII-Marker, kein Text: {bild.name}")
                try:
                    exif["Exif"][piexif.ExifIFD.UserComment] = neuer_comment
                    neues_exif = piexif.dump(exif)
                    img.save(bild, "jpeg", exif=neues_exif, quality=95, optimize=True, progressive=False)
                    print(" → EXIF-UserComment aktualisiert.")
                except Exception as e:
                    print(f" → Fehler beim Schreiben des Kommentars: {e}")
                continue

            if comment.startswith(b"ASCII\x00\x00\x00"):
                print(f"[Hinweis] ASCII-Marker am Anfang: {bild.name}")
                try:
                    exif["Exif"][piexif.ExifIFD.UserComment] = neuer_comment
                    neues_exif = piexif.dump(exif)
                    img.save(bild, "jpeg", exif=neues_exif, quality=95, optimize=True, progressive=False)
                    print(" → EXIF-UserComment aktualisiert.")
                except Exception as e:
                    print(f" → Fehler beim Schreiben des Kommentars: {e}")
                continue

        except Exception as e:
            print(f"[Fehler] {bild.name}: {e}")


if __name__ == "__main__":
    # move_save2temp_reload(PFAD_SAVE, PFAD_TEMP)
    clean_small_txt_files(PFAD_TEMP, MIN_TXT_SIZE_BYTES)
    move_image_text_pairs(PFAD_TEMP, PFAD_SAVE, False)
    # finde_leere_usercomment_ascii(PFAD_SAVE, [PFAD_TEMP, PFAD_BACK, Path(r"e:\Meine Ablage\save_txt")])
    genhtlm(PFAD_SAVE, WOERTER_KATEGORIEN)
