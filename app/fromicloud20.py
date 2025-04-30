import hashlib
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from tqdm import tqdm
except ImportError:
    print("[Hinweis] Modul 'tqdm' nicht installiert. Bitte mit 'pip install tqdm' nachinstallieren für Fortschrittsanzeige.")
    exit(1)

# Eigene Imports
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

from config import IMAGE_EXTENSIONS, PFAD_REAL, PFAD_HASHES

# Pfad für Zeitstempel der letzten Verarbeitung
PFAD_TIMESTAMP = Path("letzter_lauf.txt")
DATUMSFORMAT = "%d.%m.%Y %H:%M:%S"


def lade_letzten_timestamp(pfad: Path) -> float:
    if not pfad.exists():
        return 0.0
    try:
        inhalt = pfad.read_text().strip()
        dt = datetime.strptime(inhalt, DATUMSFORMAT)
        return dt.timestamp()
    except Exception as e:
        print(f"[Warnung] Konnte Datum nicht lesen: {e}")
        return 0.0


def speichere_timestamp(pfad: Path, timestamp: float) -> None:
    dt = datetime.fromtimestamp(timestamp)
    pfad.write_text(dt.strftime(DATUMSFORMAT))


def initialisiere_hashdatei(wurzel: Path, pfad_hashes: Path) -> None:
    if not pfad_hashes.exists():
        print("[Init] Erstelle initiale Hash-Liste durch Durchlauf...")
        bekannte = set()
        dateien = list(wurzel.rglob("*"))
        bilder = [f for f in dateien if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS]
        total = len(bilder)
        for idx, bild in enumerate(bilder, 1):
            try:
                h = hashlib.md5(bild.read_bytes()).hexdigest()
                bekannte.add(h)
            except Exception as e:
                print(f"[Fehler] {bild} → {e}")
            if idx % 100 == 0 or idx == total:
                print(f"  Fortschritt: {idx}/{total} Dateien gelesen...")
        pfad_hashes.write_text("\n".join(bekannte), encoding="utf-8")
        print(f"[Init] {len(bekannte)} Hashes gespeichert.")


def lade_hashes_aus_datei(pfad: Path) -> set[str]:
    if not pfad.exists():
        return set()
    return set(pfad.read_text(encoding="utf-8").splitlines())


def speichere_neue_hashes(pfad: Path, neue_hashes: set[str]) -> None:
    with open(pfad, "a", encoding="utf-8") as f:
        for h in neue_hashes:
            f.write(h + "\n")


def kopiere_neue_bilder(quell: Path, ziel: Path, bekannte_hashes: set[str], ab_timestamp: float, anzahl: int = 15000) -> set[str]:
    ziel.mkdir(parents=True, exist_ok=True)
    neue_hashes = set()
    kopiert = 0

    # Vorauswahl der Dateien für Fortschritt
    alle_bilder = [f for f in quell.iterdir()
                   if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS and f.stat().st_mtime > ab_timestamp]

    for datei in tqdm(alle_bilder, desc="Bilder werden geprüft und ggf. kopiert", unit="Datei"):
        try:
            md5 = hashlib.md5(datei.read_bytes()).hexdigest()
        except Exception as e:
            print(f"[Fehler] {datei} → {e}")
            continue

        if md5 in bekannte_hashes:
            continue

        ziel_pfad = ziel / datei.name
        if ziel_pfad.exists():
            continue

        shutil.copy2(datei, ziel_pfad)
        neue_hashes.add(md5)
        kopiert += 1

        if kopiert >= anzahl:
            break

    print(f"[Abgeschlossen] {kopiert} neue Bilder kopiert.")
    return neue_hashes


if __name__ == "__main__":
    # Lade Zeitstempel
    letzter_timestamp = lade_letzten_timestamp(PFAD_TIMESTAMP)
    print(f"[Info] Letzter erfolgreicher Kopierlauf: {datetime.fromtimestamp(letzter_timestamp).strftime(DATUMSFORMAT)}")

    # Initialisiere und lade Hashes
    initialisiere_hashdatei(PFAD_REAL, PFAD_HASHES)
    bekannte_hashes = lade_hashes_aus_datei(PFAD_HASHES)
