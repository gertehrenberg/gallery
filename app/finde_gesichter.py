import cv2
import os
import sys
from pathlib import Path
from tqdm import tqdm

script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

from config import PFAD_SAVE

def gesichter_erkennen_in_ordner(bilder_ordner: Path, ergebnis_datei: Path, min_gesichtsgroesse=(300, 400)) -> int:
    """
    Durchsucht einen Ordner rekursiv nach Bildern und erkennt Gesichter mithilfe von OpenCV.
    Speichert erkannte Gesichter als einzelne Bilddateien in einem gemeinsamen Unterordner "gesichter".
    Erzeugt eine HTML-Galerie mit den Gesichtsausschnitten.

    :param bilder_ordner: Pfad zum Ordner mit Bildern
    :param ergebnis_datei: Pfad zur Textdatei, in die erkannte Gesichter geschrieben werden
    :param min_gesichtsgroesse: Minimale Gesichtserkennung (Breite, Höhe)
    :return: Anzahl der Bilder mit erkannten Gesichtern
    """
    face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
    bild_dateien = list(bilder_ordner.glob("**/*.[jp][pn]g"))  # JPG/JPEG/PNG
    ergebnisse = []
    gesichtsbilder = []

    print(f"{len(bild_dateien)} Bilder gefunden in {bilder_ordner}.")

    gesichter_ordner = bilder_ordner / "gesichter"
    gesichter_ordner.mkdir(parents=True, exist_ok=True)

    ergebnis_datei = gesichter_ordner / "gesichtserkennung_ergebnisse.txt"

    with open(ergebnis_datei, "w", encoding="utf-8") as f:
        for bild_pfad in tqdm(bild_dateien, desc="Gesichtserkennung"):
            try:
                img = cv2.imread(str(bild_pfad))
                if img is None:
                    continue
                gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
                gesichter = face_cascade.detectMultiScale(
                    gray, scaleFactor=1.3, minNeighbors=8, minSize=min_gesichtsgroesse
                )

                # Keine Filterung nach Seitenverhältnis
                gefilterte_gesichter = list(gesichter)
                for (x, y, w, h) in gesichter:
                    verhältnis = w / h
                    if 0.6 < verhältnis < 1.0 and w >= min_gesichtsgroesse[0] and h >= min_gesichtsgroesse[1]:
                        gefilterte_gesichter.append((x, y, w, h))

                if len(gefilterte_gesichter) > 0:
                    eintrag = f"{bild_pfad} -> {len(gefilterte_gesichter)} Gesicht(er)"
                    ergebnisse.append(eintrag)
                    f.write(eintrag + "\n")
                    f.flush()

                    for i, (x, y, w, h) in enumerate(gefilterte_gesichter):
                        gesicht_img = img[y:y+h, x:x+w]
                        ziel_datei = gesichter_ordner / f"{bild_pfad.stem}_{i}.jpg"
                        cv2.imwrite(str(ziel_datei), gesicht_img)
                        gesichtsbilder.append(ziel_datei.relative_to(gesichter_ordner))

            except Exception as e:
                print(f"Fehler bei {bild_pfad}: {e}")

    # HTML-Galerie erstellen
    galerie_datei = gesichter_ordner / "gesichter_galerie.html"
    with open(galerie_datei, "w", encoding="utf-8") as html:
        html.write("<html><head><title>Gesichtergalerie</title><style>img{margin:4px;max-height:200px;}</style></head><body>")
        html.write("<h1>Gesichtsausschnitte</h1>")
        for pfad in gesichtsbilder:
            html.write(f'<img src="{pfad.as_posix()}" alt="Gesicht">\n')
        html.write("</body></html>")

    print(f"HTML-Galerie gespeichert unter: {galerie_datei}")
    print(f"Fertig. {len(ergebnisse)} Bilder mit Gesichtern erkannt.")
    print(f"Ergebnisse gespeichert unter: {ergebnis_datei}")
    return len(ergebnisse)


def main(bilder_ordner):
    gesamt = gesichter_erkennen_in_ordner(bilder_ordner, Path())
    print(f"Gesamtanzahl mit Gesichtern: {gesamt}")


if __name__ == "__main__":
    main(PFAD_SAVE)
