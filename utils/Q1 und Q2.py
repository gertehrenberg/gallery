import os
import sys

import cv2
import numpy as np
from skimage import feature
from tqdm import tqdm  # Fortschrittsbalken

script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

from config import IMAGE_EXTENSIONS, IMAGE_FILE_CACHE_DIR

EXPORT_SQL_PATH = "image_quality_export.sql"


def calculateq1andq2(image_path):
    """Berechnet die Fake-BRISQUE (LBP-Standardabweichung)."""
    image = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
    if image is None:
        print(f"Bild {image_path} konnte nicht geladen werden.")
        return None

    lbp = feature.local_binary_pattern(image, P=8, R=1, method="uniform")
    scoreq1 = np.std(lbp)

    h, w = image.shape[:2]
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    _, thresh = cv2.threshold(blur, 127, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return {"error": "Keine Konturen erkannt."}

    largest = max(contours, key=cv2.contourArea)
    M = cv2.moments(largest)
    if M["m00"] == 0:
        return {"error": "Ungültige Momentberechnung."}
    cx = int(M["m10"] / M["m00"])
    cy = int(M["m01"] / M["m00"])

    # Goldener Schnitt
    gx, gy = int(w * 0.618), int(h * 0.618)
    dist_golden = np.hypot(cx - gx, cy - gy) / np.hypot(w, h)
    score_golden = max(0, 1 - dist_golden)

    # Drittelregel
    thirds_x = [w // 3, 2 * w // 3]
    thirds_y = [h // 3, 2 * h // 3]
    min_dist_thirds = min([abs(cx - x) for x in thirds_x]) + min([abs(cy - y) for y in thirds_y])
    score_thirds = max(0, 1 - (min_dist_thirds / max(w, h)))

    # Symmetrie (vertikal)
    left = image[:, :w // 2]
    right = cv2.flip(image[:, w - w // 2:], 1)
    diff = cv2.absdiff(left, right)
    score_symmetry = 1 - (np.sum(diff) / (h * w * 3 * 255))

    # Kontrast
    contrast = gray.std() / 128
    score_contrast = min(contrast, 1.0)

    # Gesamt
    scoreq2 = int(round(np.mean([score_golden, score_thirds, score_symmetry, score_contrast]), 2) * 100)

    return scoreq1, scoreq2


def scale_score_to_0_100(score):
    """Skaliert den LBP-Score präzise auf 0–100."""
    scaled = (score / 5.0) * 100  # 5.0 ist ein erfahrener Maximalwert für LBP-std
    scaled = max(0, min(100, scaled))  # Clamping
    return int(round(scaled))


STR_ARRAY = []


def save_image_quality(image_name, quality_score):
    STR_ARRAY.append(
        f"INSERT OR REPLACE INTO image_quality (image_name, quality) VALUES ('{image_name}', {quality_score});")


def process_folder(folder_path):
    """Durchläuft den Ordner und bewertet alle Bilder mit schöner Fortschrittsanzeige."""
    images = [f for f in os.listdir(folder_path) if f.lower().endswith(tuple(IMAGE_EXTENSIONS))]

    for filename in tqdm(images, desc="Bilder werden analysiert", ncols=80):
        image_path = os.path.join(folder_path, filename)
        scoreq1, scoreq2 = calculateq1andq2(image_path)
        if scoreq1 is not None:
            quality_score = scale_score_to_0_100(scoreq1)
            save_image_quality(filename, quality_score)


def export_image_quality_to_sql():
    with open(EXPORT_SQL_PATH, mode="w", encoding="utf-8") as file:
        for entry in STR_ARRAY:
            file.write(f"{entry}\n")
    print(f"✅ SQL-Export abgeschlossen: {EXPORT_SQL_PATH}")


# Beispiel
if __name__ == "__main__":
    process_folder(os.path.join(IMAGE_FILE_CACHE_DIR, "sex"))
    export_image_quality_to_sql()
