import os
import cv2
import numpy as np
from skimage import feature
import sys
import sqlite3
import csv
from tqdm import tqdm  # Fortschrittsbalken

script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

from config import PFAD_REAL, IMAGE_EXTENSIONS

DB_PATH = "checkboxen.db"

def calculate_simple_brisque(image_path):
    """Berechnet die Fake-BRISQUE (LBP-Standardabweichung)."""
    image = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
    if image is None:
        print(f"Bild {image_path} konnte nicht geladen werden.")
        return None

    lbp = feature.local_binary_pattern(image, P=8, R=1, method="uniform")
    score = np.std(lbp)
    return score

def scale_score_to_0_100(score):
    """Skaliert den LBP-Score präzise auf 0–100."""
    scaled = (score / 5.0) * 100  # 5.0 ist ein erfahrener Maximalwert für LBP-std
    scaled = max(0, min(100, scaled))  # Clamping
    return int(round(scaled))

def save_image_quality(image_name, quality_score):
    """Speichert Bildname + Qualitätsscore in die Datenbank."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO image_quality (image_name, quality)
            VALUES (?, ?)
        """, (image_name, quality_score))

def process_folder(folder_path):
    """Durchläuft den Ordner und bewertet alle Bilder mit schöner Fortschrittsanzeige."""
    images = [f for f in os.listdir(folder_path) if f.lower().endswith(tuple(IMAGE_EXTENSIONS))]

    for filename in tqdm(images, desc="Bilder werden analysiert", ncols=80):
        image_path = os.path.join(folder_path, filename)
        score = calculate_simple_brisque(image_path)
        if score is not None:
            quality_score = scale_score_to_0_100(score)
            save_image_quality(filename, quality_score)

def init_quality_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS image_quality (
                image_name TEXT PRIMARY KEY,
                quality INTEGER
            )
        """)

def show_image_quality_entries():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM image_quality")
        count = cursor.fetchone()[0]
        print(f"📊 Anzahl gespeicherter Einträge: {count}")

        # Optional: ein paar Beispiele zeigen
        cursor.execute("SELECT image_name, quality FROM image_quality LIMIT 100")
        rows = cursor.fetchall()
        for row in rows:
            print(f"{row[0]} ➔ Qualität: {row[1]}")


EXPORT_SQL_PATH = "image_quality_export.sql"

def export_image_quality_to_sql():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT image_name, quality FROM image_quality")
        rows = cursor.fetchall()

        with open(EXPORT_SQL_PATH, mode="w", encoding="utf-8") as file:
            file.write("CREATE TABLE IF NOT EXISTS image_quality (image_name TEXT PRIMARY KEY, quality INTEGER);\n")
            for row in rows:
                file.write(f"INSERT OR REPLACE INTO image_quality (image_name, quality) VALUES ({repr(row[0])}, {row[1]});\n")

    print(f"✅ SQL-Export abgeschlossen: {EXPORT_SQL_PATH}")

# Beispiel
if __name__ == "__main__":
    show_image_quality_entries()
    export_image_quality_to_sql()
    #init_quality_db()
    #process_folder(PFAD_REAL)
