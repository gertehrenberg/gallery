import logging
import os
from pathlib import Path

import cv2

# Absolute Pfade zu den Haar Cascade Klassifikatoren.  Diese sollten relativ zum Projektverzeichnis sein.
HAAR_FRONTALFACE_ALT2 = cv2.data.haarcascades + 'haarcascade_frontalface_alt2.xml'
HAAR_PROFILEFACE = cv2.data.haarcascades + 'haarcascade_profileface.xml'
HAAR_FRONTALFACE_DEFAULT = cv2.data.haarcascades + 'haarcascade_frontalface_default.xml'

# Ensure that the required directories exist
os.makedirs('/app/facefiles', exist_ok=True)


def gen_faces(folder_name, image_name, min_size=(50, 50)):
    """
    Erkennt und extrahiert Gesichter aus einem Bild.

    Args:
        folder_name (str): Der Name des Ordners, in dem sich das Bild befindet.
        image_name (str): Der Name der Bilddatei.
        min_size (tuple, optional): Die Mindestgröße der zu erkennenden Gesichter.
            Standardwert ist (50, 50).

    Returns:
        bool: True, wenn Gesichter erkannt und gespeichert wurden, False sonst.
    """
    from app.config import Settings
    full_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_name / image_name
    img = cv2.imread(str(full_path))
    if img is None:
        logging.error(f"[gen_faces] Fehler beim Lesen des Bildes: {full_path}")
        return False

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    face_cascade_1 = cv2.CascadeClassifier(HAAR_FRONTALFACE_ALT2)
    face_cascade_2 = cv2.CascadeClassifier(HAAR_PROFILEFACE)
    face_cascade_3 = cv2.CascadeClassifier(HAAR_FRONTALFACE_DEFAULT)

    faces = face_cascade_1.detectMultiScale(gray, 1.1, 4, minSize=min_size)
    if len(faces) == 0:
        faces = face_cascade_2.detectMultiScale(gray, 1.1, 4, minSize=min_size)
    if len(faces) == 0:
        faces = face_cascade_3.detectMultiScale(gray, 1.1, 4, minSize=min_size)

    if len(faces) > 0:
        logging.info(f"[gen_faces] {len(faces)} Gesichter erkannt in {image_name}")
        base_name = Path(image_name).stem
        for i, (x, y, w, h) in enumerate(faces):
            face_img = img[y:y + h, x:x + w]
            face_filename = f'/app/facefiles/{base_name}_{i}.jpg'  # Zielpfad für Gesichtsausschnitt
            try:
                cv2.imwrite(face_filename, face_img)
                logging.info(f"[gen_faces] Gesichtsausschnitt gespeichert: {face_filename}")
            except Exception as e:
                logging.error(f"[gen_faces] Fehler beim Speichern des Gesichtsausschnitts: {face_filename} - {e}")
                return False
        return True
    else:
        logging.info(f"[gen_faces] Keine Gesichter erkannt in {image_name}")
        return False
