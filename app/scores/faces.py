import logging
from pathlib import Path

import cv2

from app.database import load_face_from_db, save_quality_scores

# Absolute Pfade zu den Haar Cascade Klassifikatoren.  Diese sollten relativ zum Projektverzeichnis sein.
HAAR_FRONTALFACE_ALT2 = cv2.data.haarcascades + 'haarcascade_frontalface_alt2.xml'
HAAR_PROFILEFACE = cv2.data.haarcascades + 'haarcascade_profileface.xml'
HAAR_FRONTALFACE_DEFAULT = cv2.data.haarcascades + 'haarcascade_frontalface_default.xml'

mapping = {
    "faces": 5
}
reverse_mapping = {v: k for k, v in mapping.items()}


def generate_faces(db_path, folder_key, image_name, image_id, min_size=(50, 50)):
    from app.config import Settings

    rows = load_face_from_db(db_path, image_id)

    scores = {score_type: score for score_type, score in rows}
    if set(range(5)).issubset(scores):
        return {reverse_mapping[k]: scores[k] for k in scores}

    logging.info(f"[generate_faces] nicht vollständig in DB für {image_name}")

    full_path = Path(Settings.IMAGE_FILE_CACHE_DIR) / folder_key / image_name
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
        for i, (x, y, w, h) in enumerate(faces):
            face_img = img[y:y + h, x:x + w]
            face_filename = f'/app/facefiles/{image_id}_{i}.jpg'  # Zielpfad für Gesichtsausschnitt
            try:
                cv2.imwrite(face_filename, face_img)
                logging.info(f"[gen_faces] Gesichtsausschnitt gespeichert: {face_filename}")
            except Exception as e:
                logging.error(f"[gen_faces] Fehler beim Speichern des Gesichtsausschnitts: {face_filename} - {e}")
                return False
        save(db_path, image_id, scores)
        return True
    else:
        logging.info(f"[gen_faces] Keine Gesichter erkannt in {image_name}")
        save(db_path, image_id, scores)
        return False


def save(db_path, image_id, scores: dict[str, int] | None = None):
    if scores:
        logging.info(f"[save] 📂 Schreiben für: {image_id} {scores}")
        save_quality_scores(db_path, image_id, scores, mapping)
