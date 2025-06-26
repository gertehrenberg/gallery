import cv2
import logging
from pathlib import Path

from app.config import Settings
from app.database import load_face_from_db, save_quality_scores
from app.routes.what import remove_items
from app.tools import readimages
from app.utils.progress import init_progress_state, progress_state, update_progress, stop_progress

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

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


def load_faces(db_path, folder_key: str, image_name: str, image_id: str) -> list[dict]:
    logging.info(f"🔍 Starte get_faces() für {image_id}")

    generate_faces(db_path, folder_key, image_name, image_id)

    base_url = "/static/facefiles"
    face_dir = Path(Settings.GESICHTER_FILE_CACHE_DIR)
    thumbs = sorted(face_dir.glob(f"{image_id}_*.jpg"))
    if thumbs:
        logging.info(f"[get_faces] ✅ {len(thumbs)} gefunden")
    return [
        {
            "src": f"/gallery{base_url}/{thumb.name}",
            "link": f"/gallery{base_url}/{thumb.name}",
            "image_name": f"{thumb.name}"
        }
        for thumb in thumbs
    ]


def save(db_path, image_id, scores: dict[str, int] | None = None):
    if scores:
        logging.info(f"[save] 📂 Schreiben für: {image_id} {scores}")
        save_quality_scores(db_path, image_id, scores, mapping)


async def reload_faces():
    await init_progress_state()
    progress_state["running"] = True

    logger.info("➡️  Gesichter werden gelöscht...")
    await remove_items(Path(Settings.GESICHTER_FILE_CACHE_DIR), "faces")
    logger.info("✅️  Gesichter gelöscht.")

    for eintrag in Settings.kategorien:
        folder_key = eintrag["key"]

        local_files = {}

        await readimages(Settings.IMAGE_FILE_CACHE_DIR + "/" + folder_key, local_files)

        all_files = []

        for image_name, entry in local_files.items():
            entry["image_name"] = image_name
            all_files.append(entry)

        count = 0
        label = next((k["label"] for k in Settings.kategorien if k["key"] == folder_key), folder_key)
        await update_progress(f"Bilder in \"{label}\"", 0)
        for i, file_info in enumerate(all_files, 1):
            percent = int(i / len(all_files) * 100)
            await update_progress(f"Bilder in \"{label}\": {i}/{len(all_files)} (erzeugt: {count})", percent)
            erg = load_faces(Settings.DB_PATH, folder_key, file_info["image_name"], file_info["image_id"])
            count += len(erg)

    await stop_progress()
