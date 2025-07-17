import logging
from pathlib import Path

import cv2
import numpy as np
from skimage import feature

from ..config import Settings
from ..tools import readimages
from ..utils.db_utils import load_quality_from_db
from ..utils.db_utils import save_quality_scores
from ..utils.progress import init_progress_state
from ..utils.progress import stop_progress
from ..utils.progress import update_progress
from ..utils.score_utils import delete_scores_by_type

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def scale_score_to_0_100(score):
    """Skaliert den LBP-Score präzise auf 0–100."""
    scaled = (score / 5.0) * 100  # 5.0 ist ein erfahrener Maximalwert für LBP-std
    scaled = max(0, min(100, scaled))  # Clamping
    return int(round(scaled))


mapping = {
    "q1": 1,
    "q2": 2
}
reverse_mapping = {v: k for k, v in mapping.items()}


def load_quality(db_path, image_file_path, folder_name: str, image_name: str):
    """Lädt die Qualitätsbewertung (0–100) eines Bildes aus der neuen Tabelle image_quality_scores."""
    try:
        rows = load_quality_from_db(db_path, image_name)

        scores = {score_type: score for score_type, score in rows}
        if set(range(1, 2)).issubset(scores):
            return {reverse_mapping[k]: scores[k] for k in scores}

        logging.info(f"[load_quality] nicht vollständig in DB für {image_name}")

        full_path = Path(image_file_path) / folder_name / image_name
        scoreq1, scoreq2 = calculateq1andq2(full_path)
        scores["q1"] = int(scoreq1)
        scores["q2"] = int(scoreq2)
        diff = 3
        for k in scores:
            scores[k] = min(100 - diff, max(diff, scores[k]))

        save(db_path, image_name, scores)
        return scores

    except Exception as e:
        logging.error(f"[load_quality] Fehler bei {image_name}: {e}")

    return None, None


def calculateq1andq2(image_path):
    """Berechnet die Fake-BRISQUE (LBP-Standardabweichung und einfache Bildästhetik).
    :return: Tuple[int, int] → (scoreq1, scoreq2), oder (None, None) bei Fehler
    """
    image = cv2.imread(str(image_path), cv2.IMREAD_GRAYSCALE)
    if image is None:
        print(f"Bild {image_path} konnte nicht geladen werden.")
        return None, None

    if image.shape[0] < 16 or image.shape[1] < 16:
        print(f"Bild zu klein für Analyse: {image_path}")
        return None, None

    lbp = feature.local_binary_pattern(image, P=8, R=1, method="uniform")
    scoreq1 = min(scale_score_to_0_100(np.std(lbp)), 100)

    image = cv2.imread(str(image_path))
    h, w = image.shape[:2]
    if h < 16 or w < 16:
        print(f"Bild zu klein für Analyse: {image_path}")
        return None, None

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    _, thresh = cv2.threshold(blur, 127, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        print("Keine Konturen erkannt.")
        return None, None

    largest = max(contours, key=cv2.contourArea)
    M = cv2.moments(largest)
    if M["m00"] == 0:
        print("Ungültige Momentberechnung.")
        return None, None

    cx = int(M["m10"] / M["m00"])
    cy = int(M["m01"] / M["m00"])

    gx, gy = int(w * 0.618), int(h * 0.618)
    dist_golden = np.hypot(cx - gx, cy - gy) / np.hypot(w, h)
    score_golden = max(0, 1 - dist_golden)

    thirds_x = [w // 3, 2 * w // 3]
    thirds_y = [h // 3, 2 * h // 3]
    min_dist_thirds = min([abs(cx - x) for x in thirds_x]) + min([abs(cy - y) for y in thirds_y])
    score_thirds = max(0, 1 - (min_dist_thirds / max(w, h)))

    left = image[:, :w // 2]
    right = cv2.flip(image[:, w - w // 2:], 1)
    diff = cv2.absdiff(left, right)
    denom = h * w * 3 * 255
    score_symmetry = 1 - (np.sum(diff) / denom) if denom > 0 else 0

    contrast = gray.std() / 128
    score_contrast = min(contrast, 1.0)

    scoreq2 = int(round(np.mean([score_golden, score_thirds, score_symmetry, score_contrast]), 2) * 100)

    return scoreq1, scoreq2


def save(db_path, image_id, scores: dict[str, int] | None = None):
    if scores:
        logging.info(f"[save] 📂 Schreiben für: {image_id} {scores}")
        save_quality_scores(db_path, image_id, scores, mapping)


def delete_quality_by_type():
    for score_type in mapping.values():
        delete_scores_by_type(score_type)


async def reload_quality():
    await init_progress_state()

    try:
        logger.info("➡️  Quality-Score wird gelöscht...")
        delete_quality_by_type()
        logger.info("✅️  Quality-Score gelöscht.")

        for eintrag in Settings.kategorien():
            folder_key = eintrag["key"]

            local_files = {}

            await readimages(Settings.IMAGE_FILE_CACHE_DIR + "/" + folder_key, local_files)

            all_files = []

            for image_name, entry in local_files.items():
                entry["image_name"] = image_name
                all_files.append(entry)

            label = next((k["label"] for k in Settings.kategorien() if k["key"] == folder_key), folder_key)
            await update_progress(f"Bilder in \"{label}\"", 0)
            for i, file_info in enumerate(all_files, 1):
                percent = int(i / len(all_files) * 100)
                await update_progress(f"Bilder in \"{label}\": {i}/{len(all_files)}", percent)
                load_quality(Settings.DB_PATH, Settings.IMAGE_FILE_CACHE_DIR, folder_key, file_info["image_name"])
    finally:
        await stop_progress()
