import logging
from pathlib import Path

from app.config import Settings  # Importiere die Settings-Klasse
from app.database import load_nsfw_from_db, save_nsfw_scores, load_all_nsfw_scores

NSFW_SERVICE_URL = "http://nsfw-service:8000/check-nsfw-path/"

mapping = {
    "drawings": 10,
    "hentai": 11,
    "neutral": 12,
    "porn": 13,
    "sexy": 14,
    "nsfw_score": 15
}
reverse_mapping = {v: k for k, v in mapping.items()}


def load_nsfw(db_path, folder_name: str | Path, image_name: str) -> dict[str, float] | None:
    try:
        rows = load_nsfw_from_db(db_path, image_name)

        scores = {score_type: score for score_type, score in rows}
        if set(range(10, 15)).issubset(scores):
            return {reverse_mapping[k]: scores[k] for k in scores}

        logging.info(f"[load_nsfw] nicht vollständig in DB für {folder_name}/{image_name}")

        payload = {
            "pathname": str(Path(folder_name).name),
            "filename": image_name
        }
        print(f"[load_nsfw] Rufe auf: {NSFW_SERVICE_URL} mit {payload}")
        response = httpx.post(NSFW_SERVICE_URL, json=payload, timeout=5.0)
        response.raise_for_status()
        data = response.json()
        if "scores" in data and "nsfw_score" in data:
            scores = {k: int(round(v * 100)) for k, v in data["scores"].items()}
            scores["nsfw_score"] = int(round(data["nsfw_score"] * 100))
            diff = 3
            for k in scores:
                scores[k] = min(100 - diff, max(diff, scores[k]))
            save_nsfw_scores(db_path, image_name, scores, mapping)
            return scores
        return None
    except Exception as e:
        print(f"Fehler beim NSFW-Check für {image_name}: {e}")
        return None


def save(db_path, image_name, nsfw_scores: dict[str, int] | None = None):
    if nsfw_scores:
        logging.info(f"[save] 📂 Schreiben für: {nsfw_scores}")
        save_nsfw_scores(db_path, image_name, nsfw_scores, mapping)


def load_all_scores(db_path: str) -> dict[str, dict[str, float]]:
    try:
        rows = load_all_nsfw_scores(db_path)

        result = {}
        for image_name, score_type, score in rows:
            if image_name not in result:
                result[image_name] = {}
            result[image_name][score_type] = score

        filtered = {
            name: {reverse_mapping[k]: v for k, v in scores.items()}
            for name, scores in result.items()
            if set(range(10, 16)).issubset(scores)
        }

        return filtered
    except Exception as e:
        print(f"Fehler beim Laden aller NSFW-Scores: {e}")
        return {}


def log_scores(image_name: str, scores: dict[str, float]) -> None:
    logging.info(f"[log_scores] Scores für {image_name}:")
    for k, v in scores.items():
        logging.info(f"  {k}: {v}")


def log_missing_scores_from_cache(db_path: str) -> None:
    try:
        all_scores = load_all_scores(db_path)
        available = set(name.lower() for name in all_scores.keys())

        rerun = False
        for image_name in Settings.CACHE["pair_cache"]:
            if image_name.lower() not in available:
                logging.info(f"[log_missing_scores_from_cache] 📂 Lesen für: {image_name}")
                rerun = True
                for eintrag in Settings.kategorien:
                    alt_key = eintrag["key"]
                    if load_nsfw(db_path, alt_key, image_name):
                        break
        if rerun:
            all_scores = load_all_scores(db_path)
            available = set(name.lower() for name in all_scores.keys())

            for image_name in Settings.CACHE["pair_cache"]:
                if image_name.lower() not in available:
                    logging.warning(f"[log_missing_scores] Kein vollständiger Score gefunden für {image_name}")

    except Exception as e:
        logging.error(f"Fehler bei der Prüfung fehlender NSFW-Scores: {e}")


import time
import httpx


def test_all_nsfw_urls(pathname: str, filename: str, max_retries: int = 3, delay: float = 2.0):
    urls = [
        "http://127.0.0.1/nsfw/check-nsfw-path/",
        "http://localhost:8000/check-nsfw-path/",
        "http://localhost:8000/nsfw/check-nsfw-path/",
        "http://nsfw-service:8000/check-nsfw-path/",
        "http://nsfw-service/check-nsfw-path/"
    ]
    payload = {"pathname": pathname, "filename": filename}

    for url in urls:
        for attempt in range(1, max_retries + 1):
            try:
                print(f"🌐 [{attempt}/{max_retries}] Teste NSFW-Service: {url} mit {payload}")
                response = httpx.post(url, json=payload, timeout=5.0)
                print(f"✅ Antwort von {url}: {response.status_code} → {response.text[:200]}")
                break  # Erfolgreich, nächste URL testen
            except Exception as e:
                print(f"❌ Fehler bei {url} (Versuch {attempt}): {e}")
                if attempt < max_retries:
                    time.sleep(delay)
                else:
                    print(f"⛔ {url} endgültig fehlgeschlagen.\n")
