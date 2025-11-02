import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import requests
from dotenv import load_dotenv

from ..config import Settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

_current_date = None
_cached_rate = None
_writable_cache_dir = None  # Cache the writable directory


def _find_writable_cache_dir() -> Path:
    """Find and cache a writable cache directory."""
    global _writable_cache_dir

    # Return cached result if already found
    if _writable_cache_dir is not None:
        return _writable_cache_dir

    # Try multiple cache locations in order of preference
    cache_dirs = [
        Path("/data/.cache/costs"),  # Hidden cache in /data
        Path("/data/costs"),
        Path("./cache/costs"),  # Relative to current directory
        Path.home() / ".cache" / "costs",  # User's home cache
        Path("/tmp/costs")  # System temp directory as last resort
    ]

    for cache_dir in cache_dirs:
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            # Test write permissions
            test_file = cache_dir / ".write_test"
            test_file.touch()
            test_file.unlink()
            # If we got here, directory is writable
            _writable_cache_dir = cache_dir
            logger.info(f"✅ Using writable cache directory: {cache_dir}")
            return cache_dir
        except (PermissionError, OSError) as e:
            logger.debug(f"Cannot write to {cache_dir}: {e}")
            continue

    # If all locations fail, use temp directory without testing
    fallback_dir = Path("/tmp/costs")
    fallback_dir.mkdir(parents=True, exist_ok=True)
    _writable_cache_dir = fallback_dir
    logger.warning(f"⚠️ Using fallback cache directory (may not persist): {fallback_dir}")
    return fallback_dir


def _make_runpod_request(query: str, variables: Optional[Dict] = None) -> Dict:
    """Hilfsfunktion für RunPod API Requests."""
    load_dotenv()

    GRAPHQL_URL = "https://api.runpod.io/graphql"
    API_KEY = os.getenv("RUNPOD_API_KEY")

    if not API_KEY:
        raise ValueError("❌ RUNPOD_API_KEY environment variable is not set")

    try:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        resp = requests.post(
            GRAPHQL_URL,
            json=payload,
            headers={"Authorization": f"Bearer {API_KEY}"}
        )

        response_data = resp.json()
        logger.debug(f"API Response: {response_data}")

        if resp.status_code != 200 or 'errors' in response_data:
            raise Exception(f"API Error: {resp.status_code} - {response_data}")

        return response_data["data"]

    except Exception as e:
        logger.error(f"❌ RunPod API Fehler: {e}")
        raise


def _get_cache_file_path(year: int, month: int) -> Path:
    """Gibt den Pfad zur Cache-Datei zurück."""
    cache_dir = _find_writable_cache_dir()
    return cache_dir / f"runpod_costs_{year}_{month:02d}.json"


def _save_to_cache(data: List[Dict[str, Any]], cache_file: Path) -> None:
    """Speichert Daten in einer Cache-Datei."""
    try:
        # Ensure parent directory exists and is writable
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        # Write to temporary file first, then rename (atomic operation)
        temp_file = cache_file.with_suffix('.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

        # Atomic rename
        temp_file.replace(cache_file)
        logger.info(f"✅ Cache gespeichert: {cache_file}")

    except PermissionError as e:
        logger.error(f"❌ Keine Schreibberechtigung für {cache_file}: {e}")
        # Don't raise - caching is optional, application should continue
    except Exception as e:
        logger.error(f"❌ Fehler beim Speichern des Cache: {e}")
        # Don't raise - caching is optional


def _load_from_cache(cache_file: Path) -> Optional[List[Dict[str, Any]]]:
    """Lädt Daten aus einer Cache-Datei."""
    if not cache_file.exists():
        return None

    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            logger.info(f"✅ Cache geladen: {cache_file}")
            return data
    except (PermissionError, OSError) as e:
        logger.warning(f"⚠️ Kann Cache nicht lesen: {cache_file}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.warning(f"⚠️ Ungültiger Cache: {cache_file}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Fehler beim Laden des Cache: {e}")
        return None


def get_usd_to_chf_rate() -> float:
    """Holt den aktuellen USD zu CHF Wechselkurs. Gecached für einen Tag."""
    global _current_date, _cached_rate

    today = datetime.now().strftime('%Y-%m-%d')

    # Use the writable cache directory
    cache_dir = _find_writable_cache_dir()
    cache_file = cache_dir / "exchange_rate.json"

    # Prüfe den Cache
    cached_data = _load_from_cache(cache_file)
    if cached_data and isinstance(cached_data, list) and len(cached_data) > 0:
        rate_data = cached_data[0]
        if isinstance(rate_data, dict) and rate_data.get('date') == today:
            return float(rate_data['rate'])

    try:
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        rate = float(data["rates"]["CHF"])
        logger.info(f"💱 Aktueller USD→CHF-Wechselkurs: {rate:.4f}")

        # Speichere im Cache
        _save_to_cache([{'date': today, 'rate': rate}], cache_file)

        return rate
    except Exception as e:
        logger.error(f"❌ Fehler beim Laden des Wechselkurses: {e}")
        return 0.9  # Fallback-Wert


def load_runpod_costs() -> Optional[Dict[str, Any]]:
    """Lädt die aktuellen RunPod-Kosten über die GraphQL API."""
    today = datetime.now()
    cache_file = _get_cache_file_path(today.year, today.month)

    # Prüfe ob es einen Cache von heute gibt
    cached_data = _load_from_cache(cache_file)
    if cached_data:
        today_str = today.strftime('%Y-%m-%d')
        for entry in cached_data:
            if isinstance(entry, dict) and entry.get('datum', '').startswith(today_str):
                return entry

    # Wenn kein Cache oder nicht aktuell, lade von API
    query = """
    query DailyBilling($input: UserBillingInput!) {
      myself {
        billing(input: $input) {
          summary {
            time
            storageAmount
            gpuCloudAmount
            serverlessAmount
          }
          storage {
            networkStorageAmount
          }
        }
      }
    }
    """

    variables = {
        "input": {
            "granularity": "DAILY"
        }
    }

    try:
        data = _make_runpod_request(query, variables)
        billing = data["myself"]["billing"]
        summaries = billing["summary"]
        storage_entries = billing["storage"]

        latest_summary = summaries[-1]
        latest_storage = storage_entries[-1]

        rate = get_usd_to_chf_rate()

        result = {
            "datum": latest_summary['time'],
            "storage_gesamt": round(float(latest_summary['storageAmount']) * rate, 2),
            "netzwerk_volumen": round(float(latest_storage['networkStorageAmount']) * rate, 2),
            "gpu_kosten": round(float(latest_summary['gpuCloudAmount']) * rate, 2),
            "kosten_chf": round(
                (float(latest_summary['storageAmount']) +
                 float(latest_summary['gpuCloudAmount']) +
                 float(latest_summary['serverlessAmount']) +
                 float(latest_storage['networkStorageAmount'])) * rate,
                2
            )
        }

        # Cache aktualisieren
        cached_data = cached_data if cached_data else []
        cached_data.append(result)
        _save_to_cache(cached_data, cache_file)

        return result

    except Exception as e:
        logger.error(f"❌ Fehler beim Laden der RunPod-Kosten: {e}")
        logger.error("Stack trace:", exc_info=True)
        return None


def load_runpod_costs_from_dir(year: int, month: int) -> List[Dict[str, Any]]:
    """Lädt RunPod-Kosten für einen bestimmten Monat, verwendet Cache wenn möglich."""
    cache_file = _get_cache_file_path(year, month)

    # Prüfe ob es einen vollständigen Cache für den Monat gibt
    cached_data = _load_from_cache(cache_file)
    if cached_data:
        # Validate cache data structure
        if all(isinstance(entry, dict) and 'tag' in entry and 'kosten_chf' in entry for entry in cached_data):
            month_str = f"{year}-{month:02d}"
            if any(entry.get('tag', '').startswith(month_str) for entry in cached_data):
                logger.info(f"📂 Lade Kosten für {month_str} aus Cache")
                return cached_data

    # Wenn kein Cache oder nicht vollständig, lade von API
    query = """
    query DailyBilling($input: UserBillingInput!) {
      myself {
        billing(input: $input) {
          summary {
            time
            storageAmount
            gpuCloudAmount
            serverlessAmount
          }
          storage {
            networkStorageAmount
          }
        }
      }
    }
    """

    variables = {
        "input": {
            "granularity": "DAILY"
        }
    }

    try:
        data = _make_runpod_request(query, variables)
        billing = data["myself"]["billing"]
        summaries = billing["summary"]
        storage_entries = billing["storage"]

        rate = get_usd_to_chf_rate()
        daily_costs = []

        for summary, storage in zip(summaries, storage_entries):
            date = summary['time'][:10]  # YYYY-MM-DD
            if not date.startswith(f"{year}-{month:02d}"):
                continue

            total_usd = (
                    float(summary['storageAmount']) +
                    float(summary['gpuCloudAmount']) +
                    float(summary['serverlessAmount']) +
                    float(storage['networkStorageAmount'])
            )

            total_chf = round(total_usd * rate, 4)

            daily_costs.append({
                "tag": date,
                "kosten_chf": total_chf
            })
            logger.debug(f"📅 {date}: USD {total_usd:.4f} → CHF {total_chf:.4f}")

        # Speichere im Cache
        _save_to_cache(daily_costs, cache_file)

        return sorted(daily_costs, key=lambda x: x['tag'])

    except Exception as e:
        logger.error(f"❌ Fehler beim Laden der RunPod-Kosten: {e}")
        logger.error("Stack trace:", exc_info=True)
        return []


if __name__ == "__main__":
    Settings.COSTS_FILE_DIR = "../../cache/costs"

    # Debug-Level für detailliertere Ausgaben
    logging.getLogger().setLevel(logging.DEBUG)

    # Aktuellen Wechselkurs holen
    rate = get_usd_to_chf_rate()
    logger.info(f"💱 Aktueller USD→CHF-Wechselkurs: {rate:.4f}")

    # Aktuelle Kosten anzeigen
    current = load_runpod_costs()
    if current:
        logger.info("\n📊 Aktuelle RunPod-Kosten:")
        logger.info(f"Datum:           {current['datum']}")
        logger.info(f"Storage gesamt:  CHF {current['storage_gesamt']:.2f}/Tag")
        logger.info(f"– Netzwerk-Vol.: CHF {current['netzwerk_volumen']:.2f}/Tag")
        logger.info(f"GPU-Kosten:      CHF {current['gpu_kosten']:.2f}/Tag")
        if 'kosten_chf' in current:
            logger.info(f"Gesamtkosten:    CHF {current['kosten_chf']:.2f}/Tag")

    # Monatsübersicht anzeigen
    now = datetime.now()
    monthly = load_runpod_costs_from_dir(now.year, now.month)
    if monthly:
        try:
            total = sum(day.get('kosten_chf', 0) for day in monthly)
            logger.info(f"\n📅 Kosten für {now.year}-{now.month:02d}:")
            for day in monthly:
                logger.info(f"{day['tag']}: CHF {day.get('kosten_chf', 0):.2f}")
            logger.info(f"\n💰 Gesamtkosten: CHF {total:.2f}")
        except Exception as e:
            logger.error(f"Fehler bei der Berechnung der Gesamtkosten: {e}")
            logger.debug("Monatsdaten:", monthly)