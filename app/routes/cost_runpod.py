import logging
import os
from datetime import datetime
from typing import Dict, Any, List

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

_current_date = None
_cached_rate = None

def get_usd_to_chf_rate() -> float:
    """Holt den aktuellen USD zu CHF Wechselkurs. Gecached für einen Tag."""
    global _current_date, _cached_rate

    today = datetime.now().strftime('%Y-%m-%d')

    # Wenn es einen Cache gibt und er von heute ist, verwende ihn
    if _current_date == today and _cached_rate is not None:
        return _cached_rate

    try:
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        rate = data["rates"]["CHF"]
        logger.info(f"💱 Aktueller USD→CHF-Wechselkurs: {rate:.4f}")

        # Aktualisiere den Cache
        _current_date = today
        _cached_rate = rate

        return rate
    except Exception as e:
        logger.error(f"❌ Fehler beim Laden des Wechselkurses: {e}")
        return 0.9  # Fallback-Wert

def _make_runpod_request(query: str, variables: Dict = None) -> Dict:
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

def load_runpod_costs() -> Dict[str, Any]:
    """Lädt die aktuellen RunPod-Kosten über die GraphQL API."""
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

        # Nehme den letzten (neuesten) Eintrag
        latest_summary = summaries[-1]
        latest_storage = storage_entries[-1]

        rate = get_usd_to_chf_rate()

        # Konvertiere alle USD Beträge zu CHF mit aktuellem Kurs
        storage_chf = round(float(latest_summary['storageAmount']) * rate, 2)
        network_chf = round(float(latest_storage['networkStorageAmount']) * rate, 2)
        gpu_chf = round(float(latest_summary['gpuCloudAmount']) * rate, 2)

        return {
            "datum": latest_summary['time'],
            "storage_gesamt": storage_chf,
            "netzwerk_volumen": network_chf,
            "gpu_kosten": gpu_chf
        }

    except Exception as e:
        logger.error(f"❌ Fehler beim Laden der RunPod-Kosten: {e}")
        logger.error("Stack trace:", exc_info=True)
        return None


def load_runpod_costs_from_dir(year: int, month: int) -> List[Dict[str, Any]]:
    """Lädt RunPod-Kosten für einen bestimmten Monat über die GraphQL API."""
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

        # Verarbeite jeden Tag, filtere nach dem gewünschten Monat
        for summary, storage in zip(summaries, storage_entries):
            date = summary['time'][:10]  # YYYY-MM-DD
            if not date.startswith(f"{year}-{month:02d}"):
                continue

            # Berechne Gesamtkosten für den Tag
            total_usd = (
                float(summary['storageAmount']) +
                float(summary['gpuCloudAmount']) +
                float(summary['serverlessAmount']) +
                float(storage['networkStorageAmount'])
            )

            # Konvertiere zu CHF
            total_chf = round(total_usd * rate, 4)

            daily_costs.append({
                "tag": date,
                "kosten_chf": total_chf
            })
            logger.debug(f"📅 {date}: USD {total_usd:.4f} → CHF {total_chf:.4f}")

        return sorted(daily_costs, key=lambda x: x['tag'])

    except Exception as e:
        logger.error(f"❌ Fehler beim Laden der RunPod-Kosten: {e}")
        logger.error("Stack trace:", exc_info=True)
        return []

if __name__ == "__main__":
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

    # Monatsübersicht anzeigen
    now = datetime.now()
    monthly = load_runpod_costs_from_dir(now.year, now.month)
    if monthly:
        total = sum(day['kosten_chf'] for day in monthly)
        logger.info(f"\n📅 Kosten für {now.year}-{now.month:02d}:")
        for day in monthly:
            logger.info(f"{day['tag']}: CHF {day['kosten_chf']:.2f}")
        logger.info(f"\n💰 Gesamtkosten: CHF {total:.2f}")