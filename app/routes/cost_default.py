
import logging
import calendar
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def load_default_costs(year: int, month: int) -> List[Dict[str, Any]]:
    """Verteilt 9 CHF gleichmäßig auf alle Tage des angegebenen Monats."""
    
    # Bestimme die Anzahl der Tage im Monat
    days_in_month = calendar.monthrange(year, month)[1]
    
    # Berechne den täglichen Anteil der 9 CHF
    daily_amount = round(9.0 / days_in_month, 4)
    logger.debug(f"💰 Verteile 9 CHF: {daily_amount:.4f} CHF pro Tag im {month:02d}/{year}")

    # Erstelle Einträge für jeden Tag des Monats
    result = []
    for day in range(1, days_in_month + 1):
        date = f"{year}-{month:02d}-{day:02d}"
        result.append({
            "tag": date,
            "kosten_chf": daily_amount
        })

    return result

if __name__ == "__main__":
    # Debug-Level für detailliertere Ausgaben
    logging.getLogger().setLevel(logging.DEBUG)
    
    # Beispiel für den aktuellen Monat
    now = datetime.now()
    monthly = load_default_costs(now.year, now.month)
    
    if monthly:
        total = sum(day['kosten_chf'] for day in monthly)
        logger.info(f"\n📅 Standard-Kosten für {now.year}-{now.month:02d}:")
        for day in monthly:
            logger.info(f"{day['tag']}: CHF {day['kosten_chf']:.4f}")
        logger.info(f"\n💰 Gesamtkosten: CHF {total:.2f}")
