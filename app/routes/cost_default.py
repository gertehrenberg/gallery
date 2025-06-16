
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
    """Verteilt die monatlichen Default-Kosten gleichmäßig auf alle Tage des angegebenen Monats."""

    # Zusätzliche monatliche Beträge für bestimmte Zeiträume wingo
    additional_amounts = {
        (2025, 2): 83.90,  # Februar 2025
        (2025, 3): 31.95,  # März 2025
        (2025, 4): 36.30,  # April 2025
        (2025, 5): 36.95,  # Mai 2025
        (2025, 6): 36.95,  #geschätzt
        (2025, 7): 36.95,  #geschätzt
        (2025, 8): 36.95,  #geschätzt
        (2025, 9): 36.95,  #geschätzt
        (2025, 10): 36.95,  #geschätzt
        (2025, 11): 36.95,  #geschätzt
        (2025, 12): 36.95,  #geschätzt
    }

    # Bestimme den Basisbetrag und zusätzliche Beträge JetBrain
    base_amount = 0  # Standardmäßig kein Basisbetrag
    if year > 2025 or (year == 2025 and month >= 3):
        base_amount = 17.0

    # Bestimme den Basisbetrag und zusätzliche Beträge JetBrain
    base_amount = 0  # Standardmäßig kein Basisbetrag
    if year > 2025 or (year == 2025 and month >= 6):
        base_amount = 9.0

    # Gesamtbetrag berechnen
    monthly_amount = additional_amounts.get((year, month), 0.0) + base_amount

    # Bestimme die Anzahl der Tage im Monat
    days_in_month = calendar.monthrange(year, month)[1]

    # Berechne den täglichen Anteil
    daily_amount = round(monthly_amount / days_in_month, 4)
    logger.debug(f"💰 Verteile {monthly_amount:.2f} CHF: {daily_amount:.4f} CHF pro Tag im {month:02d}/{year}")

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
