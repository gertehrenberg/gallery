# cost_hyperstack.py
import calendar
from datetime import date


def load_hyperstack_costs(year: int, month: int):
    """
    Liefert tägliche Hyperstack-Kosten für den angegebenen Monat.

    Die Werte kommen aus dem Dictionary `additional_amounts`.
    Falls ein Monat keinen Eintrag hat → es wird eine leere Liste zurückgegeben.
    """

    # 🔧 Hier gibst du einfach deine Monatskosten ein:
    additional_amounts = {
        (2025, 10): 27.97,  # geschätzt
        (2025, 11): 24.70,  # geschätzt
        (2025, 12): 26.00,  # geschätzt
        # weitere Monate kannst du einfach ergänzen
        # (2026, 1): 42.50,
        # (2026, 2): 42.50,
    }

    monthly_cost = additional_amounts.get((year, month))
    if not monthly_cost:
        return []  # keine Kosten für diesen Monat

    # Anzahl Tage ermittelt → Monat korrekt verteilt
    last_day = calendar.monthrange(year, month)[1]
    daily_value = round(monthly_cost / last_day, 2)

    result = []
    for day in range(1, last_day + 1):
        tag = date(year, month, day).strftime("%Y-%m-%d")
        result.append({
            "tag": tag,
            "kosten_chf": daily_value,
        })

    return result
