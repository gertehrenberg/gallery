import csv
import logging

from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def load_openai_costs_from_dir(cost_dir: Path, year: int, month: int) -> list[dict]:
    """Lädt OpenAI-Kosten aus CSV-Dateien und verteilt zusätzlich 20 CHF gleichmäßig auf die Tage."""
    all_rows = []
    files = list(sorted(cost_dir.glob("cost_*.csv")))
    if not files:
        logger.warning("⚠️ Keine OpenAI-Kosten-CSV-Dateien gefunden in: %s", cost_dir)

    # Sammle erst alle tatsächlichen Kosten
    for csv_file in files:
        logger.info(f"🔍 Verarbeite Datei: {csv_file.name}")
        try:
            with open(csv_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                count = 0
                for row in reader:
                    if row.get("amount_value"):
                        date = row["start_time_iso"][:10]
                        if not date.startswith(f"{year}-{month:02d}"):
                            continue
                        chf = round(float(row["amount_value"]) * 0.9, 4)
                        all_rows.append({"tag": date, "kosten_chf": chf})
                        count += 1
                logger.info(f"✅ {count} Einträge mit Betrag in {csv_file.name}")
        except Exception as e:
            logger.error(f"❌ Fehler beim Lesen von {csv_file.name}: {e}")

    # Gruppiere die tatsächlichen Kosten nach Tagen
    grouped = {}
    for entry in all_rows:
        grouped[entry["tag"]] = grouped.get(entry["tag"], 0.0) + entry["kosten_chf"]

    # Bestimme die Anzahl der Tage im Monat
    import calendar
    days_in_month = calendar.monthrange(year, month)[1]

    # Berechne den täglichen Anteil der zusätzlichen 20 CHF
    daily_extra = round(20.0 / days_in_month, 4)
    logger.debug(f"💰 Verteile zusätzliche 20 CHF: {daily_extra:.4f} CHF pro Tag")

    # Erstelle eine vollständige Liste aller Tage des Monats
    result = []
    for day in range(1, days_in_month + 1):
        date = f"{year}-{month:02d}-{day:02d}"
        # Kombiniere tatsächliche Kosten mit dem täglichen Extra-Anteil
        total = round(grouped.get(date, 0.0) + daily_extra, 4)
        result.append({
            "tag": date,
            "kosten_chf": total
        })

    return result