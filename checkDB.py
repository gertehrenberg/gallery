import sqlite3
import logging

DB_PATH = 'checkboxen.db'  # Ersetze dies mit dem Pfad zu deiner DB

# Eine einfache Funktion, um den Status aus der DB zu laden
def check_saved_data(image_name):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            logging.info(f"Verbindung zur DB hergestellt. Prüfe Status für {image_name}...")
            
            # Abfrage der Checkboxen
            logging.info(f"Abfrage der Checkboxen für {image_name}...")
            rows = conn.execute("""
                SELECT checkbox, checked FROM checkbox_status WHERE image_name = ?
            """, (image_name,))
            for row in rows:
                print(f"Checkbox: {row[0]}, Status: {row[1]}")
            
            # Abfrage der Textfelder
            logging.info(f"Abfrage der Textfelder für {image_name}...")
            rows = conn.execute("""
                SELECT field, value FROM text_status WHERE image_name = ?
            """, (image_name,))
            for row in rows:
                print(f"Textfeld: {row[0]}, Wert: {row[1]}")
    
    except Exception as e:
        logging.error(f"Fehler beim Abfragen der DB: {e}")
        print(f"Fehler: {e}")

# Teste die Funktion für ein Beispielbild
image_name = 'img_5242.jpg'  # Beispielbildname, den du testen möchtest
check_saved_data(image_name)
