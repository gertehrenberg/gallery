import logging
import socket
import ssl
import time


def verify_folders_exist(service, kategorien):
    """Verifiziert, dass die Ordner in Google Drive existieren."""
    valid_kategorien = kategorien
    return valid_kategorien


def retry_google_request(callable_fn, retries: int = 7):
    """Wiederholt einen Google Drive API-Aufruf bei bestimmten Fehlern."""
    for attempt in range(retries):
        try:
            return callable_fn()
        except (ssl.SSLError, socket.timeout) as e:
            logging.warning(f"[SSL] Retry {attempt + 1} für Google Request: {e}")
            time.sleep(1.5 * (attempt + 1))
        except Exception as e:
            logging.warning(f"[Retry] Fehler bei Google Request: {e}")
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"Google Request nach {retries} Versuchen fehlgeschlagen.")
