import time

import requests
from tqdm import tqdm
from requests.adapters import HTTPAdapter

USERNAME = "gert.ehrenberg@gmail.com"
PASSWORD = "6AcpKG3N3?@4QiQc"

LOGIN_URL = "http://localhost/gallery/login"
BASE_URL = "http://localhost/gallery/"
PARAMS = {
    "count": 33,
    "folder": "real",
    "textflag": 2
}

START_PAGE = 468
END_PAGE = 1
MAX_WAIT_TIME = 60
SLEEP_INTERVAL = 1


def login(session):
    data = {"username": USERNAME, "password": PASSWORD}
    response = session.post(LOGIN_URL, data=data)
    if response.status_code == 200 and "Logout" in response.text:
        print("✅ Login erfolgreich")
    else:
        print("❌ Login fehlgeschlagen")
        exit(1)


def wait_until_ready(session, page):
    params = PARAMS.copy()
    params["page"] = page
    expected_text = f"Seite {page} von "
    timeouts = [10, 30, 60]  # Progressiv längere Timeouts

    for attempt, to in enumerate(timeouts, 1):
        try:
            headers = {"Connection": "keep-alive"}
            response = session.get(BASE_URL, params=params, headers=headers, timeout=to)
            elapsed = response.elapsed.total_seconds()
            match = expected_text in response.text

            print(f"[Seite {page}] Versuch {attempt}: {elapsed:.3f}s – Match: {match}, Timeout: {to}s")

            if response.status_code == 200 and match:
                return True, response.text, elapsed
        except requests.RequestException as e:
            print(f"[Seite {page}] Fehler (Timeout {to}s): {e}")
        time.sleep(SLEEP_INTERVAL)

    return False, None, None


def fetch_pages():
    with requests.Session() as session:
        # Adapter setzen für optimiertes Connection-Pooling
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
        session.mount('http://', adapter)

        login(session)
        total_start = time.time()

        for page in tqdm(range(START_PAGE, END_PAGE - 1, -1), desc="Seiten abfragen"):
            page_start = time.time()
            success, _, response_time = wait_until_ready(session, page)
            total_page_time = time.time() - page_start

            if success:
                tqdm.write(
                    f"✅ Seite {page} vollständig – Antwortzeit: {response_time:.2f}s, Gesamt: {total_page_time:.2f}s")
            else:
                tqdm.write(f"❌ Seite {page} Timeout – Gesamt: {total_page_time:.2f}s")

        total_duration = time.time() - total_start
        print(f"\n🔚 Gesamtzeit: {total_duration:.1f} Sekunden")


if __name__ == "__main__":
    fetch_pages()
