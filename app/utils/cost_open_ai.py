import requests
import datetime
import os

# Dein Projekt-basierter Key
API_KEY = os.getenv("OPENAI_API_KEY") or "sk-..."

headers = {
    "Authorization": f"Bearer {API_KEY}"
}

end = datetime.datetime.utcnow()
start = end - datetime.timedelta(days=7)

def iso(dt):
    return dt.strftime("%Y-%m-%dT00:00:00Z")

def get_costs():
    url = "https://api.openai.com/v1/organization/costs"
    params = {
        "start_time": iso(start),
        "end_time": iso(end),
        "interval": "1d"
    }
    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()
    return r.json()

def get_usage():
    url = "https://api.openai.com/v1/organization/usage/completions"
    params = {
        "start_time": iso(start),
        "end_time": iso(end),
        "interval": "1d"
    }
    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()
    return r.json()

if __name__ == "__main__":
    print("📊 OpenAI Kosten und Nutzung (Project/Org API)\n")
    try:
        costs = get_costs()
        usage = get_usage()

        print("💰 Kosten:")
        for d in costs.get("daily_costs", []):
            print(f"{d['timestamp'][:10]}: {d['cost_usd']:.4f} USD")

        print("\n🔢 Nutzung (Tokens):")
        for d in usage.get("daily_usage", []):
            print(f"{d['timestamp'][:10]}: {d['n_generated_tokens_total']} Tokens")

    except Exception as e:
        print("❌ Fehler beim Abrufen:", e)
