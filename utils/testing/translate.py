#!/usr/bin/env python3
import logging
import os
import re
from typing import List, Dict

import requests

# === Konfiguration (env var overrides möglich) ===
MODEL = os.getenv("MODEL", "qwen3:8b")
BASE_URL = os.getenv("BASE_URL", "http://localhost:11434")
PS_PATH = os.getenv("PS_PATH", "/api/ps")
CHAT_PATH = os.getenv("CHAT_PATH", "/api/chat")
TIMEOUT = int(os.getenv("TIMEOUT", 180))

# Hartcodierter Queltext für Tests als mehrzeiliger String
raw = '''
The image depicts an indoor setting, likely a restaurant or café, with a person seated at a dining table.
The individual appears to be a middle-aged man with graying hair, wearing glasses and a plaid shirt, smiling slightly towards the camera.
In front of him on the table is a slice of cake on a plate, with a spoon resting beside it. To the right, there's a cup that could contain coffee or tea.
The table also has other items such as a glass bottle, possibly containing wine or water, and some dining utensils including a knife and a fork.
There is another plate with cake to the side, suggesting a shared dessert experience.
On the left side of the image, there are framed pictures on the wall, adding to the ambiance of the space.
The flooring is tiled, and there is a wooden table behind the man, indicating additional seating or dining area.
The lighting in the room is natural and warm, with windows suggesting an outdoor view or source of light.
There is no visible text or branding in the image.
'''

session = requests.Session()


def get_loaded_models() -> List[Dict]:
    """
    Holt die aktuell im Ollama-Server geladenen Modelle via /api/ps.
    """
    try:
        resp = session.get(f"{BASE_URL}{PS_PATH}", timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json().get('models', resp.json())
    except Exception as e:
        logging.error("Fehler beim Abrufen der geladenen Modelle: %s", e)
        return []


def print_loaded_models(models: List):
    """
    Druckt eine Übersicht geladenener Modelle.
    """
    if not models:
        print("Keine geladenen Modelle gefunden.")
        return
    print("\n=== Geladene Ollama-Modelle ===")
    for m in models:
        if isinstance(m, dict):
            name = m.get('name') or m.get('model', '<unknown>')
            expires = m.get('expires_at', '-')
        else:
            name, expires = str(m), '-'
        print(f"- {name} (expires: {expires})")


def translate(raw_text: str) -> str:
    """
    Sendet eine Chat-Anfrage mit dem gesamten Text ohne Chunking.
    Entfernt alle <think>-Abschnitte.
    """
    messages = [
        {"role": "system", "content": (
            "You are a professional translator. Translate the user-provided English text precisely into German, ensuring correct use of all German diacritics (ä, ö, ü, ß). Do not output any additional text or explanations.")},
        {"role": "user", "content": raw_text}
    ]
    try:
        resp = session.post(
            f"{BASE_URL}{CHAT_PATH}",
            json={"model": MODEL, "messages": messages, "stream": False},
            headers={"Content-Type": "application/json"},
            timeout=TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json()
        if 'choices' in data:
            content = data['choices'][0]['message']['content']
        else:
            content = data.get('message', {}).get('content', '')
        # Entferne <think> Blöcke
        # Entferne <think> Blöcke
        content = re.sub(r'<think>[\s\S]*?</think>', '', content, flags=re.IGNORECASE)
        content = re.sub(r'</?think>', '', content, flags=re.IGNORECASE)
        cleaned = content.strip()
        # Post-Processing-Korrekturen
        cleaned = cleaned.replace("Plaidhemd", "kariertes Hemd")
        return cleaned
    except Exception as e:
        logging.error("Übersetzung fehlgeschlagen: %s", e)
        return ''


def main():
    models = get_loaded_models()
    print_loaded_models(models)

    print("\n=== Übersetzung beginnen ===\n")
    result = translate(raw)
    print(result)


if __name__ == "__main__":
    main()
