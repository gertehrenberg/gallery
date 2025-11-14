#!/usr/bin/env python3
import json
import logging
import re
from typing import Dict, Any, Union


def clean_json_string(json_str: str) -> str:
    """
    Bereinigt den JSON-String von Escape-Zeichen und problematischen Formatierungen.
    """
    # Entferne äußere Backslashes und unescape
    json_str = json_str.replace('\\\"', '"')

    # Ersetze Unicode-Escape-Sequenzen
    json_str = json_str.encode('utf-8').decode('unicode-escape')

    # Korrigiere mögliche JSON-Formatierungsprobleme
    json_str = re.sub(r'(?<!\\)\\(?!["\\\/bfnrt])', r'\\\\', json_str)

    return json_str


def parse_robust_json(json_str: str) -> Dict[str, Any]:
    """
    Versucht, den JSON-String robust zu parsen mit mehreren Fallback-Methoden.
    """
    parsing_methods = [
        lambda s: json.loads(clean_json_string(s)),
        lambda s: json.loads(s.replace('\\"', '"')),
        lambda s: json.loads(re.sub(r'(?<!\\)\\(?!["\\\/bfnrt])', r'\\\\', s))
    ]

    for method in parsing_methods:
        try:
            return method(json_str)
        except Exception:
            continue

    logging.error(f"Kein JSON-Parsing möglich für Ausschnitt: {json_str[:500]}...")
    raise ValueError("JSON-Parsing fehlgeschlagen")


def analyze_ollama_response(json_str: str) -> Dict[str, Any]:
    """
    Analysiert die JSON-Antwort von Ollama und extrahiert wichtige Informationen.
    """
    try:
        # JSON robust parsen
        data = parse_robust_json(json_str)

        # Struktur für Analyse vorbereiten
        analysis = {
            "model": data.get("model"),
            "created_at": data.get("created_at"),
            "is_done": data.get("done", False),
            "total_duration_ms": data.get("total_duration", 0) / 1_000_000,  # Nanosekunden zu Millisekunden
            "message_details": {
                "role": data.get("message", {}).get("role"),
                "content_length": len(data.get("message", {}).get("content", "")),
                "first_50_chars": (data.get("message", {}).get("content", "")[:50] + "...")
                if len(data.get("message", {}).get("content", "")) > 50
                else data.get("message", {}).get("content", "")
            }
        }

        return analysis

    except Exception as e:
        logging.error(f"Unerwarteter Fehler: {e}")
        return {"error": str(e)}


def remove_think_blocks(text: str) -> str:
    """Entfernt <think> Blöcke robust"""
    patterns = [
        r'\u003cthink\u003e.*?\u003c/think\u003e',  # Unicode-Escape
        r'<think>.*?</think>',  # Normale Schreibweise
    ]

    for pattern in patterns:
        text = re.sub(pattern, '', text, flags=re.DOTALL | re.IGNORECASE)

    return text.strip()


def extract_content_from_json(json_str: str) -> str:
    """
    Extrahiert den Inhalt aus einem JSON-String, bereinigt Unicode-Escapes und entfernt <think>-Blöcke.
    """
    try:
        # Bereinige den JSON-String und parse
        parsed_json = parse_robust_json(json_str)

        # Extrahiere den Inhalt
        content = parsed_json.get('message', {}).get('content', '')

        content = clean_json_string(content)

        # Entferne <think>-Blöcke
        content = remove_think_blocks(content)

        return content
    except Exception as e:
        logging.error(f"Fehler bei der Extraktion: {e}")
        return ''


def main():
    # JSON aus Datei lesen
    try:
        with open('test.json', 'r', encoding='utf-8') as file:
            json_str = file.read().strip()

        # Vollständigen Inhalt extrahieren
        full_content = extract_content_from_json(json_str)
        logging.info(f"Vollständiger Inhalt: {full_content}")

    except FileNotFoundError:
        logging.error("Datei 'test.json' nicht gefunden.")
    except Exception as e:
        logging.error(f"Fehler beim Lesen der Datei: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()