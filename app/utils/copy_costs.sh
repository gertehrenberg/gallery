#!/bin/bash

# 🔁 Quelle und Ziel https://platform.openai.com/usage
SRC="/mnt/n/costs"
DST="$HOME/gallery/cache/costs"

# 🔍 Prüfen, ob Quelle existiert
if [ ! -d "$SRC" ]; then
    echo "❌ Quellverzeichnis nicht gefunden: $SRC"
    exit 1
fi

# 🗂 Zielverzeichnis erstellen, falls nicht vorhanden
mkdir -p "$DST"

# 📁 Dateien kopieren
echo "📂 Kopiere Dateien von $SRC nach $DST ..."
cp -v "$SRC"/cost_*.csv "$DST"/

echo "✅ Kopieren abgeschlossen."
