#!/bin/bash

SRC="/mnt/d/e-save/Bilder_mit_Text"
DST="../cache/imagefiles/real"

echo "🔄 Starte Kopieren aller Dateien (ohne .txt) mit Umbenennung auf Kleinbuchstaben..."

find "$SRC" -type f ! -iname "*.txt" | while read -r srcfile; do
    relpath="${srcfile#$SRC/}"                      # Relativer Pfad
    lowerpath=$(echo "$relpath" | tr '[:upper:]' '[:lower:]')  # In Kleinbuchstaben
    dstfile="$DST/$lowerpath"

    mkdir -p "$(dirname "$dstfile")"                # Zielordner anlegen
    rsync -a "$srcfile" "$dstfile"                  # Datei kopieren
    echo "✔️  $relpath → $lowerpath"
done

echo "✅ Alle Nicht-TXT-Dateien kopiert und in Kleinbuchstaben geschrieben."
