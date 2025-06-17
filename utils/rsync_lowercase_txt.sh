#!/bin/bash

SRC="/mnt/d/e-save/Bilder_mit_Text"
DST="../cache/textfiles"

echo "🔄 Starte Kopieren aller .txt-Dateien mit Umbenennung auf Kleinbuchstaben..."

find "$SRC" -type f -iname "*.txt" | while read -r srcfile; do
    relpath="${srcfile#$SRC/}"                              # Relativer Pfad
    lowerpath=$(echo "$relpath" | tr '[:upper:]' '[:lower:]')  # In Kleinbuchstaben
    dstfile="$DST/$lowerpath"

    mkdir -p "$(dirname "$dstfile")"                         # Zielverzeichnis anlegen
    rsync -a "$srcfile" "$dstfile"                           # Datei kopieren
    echo "✔️  $relpath → $lowerpath"
done

echo "✅ Alle .txt-Dateien kopiert und in Kleinbuchstaben geschrieben."
