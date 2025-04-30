#!/bin/bash

# Verzeichnis, in dem die Textfiles liegen
TEXTFILE_CACHE_DIR="./cache/imagefiles/double/"

# Wechsel ins Verzeichnis
cd "$TEXTFILE_CACHE_DIR" || exit 1

# Alle Dateien zählen
total_files=$(find . -maxdepth 1 -type f | wc -l)
current=0

echo "🔄 Starte Umbenennen auf Kleinbuchstaben ... ($total_files Dateien)"

for file in *; do
    [ -e "$file" ] || continue

    lowerfile=$(echo "$file" | tr '[:upper:]' '[:lower:]')

    if [ "$file" != "$lowerfile" ]; then
        echo "[$((++current))/$total_files] Renaming: $file -> $lowerfile"
        mv -n "$file" "$lowerfile"
    else
        ((current++))
    fi
done

echo "✅ Alle $total_files Dateien geprüft und ggf. umbenannt!"
