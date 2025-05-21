#!/bin/bash

TEXTFILE_CACHE_DIR="../cache/textfiles/"
cd "$TEXTFILE_CACHE_DIR" || exit 1

# Schritt 1: Alle Dateien zählen
total_files=$(find . -type f | wc -l)
current=0

echo "🔄 Starte rekursives Umbenennen auf Kleinbuchstaben und Löschen aller Nicht-TXT-Dateien ... ($total_files Dateien)"

# Durch alle Dateien iterieren
while IFS= read -r file; do
    [ -e "$file" ] || continue

    dirname=$(dirname "$file")
    basename=$(basename "$file")
    lowerfile=$(echo "$basename" | tr '[:upper:]' '[:lower:]')
    full_lower="$dirname/$lowerfile"

    ((current++))

    if [[ "$basename" != "$lowerfile" ]]; then
        echo "[$current/$total_files] Renaming: $file -> $full_lower"
        mv -n "$file" "$full_lower"
        file="$full_lower"
    else
        echo "[$current/$total_files] OK: $file"
    fi

    # Jetzt prüfen, ob es **keine** .txt-Datei ist → löschen
    if [[ ! "$file" == *.txt ]]; then
        echo "    🗑️ Lösche Nicht-TXT-Datei: $file"
        rm -f "$file"
    fi

done < <(find . -type f)

echo "✅ Alle $total_files Dateien geprüft, umbenannt und alle Nicht-TXT-Dateien gelöscht!"
