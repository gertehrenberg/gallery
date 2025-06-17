#!/bin/bash

TEXTFILE_CACHE_DIR="../cache/imagefiles/"
cd "$TEXTFILE_CACHE_DIR" || exit 1

total_files=$(find . -type f | wc -l)
current=0

echo "🔄 Starte rekursives Umbenennen auf Kleinbuchstaben ... ($total_files Dateien)"

# Umbenennen aller Dateien auf Kleinbuchstaben
while IFS= read -r file; do
    [ -e "$file" ] || continue

    dirname=$(dirname "$file")
    basename=$(basename "$file")
    lowerfile=$(echo "$basename" | tr '[:upper:]' '[:lower:]')
    full_lower="$dirname/$lowerfile"

    ((current++))

    if [ "$basename" != "$lowerfile" ]; then
        echo "[$current/$total_files] Renaming: $file -> $full_lower"
        mv -n "$file" "$full_lower"
    else
        echo "[$current/$total_files] OK: $file"
    fi
done < <(find . -type f)

echo "🗑️ Lösche alle .txt-Dateien ..."

# Löschen aller .txt-Dateien (rekursiv)
find . -type f -iname "*.txt" -exec rm -v {} \;

echo "✅ Alle $total_files Dateien geprüft und ggf. umbenannt. Alle .txt-Dateien wurden gelöscht."
