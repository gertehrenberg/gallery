#!/bin/bash

# Verzeichnis wechseln
cd ~/gallery/cache/imagefiles || exit 1

# Zielordner für "doppelte" anlegen
mkdir -p double

# Alle Dateinamen in Kleinbuchstaben speichern
declare -A lowercase_files

echo "🔄 Verschiebe Großbuchstaben-Duplikate nach double/ ..."

for file in *; do
    # nur echte Dateien
    [ -f "$file" ] || continue

    lc_name=$(echo "$file" | tr '[:upper:]' '[:lower:]')

    if [[ -n "${lowercase_files[$lc_name]}" ]]; then
        # Es gibt schon eine Datei mit diesem "Kleinbuchstaben-Namen"
        upper_count_current=$(echo "$file" | grep -o '[A-Z]' | wc -l)
        upper_count_existing=$(echo "${lowercase_files[$lc_name]}" | grep -o '[A-Z]' | wc -l)

        if (( upper_count_current > upper_count_existing )); then
            echo "🚚 Verschiebe $file → double/"
            mv "$file" double/
        else
            echo "🚚 Verschiebe ${lowercase_files[$lc_name]} → double/"
            mv "${lowercase_files[$lc_name]}" double/
            lowercase_files[$lc_name]="$file"
        fi
    else
        lowercase_files[$lc_name]="$file"
    fi
done

echo "✅ Fertig! Alles verschoben."
