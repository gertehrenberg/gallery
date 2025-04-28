#!/bin/bash

# Verzeichnis, in dem die Textfiles liegen
TEXTFILE_CACHE_DIR="./cache/imagefiles/"

# Wechsel ins Verzeichnis
cd "$TEXTFILE_CACHE_DIR" || exit 1

# Alle *.txt Dateien durchgehen
for file in *; do
    # Nur wenn die Datei existiert (Schutz falls keine *.txt da sind)
    [ -e "$file" ] || continue

    # Neuer Dateiname (nur Kleinbuchstaben)
    lowerfile=$(echo "$file" | tr '[:upper:]' '[:lower:]')

    # Nur umbenennen, wenn alter Name und neuer Name unterschiedlich sind
    if [ "$file" != "$lowerfile" ]; then
        echo "Renaming: $file -> $lowerfile"
        mv -n "$file" "$lowerfile"
    fi
done

echo "✅ Alle Imagefiles auf Kleinschreibung geprüft!"
