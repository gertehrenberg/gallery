#!/bin/bash

# Konstanten
TEXTFILE_CACHE_DIR="../../cache/textfiles/"
PROGRESS_BAR_WIDTH=50

# Farben für Output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Hilfsfunktionen
show_progress() {
    local current=$1
    local total=$2
    local percent=$((current * 100 / total))
    local filled=$((current * PROGRESS_BAR_WIDTH / total))
    local empty=$((PROGRESS_BAR_WIDTH - filled))

    printf "\r[%${filled}s%${empty}s] %3d%% (%d/%d)" | sed "s/ /▓/g;s/\./_/g" "" "$percent" "$current" "$total"
}

error_exit() {
    echo -e "${RED}❌ Fehler: $1${NC}" >&2
    exit 1
}

# Verzeichniswechsel mit Fehlerprüfung
cd "$TEXTFILE_CACHE_DIR" || error_exit "Kann nicht in Verzeichnis $TEXTFILE_CACHE_DIR wechseln"

# Dateien zählen
total_files=$(find . -type f | wc -l)
if [ "$total_files" -eq 0 ]; then
    error_exit "Keine Dateien gefunden"
fi

# Statistik-Variablen
current=0
renamed=0
deleted=0

echo -e "${YELLOW}🔄 Verarbeite $total_files Dateien...${NC}\n"

# Hauptverarbeitung
while IFS= read -r file; do
    [ -e "$file" ] || continue
    ((current++))

    # Fortschrittsanzeige aktualisieren
    show_progress "$current" "$total_files"

    dirname=$(dirname "$file")
    basename=$(basename "$file")
    lowerfile=$(echo "$basename" | tr '[:upper:]' '[:lower:]')
    full_lower="$dirname/$lowerfile"

    # Umbenennen wenn nötig
    if [[ "$basename" != "$lowerfile" ]]; then
        if mv -n "$file" "$full_lower"; then
            ((renamed++))
            file="$full_lower"
        fi
    fi

    # Nicht-TXT Dateien löschen
    if [[ ! "$file" == *.txt ]]; then
        if rm -f "$file"; then
            ((deleted++))
        fi
    fi

done < <(find . -type f)

# Abschlussbericht
echo -e "\n\n${GREEN}✅ Verarbeitung abgeschlossen${NC}"
echo -e "📊 Statistik:"
echo -e "   • Verarbeitete Dateien: $total_files"
echo -e "   • Umbenannte Dateien: $renamed"
echo -e "   • Gelöschte Nicht-TXT-Dateien: $deleted"

if [ $((renamed + deleted)) -eq 0 ]; then
    echo -e "\n${YELLOW}ℹ️ Keine Änderungen notwendig${NC}"
fi