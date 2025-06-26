#!/bin/bash

# Konstanten
TEXTFILE_CACHE_DIR="../../cache/imagefiles/"
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

# Zähle zuerst alle zu verarbeitenden Dateien und Verzeichnisse
echo -e "${YELLOW}🔍 Suche nach Dateien und Verzeichnissen mit Großbuchstaben...${NC}"
total_files=$(find . \( -type f -o -type d \) -name '*[A-Z]*' | wc -l)

if [ "$total_files" -eq 0 ]; then
    echo -e "${GREEN}✅ Keine Dateien oder Verzeichnisse mit Großbuchstaben gefunden.${NC}"
else
    echo -e "${YELLOW}🔄 Gefunden: $total_files Einträge mit Großbuchstaben${NC}\n"
fi

# Statistik-Variablen
current=0
renamed_files=0
renamed_dirs=0

# Verzeichnisse zuerst (von unten nach oben) umbenennen
find . -depth -type d -name '*[A-Z]*' -print0 | while IFS= read -r -d '' dir; do
    ((current++))
    show_progress "$current" "$total_files"

    dirname=$(dirname "$dir")
    basename=$(basename "$dir")
    lowerdir=$(echo "$basename" | tr '[:upper:]' '[:lower:]')
    full_lower="$dirname/$lowerdir"

    if [ "$basename" != "$lowerdir" ]; then
        if mv -n "$dir" "$full_lower"; then
            ((renamed_dirs++))
        fi
    fi
done

# Dann die Dateien umbenennen
find . -type f -name '*[A-Z]*' -print0 | while IFS= read -r -d '' file; do
    ((current++))
    show_progress "$current" "$total_files"

    dirname=$(dirname "$file")
    basename=$(basename "$file")
    lowerfile=$(echo "$basename" | tr '[:upper:]' '[:lower:]')
    full_lower="$dirname/$lowerfile"

    if [ "$basename" != "$lowerfile" ]; then
        if mv -n "$file" "$full_lower"; then
            ((renamed_files++))
        fi
    fi
done

echo -e "\n\n${YELLOW}🗑️ Lösche .txt Dateien...${NC}"
deleted_count=0
while IFS= read -r -d '' txt_file; do
    rm -f "$txt_file"
    ((deleted_count++))
done < <(find . -type f -name "*.txt" -print0)

# Abschlussbericht
echo -e "\n${GREEN}✅ Verarbeitung abgeschlossen${NC}"
echo -e "📊 Statistik:"
echo -e "   • Umbenannte Verzeichnisse: $renamed_dirs"
echo -e "   • Umbenannte Dateien: $renamed_files"
echo -e "   • Gelöschte .txt Dateien: $deleted_count"

if [ $((renamed_files + renamed_dirs + deleted_count)) -eq 0 ]; then
    echo -e "\n${YELLOW}ℹ️ Keine Änderungen notwendig${NC}"
fi