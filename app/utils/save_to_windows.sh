#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# Variablen für Farben
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging-Funktion
log() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') $1"
}

# Error-Logging-Funktion
error() {
    echo -e "${RED}$(date '+%Y-%m-%d %H:%M:%S') ERROR: $1${NC}" >&2
}

# Initialize variables
EXCLUDE_FILE=""
LOGFILE=""
PARTIAL_DIR=".rsync-partial"

# Definiere wichtige Pfade zuerst
SRC="/home/gert_ehrenberg/gallery"
DEST="/mnt/f/Sicherungen/$(hostname)"
NOW=$(date +"%Y-%m-%d_%H-%M")
LATEST="$DEST/backup_$NOW"
PREV=$(readlink -f "$DEST/letzte" 2>/dev/null || echo "")

# Cleanup-Function mit zusätzlicher Fehlerbehandlung
cleanup() {
    local exit_code=$?
    if [[ -n "$EXCLUDE_FILE" && -f "$EXCLUDE_FILE" ]]; then
        rm -f "$EXCLUDE_FILE"
    fi
    if [[ $exit_code -ne 0 ]]; then
        error "Skript wurde mit Fehlercode $exit_code beendet"
    fi
}

# Register cleanup function
trap cleanup EXIT
trap 'error "Skript wurde unterbrochen"; exit 1' INT TERM

# 0) Prüfen, ob Ziel gemountet ist
if ! mountpoint -q /mnt/f; then
    error "Fehler: /mnt/f ist nicht gemountet."
    exit 1
fi

# 1) Prüfe Quellverzeichnis
if [[ ! -d "$SRC" ]]; then
    error "Fehler: Quellverzeichnis $SRC existiert nicht."
    exit 1
fi

# Erstelle Zielverzeichnis
mkdir -p "$DEST" || {
    error "Fehler: Konnte Zielverzeichnis $DEST nicht erstellen."
    exit 1
}

# 2) Überprüfe Festplattenplatz
required_space=$(du -sb "$SRC" | cut -f1)
available_space=$(df -B1 --output=avail "$DEST" | tail -n1)
if [[ $available_space -lt $required_space ]]; then
    error "Nicht genügend Speicherplatz verfügbar."
    echo -e "${YELLOW}Benötigt: $(numfmt --to=iec $required_space)"
    echo -e "Verfügbar: $(numfmt --to=iec $available_space)${NC}"
    exit 1
fi

# 3) Exclude-Datei erstellen
EXCLUDE_FILE=$(mktemp) || {
    error "Fehler: Konnte temporäre Exclude-Datei nicht erstellen."
    exit 1
}

# Exclude-Patterns
cat <<EOF > "$EXCLUDE_FILE"
*.pyc
*.log
*.zip
.git/
__pycache__/
.idea/
.venv/
lost+found/
tmp/
temp/
.Trash*/
.rsync-partial/
*.swp
*.tmp
thumbnails/
EOF

# 4) Logging aktivieren
LOGFILE="$DEST/backup_$NOW.log"
exec > >(tee -a "$LOGFILE") 2>&1

log "${GREEN}Starte Backup: $NOW${NC}"
log "Quelle: $SRC"
log "Ziel: $LATEST"

# 5) Rsync mit maximaler Fortschrittsanzeige und Wiederaufnahme-Funktion
RSYNC_OPTS=(
    -aHAX                    # Archive mode + ACLs + extended attributes
    --delete                 # Delete extraneous files
    --delete-excluded        # Delete excluded files too
    --exclude-from="$EXCLUDE_FILE"
    --partial               # Behalte teilweise übertragene Dateien
    --partial-dir="$PARTIAL_DIR"  # Relativer Pfad für partial-dir
    --delay-updates         # Verzögere Updates bis zum Schluss
    --progress             # Zeige Fortschritt
    --verbose              # Ausführliche Ausgabe
    --stats                # Zeige Statistiken am Ende
    --human-readable       # Menschenlesbare Größen
    --numeric-ids          # Keine UID/GID-Auflösung
    --one-file-system      # Bleibe im selben Dateisystem
    --checksum            # Nutze Checksummen statt Zeitstempel
    --timeout=180         # Timeout nach 3 Minuten
)

# Verbesserte Backup-Fortsetzungslogik
if [[ -d "$LATEST" ]]; then
    if [[ -d "$LATEST/$PARTIAL_DIR" ]]; then
        log "${YELLOW}Vorheriges unvollständiges Backup gefunden: $LATEST"
        log "Setze Backup fort...${NC}"
    else
        mkdir -p "$LATEST/$PARTIAL_DIR"
        chmod 755 "$LATEST/$PARTIAL_DIR"
    fi
else
    log "${GREEN}Starte neues Backup: $LATEST${NC}"
    mkdir -p "$LATEST/$PARTIAL_DIR"
    chmod 755 "$LATEST/$PARTIAL_DIR"
fi

# Wenn ein vorheriges vollständiges Backup existiert
if [[ -d "$PREV" ]]; then
    log "Nutze vorheriges Backup für Hardlinks: $PREV"
    RSYNC_OPTS+=("--link-dest=$PREV")
fi

# Führe rsync aus
log "${GREEN}Starte Synchronisation...${NC}"
if ! rsync "${RSYNC_OPTS[@]}" "$SRC/" "$LATEST/"; then
    error "rsync wurde unterbrochen oder ist fehlgeschlagen."
    log "${YELLOW}Sie können das Backup später mit demselben Befehl fortsetzen."
    log "Teilweise übertragene Dateien bleiben in $LATEST/$PARTIAL_DIR${NC}"
    exit 1
fi

# Nach erfolgreichem rsync
rm -rf "$LATEST/$PARTIAL_DIR"

# 6) "letzte"-Symlink aktualisieren
if ! ln -nfs "$LATEST" "$DEST/letzte"; then
    error "Warnung: Konnte Symlink nicht aktualisieren"
fi

# Erfolgreicher Abschluss
log "${GREEN}Backup erfolgreich abgeschlossen: $LATEST"
log "Logdatei: $LOGFILE${NC}"

# Zeige Backup-Statistiken
echo -e "\n${GREEN}Backup-Statistiken:${NC}"
du -sh "$LATEST"
echo "Anzahl Dateien: $(find "$LATEST" -type f | wc -l)"
echo "Anzahl Verzeichnisse: $(find "$LATEST" -type d | wc -l)"
echo "Backup-Größe: $(du -sh "$LATEST" | cut -f1)"
if [[ -n "$PREV" ]]; then
    echo "Vorheriges Backup: $(du -sh "$PREV" | cut -f1)"
fi