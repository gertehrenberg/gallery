#!/bin/bash

# recoll_test.sh
# Logging-Funktion
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Konfiguration
CONFIG_DIR="/home/gert_ehrenberg/gallery/cache/recoll_config"
TEXT_DIR="/home/gert_ehrenberg/gallery/cache/textfiles"

log "🔵 Start Recoll Test"
log "📁 Config Dir: $CONFIG_DIR"
log "📁 Text Dir: $TEXT_DIR"

# Alte Prozesse beenden
log "🔄 Beende alte recoll Prozesse..."
pkill -f recollindex
sleep 2
log "✅ Alte Prozesse beendet"

# Verzeichnisse vorbereiten
log "🔄 Lösche altes Config-Verzeichnis..."
rm -rf "$CONFIG_DIR"
log "✅ Config-Verzeichnis gelöscht"

log "🔄 Erstelle neue Verzeichnisse..."
mkdir -p "$CONFIG_DIR"
mkdir -p "$TEXT_DIR"
log "✅ Verzeichnisse erstellt"

# Basis-Konfiguration erstellen
log "🔄 Erstelle recoll.conf..."
cat > "$CONFIG_DIR/recoll.conf" << EOL
topdirs = $TEXT_DIR
loglevel = 6
logfilename = $CONFIG_DIR/recoll.log
# Zusätzliche Debug-Optionen
pidfile = $CONFIG_DIR/recoll.pid
EOL
log "✅ recoll.conf erstellt"

# In das Config-Verzeichnis wechseln
cd "$CONFIG_DIR" || exit 1
log "✅ Arbeitsverzeichnis gewechselt zu: $CONFIG_DIR"

# Indexierung starten
log "🔄 Starte Indexierung..."
recollindex -c "$CONFIG_DIR" -i
INDEX_STATUS=$?
log "Indexierung beendet mit Status: $INDEX_STATUS"

# Wenn Indexierung erfolgreich, führe eine Test-Suche durch
if [ $INDEX_STATUS -eq 0 ]; then
    log "🔄 Führe Test-Suche durch..."
    recollq -c "$CONFIG_DIR" "test"
    SEARCH_STATUS=$?
    log "Suche beendet mit Status: $SEARCH_STATUS"
else
    log "❌ Indexierung fehlgeschlagen!"
fi

# Zeige Inhalt des Log-Files
if [ -f "$CONFIG_DIR/recoll.log" ]; then
    log "📝 Inhalt von recoll.log:"
    cat "$CONFIG_DIR/recoll.log"
else
    log "❌ Keine Log-Datei gefunden!"
fi

# Zeige laufende recoll Prozesse
log "📊 Laufende recoll Prozesse:"
ps aux | grep recoll | grep -v grep

log "🔵 Test beendet"
