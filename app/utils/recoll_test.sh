
#!/bin/bash

# Fortschrittsbalken-Funktion
show_progress() {
    local duration=$1
    local msg=$2
    local width=50
    local progress=0

    echo -n "$msg ["
    while [ $progress -lt $width ]; do
        echo -n " "
        ((progress++))
    done
    echo -n "] 0%"

    progress=0
    while [ $progress -lt $width ]; do
        echo -ne "\r$msg ["
        local pos=0
        while [ $pos -lt $progress ]; do
            echo -n "="
            ((pos++))
        done
        while [ $pos -lt $width ]; do
            echo -n " "
            ((pos++))
        done
        local percent=$((progress*100/width))
        echo -n "] $percent%"
        ((progress++))
        sleep $(echo "scale=3; $duration/$width" | bc)
    done
    echo
}

# Logging-Funktion
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Fehlerprüfung-Funktion
check_error() {
    if [ $? -ne 0 ]; then
        log "❌ Fehler: $1"
        exit 1
    fi
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
show_progress 2 "Warte auf Prozessende"
log "✅ Alte Prozesse beendet"

# Verzeichnisse vorbereiten
log "🔄 Lösche altes Config-Verzeichnis..."
rm -rf "$CONFIG_DIR"
show_progress 1 "Lösche Verzeichnis"
log "✅ Config-Verzeichnis gelöscht"

log "🔄 Erstelle neue Verzeichnisse..."
mkdir -p "$CONFIG_DIR/xapiandb"
check_error "Konnte Config-Verzeichnis nicht erstellen"
mkdir -p "$TEXT_DIR"
check_error "Konnte Text-Verzeichnis nicht erstellen"
show_progress 1 "Erstelle Verzeichnisse"
log "✅ Verzeichnisse erstellt"

# Beispiel-Testdatei erstellen
log "🔄 Erstelle Test-Datei..."
echo "Dies ist ein Testdokument für Recoll" > "$TEXT_DIR/test.txt"
check_error "Konnte Test-Datei nicht erstellen"
log "✅ Test-Datei erstellt"

# Basis-Konfiguration erstellen
log "🔄 Erstelle recoll.conf..."
cat > "$CONFIG_DIR/recoll.conf" << EOL
topdirs = $TEXT_DIR
indexedmimetypes = text/plain text/*
skippednames = .* *~
followLinks = 1
loglevel = 6
logfilename = $CONFIG_DIR/recoll.log
daemloglevel = 6
dbdir = $CONFIG_DIR/xapiandb
idxflushmb = 10
filtermaxmbytes = 100
nomd5types = .txt
aspellLanguage = en
defaultcharset = UTF-8
EOL
check_error "Konnte recoll.conf nicht erstellen"

# Erstelle mimeconf
cat > "$CONFIG_DIR/mimeconf" << EOL
[index]
text/plain = txt;
EOL
check_error "Konnte mimeconf nicht erstellen"

show_progress 1 "Schreibe Konfiguration"
log "✅ Konfiguration erstellt"

# In das Config-Verzeichnis wechseln
cd "$CONFIG_DIR" || exit 1
log "✅ Arbeitsverzeichnis gewechselt zu: $CONFIG_DIR"

# Indexierung starten mit -Z für komplette Neuindexierung
log "🔄 Starte Indexierung..."
recollindex -c "$CONFIG_DIR" -Z -i > indexing.log 2>&1
INDEX_STATUS=$?

# Prüfe ob die Xapian-Datenbank erstellt wurde
if [ ! -d "$CONFIG_DIR/xapiandb" ]; then
    log "❌ Fehler: Xapian-Datenbank wurde nicht erstellt"
    log "📝 Indexierungs-Log:"
    cat indexing.log
    log "📊 Verzeichnisinhalt:"
    ls -la "$CONFIG_DIR"
    exit 1
fi

log "Indexierung beendet mit Status: $INDEX_STATUS"

# Wenn Indexierung erfolgreich, führe eine Test-Suche durch
if [ $INDEX_STATUS -eq 0 ]; then
    log "🔄 Führe Test-Suche durch..."
    recollq -c "$CONFIG_DIR" "test"
    SEARCH_STATUS=$?
    log "Suche beendet mit Status: $SEARCH_STATUS"
else
    log "❌ Indexierung fehlgeschlagen!"
    log "📝 Indexierungs-Log:"
    cat indexing.log
fi

# Zeige Inhalt des Log-Files
if [ -f "$CONFIG_DIR/recoll.log" ]; then
    log "📝 Inhalt von recoll.log:"
    cat "$CONFIG_DIR/recoll.log"
else
    log "❌ Keine Log-Datei gefunden!"
fi

# Zeige Datenbankstatus
log "📊 Datenbank-Status:"
ls -la "$CONFIG_DIR/xapiandb"

# Zeige laufende recoll Prozesse
log "📊 Laufende recoll Prozesse:"
ps aux | grep recoll | grep -v grep

log "🔵 Test beendet"