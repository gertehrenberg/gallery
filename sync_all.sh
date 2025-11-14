#!/bin/bash

# Dieses Script synchronisiert Dateien zwischen lokalem Ordner und Google Drive
# und sorgt zusätzlich dafür, dass *alle Dateinamen klein geschrieben* werden –
# sowohl lokal als auch remote (Drive). "Lowercasing" wird vor jedem Sync ausgeführt.

BASE_LOCAL="/home/ubuntu/gallery/cache/imagefiles"
BASE_REMOTE="gdrive_comfyui:cache/imagefiles"

FOLDERS=("bad" "comfyui" "delete" "document" "double" "gemini" "ki" "real" "recheck" "sex" "todo" "top")

LOG="/home/ubuntu/gallery/sync_all.log"
RC_OPTS="--fast-list --transfers 1 --checkers 1 --drive-chunk-size 32M"

# ----------------------------------------------
# Funktion: Alle lokalen Dateien kleinschreiben
# ----------------------------------------------
lowercase_local() {
    local DIR="$1"
    find "$DIR" -depth -exec bash -c '
        SRC="$0"; DIRNAME="$(dirname "$SRC")"; BASENAME="$(basename "$SRC")";
        LOWER="${BASENAME,,}";
        if [[ "$BASENAME" != "$LOWER" ]]; then
            mv -v "$SRC" "$DIRNAME/$LOWER"
        fi
    ' {} \;
}

# ----------------------------------------------
# Funktion: Remote-Dateien kleinschreiben via rclone moveto
# ----------------------------------------------
lowercase_remote() {
    local REMOTE_DIR="$1"

    rclone lsf -R "$REMOTE_DIR" | while read -r FILE; do
        LOWER="${FILE,,}"
        if [[ "$FILE" != "$LOWER" ]]; then
            echo "Remote rename: $FILE → $LOWER" >>"$LOG"
            rclone moveto "$REMOTE_DIR/$FILE" "$REMOTE_DIR/$LOWER" >>"$LOG" 2>&1
        fi
    done
}

# ------------------------------------------------
# Schritt 1: Start-Log
# ------------------------------------------------
echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Starte Sync + Lowercase..." >>"$LOG"

# ------------------------------------------------
# Schritt 2: Initial Sync (Drive → Lokal) ohne Remote-Rename
# ------------------------------------------------
for DIR in "${FOLDERS[@]}"; do
    LOCAL_DIR="$BASE_LOCAL/$DIR"
    REMOTE_DIR="$BASE_REMOTE/$DIR"

    mkdir -p "$LOCAL_DIR"

    echo "→ Initial Download: $DIR (Drive → Lokal, ohne Remote-Rename)" >>"$LOG"
    rclone sync "$REMOTE_DIR" "$LOCAL_DIR" $RC_OPTS >>"$LOG" 2>&1

    echo "→ Local Lowercase: $DIR" >>"$LOG"
    lowercase_local "$LOCAL_DIR"
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Initial-Sync abgeschlossen"." >>"$LOG"

# ------------------------------------------------
# Schritt 3: Periodischer Pull (Drive → Lokal) – nur lokal lowercase
# ------------------------------------------------
(
    while true; do
        sleep 600
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Periodischer Sync (Drive → Lokal)" >>"$LOG"
        for DIR in "${FOLDERS[@]}"; do
            LOCAL_DIR="$BASE_LOCAL/$DIR"
            REMOTE_DIR="$BASE_REMOTE/$DIR"

            rclone sync "$REMOTE_DIR" "$LOCAL_DIR" $RC_OPTS >>"$LOG" 2>&1
            lowercase_local "$LOCAL_DIR"
        done
    done
) &

# ------------------------------------------------
# Schritt 4: Watcher Upload (Lokal → Drive) – lokale Renames reichen!
# ------------------------------------------------
# ------------------------------------------------
# Schritt 4: Watcher Upload (Lokal → Drive) – wird jetzt NACH dem Initial-Sync gestartet
# ------------------------------------------------
start_watcher() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Watcher startet..." >>"$LOG"

    for DIR in "${FOLDERS[@]}"; do
    (
        SRC="$BASE_LOCAL/$DIR"
        DST="$BASE_REMOTE/$DIR"

        echo "→ Watch: $SRC" >>"$LOG"

        while true; do
            inotifywait -r -e close_write,create,delete,move "$SRC" >/dev/null 2>&1
            sleep 2
            lowercase_local "$SRC"
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Upload: $DIR..." >>"$LOG"
            rclone copy "$SRC" "$DST" $RC_OPTS >>"$LOG" 2>&1
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Upload abgeschlossen." >>"$LOG"
        done
    ) &
    done
}

# WICHTIG: Watcher erst nach vollständigem Initial-Sync starten
start_watcher # <<< hinzugefügt..." >>"$LOG"

for DIR in "${FOLDERS[@]}"; do
(
    SRC="$BASE_LOCAL/$DIR"
    DST="$BASE_REMOTE/$DIR"

    echo "→ Watch: $SRC" >>"$LOG"

    while true; do
        inotifywait -r -e close_write,create,delete,move "$SRC" >/dev/null 2>&1

        sleep 2

        lowercase_local "$SRC"

        echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Upload: $DIR..." >>"$LOG"
        rclone copy "$SRC" "$DST" $RC_OPTS >>"$LOG" 2>&1

        echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Upload abgeschlossen." >>"$LOG"
    done
) &
done
) &
done

wait
