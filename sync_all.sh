#!/bin/bash

BASE_LOCAL="/home/ubuntu/gallery/cache/imagefiles"
BASE_REMOTE="gdrive_comfyui:cache/imagefiles"

# Nur diese beiden Ordner syncen
FOLDERS=("ki" "sex")

LOG="/home/ubuntu/gallery/sync_all.log"

RC_OPTS="--fast-list --transfers 1 --checkers 1 --drive-chunk-size 32M"

# -----------------------------
# Schritt 1: Start-Log
# -----------------------------
echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Starte Mini-Sync (ki + sex)..." >>"$LOG"

# -----------------------------
# Schritt 2: Initial Sync (Drive → Lokal)
# -----------------------------
for DIR in "${FOLDERS[@]}"; do
    echo "→ Initial Download $DIR (Drive → Lokal)" >>"$LOG"
    mkdir -p "$BASE_LOCAL/$DIR"
    rclone sync "$BASE_REMOTE/$DIR" "$BASE_LOCAL/$DIR" $RC_OPTS >>"$LOG" 2>&1
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Initial-Sync abgeschlossen." >>"$LOG"

# -----------------------------
# Schritt 3: Periodischer Pull (Drive → Lokal)
# -----------------------------
(
    while true; do
        sleep 600
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Periodischer Sync (Drive → Lokal)" >>"$LOG"
        for DIR in "${FOLDERS[@]}"; do
            rclone sync "$BASE_REMOTE/$DIR" "$BASE_LOCAL/$DIR" $RC_OPTS >>"$LOG" 2>&1
        done
    done
) &

# -----------------------------
# Schritt 4: Watcher Upload (Lokal → Drive)
# -----------------------------
echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Watcher startet..." >>"$LOG"

for DIR in "${FOLDERS[@]}"; do
(
    SRC="$BASE_LOCAL/$DIR"
    DST="$BASE_REMOTE/$DIR"

    echo "→ Watch: $SRC" >>"$LOG"

    while true; do
        inotifywait -r -e close_write,create,delete,move "$SRC" >/dev/null 2>&1

        sleep 2

        echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Upload $DIR..." >>"$LOG"
        rclone copy "$SRC" "$DST" $RC_OPTS >>"$LOG" 2>&1
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Upload $DIR abgeschlossen." >>"$LOG"
    done
) &
done

wait
