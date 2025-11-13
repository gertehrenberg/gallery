#!/bin/bash
# ============================================================
#  Live-Sync zwischen imagefiles und Google Drive (Drive gewinnt)
#  Rekursive, intelligente Änderungserkennung mit Cooldown
#  Optimiert für geringe CPU- und I/O-Last
# ============================================================

SRC="/home/ubuntu/gallery/cache/imagefiles"
DST="gdrive_comfyui:cache/imagefiles"
LOG="/home/ubuntu/gallery/sync_imagefiles.log"

# rclone-Optionen für Performance und Stabilität
RC_OPTS="--fast-list --transfers 2 --checkers 2 --drive-chunk-size 64M --delete-during"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Initialer Download (Drive → Lokal)..." | tee -a "$LOG"
rclone sync "$DST" "$SRC" $RC_OPTS >>"$LOG" 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Initialer Download abgeschlossen." | tee -a "$LOG"

# Hintergrundprozess: periodischer Pull von Drive → Lokal (alle 10 Minuten)
(
    while true; do
        sleep 600
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Periodischer Sync (Drive → Lokal)..." | tee -a "$LOG"
        rclone sync "$DST" "$SRC" $RC_OPTS >>"$LOG" 2>&1
    done
) &

# Haupt-Loop: Rekursive Überwachung mit Cooldown
echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Überwache lokale Änderungen in $SRC (rekursiv)" | tee -a "$LOG"

while true; do
    # Auf Änderungen in allen Unterordnern warten
    inotifywait -r -e close_write,create,delete,move "$SRC" >/dev/null 2>&1

    # Cooldown – warte 5 Sekunden Ruhe, um Änderungen zu bündeln
    LAST_CHANGE=$(date +%s)
    while true; do
        sleep 2
        inotifywait -r -t 2 -e close_write,create,delete,move "$SRC" >/dev/null 2>&1 && LAST_CHANGE=$(date +%s)
        NOW=$(date +%s)
        if (( NOW - LAST_CHANGE >= 5 )); then
            break
        fi
    done

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Änderungen erkannt, starte Upload..." | tee -a "$LOG"
    rclone copy "$SRC" "$DST" $RC_OPTS >>"$LOG" 2>&1
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] → Upload abgeschlossen." | tee -a "$LOG"
done
