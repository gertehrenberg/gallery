#!/bin/bash
# Gehe von utils/ in das übergeordnete gallery/
cd "$(dirname "$0")/.." || exit 1

# Aktuelles Datum/Zeit-Format: Jahr-Monat-Tag_Stunde-Minute
timestamp=$(date +"%Y-%m-%d_%H-%M")

# Ziel-Dateiname mit Zeitstempel
zipfile="./cache/zips/${timestamp}_gallery_export.zip"

zip -r "$zipfile" . \
  -x "*.db" \
  -x "*.pyc" \
  -x "__pycache__/*" \
  -x ".git*" \
  -x ".idea/*" \
  -x "cache/*" \
  -x "*.zip"
