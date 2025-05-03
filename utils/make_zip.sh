#!/bin/bash
# Gehe von utils/ in das übergeordnete gallery/
cd "$(dirname "$0")/.." || exit 1

zip -r gallery_export.zip . \
  -x "*.db" \
  -x "*.pyc" \
  -x "__pycache__/*" \
  -x ".git*" \
  -x ".idea/*" \
  -x "cache/*" \
  -x "*.zip"
