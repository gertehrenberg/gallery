#!/usr/bin/env bash
# clear_docker_logs.sh
# Truncate Docker container logs without restarting the container.
# Usage: ./clear_docker_logs.sh <container_name_or_id>

set -euo pipefail

CONTAINER="gallery"

# Header
cat << EOF

=== Docker Log Truncation Script ===
Container: $CONTAINER
=================================

EOF

# Verify container exists
if ! docker inspect "$CONTAINER" &>/dev/null; then
  echo "[ERROR] Container '$CONTAINER' does not exist."
  exit 1
fi

# Fetch log driver
DRIVER=$(docker inspect --format='{{.HostConfig.LogConfig.Type}}' "$CONTAINER")
echo "Log Driver: $DRIVER"
if [ "$DRIVER" != "json-file" ]; then
  echo
  echo "[ERROR] Unsupported log driver: '$DRIVER'"
  echo "This script only supports the 'json-file' driver."
  echo
  exit 1
fi

echo
# Fetch log file path
LOGPATH=$(docker inspect --format='{{.LogPath}}' "$CONTAINER")
echo "Log Path: $LOGPATH"

echo
# Validate log path (allow for permission issues)
if [ ! -e "$LOGPATH" ] && ! sudo test -e "$LOGPATH"; then
  echo "[ERROR] Log file not found at '$LOGPATH'."
  exit 1
fi

# Truncate the log file
echo
echo "Truncating log file..."
if [ -w "$LOGPATH" ]; then
  : > "$LOGPATH"
  echo "[OK] Log file truncated."
else
  echo "[INFO] Insufficient permissions; truncating with sudo..."
  sudo sh -c "> '$LOGPATH'"
  echo "[OK] Log file truncated with sudo."
fi


#!/bin/bash
for cid in $(docker ps -aq); do
    LOGFILE=$(docker inspect --format='{{.LogPath}}' $cid)
    [ -f "$LOGFILE" ] && sudo truncate -s 0 "$LOGFILE"
done

echo
# Restart the container to ensure fresh logging
echo "Restarting container '$CONTAINER'..."
docker restart "$CONTAINER" >/dev/null
if [ $? -eq 0 ]; then
  echo "[OK] Container '$CONTAINER' restarted successfully."
else
  echo "[ERROR] Failed to restart container '$CONTAINER'."
fi
