#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${1:-docker-compose.yml}"

echo "=== Docker Compose: Clean Restart ==="
echo "Using compose file: $COMPOSE_FILE"
echo

# In Verzeichnis der YML wechseln
COMPOSE_DIR=$(dirname "$COMPOSE_FILE")
cd "$COMPOSE_DIR"

# [1/3] Stoppen + Entfernen
echo "[1/3] Stopping and removing containers..."
docker-compose -f "$COMPOSE_FILE" down

# [2/3] Neu erstellen & starten
echo "[2/3] Recreating containers..."
docker-compose -f "$COMPOSE_FILE" up -d --force-recreate --remove-orphans

# [3/3] Logs anzeigen
echo
echo "[3/3] Showing logs..."
echo "--------------------------------------"
docker-compose -f "$COMPOSE_FILE" logs --tail=50
echo "--------------------------------------"

echo
echo "[✔] Clean restart complete."
