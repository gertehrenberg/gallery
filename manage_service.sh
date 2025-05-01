#!/bin/bash

# Konfiguration
ZIEL="$HOME/gallery"
IMAGE_NAME="gallery"
CONTAINER_NAME="gallery"
NETWORK_NAME="n8n-netz"

# Zielverzeichnis sicherstellen
mkdir -p "$ZIEL" "$AUTOBACKUP"
cd "$ZIEL" || exit 1

# 1. Bestehenden Container stoppen und entfernen
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "[Info] Stoppe und entferne bestehenden Container '${CONTAINER_NAME}'..."
    docker stop "${CONTAINER_NAME}"
    docker rm "${CONTAINER_NAME}"
else
    echo "[Info] Kein bestehender Container '${CONTAINER_NAME}' gefunden."
fi

# 2. Docker-Netzwerk prüfen oder erstellen
if ! docker network ls --format '{{.Name}}' | grep -q "^${NETWORK_NAME}$"; then
    echo "[Info] Erstelle Docker-Netzwerk '${NETWORK_NAME}'..."
    docker network create "${NETWORK_NAME}"
else
    echo "[Info] Docker-Netzwerk '${NETWORK_NAME}' existiert bereits."
fi

# 3. Image bauen
echo "[Info] Baue Docker-Image '${IMAGE_NAME}'..."
docker build -t "${IMAGE_NAME}" .

# 4. Container starten

echo "[Info] Starte neuen Container '${CONTAINER_NAME}'..."
docker run -d \
  --name "${CONTAINER_NAME}" \
  --user 1000:1000 \
  --network "${NETWORK_NAME}" \
  -p 8000:8000 \
  -v "$(pwd):/app" \
  -v "$PWD/secrets:/app/secrets" \
  -v "$PWD/cache:/data" \
  -v "$PWD/cache/thumbnailfiles300:/app/thumbnails" \
  -v "$PWD/cache/imagefiles:/app/imagefiles" \
  gallery

echo "[Fertig] läuft unter: https://levellevel.me/gallery/?page=1&count=3&folder=real"
