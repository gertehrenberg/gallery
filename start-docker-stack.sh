#!/bin/bash

# Netzwerkname
NETWORK_NAME="n8n-netz"

# 1. Docker-Daemon starten, falls nicht läuft
if ! pgrep dockerd > /dev/null; then
  echo "🛠️ Starte Docker-Daemon ..."
  nohup dockerd > ~/.log/dockerd.log 2>&1 &
  sleep 3
fi

# 2. Docker-Netzwerk erstellen (wenn noch nicht vorhanden)
if ! docker network ls | grep -q "$NETWORK_NAME"; then
  echo "🌐 Erzeuge Docker-Netzwerk: $NETWORK_NAME"
  docker network create "$NETWORK_NAME"
else
  echo "🌐 Netzwerk $NETWORK_NAME existiert bereits"
fi

# 3. Container in Reihenfolge neustarten
echo "🔁 Starte n8n..."
docker restart n8n-docker

echo "🔁 Starte gallery..."
docker restart gallery

echo "🔁 Starte nsfw-service..."
docker restart nsfw-service

sleep 3  # kleine Wartezeit

echo "🔁 Starte caddy..."
docker restart caddy

echo "✅ Alles läuft wieder!"
