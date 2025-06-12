# 2. Services stoppen und Images entfernen
cd /home/gert_ehrenberg/n8n-docker/ && docker-compose down --rmi all
cd /home/gert_ehrenberg/gallery/ && docker-compose down --rmi all
cd /home/gert_ehrenberg/nsfw_service/ && docker-compose down --rmi all

# 3. System bereinigen
docker volume prune -f
docker builder prune -f
docker system prune -f

# 1. Netzwerk erstellen (falls noch nicht vorhanden)
docker network create n8n-netz

# 4. Services neu bauen
cd /home/gert_ehrenberg/n8n-docker/ && docker-compose build --no-cache
cd /home/gert_ehrenberg/gallery/ && docker-compose build --no-cache
cd /home/gert_ehrenberg/nsfw_service/ && docker-compose build --no-cache

# 5. Services starten
cd /home/gert_ehrenberg/n8n-docker/ && docker-compose up -d
cd /home/gert_ehrenberg/gallery/ && docker-compose up -d
cd /home/gert_ehrenberg/nsfw_service/ && docker-compose up -d