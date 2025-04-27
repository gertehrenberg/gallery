# Basis-Image: kleines, aktuelles Python
FROM python:3.11-slim

# Arbeitsverzeichnis setzen
WORKDIR /app

# System-Updates und wichtige Pakete installieren
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    libgl1 \
    libglib2.0-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Requirements zuerst kopieren und installieren (Caching!)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Jetzt erst den App-Code kopieren
COPY . .

# /app als Python-Importpfad setzen
ENV PYTHONPATH=/app

# Exponiere Port 8000 für FastAPI
EXPOSE 8000

# Healthcheck für Docker-Überwachung
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/ || exit 1

# Startbefehl für die App
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
