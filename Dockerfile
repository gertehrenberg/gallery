FROM python:3.11-slim-bullseye

# Setze Arbeitsverzeichnis
WORKDIR /app

# System-Pakete installieren
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    apt-get update --fix-missing && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        libgl1 \
        libglib2.0-0 \
        zlib1g \
        zlib1g-dev \
        sqlite3 \
        curl \
        recoll \
        python3-recoll \
        aspell \
        aspell-de \
        aspell-en \
        libssl-dev \
        file \
        locales && \
    # Locale auf Deutsch setzen
    sed -i -e 's/# de_DE.UTF-8 UTF-8/de_DE.UTF-8 UTF-8/' /etc/locale.gen && \
    dpkg-reconfigure --frontend=noninteractive locales && \
    # Aufräumen
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Erstelle Benutzer und Verzeichnisse
RUN useradd -m -u 1000 gallery && \
    mkdir -p /data/recoll_config /data/textfiles && \
    chown -R gallery:gallery /data /app

# Projektdateien kopieren
COPY --chown=gallery:gallery requirements.txt .
COPY --chown=gallery:gallery app ./app

# Python-Abhängigkeiten installieren
RUN pip install --no-cache-dir -r requirements.txt

# Benutzer wechseln
USER gallery

# Recoll-Konfiguration erstellen
RUN echo "\
topdirs = /data/textfiles\n\
indexedmimetypes = text/plain text/*\n\
skippednames = .* *~\n\
followLinks = 1\n\
loglevel = 6\n\
logfilename = /data/recoll_config/recoll.log\n\
daemloglevel = 6\n\
idxflushmb = 10\n\
filtermaxmbytes = 100\n\
nomd5types = .txt\n\
aspellLanguage = de\n\
defaultcharset = UTF-8\n" > /data/recoll_config/recoll.conf

# Umgebungsvariablen
ENV LANG=de_DE.UTF-8 \
    LANGUAGE=de_DE:de \
    LC_ALL=de_DE.UTF-8 \
    RECOLL_CONFDIR=/data/recoll_config

# Port freigeben
EXPOSE 8000

# Anwendung starten
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--reload"]
