FROM python:3.11-slim-bookworm

# ---------------------------------------------
# SYSTEM-PAKETE
# ---------------------------------------------
RUN apt-get update && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
        libgl1 \
        libglib2.0-0 \
        zlib1g \
        zlib1g-dev \
        sqlite3 \
        curl \
        aspell \
        aspell-de \
        aspell-en \
        libssl-dev \
        file \
        locales \
        recoll \
        python3-recoll \
        wget \
    && sed -i -e 's/# de_DE.UTF-8 UTF-8/de_DE.UTF-8 UTF-8/' /etc/locale.gen \
    && dpkg-reconfigure --frontend=noninteractive locales \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------
# PYTHON-PAKETE (mutagen + deine requirements)
# ---------------------------------------------
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir mutagen && \
    pip install --no-cache-dir -r /app/requirements.txt

# ---------------------------------------------
# BENUTZER UND VERZEICHNISSE
# ---------------------------------------------
RUN useradd -m -u 1000 gallery && \
    mkdir -p /data/recoll_config /data/textfiles && \
    chown -R gallery:gallery /data /app

WORKDIR /app

# ---------------------------------------------
# APP kopieren
# ---------------------------------------------
COPY --chown=gallery:gallery app ./app

# ---------------------------------------------
# ENVIRONMENT
# ---------------------------------------------
ENV LANG=de_DE.UTF-8 \
    LANGUAGE=de_DE:de \
    LC_ALL=de_DE.UTF-8 \
    RECOLL_CONFDIR=/data/recoll_config \
    HOME=/data \
    XDG_CONFIG_HOME=/data

# ---------------------------------------------
# APP ALS USER STARTEN
# ---------------------------------------------
USER gallery

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload", "--log-level", "warning", "--no-access-log"]

