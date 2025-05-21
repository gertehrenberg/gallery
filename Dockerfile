FROM python:3.11-slim-bullseye

WORKDIR /app

# Nur wirklich nötige Pakete installieren
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    sqlite3 \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
COPY app ./app

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
