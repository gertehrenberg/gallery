#!/usr/bin/env bash

# create_pod.sh
# Skript: On-Demand-Pod mit persistentem Volume auf Runpod
# Unterstützt Fallback auf alternative GPU-Typen bei Ressourcenknappheit
# Konfigurierbar via Umgebungsvariablen:
#   RUNPOD_API_KEY, GPU_TYPE_IDS (CSV), GPU_COUNT, VCPU_COUNT,
#   MEMORY_GB, CONTAINER_DISK_GB, VOLUME_GB, VOLUME_KEY, SSH_PUBLIC_KEY

# ------------------------- Konfiguration -------------------------
API_KEY="${RUNPOD_API_KEY:-DEIN_RUNPOD_API_KEY}"
GRAPHQL_URL="https://api.runpod.io/graphql?api_key=$API_KEY"

# Liste möglicher GPU-Typ-IDs (CSV), z.B.: "NVIDIA L40,NVIDIA A100"
GPU_TYPE_IDS="${GPU_TYPE_IDS:-NVIDIA L40}"
IFS=',' read -r -a GPU_TYPES <<< "$GPU_TYPE_IDS"

# Standard-Pod-Parameter (überschreibbar via Umgebungsvariablen)
POD_NAME="${POD_NAME:-Runpod Pytorch 2.4.0}"
IMAGE_NAME="${IMAGE_NAME:-runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04}"
GPU_COUNT="${GPU_COUNT:-1}"
VCPU_COUNT="${VCPU_COUNT:-8}"
MEMORY_GB="${MEMORY_GB:-94}"
CONTAINER_DISK_GB="${CONTAINER_DISK_GB:-150}"
VOLUME_GB="${VOLUME_GB:-150}"
VOLUME_KEY="${VOLUME_KEY:-9qrithbo7q}"
VOLUME_MOUNT_PATH="${VOLUME_MOUNT_PATH:-/workspace}"

# SSH-Public-Key für den Pod (oder aus Env SSH_PUBLIC_KEY)
SSH_KEY="${SSH_PUBLIC_KEY:-ssh-ed25519 AAAAC3Nza…/gert_ehrenberg@GERT-NITRO5}"
# Ports im CSV-Format
PORTS="${PORTS:-80/http,8188/http,8888/http,5678/http,11434/http,8001/http,8002/http,8003/http,8004/http,22/tcp}"

# ----------------------- Deploy-Funktion -------------------------
deploy_with_gpu() {
  local gpu_id="$1"
  echo "Versuche Deployment mit GPU-Typ: $gpu_id"

    # Baue JSON-Payload (einzeilig im Query-String, neue Zeilen außerhalb des Strings)
  PAYLOAD=$(cat <<EOF
{
  "query": "mutation podFindAndDeployOnDemand { podFindAndDeployOnDemand(input: { cloudType: ALL, gpuCount: ${GPU_COUNT}, volumeInGb: ${VOLUME_GB}, containerDiskInGb: ${CONTAINER_DISK_GB}, minVcpuCount: ${VCPU_COUNT}, minMemoryInGb: ${MEMORY_GB}, gpuTypeId: \"${gpu_id}\", name: \"${POD_NAME}\", imageName: \"${IMAGE_NAME}\", dockerArgs: \"\", ports: \"${PORTS}\", volumeMountPath: \"${VOLUME_MOUNT_PATH}\", env: [{key: \"PUBLIC_KEY\", value: \"${SSH_KEY}\"}], volumeKey: \"${VOLUME_KEY}\" }){ id desiredStatus } }",
  "variables": {}
}
EOF
)

  RESPONSE=$(curl --silent --show-error \
    --request POST \
    --url "$GRAPHQL_URL" \
    --header 'Content-Type: application/json' \
    --data "$PAYLOAD")

  # Prüfen auf GraphQL-Fehler
  if echo "$RESPONSE" | grep -q '"errors"'; then
    local err_msg
    err_msg=$(echo "$RESPONSE" | grep -o '"message":"[^"]*"' | head -1 | sed 's/"message":"\([^"]*\)"/\1/')
    echo "  Fehler: $err_msg"
    # Bei fehlenden Ressourcen weiter versuchen
    if [[ "$err_msg" == *"no longer any instances available"* ]]; then
      return 1
    else
      echo "  Abbruch aufgrund unerwartetem Fehler."
      exit 1
    fi
  fi

  # Erfolg: ID und Status extrahieren
  local pod_id status
  pod_id=$(echo "$RESPONSE" | grep -o '"id":"[^"]*"' | sed 's/"id":"\([^"]*\)"/\1/')
  status=$(echo "$RESPONSE" | grep -o '"desiredStatus":"[^"]*"' | sed 's/"desiredStatus":"\([^"]*\)"/\1/')
  echo "Pod erstellt: ID=$pod_id, Status=$status, GPU-Typ=$gpu_id"
  exit 0
}

# --------------- Hauptskript: GPU-Fallback ----------------------
echo "Starte Deployment für Pod: $POD_NAME"
for gpu in "${GPU_TYPES[@]}"; do
  deploy_with_gpu "$gpu" && break
  echo "  Nächster GPU-Typ…"
done

echo "Alle GPU-Typen ausprobiert – Deployment fehlgeschlagen."
exit 1

# -----------------------------------------------------------------
