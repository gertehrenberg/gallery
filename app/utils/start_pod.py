import os
import requests
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create():
    """
    Erstellt einen neuen On-Demand-Pod mit persistentem Volume via podFindAndDeployOnDemand.
    """
    load_dotenv()
    API_KEY = os.getenv("RUNPOD_API_KEY")

    if not API_KEY:
        logger.error("RUNPOD_API_KEY nicht gefunden. Bitte Umgebungsvariable setzen.")
        return

    # GraphQL-Endpoint mit API-Key im Query-String
    url = f"https://api.runpod.io/graphql?api_key={API_KEY}"
    headers = {"Content-Type": "application/json"}

    # Dynamische Werte für Mutation
    name = "Runpod Pytorch 2.4.0"
    image = "runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04"
    gpu_type = "r43bddujf2bd"
    env_entry = os.getenv("SSH_PUBLIC_KEY", "ssh-ed25519 AAAAC3Nza…/gert_ehrenberg@GERT-NITRO5")
    env_list = [{"key": "PUBLIC_KEY", "value": env_entry}]
    ports = ",".join([
        "80/http", "8188/http", "8888/http", "5678/http",
        "11434/http", "8001/http", "8002/http", "8003/http",
        "8004/http", "22/tcp"
    ])

    # Inline-Mutation für On-Demand
    mutation = f'''{{
      podFindAndDeployOnDemand(
        input: {{
          cloudType: ALL,
          gpuCount: 1,
          volumeInGb: 150,
          containerDiskInGb: 150,
          minVcpuCount: 8,
          minMemoryInGb: 94,
          gpuTypeId: "{gpu_type}",
          name: "{name}",
          imageName: "{image}",
          dockerArgs: "",
          ports: "{ports}",
          volumeMountPath: "/workspace",
          env: [{{ key: "PUBLIC_KEY", value: "{env_entry}" }}]
        }}
      ) {{ id desiredStatus volumeId }}
    }}'''

    try:
        resp = requests.post(url, headers=headers, json={"query": mutation})
        resp.raise_for_status()
        payload = resp.json()
        if errors := payload.get("errors"):
            logger.error(f"GraphQL-Fehler: {errors}")
            return
        data = payload.get("data", {}).get("podFindAndDeployOnDemand")
        if data and data.get("id"):
            logger.info(
                f"Pod erstellt: ID={data['id']}, "
                f"Status={data['desiredStatus']}, Volume={data['volumeId']}"
            )
        else:
            logger.error(f"Unerwartete Antwort beim Erstellen des Pods: {payload}")
    except requests.exceptions.RequestException as err:
        logger.error(f"HTTP-Fehler bei Pod-Erstellung: {err}")
    except Exception:
        logger.exception("Unerwarteter Fehler beim Erstellen des Pods.")


def p2():
    """
    Listet alle Pods und Network-Volumes auf.
    """
    load_dotenv()
    API_KEY = os.getenv("RUNPOD_API_KEY")
    if not API_KEY:
        logger.error("RUNPOD_API_KEY nicht gefunden. Bitte Umgebungsvariable setzen.")
        return

    # Pods auflisten mittels Python-SDK
    import runpod
    runpod.api_key = API_KEY
    try:
        pods = runpod.get_pods()
        for pod in pods:
            logger.info(pod)
    except Exception as e:
        logger.error(f"Fehler beim Abruf der Pods: {e}")

    # Volumes auflisten via REST-API
    url_vol = "https://rest.runpod.io/v1/networkvolumes"
    headers_vol = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    try:
        resp = requests.get(url_vol, headers=headers_vol)
        resp.raise_for_status()
        volumes = resp.json()
        for v in volumes:
            logger.info(
                f"{v['id']}: {v['name']} ({v['size']} GB) "
                f"in DC {v['dataCenterId']}"
            )
    except requests.exceptions.RequestException as err:
        logger.error(f"HTTP-Fehler beim Abruf der Volumes: {err}")
    except Exception:
        logger.exception("Unerwarteter Fehler beim Abrufen der Volumes.")


def terminate_all():
    """
    Terminiert alle existierenden Pods.
    """
    load_dotenv()
    API_KEY = os.getenv("RUNPOD_API_KEY")
    if not API_KEY:
        logger.error("RUNPOD_API_KEY nicht gefunden. Bitte Umgebungsvariable setzen.")
        return

    import runpod
    runpod.api_key = API_KEY
    try:
        pods = runpod.get_pods()
    except Exception as e:
        logger.error(f"Fehler beim Abruf der Pods: {e}")
        return

    for pod in pods:
        pod_id = pod.get("id") or pod["id"]
        try:
            runpod.terminate_pod(pod_id)
            logger.info(f"Pod {pod_id} terminiert.")
        except Exception as e:
            logger.error(f"Fehler beim Terminieren von Pod {pod_id}: {e}")


def list_gpu_types():
    load_dotenv()
    API_KEY = os.getenv("RUNPOD_API_KEY")
    if not API_KEY:
        raise RuntimeError("RUNPOD_API_KEY nicht gesetzt. Bitte Umgebungsvariablen definieren.")

    # GraphQL-Endpoint
    graphql_url = f"https://api.runpod.io/graphql?api_key={API_KEY}"
    headers = {
        "Content-Type": "application/json"
    }

    """
        Ruft alle GPU-Typen ab und gibt ihre ID, Bezeichnung und VRAM-Größe aus.
        """
    query = """
        query GpuTypes {
          gpuTypes {
            id
            displayName
            memoryInGb
          }
        }
        """
    response = requests.post(
        graphql_url,
        headers=headers,
        json={"query": query}
    )
    response.raise_for_status()
    result = response.json()
    if errors := result.get("errors"):
        raise RuntimeError(f"GraphQL-Fehler: {errors}")

    gpu_types = result["data"]["gpuTypes"]
    print(f"Gefundene GPU-Typen: {len(gpu_types)}")
    print("TypeId                          | Name        | VRAM (GB)")


    print("--------------------------------|-------------|-----------")
    for gpu in gpu_types:
        # Die 'id' ist der TypeId, den man für gpuTypeId im Pod-Deployment verwendet
        type_id = gpu['id']
        name = gpu['displayName']
        vram = gpu['memoryInGb']
        print(f"{type_id:60} | {name:11} | {vram}")


if __name__ == "__main__":
    p2()
    terminate_all()
    p2()
    pass
