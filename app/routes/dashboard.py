import json
import logging
import os
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))
logger = logging.getLogger(__name__)

from fastapi import Request
from fastapi.responses import HTMLResponse


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/innate-setup-454010-i9-f92b1b6a1c44.json"

    gdrive_stats = compare_hashfile_counts_dash(Settings.IMAGE_FILE_CACHE_DIR, subfolders=True)
    dataset = "gcp_billing_export_n8n"  # z. B. gcp_billing_export
    table = "gcp_billing_export_resource_v1_01003C_0EEFF2_E60D9D"
    start = "2025-05-01"
    end = "2025-05-31"

    info = get_monthly_costs(dataset, table, start, end)
    # deutsches Datumsformat erzeugen
    start_fmt = datetime.strptime(start, "%Y-%m-%d").strftime("%d.%m.%Y")
    end_fmt = datetime.strptime(end, "%Y-%m-%d").strftime("%d.%m.%Y")
    info.append({"from_to": f"{start_fmt}–{end_fmt}"})

    logger.info(info)

    return templates.TemplateResponse("dashboard.j2", {
        "request": request,
        "gdrive_stats": gdrive_stats,
        "info": info
    })


def compare_hashfile_counts_dash(file_folder_dir, subfolders: bool = True):
    root = Path(file_folder_dir)
    all_dirs = [root] if not subfolders else [root] + [d for d in root.iterdir() if d.is_dir()]
    result = []

    for subdir in sorted(all_dirs):
        gdrive_path = subdir / "hashes.json"
        local_path = subdir / "gallery202505_hashes.json"

        try:
            with gdrive_path.open("r", encoding="utf-8") as f:
                gdrive_data = json.load(f)
                gdrive_data = gdrive_data if isinstance(gdrive_data, dict) else {}
        except:
            gdrive_data = {}

        try:
            with local_path.open("r", encoding="utf-8") as f:
                local_data = json.load(f)
                local_data = local_data if isinstance(local_data, dict) else {}
        except:
            local_data = {}

        result.append({
            "ordner": subdir.name,
            "gdrive_count": len(gdrive_data),
            "local_count": len(local_data)
        })
    return result


def compare_hashfile_counts(file_folder_dir, subfolders: bool = True):
    results = compare_hashfile_counts_dash(file_folder_dir, subfolders=subfolders)

    header = f"{'Ordner':<15}{'GDrive-Hashes':>15}{'Lokal-Hashes':>15}"
    print(header)
    print("-" * len(header))

    for entry in results:
        print(f"{entry['ordner']:<15}{entry['gdrive_count']:>15}{entry['local_count']:>15}")


from google.cloud import bigquery


def get_monthly_costs(dataset: str, table: str, start: str, end: str):
    client = bigquery.Client()
    query = f"""
    SELECT
      project.id AS projekt_id,
      ROUND(SUM(cost), 2) AS kosten_chf
    FROM
      `{dataset}.{table}`
    WHERE
      usage_start_time >= '{start}'
      AND usage_start_time < '{end}'
    GROUP BY
      projekt_id
    ORDER BY
      kosten_chf DESC
    """
    query_job = client.query(query)
    results = query_job.result()
    return [
        {"projekt_id": row["projekt_id"], "kosten_chf": row["kosten_chf"]}
        for row in results
    ]


from app.config import Settings


def local():
    global service
    Settings.IMAGE_FILE_CACHE_DIR = "../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../cache/textfiles"
    import os
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../secrets/innate-setup-454010-i9-f92b1b6a1c44.json"


if __name__ == "__main__":
    local()

    dataset = "gcp_billing_export_n8n"  # z. B. gcp_billing_export
    table = "gcp_billing_export_resource_v1_01003C_0EEFF2_E60D9D"
    start = "2025-05-01"
    end = "2025-05-31"

    print(get_monthly_costs(dataset, table, start, end))
