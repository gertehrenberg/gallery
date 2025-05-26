import calendar
import json
import logging
import os
from datetime import datetime, date
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from google.cloud import bigquery

from app.config import Settings

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "../templates"))
logger = logging.getLogger(__name__)


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/innate-setup-454010-i9-f92b1b6a1c44.json"

    gdrive_stats = compare_hashfile_counts_dash(Settings.IMAGE_FILE_CACHE_DIR, subfolders=True)
    dataset = "gcp_billing_export_n8n"
    table = "gcp_billing_export_resource_v1_01003C_0EEFF2_E60D9D"

    today = datetime.today()

    daily_info = [d for d in get_daily_costs(dataset, table, today.year, today.month) if d["tag"] >= "2025-05-11"]

    if daily_info:
        first_day = datetime.strptime(daily_info[0]["tag"], "%Y-%m-%d").strftime("%d.%m.%Y")
        last_day = datetime.strptime(daily_info[-1]["tag"], "%Y-%m-%d").strftime("%d.%m.%Y")
        total = round(sum(float(d["kosten_chf"]) for d in daily_info), 2)
        info = [
            {"from_to": f"{first_day}–{last_day}"},
            {"kosten_chf": f"CHF {total}"}
        ]
    else:
        info = [
            {"from_to": f"unbekannt"},
            {"kosten_chf": f"CHF 0.0"}
        ]

    labels = [datetime.strptime(d["tag"], "%Y-%m-%d").strftime("%d.%m.") for d in daily_info]
    values = [float(d["kosten_chf"]) for d in daily_info]

    logger.info(info)

    return templates.TemplateResponse("dashboard.j2", {
        "request": request,
        "gdrive_stats": gdrive_stats,
        "info": info,
        "labels": labels,
        "values": values
    })


def compare_hashfile_counts_dash(file_folder_dir, subfolders: bool = True):
    icon_map = {k["key"]: (k["icon"], k["label"]) for k in Settings.kategorien}

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

        entry = icon_map.get(subdir.name)
        if entry:
            icon, label = entry
            result.append({
                "ordner": f'<a href="http://localhost/gallery/?page=1&count=6&folder={subdir.name}&textflag=1">{icon} {label}</a>',
                "gdrive_count": len(gdrive_data),
                "local_count": len(local_data)
            })
    return sorted(result, key=lambda x: x["local_count"], reverse=True)


def compare_hashfile_counts(file_folder_dir, subfolders: bool = True):
    results = compare_hashfile_counts_dash(file_folder_dir, subfolders=subfolders)

    header = f"{'Ordner':<15}{'GDrive-Hashes':>15}{'Lokal-Hashes':>15}"
    print(header)
    print("-" * len(header))

    for entry in results:
        print(f"{entry['ordner']:<15}{entry['gdrive_count']:>15}{entry['local_count']:>15}")


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


def get_daily_costs(dataset: str, table: str, year: int, month: int):
    client = bigquery.Client()
    start = date(year, month, 1).strftime("%Y-%m-%d")
    last_day = calendar.monthrange(year, month)[1]
    end = date(year, month, last_day).strftime("%Y-%m-%d")

    query = f"""
    SELECT
      DATE(usage_start_time) AS tag,
      ROUND(SUM(cost), 2) AS kosten_chf
    FROM
      `{dataset}.{table}`
    WHERE
      usage_start_time >= '{start}' AND usage_start_time <= '{end}'
    GROUP BY
      tag
    ORDER BY
      tag
    """
    query_job = client.query(query)
    results = query_job.result()
    return [{"tag": row["tag"].strftime("%Y-%m-%d"), "kosten_chf": row["kosten_chf"]} for row in results]


def local():
    global service
    Settings.IMAGE_FILE_CACHE_DIR = "../cache/imagefiles"
    Settings.TEXT_FILE_CACHE_DIR = "../cache/textfiles"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../secrets/innate-setup-454010-i9-f92b1b6a1c44.json"


if __name__ == "__main__":
    local()

    dataset = "gcp_billing_export_n8n"
    table = "gcp_billing_export_resource_v1_01003C_0EEFF2_E60D9D"
    start = "2025-05-01"
    end = "2025-05-31"

    print(get_monthly_costs(dataset, table, start, end))
