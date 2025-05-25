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
