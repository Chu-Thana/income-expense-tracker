import os
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from fastapi import HTTPException

BACKEND_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BACKEND_DIR / ".env")

AIRFLOW_URL = os.getenv(
    "AIRFLOW_API_URL",
    "http://127.0.0.1:8080",
).rstrip("/")

AIRFLOW_USERNAME = os.getenv("AIRFLOW_API_USERNAME")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_API_PASSWORD")

DAG_ID = "income_expense_batch_dag"


def get_auth():
    if not AIRFLOW_USERNAME or not AIRFLOW_PASSWORD:
        raise HTTPException(
            status_code=503,
            detail="Airflow credentials are not configured",
        )
    return (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)


def trigger_pipeline():
    url = f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns"

    try:
        response = requests.post(
            url,
            json={"conf": {}},
            auth=get_auth(),
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        return {
            "dag_run_id": data["dag_run_id"],
            "state": data["state"],
        }
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=502,
            detail="Unable to trigger Airflow pipeline",
        ) from exc


def get_pipeline_status(run_id: str):
    encoded_id = quote(run_id, safe="")
    url = (
        f"{AIRFLOW_URL}/api/v1/dags/"
        f"{DAG_ID}/dagRuns/{encoded_id}"
    )

    try:
        response = requests.get(
            url,
            auth=get_auth(),
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        return {
            "dag_run_id": data["dag_run_id"],
            "state": data["state"],
        }
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=502,
            detail="Unable to read Airflow pipeline status",
        ) from exc
