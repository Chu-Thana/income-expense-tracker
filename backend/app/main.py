from datetime import date
from decimal import Decimal
from typing import Literal

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from app.database import get_connection
from app.redshift import get_monthly_summary
from app.airflow_client import trigger_pipeline, get_pipeline_status

from pathlib import Path
from dotenv import load_dotenv
import os

BACKEND_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BACKEND_DIR / ".env")


app = FastAPI(title="Income & Expense Tracker API")


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)


class TransactionCreate(BaseModel):
    user_id: int
    transaction_date: date
    transaction_type: Literal["income", "expense"]
    category: str
    amount: Decimal = Field(gt=0)
    description: str | None = None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/transactions", status_code=201)
def create_transaction(data: TransactionCreate):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transactions (
                    user_id,
                    transaction_date,
                    transaction_type,
                    category,
                    amount,
                    description
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING transaction_id
                """,
                (
                    data.user_id,
                    data.transaction_date,
                    data.transaction_type,
                    data.category,
                    data.amount,
                    data.description,
                ),
            )
            transaction_id = cur.fetchone()[0]

    return {
        "message": "Transaction created",
        "transaction_id": transaction_id,
    }


@app.get("/transactions")
def list_transactions(user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    transaction_id,
                    user_id,
                    transaction_date,
                    transaction_type,
                    category,
                    amount,
                    description
                FROM transactions
                WHERE user_id = %s
                ORDER BY transaction_date DESC, transaction_id DESC
                """,
                (user_id,),
            )

            columns = [col.name for col in cur.description]
            rows = cur.fetchall()

    return [dict(zip(columns, row)) for row in rows]


@app.get("/analytics/monthly")
def monthly_analytics(user_id: int):
    return get_monthly_summary(user_id)


@app.post("/pipeline/trigger")
def trigger_analytics_pipeline():
    if os.getenv("ENABLE_LOCAL_PIPELINE_TRIGGER") != "true":
        raise HTTPException(
            status_code=403,
            detail="Pipeline trigger is disabled",
        )
    return trigger_pipeline()


@app.get("/pipeline/status/{run_id}")
def read_analytics_pipeline_status(run_id: str):
    if os.getenv("ENABLE_LOCAL_PIPELINE_TRIGGER") != "true":
        raise HTTPException(
            status_code=403,
            detail="Pipeline status API is disabled",
        )
    return get_pipeline_status(run_id)
