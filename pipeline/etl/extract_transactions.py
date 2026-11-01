import csv
import os
from pathlib import Path

from dotenv import load_dotenv

try:
    import psycopg
except ImportError:
    import psycopg2 as psycopg

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "pipeline" / "data" / "raw"

load_dotenv(PROJECT_ROOT / ".env")


def extract_transactions():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    output_file = RAW_DIR / "transactions.csv"

    with psycopg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5435")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    ) as conn:
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
                    description,
                    created_at
                FROM transactions
                ORDER BY transaction_id
                """
            )

            columns = [column.name for column in cur.description]
            rows = cur.fetchall()

    with output_file.open(
        "w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"Extract completed: {len(rows)} rows")
    print(f"Raw file: {output_file}")


if __name__ == "__main__":
    extract_transactions()
