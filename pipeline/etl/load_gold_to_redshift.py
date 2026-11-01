import csv
import time
from pathlib import Path

import boto3


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = PROJECT_ROOT / "pipeline" / "data" / "gold"

REGION = "ap-southeast-1"
WORKGROUP = "default-workgroup"
DATABASE = "dev"

S3_BUCKET = "vendor-payments-data-platform-thana"
S3_PREFIX = "income-expense/gold"

SCHEMA = "income_expense_analytics"

TABLES = {
    "monthly_summary": "monthly_summary.csv",
    "expense_by_category": "expense_by_category.csv",
}


def count_csv_rows(filename):
    file_path = GOLD_DIR / filename

    with file_path.open(
        "r", encoding="utf-8", newline=""
    ) as file:
        reader = csv.DictReader(file)
        return sum(1 for _ in reader)


def load_gold_to_redshift():
    client = boto3.client(
        "redshift-data",
        region_name=REGION,
    )

    expected_counts = {
        table: count_csv_rows(filename)
        for table, filename in TABLES.items()
    }

    for table, count in expected_counts.items():
        if count == 0:
            raise ValueError(f"Empty Gold mart: {table}")

    sqls = []

    for table in TABLES:
        sqls.append(
            f"DELETE FROM {SCHEMA}.{table}"
        )

    for table, filename in TABLES.items():
        s3_path = (
            f"s3://{S3_BUCKET}/{S3_PREFIX}/{filename}"
        )

        sqls.append(
            f"""
            COPY {SCHEMA}.{table}
            FROM '{s3_path}'
            IAM_ROLE default
            FORMAT AS CSV
            IGNOREHEADER 1
            REGION '{REGION}'
            """
        )

    print("Starting Redshift Full Refresh...")

    response = client.batch_execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sqls=sqls,
        StatementName="income-expense-gold-refresh",
    )

    statement_id = response["Id"]

    while True:
        result = client.describe_statement(
            Id=statement_id
        )

        status = result["Status"]

        if status == "FINISHED":
            print("Redshift batch: FINISHED")
            break

        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(
                result.get("Error", "Redshift load failed")
            )

        time.sleep(3)

    # Verify row counts after loading
    for table, expected in expected_counts.items():
        result = client.execute_statement(
            WorkgroupName=WORKGROUP,
            Database=DATABASE,
            Sql=f"SELECT COUNT(*) FROM {SCHEMA}.{table}",
        )

        query_id = result["Id"]

        while True:
            status_result = client.describe_statement(
                Id=query_id
            )

            status = status_result["Status"]

            if status == "FINISHED":
                break

            if status in ("FAILED", "ABORTED"):
                raise RuntimeError(
                    status_result.get(
                        "Error", "Validation query failed"
                    )
                )

            time.sleep(2)

        records = client.get_statement_result(
            Id=query_id
        )["Records"]

        actual = int(records[0][0]["longValue"])

        print(
            f"{table}: expected={expected}, actual={actual}"
        )

        if actual != expected:
            raise ValueError(
                f"Row count mismatch: {table}"
            )

    print("Redshift Gold Load: PASS")


if __name__ == "__main__":
    load_gold_to_redshift()
