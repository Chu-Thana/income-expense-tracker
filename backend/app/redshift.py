import time

import boto3


REGION = "ap-southeast-1"
WORKGROUP = "default-workgroup"
DATABASE = "dev"


def get_monthly_summary(user_id: int):
    client = boto3.client(
        "redshift-data",
        region_name=REGION,
    )

    response = client.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql="""
            SELECT
                month,
                total_income,
                total_expense,
                net_amount
            FROM income_expense_analytics.monthly_summary
            WHERE user_id = :user_id
            ORDER BY month
        """,
        Parameters=[
            {
                "name": "user_id",
                "value": str(user_id),
            }
        ],
    )

    statement_id = response["Id"]

    for _ in range(30):
        result = client.describe_statement(
            Id=statement_id
        )

        status = result["Status"]

        if status == "FINISHED":
            break

        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(
                result.get("Error", "Redshift query failed")
            )

        time.sleep(1)
    else:
        raise TimeoutError("Redshift query timed out")

    result = client.get_statement_result(
        Id=statement_id
    )

    rows = []

    for record in result["Records"]:
        rows.append({
            "month": record[0]["stringValue"],
            "total_income": float(record[1]["stringValue"]),
            "total_expense": float(record[2]["stringValue"]),
            "net_amount": float(record[3]["stringValue"]),
        })

    return rows
