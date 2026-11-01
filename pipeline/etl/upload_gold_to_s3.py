import os
from pathlib import Path

import boto3


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = PROJECT_ROOT / "pipeline" / "data" / "gold"

AWS_REGION = "ap-southeast-1"

S3_BUCKET = os.getenv(
    "INCOME_EXPENSE_S3_BUCKET",
    "vendor-payments-data-platform-thana",
)

S3_PREFIX = "income-expense/gold"

GOLD_FILES = [
    "monthly_summary.csv",
    "expense_by_category.csv",
]


def upload_gold_to_s3():
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
    )

    # Check all local files before uploading
    for filename in GOLD_FILES:
        file_path = GOLD_DIR / filename

        if not file_path.is_file():
            raise FileNotFoundError(file_path)

        if file_path.stat().st_size == 0:
            raise ValueError(f"Empty Gold file: {filename}")

    # Upload validated Gold files
    for filename in GOLD_FILES:
        file_path = GOLD_DIR / filename
        s3_key = f"{S3_PREFIX}/{filename}"

        s3.upload_file(
            str(file_path),
            S3_BUCKET,
            s3_key,
        )

        response = s3.head_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
        )

        uploaded_size = response["ContentLength"]

        if uploaded_size != file_path.stat().st_size:
            raise ValueError(
                f"S3 file size mismatch: {filename}"
            )

        print(
            f"Uploaded: s3://{S3_BUCKET}/{s3_key}"
        )

    print("Gold S3 upload: PASS")


if __name__ == "__main__":
    upload_gold_to_s3()
