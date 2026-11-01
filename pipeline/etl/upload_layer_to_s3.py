import argparse
import os
from pathlib import Path

import boto3


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "pipeline" / "data"

AWS_REGION = "ap-southeast-1"
S3_BUCKET = os.getenv(
    "INCOME_EXPENSE_S3_BUCKET",
    "vendor-payments-data-platform-thana",
)

S3_PREFIX = "income-expense"

LAYERS = {
    "raw": ["transactions.csv"],
    "silver": ["transactions.csv"],
}


def upload_layer(layer: str):
    if layer not in LAYERS:
        raise ValueError(f"Unsupported layer: {layer}")

    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
    )

    for filename in LAYERS[layer]:
        local_file = DATA_DIR / layer / filename

        if not local_file.is_file():
            raise FileNotFoundError(local_file)

        if local_file.stat().st_size == 0:
            raise ValueError(f"Empty file: {local_file}")

        s3_key = f"{S3_PREFIX}/{layer}/{filename}"

        s3.upload_file(
            str(local_file),
            S3_BUCKET,
            s3_key,
        )

        response = s3.head_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
        )

        if response["ContentLength"] != local_file.stat().st_size:
            raise ValueError(f"S3 size mismatch: {s3_key}")

        print(f"Uploaded: s3://{S3_BUCKET}/{s3_key}")

    print(f"{layer.upper()} S3 upload: PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--layer",
        required=True,
        choices=["raw", "silver"],
    )
    args = parser.parse_args()

    upload_layer(args.layer)
