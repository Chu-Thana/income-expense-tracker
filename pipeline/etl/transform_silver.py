
import csv
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_FILE = PROJECT_ROOT / "pipeline/data/raw/transactions.csv"
SILVER_DIR = PROJECT_ROOT / "pipeline/data/silver"
SILVER_FILE = SILVER_DIR / "transactions.csv"

REQUIRED_COLUMNS = [
    "transaction_id",
    "user_id",
    "transaction_date",
    "transaction_type",
    "category",
    "amount",
    "description",
    "created_at",
]


def transform_silver():
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    validated_rows = []
    seen_ids = set()

    with RAW_FILE.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)

        if not reader.fieldnames or not all(
            col in reader.fieldnames for col in REQUIRED_COLUMNS
        ):
            raise ValueError("Missing required columns")

        for row in reader:
            transaction_id = row["transaction_id"]
            user_id = row["user_id"]
            transaction_type = row["transaction_type"]
            category = row["category"].strip()

            if not transaction_id or not user_id or not category:
                raise ValueError("Missing required values")

            if transaction_id in seen_ids:
                raise ValueError(f"Duplicate ID: {transaction_id}")

            seen_ids.add(transaction_id)

            if int(transaction_id) <= 0 or int(user_id) <= 0:
                raise ValueError("Invalid transaction_id or user_id")

            if transaction_type not in ("income", "expense"):
                raise ValueError("Invalid transaction type")

            date.fromisoformat(row["transaction_date"])

            try:
                amount = Decimal(row["amount"])
            except InvalidOperation as exc:
                raise ValueError("Invalid amount") from exc

            if not amount.is_finite() or amount <= 0:
                raise ValueError("Amount must be positive and finite")

            row["category"] = category
            row["amount"] = str(amount)

            validated_rows.append(row)

    # Publish only after every row passes validation
    candidate_file = SILVER_DIR / "transactions_candidate.csv"

    with candidate_file.open(
        "w", encoding="utf-8", newline=""
    ) as file:
        writer = csv.DictWriter(file, fieldnames=REQUIRED_COLUMNS)
        writer.writeheader()
        writer.writerows(validated_rows)

    candidate_file.replace(SILVER_FILE)

    print(f"Silver completed: {len(validated_rows)} rows")
    print(f"Silver file: {SILVER_FILE}")


if __name__ == "__main__":
    transform_silver()
