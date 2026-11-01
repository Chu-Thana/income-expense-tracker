import csv
from collections import defaultdict
from decimal import Decimal
from pathlib import Path


DATA_DIR = Path(__file__).resolve().parents[1] / "data"

SILVER_FILE = DATA_DIR / "silver" / "transactions.csv"
MONTHLY_FILE = DATA_DIR / "gold" / "monthly_summary.csv"
CATEGORY_FILE = DATA_DIR / "gold" / "expense_by_category.csv"


def read_csv(path, required_columns):
    if not path.is_file():
        raise FileNotFoundError(path)

    with path.open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)

        if not reader.fieldnames:
            raise ValueError(f"Missing header: {path.name}")

        if not set(required_columns).issubset(reader.fieldnames):
            raise ValueError(f"Invalid schema: {path.name}")

        return list(reader)


def validate_gold():
    silver = read_csv(
        SILVER_FILE,
        ["user_id", "transaction_date",
         "transaction_type", "category", "amount"],
    )

    monthly = read_csv(
        MONTHLY_FILE,
        ["user_id", "month", "total_income",
         "total_expense", "net_amount"],
    )

    categories = read_csv(
        CATEGORY_FILE,
        ["user_id", "month", "category", "total_expense"],
    )

    if not silver or not monthly or not categories:
        raise ValueError("Required dataset is empty")

    expected_monthly = defaultdict(
        lambda: {
            "income": Decimal("0"),
            "expense": Decimal("0"),
        }
    )
    expected_categories = defaultdict(lambda: Decimal("0"))

    for row in silver:
        key = (row["user_id"], row["transaction_date"][:7])
        amount = Decimal(row["amount"])
        tx_type = row["transaction_type"]

        if tx_type not in ("income", "expense"):
            raise ValueError("Invalid transaction type")

        expected_monthly[key][tx_type] += amount

        if tx_type == "expense":
            category_key = (*key, row["category"])
            expected_categories[category_key] += amount

    actual_monthly = {}

    for row in monthly:
        key = (row["user_id"], row["month"])

        if key in actual_monthly:
            raise ValueError(f"Duplicate monthly key: {key}")

        income = Decimal(row["total_income"])
        expense = Decimal(row["total_expense"])
        net = Decimal(row["net_amount"])

        if net != income - expense:
            raise ValueError(f"Invalid net amount: {key}")

        actual_monthly[key] = {
            "income": income,
            "expense": expense,
        }

    actual_categories = {}

    for row in categories:
        key = (row["user_id"], row["month"], row["category"])

        if key in actual_categories:
            raise ValueError(f"Duplicate category key: {key}")

        actual_categories[key] = Decimal(row["total_expense"])

    if actual_monthly != dict(expected_monthly):
        raise ValueError("Monthly Gold metrics do not match Silver")

    if actual_categories != dict(expected_categories):
        raise ValueError("Category Gold metrics do not match Silver")

    print(f"Silver rows: {len(silver)}")
    print(f"Monthly Gold rows: {len(monthly)}")
    print(f"Category Gold rows: {len(categories)}")
    print("Gold validation: PASS")


if __name__ == "__main__":
    validate_gold()
