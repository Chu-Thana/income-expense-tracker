import csv
from collections import defaultdict
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_FILE = PROJECT_ROOT / "pipeline/data/silver/transactions.csv"
GOLD_DIR = PROJECT_ROOT / "pipeline/data/gold"

MONTHLY_FILE = GOLD_DIR / "monthly_summary.csv"
CATEGORY_FILE = GOLD_DIR / "expense_by_category.csv"


def build_gold():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    monthly = defaultdict(
        lambda: {
            "income": Decimal("0.00"),
            "expense": Decimal("0.00"),
        }
    )

    categories = defaultdict(lambda: Decimal("0.00"))

    with SILVER_FILE.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            raise ValueError("Silver file has no header")

        required = {
            "user_id",
            "transaction_date",
            "transaction_type",
            "category",
            "amount",
        }

        if not required.issubset(reader.fieldnames):
            raise ValueError("Silver file missing required columns")

        for row in reader:
            user_id = int(row["user_id"])
            month = row["transaction_date"][:7]
            amount = Decimal(row["amount"])
            transaction_type = row["transaction_type"]

            key = (user_id, month)

            if transaction_type == "income":
                monthly[key]["income"] += amount

            elif transaction_type == "expense":
                monthly[key]["expense"] += amount

                category_key = (
                    user_id,
                    month,
                    row["category"],
                )

                categories[category_key] += amount

            else:
                raise ValueError(
                    f"Invalid transaction type: {transaction_type}"
                )

    monthly_rows = []

    for (user_id, month), totals in sorted(monthly.items()):
        income = totals["income"]
        expense = totals["expense"]

        monthly_rows.append({
            "user_id": user_id,
            "month": month,
            "total_income": str(income),
            "total_expense": str(expense),
            "net_amount": str(income - expense),
        })

    category_rows = []

    for (user_id, month, category), amount in sorted(categories.items()):
        category_rows.append({
            "user_id": user_id,
            "month": month,
            "category": category,
            "total_expense": str(amount),
        })

    outputs = [
        (
            MONTHLY_FILE,
            [
                "user_id",
                "month",
                "total_income",
                "total_expense",
                "net_amount",
            ],
            monthly_rows,
        ),
        (
            CATEGORY_FILE,
            [
                "user_id",
                "month",
                "category",
                "total_expense",
            ],
            category_rows,
        ),
    ]

    # Prepare both candidate files before publishing
    for output_file, columns, rows in outputs:
        candidate = output_file.with_name(
            output_file.stem + "_candidate.csv"
        )

        with candidate.open(
            "w", encoding="utf-8", newline=""
        ) as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)

    # Simple publish for prototype
    for output_file, _, _ in outputs:
        candidate = output_file.with_name(
            output_file.stem + "_candidate.csv"
        )
        candidate.replace(output_file)

    print(f"Gold monthly summary: {len(monthly_rows)} rows")
    print(f"Gold expense by category: {len(category_rows)} rows")


if __name__ == "__main__":
    build_gold()
