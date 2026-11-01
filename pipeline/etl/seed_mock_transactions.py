import os
from datetime import date
from pathlib import Path

import psycopg
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

USERS = [1, 2, 3]
MONTHS = range(5, 11)  # May - October 2026

CATEGORIES = [
    ("Food", 3500),
    ("Transport", 1200),
    ("Shopping", 2500),
]


def seed_mock_transactions():
    inserted = 0

    with psycopg.connect(
        host="127.0.0.1",
        port=5435,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    ) as conn:

        with conn.cursor() as cur:
            for user_id in USERS:
                for month in MONTHS:
                    transactions = [
                        (
                            date(2026, month, 1),
                            "income",
                            "Salary",
                            25000 + user_id * 5000,
                        )
                    ]

                    for category, base_amount in CATEGORIES:
                        transactions.append(
                            (
                                date(2026, month, 10),
                                "expense",
                                category,
                                base_amount + user_id * 200 + month * 50,
                            )
                        )

                    for tx_date, tx_type, category, amount in transactions:
                        description = (
                            f"DEMO-SEED-{user_id}-{month}-{category}"
                        )

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
                            SELECT %s, %s, %s, %s, %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM transactions
                                WHERE user_id = %s
                                  AND description = %s
                            )
                            RETURNING transaction_id
                            """,
                            (
                                user_id,
                                tx_date,
                                tx_type,
                                category,
                                amount,
                                description,
                                user_id,
                                description,
                            ),
                        )

                        if cur.fetchone() is not None:
                            inserted += 1

    print(f"Mock transactions inserted: {inserted}")
    print("Mock data seeding completed!")


if __name__ == "__main__":
    seed_mock_transactions()
