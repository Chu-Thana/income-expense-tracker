
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    transaction_date DATE NOT NULL,
    transaction_type VARCHAR(10) NOT NULL
        CHECK (transaction_type IN ('income', 'expense')),
    category VARCHAR(50) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL
        CHECK (amount > 0),
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_date
ON transactions (user_id, transaction_date);
