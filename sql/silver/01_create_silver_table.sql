CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.gold_price_clean (
    observation_ts TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    currency TEXT NOT NULL,
    price_usd NUMERIC(18, 6) NOT NULL,
    source TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (observation_ts, symbol, currency)
);

CREATE INDEX IF NOT EXISTS idx_gold_price_clean_symbol_ts
    ON silver.gold_price_clean (symbol, observation_ts DESC);
