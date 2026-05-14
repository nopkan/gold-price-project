CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.gold_price_raw (
    id BIGSERIAL PRIMARY KEY,
    load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    request_ts TIMESTAMPTZ,
    symbol TEXT NOT NULL,
    currency TEXT NOT NULL,
    source TEXT NOT NULL,
    api_status INTEGER,
    raw_payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gold_price_raw_load_ts
    ON bronze.gold_price_raw (load_ts DESC);
