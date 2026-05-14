CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.gold_price_hourly_mart (
    observation_hour TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    currency TEXT NOT NULL,
    last_price NUMERIC(18, 6) NOT NULL,
    sma_24h NUMERIC(18, 6),
    day_high NUMERIC(18, 6),
    day_low NUMERIC(18, 6),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (observation_hour, symbol, currency)
);
