WITH extracted AS (
    SELECT
        COALESCE(
            TO_TIMESTAMP(NULLIF(raw_payload ->> 'timestamp', '')::DOUBLE PRECISION),
            TO_TIMESTAMP(NULLIF(raw_payload #>> '{data,timestamp}', '')::DOUBLE PRECISION),
            request_ts,
            load_ts
        ) AS observation_ts,
        symbol,
        currency,
        COALESCE(
            NULLIF(raw_payload ->> 'price', '')::NUMERIC,
            NULLIF(raw_payload ->> 'rate', '')::NUMERIC,
            NULLIF(raw_payload #>> '{data,price}', '')::NUMERIC,
            NULLIF(raw_payload -> 'rates' ->> symbol, '')::NUMERIC
        ) AS extracted_price,
        source,
        load_ts
    FROM bronze.gold_price_raw
    WHERE raw_payload IS NOT NULL
), ranked AS (
    SELECT
        observation_ts,
        symbol,
        currency,
        extracted_price,
        source,
        load_ts,
        ROW_NUMBER() OVER (
            PARTITION BY observation_ts, symbol, currency
            ORDER BY load_ts DESC
        ) AS rn
    FROM extracted
    WHERE extracted_price IS NOT NULL
)
INSERT INTO silver.gold_price_clean (
    observation_ts,
    symbol,
    currency,
    price_usd,
    source,
    ingested_at
)
SELECT
    observation_ts,
    symbol,
    currency,
    extracted_price,
    source,
    NOW()
FROM ranked
WHERE rn = 1
ON CONFLICT (observation_ts, symbol, currency) DO UPDATE
SET
    price_usd = EXCLUDED.price_usd,
    source = EXCLUDED.source,
    ingested_at = NOW();
