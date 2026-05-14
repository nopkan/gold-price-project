WITH hourly AS (
    SELECT
        DATE_TRUNC('hour', observation_ts) AS observation_hour,
        symbol,
        currency,
        -- Latest observed value in the hour
        (ARRAY_AGG(price_usd ORDER BY observation_ts DESC))[1] AS last_price
    FROM silver.gold_price_clean
    GROUP BY 1, 2, 3
), enriched AS (
    SELECT
        observation_hour,
        symbol,
        currency,
        last_price,
        AVG(last_price) OVER (
            PARTITION BY symbol, currency
            ORDER BY observation_hour
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS sma_24h,
        MAX(last_price) OVER (
            PARTITION BY symbol, currency, DATE_TRUNC('day', observation_hour)
        ) AS day_high,
        MIN(last_price) OVER (
            PARTITION BY symbol, currency, DATE_TRUNC('day', observation_hour)
        ) AS day_low
    FROM hourly
)
INSERT INTO gold.gold_price_hourly_mart (
    observation_hour,
    symbol,
    currency,
    last_price,
    sma_24h,
    day_high,
    day_low,
    refreshed_at
)
SELECT
    observation_hour,
    symbol,
    currency,
    last_price,
    sma_24h,
    day_high,
    day_low,
    NOW()
FROM enriched
ON CONFLICT (observation_hour, symbol, currency) DO UPDATE
SET
    last_price = EXCLUDED.last_price,
    sma_24h = EXCLUDED.sma_24h,
    day_high = EXCLUDED.day_high,
    day_low = EXCLUDED.day_low,
    refreshed_at = NOW();
