

WITH source AS (
    SELECT * FROM `dezoomcampproject-493714`.`market_data_bronze`.`stock_prices`
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, date 
            ORDER BY date DESC -- In this simple case, date is enough, but ideally we'd have a load_timestamp
        ) as rn
    FROM source
)

SELECT 
    symbol,
    CAST(date AS DATE) as price_date,
    CAST(open AS FLOAT64) as open_price,
    CAST(high AS FLOAT64) as high_price,
    CAST(low AS FLOAT64) as low_price,
    CAST(close AS FLOAT64) as close_price,
    CAST(volume AS INT64) as volume
FROM deduplicated
WHERE rn = 1