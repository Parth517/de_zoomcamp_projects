
  
    

    create or replace table `dezoomcampproject-493714`.`market_data_bronze_gold`.`fct_stock_metrics`
      
    
    

    OPTIONS()
    as (
      

WITH silver_data AS (
    SELECT * FROM `dezoomcampproject-493714`.`market_data_bronze_silver`.`stg_stock_prices`
),

computed_metrics AS (
    SELECT 
        symbol,
        price_date,
        close_price,
        -- 7-Day Moving Average
        AVG(close_price) OVER (
            PARTITION BY symbol 
            ORDER BY price_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as moving_avg_7d,
        -- Daily Change %
        (close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY price_date)) 
        / LAG(close_price) OVER (PARTITION BY symbol ORDER BY price_date) * 100 as daily_pct_change
    FROM silver_data
)

SELECT 
    symbol,
    price_date,
    close_price,
    ROUND(moving_avg_7d, 2) as moving_avg_7d,
    ROUND(daily_pct_change, 2) as daily_pct_change
FROM computed_metrics
ORDER BY symbol, price_date DESC
    );
  