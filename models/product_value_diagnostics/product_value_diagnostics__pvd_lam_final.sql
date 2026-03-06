{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH pvd_lam_products AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__dim__products') }}

),

pvd_lam_net_value AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__order_item_net_value') }}

),

pvd_lam_item_with_product AS (

  SELECT 
    nv.order_item_id,
    nv.order_item_date,
    nv.product_id,
    nv.price_usd,
    nv.cogs_usd,
    nv.gross_margin_usd,
    nv.refund_amount_usd,
    nv.is_refunded,
    nv.net_revenue_usd,
    p.product_name,
    p.product_short_name,
    p.product_launch_date,
    DATEDIFF(nv.order_item_date, p.product_launch_date) AS days_since_launch
  
  FROM pvd_lam_net_value AS nv
  INNER JOIN pvd_lam_products AS p
     ON nv.product_id = p.product_id

),

pvd_lam_launch_window AS (

  SELECT * 
  
  FROM pvd_lam_item_with_product
  
  WHERE days_since_launch >= 0 AND days_since_launch <= 90

),

pvd_lam_aggregated AS (

  SELECT 
    product_id,
    product_name,
    product_short_name,
    product_launch_date,
    CASE
      WHEN days_since_launch <= 7
        THEN 'week_1'
      WHEN days_since_launch <= 14
        THEN 'week_2'
      WHEN days_since_launch <= 30
        THEN 'month_1'
      WHEN days_since_launch <= 60
        THEN 'month_2'
      ELSE 'month_3'
    END AS launch_period,
    COUNT(*) AS items_sold,
    SUM(price_usd) AS total_gross_revenue,
    SUM(CASE
      WHEN is_refunded
        THEN 1
      ELSE 0
    END) AS refunded_items,
    SUM(COALESCE(refund_amount_usd, 0.0)) AS total_refund_amount,
    SUM(net_revenue_usd) AS total_net_revenue,
    SUM(gross_margin_usd) AS total_gross_margin
  
  FROM pvd_lam_launch_window
  
  GROUP BY 
    product_id, 
    product_name, 
    product_short_name, 
    product_launch_date, 
    CASE
      WHEN days_since_launch <= 7
        THEN 'week_1'
      
      WHEN days_since_launch <= 14
        THEN 'week_2'
      
      WHEN days_since_launch <= 30
        THEN 'month_1'
      
      WHEN days_since_launch <= 60
        THEN 'month_2'
      
      ELSE 'month_3'
    END

),

pvd_lam_final AS (

  SELECT 
    CAST(product_id AS BIGINT) AS product_id,
    CAST(product_name AS STRING) AS product_name,
    CAST(product_short_name AS STRING) AS product_short_name,
    CAST(product_launch_date AS DATE) AS product_launch_date,
    CAST(launch_period AS STRING) AS launch_period,
    CAST(items_sold AS BIGINT) AS items_sold,
    CAST(total_gross_revenue AS DOUBLE) AS total_gross_revenue,
    CAST(total_gross_margin AS DOUBLE) AS total_gross_margin,
    CAST(refunded_items AS BIGINT) AS refunded_items,
    CAST(total_refund_amount AS DOUBLE) AS total_refund_amount,
    CAST(total_net_revenue AS DOUBLE) AS total_net_revenue,
    CAST(CASE
      WHEN items_sold > 0
        THEN (refunded_items * 1.0) / items_sold
      ELSE 0.0
    END AS DOUBLE) AS launch_refund_rate,
    CAST(CASE
      WHEN items_sold > 0
        THEN total_net_revenue / items_sold
      ELSE 0.0
    END AS DOUBLE) AS net_revenue_per_item
  
  FROM pvd_lam_aggregated

)

SELECT *

FROM pvd_lam_final
