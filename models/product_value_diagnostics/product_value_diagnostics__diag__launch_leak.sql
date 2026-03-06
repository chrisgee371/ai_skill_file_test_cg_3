{{
  config({    
    "materialized": "table",
    "alias": "diag__launch_leak",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH dll_order_item_net AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'int__order_item_net_value') }}

),

dll_item_with_date AS (

  SELECT 
    oin.order_item_id,
    oin.product_id,
    DATE(oin.created_at) AS item_date,
    oin.price_usd,
    oin.refund_amount_usd,
    CASE
      WHEN oin.refund_amount_usd > 0
        THEN 1
      ELSE 0
    END AS is_refunded
  
  FROM dll_order_item_net AS oin

),

dll_products AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'dim__products') }}

),

dll_product_launch AS (

  SELECT 
    product_id,
    DATE(created_at) AS launch_window_start
  
  FROM dll_products

),

dll_item_with_launch AS (

  SELECT 
    iwd.order_item_id,
    iwd.product_id,
    iwd.item_date,
    iwd.price_usd,
    iwd.refund_amount_usd,
    iwd.is_refunded,
    pl.launch_window_start,
    DATEDIFF(iwd.item_date, pl.launch_window_start) AS days_since_launch
  
  FROM dll_item_with_date AS iwd
  INNER JOIN dll_product_launch AS pl
     ON iwd.product_id = pl.product_id

),

dll_filtered AS (

  SELECT * 
  
  FROM dll_item_with_launch
  
  WHERE days_since_launch >= 0

),

dll_first_30d_source_table_000 AS (

  SELECT * 
  
  FROM dll_filtered

),

dll_lifetime AS (

  SELECT 
    product_id,
    launch_window_start,
    COUNT(*) AS items_sold_lifetime,
    SUM(is_refunded) AS refunds_lifetime
  
  FROM dll_filtered
  
  GROUP BY 
    product_id, launch_window_start

),

dll_first_30d_from_000 AS (

  SELECT 
    product_id,
    launch_window_start,
    is_refunded,
    days_since_launch
  
  FROM dll_first_30d_source_table_000

),

dll_first_30d_filter_001 AS (

  SELECT * 
  
  FROM dll_first_30d_from_000
  
  WHERE days_since_launch <= 30

),

dll_first_30d_groupBy_002 AS (

  SELECT 
    product_id,
    launch_window_start,
    COUNT(*) AS items_sold_first_30d,
    SUM(is_refunded) AS refunds_first_30d
  
  FROM dll_first_30d_filter_001
  
  GROUP BY 
    product_id, launch_window_start

),

dll_combined AS (

  SELECT 
    lt.product_id,
    lt.launch_window_start,
    COALESCE(f30.items_sold_first_30d, 0) AS items_sold_first_30d,
    COALESCE(f30.refunds_first_30d, 0) AS refunds_first_30d,
    lt.items_sold_lifetime,
    lt.refunds_lifetime
  
  FROM dll_lifetime AS lt
  LEFT JOIN dll_first_30d_groupBy_002 AS f30
     ON lt.product_id = f30.product_id AND lt.launch_window_start = f30.launch_window_start

),

dll_with_rates AS (

  SELECT 
    product_id,
    launch_window_start,
    items_sold_first_30d,
    refunds_first_30d,
    items_sold_lifetime,
    refunds_lifetime,
    CASE
      WHEN items_sold_first_30d > 0
        THEN CAST(refunds_first_30d AS DOUBLE) / items_sold_first_30d
      ELSE NULL
    END AS first_30d_refund_rate,
    CASE
      WHEN items_sold_lifetime > 0
        THEN CAST(refunds_lifetime AS DOUBLE) / items_sold_lifetime
      ELSE NULL
    END AS lifetime_refund_rate
  
  FROM dll_combined

),

dll_scored AS (

  SELECT 
    product_id,
    launch_window_start,
    items_sold_first_30d,
    refunds_first_30d,
    items_sold_lifetime,
    refunds_lifetime,
    first_30d_refund_rate,
    lifetime_refund_rate,
    CASE
      WHEN items_sold_first_30d < 5
        THEN 0.0
      ELSE LEAST(
        100.0, 
        GREATEST(
          0.0, 
          COALESCE(first_30d_refund_rate, 0.0)
          * 150.0
          + CASE
              WHEN lifetime_refund_rate IS NOT NULL
              AND first_30d_refund_rate IS NOT NULL
              AND lifetime_refund_rate > first_30d_refund_rate
                THEN (lifetime_refund_rate - first_30d_refund_rate) * 200.0
              ELSE 0.0
            END))
    END AS severity_score
  
  FROM dll_with_rates

),

dll_final AS (

  SELECT 
    CAST(product_id AS BIGINT) AS product_id,
    CAST(launch_window_start AS DATE) AS launch_window_start,
    CAST(items_sold_first_30d AS BIGINT) AS items_sold_first_30d,
    CAST(refunds_first_30d AS BIGINT) AS refunds_first_30d,
    CAST(items_sold_lifetime AS BIGINT) AS items_sold_lifetime,
    CAST(refunds_lifetime AS BIGINT) AS refunds_lifetime,
    CAST(first_30d_refund_rate AS DOUBLE) AS first_30d_refund_rate,
    CAST(lifetime_refund_rate AS DOUBLE) AS lifetime_refund_rate,
    CAST(severity_score AS DOUBLE) AS severity_score,
    CAST(CASE
      WHEN severity_score >= 70
        THEN 'Launch excitement did not translate to durable quality - investigate product issues'
      WHEN severity_score >= 40
        THEN 'Elevated post-launch refunds - review customer expectations vs reality'
      WHEN severity_score >= 20
        THEN 'Monitor launch quality metrics'
      ELSE NULL
    END AS STRING) AS recommended_action
  
  FROM dll_scored

)

SELECT *

FROM dll_final
