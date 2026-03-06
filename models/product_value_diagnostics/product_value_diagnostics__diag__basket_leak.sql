{{
  config({    
    "materialized": "table",
    "alias": "diag__basket_leak",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH dbl_order_items AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'stg__order_items') }}

),

dbl_order_cross_sell AS (

  SELECT 
    oi.order_id,
    SUM(CASE
      WHEN oi.is_primary_item = 0
        THEN 1
      ELSE 0
    END) AS cross_sell_items
  
  FROM dbl_order_items AS oi
  
  GROUP BY oi.order_id

),

dbl_orders AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'stg__orders') }}

),

dbl_order_primary AS (

  SELECT 
    o.order_id,
    o.order_date,
    o.primary_product_id,
    o.items_purchased,
    o.price_usd AS order_price_usd
  
  FROM dbl_orders AS o

),

dbl_order_enriched AS (

  SELECT 
    op.order_id,
    op.order_date,
    op.primary_product_id,
    op.items_purchased,
    op.order_price_usd,
    COALESCE(ocs.cross_sell_items, 0) AS cross_sell_items,
    CASE
      WHEN COALESCE(ocs.cross_sell_items, 0) > 0
        THEN 1
      ELSE 0
    END AS has_cross_sell
  
  FROM dbl_order_primary AS op
  LEFT JOIN dbl_order_cross_sell AS ocs
     ON op.order_id = ocs.order_id

),

dbl_product_day_agg AS (

  SELECT 
    oe.order_date AS analysis_date,
    oe.primary_product_id AS product_id,
    COUNT(*) AS orders,
    SUM(oe.has_cross_sell) AS orders_with_cross_sell,
    SUM(oe.order_price_usd) AS total_revenue,
    SUM(oe.items_purchased) AS total_items
  
  FROM dbl_order_enriched AS oe
  
  GROUP BY 
    oe.order_date, oe.primary_product_id

),

dbl_with_rates AS (

  SELECT 
    pda.analysis_date,
    pda.product_id,
    pda.orders,
    pda.orders_with_cross_sell,
    CASE
      WHEN pda.orders > 0
        THEN CAST(pda.orders_with_cross_sell AS DOUBLE) / pda.orders
      ELSE 0.0
    END AS bundle_attachment_rate,
    CASE
      WHEN pda.orders > 0
        THEN pda.total_revenue / pda.orders
      ELSE NULL
    END AS average_order_value_usd,
    CASE
      WHEN pda.orders > 0
        THEN CAST(pda.total_items AS DOUBLE) / pda.orders
      ELSE NULL
    END AS average_items_per_order
  
  FROM dbl_product_day_agg AS pda

),

dbl_scored AS (

  SELECT 
    analysis_date,
    product_id,
    orders,
    orders_with_cross_sell,
    bundle_attachment_rate,
    average_order_value_usd,
    average_items_per_order,
    CASE
      WHEN orders < 5
        THEN 0.0
      ELSE LEAST(
        100.0, 
        GREATEST(
          0.0, 
          (1.0 - bundle_attachment_rate)
          * 40.0
          + CASE
              WHEN average_items_per_order < 1.5
                THEN 30.0
              ELSE 0.0
            END
          + CASE
              WHEN average_order_value_usd < 50.0
                THEN 30.0
              ELSE 0.0
            END))
    END AS severity_score
  
  FROM dbl_with_rates

),

dbl_final AS (

  SELECT 
    CAST(analysis_date AS DATE) AS analysis_date,
    CAST(product_id AS BIGINT) AS product_id,
    CAST(orders AS BIGINT) AS orders,
    CAST(orders_with_cross_sell AS BIGINT) AS orders_with_cross_sell,
    CAST(bundle_attachment_rate AS DOUBLE) AS bundle_attachment_rate,
    CAST(average_order_value_usd AS DOUBLE) AS average_order_value_usd,
    CAST(average_items_per_order AS DOUBLE) AS average_items_per_order,
    CAST(severity_score AS DOUBLE) AS severity_score,
    CAST(CASE
      WHEN severity_score >= 70
        THEN 'Investigate bundle and cross-sell strategy - weak attachment'
      WHEN severity_score >= 40
        THEN 'Review product pricing or promotion strategy'
      WHEN severity_score >= 20
        THEN 'Monitor basket metrics for trends'
      ELSE NULL
    END AS STRING) AS recommended_action
  
  FROM dbl_scored

)

SELECT *

FROM dbl_final
