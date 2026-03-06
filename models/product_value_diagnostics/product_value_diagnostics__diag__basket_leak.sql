{{
  config({    
    "materialized": "table",
    "alias": "product_value_diagnostics__diag__basket_leak",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH pvd_blk_order_items AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__order_items') }}

),

pvd_blk_crosssell_value_source_table_000 AS (

  SELECT * 
  
  FROM pvd_blk_order_items

),

pvd_blk_crosssell_value_from_000 AS (

  SELECT 
    oi.order_id,
    oi.price_usd,
    oi.is_primary_item
  
  FROM pvd_blk_crosssell_value_source_table_000 AS oi

),

pvd_blk_crosssell_value_filter_001 AS (

  SELECT * 
  
  FROM pvd_blk_crosssell_value_from_000
  
  WHERE is_primary_item = 0

),

pvd_blk_crosssell_value_groupBy_002 AS (

  SELECT 
    order_id,
    SUM(price_usd) AS crosssell_value
  
  FROM pvd_blk_crosssell_value_filter_001
  
  GROUP BY order_id

),

pvd_blk_orders AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__orders') }}

),

pvd_blk_order_metrics AS (

  SELECT 
    o.order_id,
    o.order_date,
    o.primary_product_id,
    o.items_purchased,
    o.price_usd AS order_price,
    CASE
      WHEN o.items_purchased > 1
        THEN 1
      ELSE 0
    END AS has_crosssell
  
  FROM pvd_blk_orders AS o

),

pvd_blk_order_with_crosssell AS (

  SELECT 
    om.order_id,
    om.order_date,
    om.primary_product_id,
    om.items_purchased,
    om.order_price,
    om.has_crosssell,
    COALESCE(cv.crosssell_value, 0.0) AS crosssell_value
  
  FROM pvd_blk_order_metrics AS om
  LEFT JOIN pvd_blk_crosssell_value_groupBy_002 AS cv
     ON om.order_id = cv.order_id

),

pvd_blk_aggregated AS (

  SELECT 
    order_date AS analysis_date,
    primary_product_id,
    COUNT(*) AS total_orders,
    SUM(has_crosssell) AS orders_with_crosssell,
    SUM(order_price) AS total_order_value,
    SUM(crosssell_value) AS total_crosssell_value
  
  FROM pvd_blk_order_with_crosssell
  
  GROUP BY 
    order_date, primary_product_id

),

pvd_blk_products AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__dim__products') }}

),

pvd_blk_with_products AS (

  SELECT 
    ba.analysis_date,
    'primary_product' AS entity_type,
    p.product_short_name AS entity_key,
    ba.primary_product_id,
    ba.total_orders,
    ba.orders_with_crosssell,
    ba.total_order_value,
    ba.total_crosssell_value
  
  FROM pvd_blk_aggregated AS ba
  INNER JOIN pvd_blk_products AS p
     ON ba.primary_product_id = p.product_id

),

pvd_blk_with_rates AS (

  SELECT 
    analysis_date,
    entity_type,
    entity_key,
    total_orders,
    orders_with_crosssell,
    total_order_value,
    total_crosssell_value,
    CASE
      WHEN total_orders > 0
        THEN (orders_with_crosssell * 1.0) / total_orders
      ELSE 0.0
    END AS crosssell_rate,
    CASE
      WHEN total_order_value > 0
        THEN total_crosssell_value / total_order_value
      ELSE 0.0
    END AS crosssell_value_share,
    LEAST(
      100.0, 
      (
        1.0
        - CASE
            WHEN total_orders > 0
              THEN (orders_with_crosssell * 1.0) / total_orders
            ELSE 0.0
          END
      )
      * 50.0
      + (LEAST(CAST(total_orders AS DOUBLE), 500.0) / 500.0 * 50.0)) AS severity_score
  
  FROM pvd_blk_with_products

),

pvd_blk_final AS (

  SELECT 
    CAST(analysis_date AS DATE) AS analysis_date,
    CAST(entity_type AS STRING) AS entity_type,
    CAST(entity_key AS STRING) AS entity_key,
    CAST(total_orders AS BIGINT) AS total_orders,
    CAST(orders_with_crosssell AS BIGINT) AS orders_with_crosssell,
    CAST(total_order_value AS DOUBLE) AS total_order_value,
    CAST(total_crosssell_value AS DOUBLE) AS total_crosssell_value,
    CAST(crosssell_rate AS DOUBLE) AS crosssell_rate,
    CAST(crosssell_value_share AS DOUBLE) AS crosssell_value_share,
    CAST(severity_score AS DOUBLE) AS severity_score,
    CAST(CASE
      WHEN crosssell_rate < 0.1 AND total_orders > 50
        THEN 'Implement product bundling or recommendations'
      WHEN crosssell_rate < 0.2 AND total_orders > 100
        THEN 'Improve product page cross-sell visibility'
      WHEN crosssell_value_share < 0.05 AND orders_with_crosssell > 10
        THEN 'Promote higher-value cross-sell items'
      ELSE NULL
    END AS STRING) AS recommended_action
  
  FROM pvd_blk_with_rates

)

SELECT *

FROM pvd_blk_final
