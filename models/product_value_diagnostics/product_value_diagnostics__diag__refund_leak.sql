{{
  config({    
    "materialized": "table",
    "alias": "product_value_diagnostics__diag__refund_leak",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH pvd_rlk_net_value AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__order_item_net_value') }}

),

pvd_rlk_products AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__dim__products') }}

),

pvd_rlk_item_with_product AS (

  SELECT 
    nv.order_item_id,
    nv.order_item_date,
    nv.product_id,
    nv.price_usd,
    nv.refund_amount_usd,
    nv.is_refunded,
    nv.net_revenue_usd,
    p.product_name,
    p.product_short_name
  
  FROM pvd_rlk_net_value AS nv
  INNER JOIN pvd_rlk_products AS p
     ON nv.product_id = p.product_id

),

pvd_rlk_aggregated AS (

  SELECT 
    order_item_date AS analysis_date,
    'product' AS entity_type,
    product_short_name AS entity_key,
    product_id,
    COUNT(*) AS items_sold,
    SUM(price_usd) AS total_gross_revenue,
    SUM(CASE
      WHEN is_refunded
        THEN 1
      ELSE 0
    END) AS refunded_items,
    SUM(COALESCE(refund_amount_usd, 0.0)) AS total_refund_amount,
    SUM(net_revenue_usd) AS total_net_revenue
  
  FROM pvd_rlk_item_with_product
  
  GROUP BY 
    order_item_date, product_id, product_short_name

),

pvd_rlk_with_rates AS (

  SELECT 
    analysis_date,
    entity_type,
    entity_key,
    items_sold,
    refunded_items,
    total_gross_revenue,
    total_refund_amount,
    total_net_revenue,
    CASE
      WHEN items_sold > 0
        THEN (refunded_items * 1.0) / items_sold
      ELSE 0.0
    END AS refund_rate,
    CASE
      WHEN total_gross_revenue > 0
        THEN total_refund_amount / total_gross_revenue
      ELSE 0.0
    END AS refund_value_rate,
    LEAST(
      100.0, 
      (
        CASE
          WHEN items_sold > 0
            THEN (refunded_items * 1.0) / items_sold
          ELSE 0.0
        END
      )
      * 80.0
      + (LEAST(CAST(refunded_items AS DOUBLE), 50.0) / 50.0 * 20.0)) AS severity_score
  
  FROM pvd_rlk_aggregated

),

pvd_rlk_final AS (

  SELECT 
    CAST(analysis_date AS DATE) AS analysis_date,
    CAST(entity_type AS STRING) AS entity_type,
    CAST(entity_key AS STRING) AS entity_key,
    CAST(items_sold AS BIGINT) AS items_sold,
    CAST(refunded_items AS BIGINT) AS refunded_items,
    CAST(total_gross_revenue AS DOUBLE) AS total_gross_revenue,
    CAST(total_refund_amount AS DOUBLE) AS total_refund_amount,
    CAST(total_net_revenue AS DOUBLE) AS total_net_revenue,
    CAST(refund_rate AS DOUBLE) AS refund_rate,
    CAST(refund_value_rate AS DOUBLE) AS refund_value_rate,
    CAST(severity_score AS DOUBLE) AS severity_score,
    CAST(CASE
      WHEN refund_rate > 0.15 AND items_sold > 20
        THEN 'Investigate product quality issues'
      WHEN refund_rate > 0.1 AND items_sold > 50
        THEN 'Review product description accuracy'
      WHEN refund_value_rate > 0.1 AND total_gross_revenue > 1000
        THEN 'Analyze refund request patterns'
      ELSE NULL
    END AS STRING) AS recommended_action
  
  FROM pvd_rlk_with_rates

)

SELECT *

FROM pvd_rlk_final
