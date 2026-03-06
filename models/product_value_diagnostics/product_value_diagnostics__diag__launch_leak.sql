{{
  config({    
    "materialized": "table",
    "alias": "product_value_diagnostics__diag__launch_leak",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH pvd_llk_net_value AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__order_item_net_value') }}

),

pvd_llk_products AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__dim__products') }}

),

pvd_llk_item_with_product AS (

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
  
  FROM pvd_llk_net_value AS nv
  INNER JOIN pvd_llk_products AS p
     ON nv.product_id = p.product_id

),

pvd_llk_launch_window AS (

  SELECT * 
  
  FROM pvd_llk_item_with_product
  
  WHERE days_since_launch >= 0 AND days_since_launch <= 30

),

pvd_llk_aggregated AS (

  SELECT 
    product_launch_date AS analysis_date,
    'product_launch' AS entity_type,
    product_short_name AS entity_key,
    product_id,
    product_name,
    COUNT(*) AS launch_items_sold,
    SUM(price_usd) AS launch_gross_revenue,
    SUM(gross_margin_usd) AS launch_gross_margin,
    SUM(CASE
      WHEN is_refunded
        THEN 1
      ELSE 0
    END) AS launch_refunded_items,
    SUM(COALESCE(refund_amount_usd, 0.0)) AS launch_refund_amount,
    SUM(net_revenue_usd) AS launch_net_revenue
  
  FROM pvd_llk_launch_window
  
  GROUP BY 
    product_launch_date, product_id, product_name, product_short_name

),

pvd_llk_with_rates AS (

  SELECT 
    analysis_date,
    entity_type,
    entity_key,
    product_id,
    product_name,
    launch_items_sold,
    launch_gross_revenue,
    launch_gross_margin,
    launch_refunded_items,
    launch_refund_amount,
    launch_net_revenue,
    CASE
      WHEN launch_items_sold > 0
        THEN (launch_refunded_items * 1.0) / launch_items_sold
      ELSE 0.0
    END AS launch_refund_rate,
    CASE
      WHEN launch_items_sold > 0
        THEN launch_net_revenue / launch_items_sold
      ELSE 0.0
    END AS net_revenue_per_item,
    LEAST(
      100.0, 
      (
        CASE
          WHEN launch_items_sold > 0
            THEN (launch_refunded_items * 1.0) / launch_items_sold
          ELSE 0.0
        END
      )
      * 50.0
      + (100.0 - LEAST(CAST(launch_items_sold AS DOUBLE), 100.0)) * 0.5) AS severity_score
  
  FROM pvd_llk_aggregated

),

pvd_llk_final AS (

  SELECT 
    CAST(analysis_date AS DATE) AS analysis_date,
    CAST(entity_type AS STRING) AS entity_type,
    CAST(entity_key AS STRING) AS entity_key,
    CAST(launch_items_sold AS BIGINT) AS launch_items_sold,
    CAST(launch_gross_revenue AS DOUBLE) AS launch_gross_revenue,
    CAST(launch_gross_margin AS DOUBLE) AS launch_gross_margin,
    CAST(launch_refunded_items AS BIGINT) AS launch_refunded_items,
    CAST(launch_refund_amount AS DOUBLE) AS launch_refund_amount,
    CAST(launch_net_revenue AS DOUBLE) AS launch_net_revenue,
    CAST(launch_refund_rate AS DOUBLE) AS launch_refund_rate,
    CAST(net_revenue_per_item AS DOUBLE) AS net_revenue_per_item,
    CAST(severity_score AS DOUBLE) AS severity_score,
    CAST(CASE
      WHEN launch_refund_rate > 0.1 AND launch_items_sold > 10
        THEN 'Investigate early quality or expectation issues'
      WHEN launch_items_sold < 20 AND DATEDIFF(CURRENT_DATE(), analysis_date) > 30
        THEN 'Review launch marketing effectiveness'
      WHEN net_revenue_per_item < 30.0 AND launch_items_sold > 50
        THEN 'Evaluate launch pricing strategy'
      ELSE NULL
    END AS STRING) AS recommended_action
  
  FROM pvd_llk_with_rates

)

SELECT *

FROM pvd_llk_final
