{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH pvd_ppm_products AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__dim__products') }}

),

pvd_ppm_net_value AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__order_item_net_value') }}

),

pvd_ppm_item_with_product AS (

  SELECT 
    nv.order_item_id,
    nv.order_item_date,
    nv.order_id,
    nv.product_id,
    nv.is_primary_item,
    nv.price_usd,
    nv.cogs_usd,
    nv.gross_margin_usd,
    nv.refund_amount_usd,
    nv.is_refunded,
    nv.net_revenue_usd,
    nv.net_margin_usd,
    p.product_name,
    p.product_short_name,
    p.product_launch_date
  
  FROM pvd_ppm_net_value AS nv
  INNER JOIN pvd_ppm_products AS p
     ON nv.product_id = p.product_id

),

pvd_ppm_aggregated AS (

  SELECT 
    order_item_date AS analysis_date,
    product_id,
    product_name,
    product_short_name,
    COUNT(*) AS items_sold,
    SUM(CASE
      WHEN is_primary_item = 1
        THEN 1
      ELSE 0
    END) AS primary_items,
    SUM(CASE
      WHEN is_primary_item = 0
        THEN 1
      ELSE 0
    END) AS crosssell_items,
    SUM(price_usd) AS total_gross_revenue,
    SUM(cogs_usd) AS total_cogs,
    SUM(gross_margin_usd) AS total_gross_margin,
    SUM(COALESCE(refund_amount_usd, 0.0)) AS total_refunds,
    SUM(CASE
      WHEN is_refunded
        THEN 1
      ELSE 0
    END) AS refunded_items,
    SUM(net_revenue_usd) AS total_net_revenue,
    SUM(net_margin_usd) AS total_net_margin
  
  FROM pvd_ppm_item_with_product
  
  GROUP BY 
    order_item_date, product_id, product_name, product_short_name

),

pvd_ppm_final AS (

  SELECT 
    CAST(analysis_date AS DATE) AS analysis_date,
    CAST(product_id AS BIGINT) AS product_id,
    CAST(product_name AS STRING) AS product_name,
    CAST(product_short_name AS STRING) AS product_short_name,
    CAST(items_sold AS BIGINT) AS items_sold,
    CAST(primary_items AS BIGINT) AS primary_items,
    CAST(crosssell_items AS BIGINT) AS crosssell_items,
    CAST(total_gross_revenue AS DOUBLE) AS total_gross_revenue,
    CAST(total_cogs AS DOUBLE) AS total_cogs,
    CAST(total_gross_margin AS DOUBLE) AS total_gross_margin,
    CAST(total_refunds AS DOUBLE) AS total_refunds,
    CAST(refunded_items AS BIGINT) AS refunded_items,
    CAST(total_net_revenue AS DOUBLE) AS total_net_revenue,
    CAST(total_net_margin AS DOUBLE) AS total_net_margin,
    CAST(CASE
      WHEN items_sold > 0
        THEN (refunded_items * 1.0) / items_sold
      ELSE 0.0
    END AS DOUBLE) AS refund_rate,
    CAST(CASE
      WHEN total_gross_revenue > 0
        THEN total_gross_margin / total_gross_revenue
      ELSE 0.0
    END AS DOUBLE) AS gross_margin_pct
  
  FROM pvd_ppm_aggregated

)

SELECT *

FROM pvd_ppm_final
