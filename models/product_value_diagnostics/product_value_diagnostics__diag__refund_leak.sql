{{
  config({    
    "materialized": "table",
    "alias": "diag__refund_leak",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH drl_order_item_net AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'int__order_item_net_value') }}

),

drl_item_with_refund AS (

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
  
  FROM drl_order_item_net AS oin

),

drl_product_day_agg AS (

  SELECT 
    iwr.item_date AS analysis_date,
    iwr.product_id,
    COUNT(*) AS items_sold,
    SUM(iwr.is_refunded) AS items_refunded,
    SUM(iwr.price_usd) AS total_gross_revenue,
    SUM(iwr.refund_amount_usd) AS total_refund_value
  
  FROM drl_item_with_refund AS iwr
  
  GROUP BY 
    iwr.item_date, iwr.product_id

),

drl_with_rates AS (

  SELECT 
    pda.analysis_date,
    pda.product_id,
    pda.items_sold,
    pda.items_refunded,
    CASE
      WHEN pda.total_gross_revenue > 0
        THEN pda.total_refund_value / pda.total_gross_revenue
      ELSE 0.0
    END AS refund_value_share,
    CASE
      WHEN pda.items_sold > 0
        THEN CAST(pda.items_refunded AS DOUBLE) / pda.items_sold
      ELSE 0.0
    END AS refunded_item_rate
  
  FROM drl_product_day_agg AS pda

),

drl_scored AS (

  SELECT 
    analysis_date,
    product_id,
    items_sold,
    items_refunded,
    refund_value_share,
    refunded_item_rate,
    CASE
      WHEN items_sold < 5
        THEN 0.0
      ELSE LEAST(100.0, GREATEST(0.0, refund_value_share * 200.0 + refunded_item_rate * 100.0))
    END AS severity_score
  
  FROM drl_with_rates

),

drl_final AS (

  SELECT 
    CAST(analysis_date AS DATE) AS analysis_date,
    CAST(product_id AS BIGINT) AS product_id,
    CAST(items_sold AS BIGINT) AS items_sold,
    CAST(items_refunded AS BIGINT) AS items_refunded,
    CAST(refund_value_share AS DOUBLE) AS refund_value_share,
    CAST(refunded_item_rate AS DOUBLE) AS refunded_item_rate,
    CAST(severity_score AS DOUBLE) AS severity_score,
    CAST(CASE
      WHEN severity_score >= 70
        THEN 'Critical refund leak - review product quality or returns policy'
      WHEN severity_score >= 40
        THEN 'Elevated refund rate - investigate customer feedback'
      WHEN severity_score >= 20
        THEN 'Monitor refund trends'
      ELSE NULL
    END AS STRING) AS recommended_action
  
  FROM drl_scored

)

SELECT *

FROM drl_final
