{{
  config({    
    "materialized": "table",
    "alias": "int__order_item_net_value",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH int_oinv_refunds AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'order_item_refunds') }}

),

int_oinv_items AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'order_items') }}

),

int_oinv_joined AS (

  SELECT 
    i.order_item_id,
    i.order_id,
    i.product_id,
    i.created_at,
    i.price_usd,
    i.cogs_usd,
    r.refund_amount_usd
  
  FROM int_oinv_items AS i
  LEFT JOIN int_oinv_refunds AS r
     ON i.order_item_id = r.order_item_id

),

int_oinv_final AS (

  SELECT 
    CAST(order_item_id AS BIGINT) AS order_item_id,
    CAST(order_id AS BIGINT) AS order_id,
    CAST(product_id AS BIGINT) AS product_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(price_usd AS DOUBLE) AS price_usd,
    CAST(cogs_usd AS DOUBLE) AS cogs_usd,
    CAST(refund_amount_usd AS DOUBLE) AS refund_amount_usd,
    CAST(price_usd - COALESCE(refund_amount_usd, 0) AS DOUBLE) AS net_revenue_usd
  
  FROM int_oinv_joined

)

SELECT *

FROM int_oinv_final
