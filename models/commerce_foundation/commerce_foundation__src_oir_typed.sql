{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH src_oir_raw AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'order_item_refunds') }}

),

src_oir_typed AS (

  SELECT 
    CAST(order_item_refund_id AS BIGINT) AS order_item_refund_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(order_item_id AS BIGINT) AS order_item_id,
    CAST(order_id AS BIGINT) AS order_id,
    CAST(refund_amount_usd AS DOUBLE) AS refund_amount_usd
  
  FROM src_oir_raw

)

SELECT *

FROM src_oir_typed
