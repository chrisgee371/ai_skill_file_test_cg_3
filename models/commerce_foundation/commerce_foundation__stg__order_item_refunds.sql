{{
  config({    
    "materialized": "table",
    "alias": "commerce_foundation__stg__order_item_refunds",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH stg_oir_source AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'order_item_refunds') }}

),

stg_oir_typed AS (

  SELECT 
    CAST(order_item_refund_id AS BIGINT) AS order_item_refund_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(DATE(created_at) AS DATE) AS refund_date,
    CAST(order_item_id AS BIGINT) AS order_item_id,
    CAST(order_id AS BIGINT) AS order_id,
    CAST(refund_amount_usd AS DOUBLE) AS refund_amount_usd
  
  FROM stg_oir_source

)

SELECT *

FROM stg_oir_typed
