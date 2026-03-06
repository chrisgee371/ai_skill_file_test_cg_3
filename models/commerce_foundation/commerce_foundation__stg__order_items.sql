{{
  config({    
    "materialized": "table",
    "alias": "stg__order_items",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH stg_oi_source AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'order_items') }}

),

stg_oi_final AS (

  SELECT 
    CAST(order_item_id AS BIGINT) AS order_item_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(order_id AS BIGINT) AS order_id,
    CAST(product_id AS BIGINT) AS product_id,
    CAST(is_primary_item AS BIGINT) AS is_primary_item,
    CAST(price_usd AS DOUBLE) AS price_usd,
    CAST(cogs_usd AS DOUBLE) AS cogs_usd
  
  FROM stg_oi_source

)

SELECT *

FROM stg_oi_final
