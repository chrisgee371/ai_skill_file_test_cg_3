{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH src_prod_raw AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'products') }}

),

src_prod_typed AS (

  SELECT 
    CAST(product_id AS BIGINT) AS product_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(product_name AS STRING) AS product_name
  
  FROM src_prod_raw

)

SELECT *

FROM src_prod_typed
