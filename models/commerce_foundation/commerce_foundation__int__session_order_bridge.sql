{{
  config({    
    "materialized": "table",
    "alias": "commerce_foundation__int__session_order_bridge",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH int_sob_orders AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'orders') }}

),

int_sob_sessions AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'website_sessions') }}

),

int_sob_joined AS (

  SELECT 
    s.website_session_id,
    DATE(s.created_at) AS session_date,
    o.order_id,
    CASE
      WHEN o.order_id IS NOT NULL
        THEN 1
      ELSE 0
    END AS ordered_flag,
    o.price_usd AS gross_revenue_usd
  
  FROM int_sob_sessions AS s
  LEFT JOIN int_sob_orders AS o
     ON s.website_session_id = o.website_session_id

),

int_sob_final AS (

  SELECT 
    CAST(website_session_id AS BIGINT) AS website_session_id,
    CAST(session_date AS DATE) AS session_date,
    CAST(order_id AS BIGINT) AS order_id,
    CAST(ordered_flag AS BIGINT) AS ordered_flag,
    CAST(gross_revenue_usd AS DOUBLE) AS gross_revenue_usd
  
  FROM int_sob_joined

)

SELECT *

FROM int_sob_final
