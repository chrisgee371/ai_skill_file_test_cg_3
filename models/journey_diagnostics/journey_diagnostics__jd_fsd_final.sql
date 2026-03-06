{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH jd_fsd_sequence AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__session_page_sequence') }}

),

jd_fsd_session_max_category AS (

  SELECT 
    website_session_id,
    MAX(CASE
      WHEN page_category IN ('home', 'lander')
        THEN 1
      ELSE 0
    END) AS reached_landing,
    MAX(CASE
      WHEN page_category = 'products'
        THEN 1
      ELSE 0
    END) AS reached_products,
    MAX(CASE
      WHEN page_category = 'product_detail'
        THEN 1
      ELSE 0
    END) AS reached_detail,
    MAX(CASE
      WHEN page_category = 'cart'
        THEN 1
      ELSE 0
    END) AS reached_cart,
    MAX(CASE
      WHEN page_category = 'shipping'
        THEN 1
      ELSE 0
    END) AS reached_shipping,
    MAX(CASE
      WHEN page_category = 'billing'
        THEN 1
      ELSE 0
    END) AS reached_billing,
    MAX(CASE
      WHEN page_category = 'thankyou'
        THEN 1
      ELSE 0
    END) AS reached_thankyou
  
  FROM jd_fsd_sequence
  
  GROUP BY website_session_id

),

jd_fsd_sessions AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__website_sessions') }}

),

jd_fsd_session_with_date AS (

  SELECT 
    s.session_date,
    m.website_session_id,
    m.reached_landing,
    m.reached_products,
    m.reached_detail,
    m.reached_cart,
    m.reached_shipping,
    m.reached_billing,
    m.reached_thankyou
  
  FROM jd_fsd_session_max_category AS m
  INNER JOIN jd_fsd_sessions AS s
     ON m.website_session_id = s.website_session_id

),

jd_fsd_detail_agg AS (

  SELECT 
    session_date,
    'product_detail' AS funnel_step,
    SUM(reached_detail) AS sessions_entering,
    SUM(reached_cart) AS sessions_continuing,
    SUM(reached_detail) - SUM(reached_cart) AS sessions_dropped
  
  FROM jd_fsd_session_with_date
  
  GROUP BY session_date

),

jd_fsd_products_agg AS (

  SELECT 
    session_date,
    'products' AS funnel_step,
    SUM(reached_products) AS sessions_entering,
    SUM(reached_detail) AS sessions_continuing,
    SUM(reached_products) - SUM(reached_detail) AS sessions_dropped
  
  FROM jd_fsd_session_with_date
  
  GROUP BY session_date

),

jd_fsd_cart_agg AS (

  SELECT 
    session_date,
    'cart' AS funnel_step,
    SUM(reached_cart) AS sessions_entering,
    SUM(reached_shipping) AS sessions_continuing,
    SUM(reached_cart) - SUM(reached_shipping) AS sessions_dropped
  
  FROM jd_fsd_session_with_date
  
  GROUP BY session_date

),

jd_fsd_billing_agg AS (

  SELECT 
    session_date,
    'billing' AS funnel_step,
    SUM(reached_billing) AS sessions_entering,
    SUM(reached_thankyou) AS sessions_continuing,
    SUM(reached_billing) - SUM(reached_thankyou) AS sessions_dropped
  
  FROM jd_fsd_session_with_date
  
  GROUP BY session_date

),

jd_fsd_landing_agg AS (

  SELECT 
    session_date,
    'landing' AS funnel_step,
    SUM(reached_landing) AS sessions_entering,
    SUM(reached_products) AS sessions_continuing,
    SUM(reached_landing) - SUM(reached_products) AS sessions_dropped
  
  FROM jd_fsd_session_with_date
  
  GROUP BY session_date

),

jd_fsd_shipping_agg AS (

  SELECT 
    session_date,
    'shipping' AS funnel_step,
    SUM(reached_shipping) AS sessions_entering,
    SUM(reached_billing) AS sessions_continuing,
    SUM(reached_shipping) - SUM(reached_billing) AS sessions_dropped
  
  FROM jd_fsd_session_with_date
  
  GROUP BY session_date

),

jd_fsd_combined AS (

  SELECT * 
  
  FROM jd_fsd_landing_agg
  
  UNION ALL
  
  SELECT * 
  
  FROM jd_fsd_products_agg
  
  UNION ALL
  
  SELECT * 
  
  FROM jd_fsd_detail_agg
  
  UNION ALL
  
  SELECT * 
  
  FROM jd_fsd_cart_agg
  
  UNION ALL
  
  SELECT * 
  
  FROM jd_fsd_shipping_agg
  
  UNION ALL
  
  SELECT * 
  
  FROM jd_fsd_billing_agg

),

jd_fsd_final AS (

  SELECT 
    CAST(session_date AS DATE) AS session_date,
    CAST(funnel_step AS STRING) AS funnel_step,
    CAST(sessions_entering AS BIGINT) AS sessions_entering,
    CAST(sessions_continuing AS BIGINT) AS sessions_continuing,
    CAST(sessions_dropped AS BIGINT) AS sessions_dropped,
    CAST(CASE
      WHEN sessions_entering > 0
        THEN (CAST(sessions_continuing AS DOUBLE) / CAST(sessions_entering AS DOUBLE))
      ELSE 0.0
    END AS DOUBLE) AS step_through_rate,
    CAST(CASE
      WHEN sessions_entering > 0
        THEN (CAST(sessions_dropped AS DOUBLE) / CAST(sessions_entering AS DOUBLE))
      ELSE 0.0
    END AS DOUBLE) AS dropoff_rate
  
  FROM jd_fsd_combined

)

SELECT *

FROM jd_fsd_final
