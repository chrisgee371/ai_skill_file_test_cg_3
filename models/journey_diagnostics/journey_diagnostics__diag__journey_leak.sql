{{
  config({    
    "materialized": "table",
    "alias": "journey_diagnostics__diag__journey_leak",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH jd_jlk_sequence AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__session_page_sequence') }}

),

jd_jlk_session_max_category AS (

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
  
  FROM jd_jlk_sequence
  
  GROUP BY website_session_id

),

jd_jlk_sessions AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__website_sessions') }}

),

jd_jlk_session_with_date AS (

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
  
  FROM jd_jlk_session_max_category AS m
  INNER JOIN jd_jlk_sessions AS s
     ON m.website_session_id = s.website_session_id

),

jd_jlk_shipping_agg AS (

  SELECT 
    session_date AS analysis_date,
    'funnel_step' AS entity_type,
    'shipping' AS entity_key,
    SUM(reached_shipping) AS sessions_entering,
    SUM(reached_billing) AS sessions_continuing,
    SUM(reached_shipping) - SUM(reached_billing) AS sessions_dropped
  
  FROM jd_jlk_session_with_date
  
  GROUP BY session_date

),

jd_jlk_billing_agg AS (

  SELECT 
    session_date AS analysis_date,
    'funnel_step' AS entity_type,
    'billing' AS entity_key,
    SUM(reached_billing) AS sessions_entering,
    SUM(reached_thankyou) AS sessions_continuing,
    SUM(reached_billing) - SUM(reached_thankyou) AS sessions_dropped
  
  FROM jd_jlk_session_with_date
  
  GROUP BY session_date

),

jd_jlk_products_agg AS (

  SELECT 
    session_date AS analysis_date,
    'funnel_step' AS entity_type,
    'products' AS entity_key,
    SUM(reached_products) AS sessions_entering,
    SUM(reached_detail) AS sessions_continuing,
    SUM(reached_products) - SUM(reached_detail) AS sessions_dropped
  
  FROM jd_jlk_session_with_date
  
  GROUP BY session_date

),

jd_jlk_landing_agg AS (

  SELECT 
    session_date AS analysis_date,
    'funnel_step' AS entity_type,
    'landing' AS entity_key,
    SUM(reached_landing) AS sessions_entering,
    SUM(reached_products) AS sessions_continuing,
    SUM(reached_landing) - SUM(reached_products) AS sessions_dropped
  
  FROM jd_jlk_session_with_date
  
  GROUP BY session_date

),

jd_jlk_detail_agg AS (

  SELECT 
    session_date AS analysis_date,
    'funnel_step' AS entity_type,
    'product_detail' AS entity_key,
    SUM(reached_detail) AS sessions_entering,
    SUM(reached_cart) AS sessions_continuing,
    SUM(reached_detail) - SUM(reached_cart) AS sessions_dropped
  
  FROM jd_jlk_session_with_date
  
  GROUP BY session_date

),

jd_jlk_cart_agg AS (

  SELECT 
    session_date AS analysis_date,
    'funnel_step' AS entity_type,
    'cart' AS entity_key,
    SUM(reached_cart) AS sessions_entering,
    SUM(reached_shipping) AS sessions_continuing,
    SUM(reached_cart) - SUM(reached_shipping) AS sessions_dropped
  
  FROM jd_jlk_session_with_date
  
  GROUP BY session_date

),

jd_jlk_combined AS (

  SELECT * 
  
  FROM jd_jlk_landing_agg
  
  UNION ALL
  
  SELECT * 
  
  FROM jd_jlk_products_agg
  
  UNION ALL
  
  SELECT * 
  
  FROM jd_jlk_detail_agg
  
  UNION ALL
  
  SELECT * 
  
  FROM jd_jlk_cart_agg
  
  UNION ALL
  
  SELECT * 
  
  FROM jd_jlk_shipping_agg
  
  UNION ALL
  
  SELECT * 
  
  FROM jd_jlk_billing_agg

),

jd_jlk_with_rates AS (

  SELECT 
    analysis_date,
    entity_type,
    entity_key,
    sessions_entering,
    sessions_continuing,
    sessions_dropped,
    CASE
      WHEN entity_key = 'billing' AND sessions_entering > 0
        THEN CAST(sessions_continuing AS DOUBLE) / CAST(sessions_entering AS DOUBLE)
      ELSE NULL
    END AS checkout_completion_rate,
    LEAST(
      100.0, 
      CASE
        WHEN sessions_entering > 0
          THEN (CAST(sessions_dropped AS DOUBLE) / CAST(sessions_entering AS DOUBLE)) * 50.0
        ELSE 0.0
      END
      + (LEAST(CAST(sessions_dropped AS DOUBLE), 1000.0) / 1000.0 * 50.0)) AS severity_score
  
  FROM jd_jlk_combined

),

jd_jlk_final AS (

  SELECT 
    CAST(analysis_date AS DATE) AS analysis_date,
    CAST(entity_type AS STRING) AS entity_type,
    CAST(entity_key AS STRING) AS entity_key,
    CAST(sessions_entering AS BIGINT) AS sessions_entering,
    CAST(sessions_continuing AS BIGINT) AS sessions_continuing,
    CAST(sessions_dropped AS BIGINT) AS sessions_dropped,
    CAST(checkout_completion_rate AS DOUBLE) AS checkout_completion_rate,
    CAST(severity_score AS DOUBLE) AS severity_score,
    CAST(CASE
      WHEN entity_key = 'products' AND sessions_dropped > sessions_continuing
        THEN 'Improve product page discovery and relevance'
      WHEN entity_key = 'product_detail' AND sessions_dropped > sessions_continuing
        THEN 'Enhance product detail page with better CTAs'
      WHEN entity_key = 'cart' AND sessions_dropped > sessions_continuing
        THEN 'Simplify cart experience and add urgency'
      WHEN entity_key = 'shipping' AND sessions_dropped > sessions_continuing
        THEN 'Review shipping costs and delivery options'
      WHEN entity_key = 'billing' AND checkout_completion_rate IS NOT NULL AND checkout_completion_rate < 0.5
        THEN 'Simplify checkout and payment options'
      ELSE NULL
    END AS STRING) AS recommended_action
  
  FROM jd_jlk_with_rates

)

SELECT *

FROM jd_jlk_final
