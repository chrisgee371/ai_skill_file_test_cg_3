{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH jd_chkd_sequence AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__session_page_sequence') }}

),

jd_chkd_thankyou_sessions_source_table_000 AS (

  SELECT * 
  
  FROM jd_chkd_sequence

),

jd_chkd_billing_sessions_source_table_000 AS (

  SELECT * 
  
  FROM jd_chkd_sequence

),

jd_chkd_billing_sessions_from_000 AS (

  SELECT 
    website_session_id,
    pageview_url,
    CASE
      WHEN pageview_url = '/billing'
        THEN 'billing_v1'
      WHEN pageview_url = '/billing-2'
        THEN 'billing_v2'
      ELSE 'unknown'
    END AS billing_variant
  
  FROM jd_chkd_billing_sessions_source_table_000

),

jd_chkd_billing_sessions_filter_001 AS (

  SELECT * 
  
  FROM jd_chkd_billing_sessions_from_000
  
  WHERE pageview_url IN ('/billing', '/billing-2')

),

jd_chkd_billing_sessions_projection_002 AS (

  SELECT 
    website_session_id,
    pageview_url,
    billing_variant
  
  FROM jd_chkd_billing_sessions_filter_001

),

jd_chkd_thankyou_sessions_from_000 AS (

  SELECT 
    website_session_id,
    pageview_url,
    1 AS completed_order
  
  FROM jd_chkd_thankyou_sessions_source_table_000

),

jd_chkd_thankyou_sessions_filter_001 AS (

  SELECT * 
  
  FROM jd_chkd_thankyou_sessions_from_000
  
  WHERE pageview_url = '/thank-you-for-your-order'

),

jd_chkd_thankyou_sessions_projection_002 AS (

  SELECT 
    DISTINCT website_session_id,
    1 AS completed_order
  
  FROM jd_chkd_thankyou_sessions_filter_001

),

jd_chkd_sessions AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__website_sessions') }}

),

jd_chkd_joined AS (

  SELECT 
    s.session_date,
    b.billing_variant,
    b.website_session_id,
    COALESCE(t.completed_order, 0) AS completed_order
  
  FROM jd_chkd_billing_sessions_projection_002 AS b
  INNER JOIN jd_chkd_sessions AS s
     ON b.website_session_id = s.website_session_id
  LEFT JOIN jd_chkd_thankyou_sessions_projection_002 AS t
     ON b.website_session_id = t.website_session_id

),

jd_chkd_aggregated AS (

  SELECT 
    session_date,
    billing_variant,
    COUNT(DISTINCT website_session_id) AS sessions_reaching_billing,
    SUM(completed_order) AS orders_completed
  
  FROM jd_chkd_joined
  
  GROUP BY 
    session_date, billing_variant

),

jd_chkd_final AS (

  SELECT 
    CAST(session_date AS DATE) AS session_date,
    CAST(billing_variant AS STRING) AS billing_variant,
    CAST(sessions_reaching_billing AS BIGINT) AS sessions_reaching_billing,
    CAST(orders_completed AS BIGINT) AS orders_completed,
    CAST(CASE
      WHEN sessions_reaching_billing > 0
        THEN (orders_completed * 1.0) / sessions_reaching_billing
      ELSE 0.0
    END AS DOUBLE) AS checkout_completion_rate
  
  FROM jd_chkd_aggregated

)

SELECT *

FROM jd_chkd_final
