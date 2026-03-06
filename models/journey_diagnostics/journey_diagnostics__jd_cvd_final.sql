{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH jd_cvd_pageviews AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__website_pageviews') }}

),

jd_cvd_session_pagecount AS (

  SELECT 
    website_session_id,
    COUNT(*) AS pageview_count
  
  FROM jd_cvd_pageviews
  
  GROUP BY website_session_id

),

jd_cvd_sessions AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__website_sessions') }}

),

jd_cvd_orders AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__session_order_bridge') }}

),

jd_cvd_joined AS (

  SELECT 
    s.session_date,
    s.utm_source,
    s.utm_campaign,
    s.utm_content,
    s.website_session_id,
    o.ordered_flag,
    COALESCE(pc.pageview_count, 0) AS pageview_count
  
  FROM jd_cvd_sessions AS s
  LEFT JOIN jd_cvd_orders AS o
     ON s.website_session_id = o.website_session_id
  LEFT JOIN jd_cvd_session_pagecount AS pc
     ON s.website_session_id = pc.website_session_id

),

jd_cvd_aggregated AS (

  SELECT 
    session_date,
    utm_source,
    utm_campaign,
    utm_content,
    COUNT(*) AS total_sessions,
    SUM(ordered_flag) AS conversions,
    SUM(CASE
      WHEN pageview_count = 1
        THEN 1
      ELSE 0
    END) AS bounces
  
  FROM jd_cvd_joined
  
  GROUP BY 
    session_date, utm_source, utm_campaign, utm_content

),

jd_cvd_final AS (

  SELECT 
    CAST(session_date AS DATE) AS session_date,
    CAST(utm_source AS STRING) AS utm_source,
    CAST(utm_campaign AS STRING) AS utm_campaign,
    CAST(utm_content AS STRING) AS utm_content,
    CAST(total_sessions AS BIGINT) AS total_sessions,
    CAST(conversions AS BIGINT) AS conversions,
    CAST(bounces AS BIGINT) AS bounces,
    CAST(CASE
      WHEN total_sessions > 0
        THEN (conversions * 1.0) / total_sessions
      ELSE 0.0
    END AS DOUBLE) AS session_conversion_rate,
    CAST(CASE
      WHEN total_sessions > 0
        THEN (bounces * 1.0) / total_sessions
      ELSE 0.0
    END AS DOUBLE) AS bounce_rate
  
  FROM jd_cvd_aggregated

)

SELECT *

FROM jd_cvd_final
