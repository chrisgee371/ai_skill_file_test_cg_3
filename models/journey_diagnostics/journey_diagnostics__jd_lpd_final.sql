{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH jd_lpd_net_value AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__order_item_net_value') }}

),

jd_lpd_sessions AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__website_sessions') }}

),

jd_lpd_entry AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__session_entry_page') }}

),

jd_lpd_order_net AS (

  SELECT 
    order_id,
    SUM(net_revenue_usd) AS order_net_revenue_usd
  
  FROM jd_lpd_net_value
  
  GROUP BY order_id

),

jd_lpd_pageviews AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__website_pageviews') }}

),

jd_lpd_session_pagecount AS (

  SELECT 
    website_session_id,
    COUNT(*) AS pageview_count
  
  FROM jd_lpd_pageviews
  
  GROUP BY website_session_id

),

jd_lpd_orders AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__session_order_bridge') }}

),

jd_lpd_joined AS (

  SELECT 
    s.session_date,
    e.entry_page,
    s.website_session_id,
    o.ordered_flag,
    o.gross_revenue_usd,
    COALESCE(n.order_net_revenue_usd, 0.0) AS net_revenue_usd,
    COALESCE(pc.pageview_count, 0) AS pageview_count
  
  FROM jd_lpd_sessions AS s
  INNER JOIN jd_lpd_entry AS e
     ON s.website_session_id = e.website_session_id
  LEFT JOIN jd_lpd_orders AS o
     ON s.website_session_id = o.website_session_id
  LEFT JOIN jd_lpd_order_net AS n
     ON o.order_id = n.order_id
  LEFT JOIN jd_lpd_session_pagecount AS pc
     ON s.website_session_id = pc.website_session_id

),

jd_lpd_aggregated AS (

  SELECT 
    session_date,
    entry_page,
    COUNT(*) AS total_sessions,
    SUM(ordered_flag) AS ordered_sessions,
    SUM(CASE
      WHEN pageview_count = 1
        THEN 1
      ELSE 0
    END) AS bounced_sessions,
    SUM(COALESCE(gross_revenue_usd, 0.0)) AS total_gross_revenue,
    SUM(COALESCE(net_revenue_usd, 0.0)) AS total_net_revenue
  
  FROM jd_lpd_joined
  
  GROUP BY 
    session_date, entry_page

),

jd_lpd_final AS (

  SELECT 
    CAST(session_date AS DATE) AS session_date,
    CAST(entry_page AS STRING) AS entry_page,
    CAST(total_sessions AS BIGINT) AS total_sessions,
    CAST(ordered_sessions AS BIGINT) AS ordered_sessions,
    CAST(CASE
      WHEN total_sessions > 0
        THEN (ordered_sessions * 1.0) / total_sessions
      ELSE 0.0
    END AS DOUBLE) AS session_conversion_rate,
    CAST(CASE
      WHEN total_sessions > 0
        THEN (bounced_sessions * 1.0) / total_sessions
      ELSE 0.0
    END AS DOUBLE) AS bounce_rate,
    CAST(CASE
      WHEN total_sessions > 0
        THEN total_gross_revenue / total_sessions
      ELSE 0.0
    END AS DOUBLE) AS revenue_per_session,
    CAST(CASE
      WHEN total_sessions > 0
        THEN total_net_revenue / total_sessions
      ELSE 0.0
    END AS DOUBLE) AS net_revenue_per_session
  
  FROM jd_lpd_aggregated

)

SELECT *

FROM jd_lpd_final
