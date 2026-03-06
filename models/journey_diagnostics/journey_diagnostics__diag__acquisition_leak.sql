{{
  config({    
    "materialized": "table",
    "alias": "journey_diagnostics__diag__acquisition_leak",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH jd_acq_sessions AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__website_sessions') }}

),

jd_acq_orders AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__session_order_bridge') }}

),

jd_acq_entry AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__session_entry_page') }}

),

jd_acq_net_value AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__int__order_item_net_value') }}

),

jd_acq_order_net AS (

  SELECT 
    order_id,
    SUM(net_revenue_usd) AS order_net_revenue_usd
  
  FROM jd_acq_net_value
  
  GROUP BY order_id

),

jd_acq_pageviews AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'commerce_foundation__stg__website_pageviews') }}

),

jd_acq_session_pagecount AS (

  SELECT 
    website_session_id,
    COUNT(*) AS pageview_count
  
  FROM jd_acq_pageviews
  
  GROUP BY website_session_id

),

jd_acq_joined AS (

  SELECT 
    s.session_date,
    s.utm_source,
    s.utm_campaign,
    e.entry_page,
    s.website_session_id,
    o.ordered_flag,
    o.gross_revenue_usd,
    COALESCE(n.order_net_revenue_usd, 0.0) AS net_revenue_usd,
    COALESCE(pc.pageview_count, 0) AS pageview_count
  
  FROM jd_acq_sessions AS s
  LEFT JOIN jd_acq_entry AS e
     ON s.website_session_id = e.website_session_id
  LEFT JOIN jd_acq_orders AS o
     ON s.website_session_id = o.website_session_id
  LEFT JOIN jd_acq_order_net AS n
     ON o.order_id = n.order_id
  LEFT JOIN jd_acq_session_pagecount AS pc
     ON s.website_session_id = pc.website_session_id

),

jd_acq_campaign_agg AS (

  SELECT 
    session_date AS analysis_date,
    'campaign' AS entity_type,
    COALESCE(utm_source, 'direct') || ':' || COALESCE(utm_campaign, 'none') AS entity_key,
    COUNT(*) AS sessions,
    SUM(ordered_flag) AS conversions,
    SUM(CASE
      WHEN pageview_count = 1
        THEN 1
      ELSE 0
    END) AS bounces,
    SUM(COALESCE(gross_revenue_usd, 0.0)) AS total_gross_revenue,
    SUM(COALESCE(net_revenue_usd, 0.0)) AS total_net_revenue
  
  FROM jd_acq_joined
  
  GROUP BY 
    session_date, utm_source, utm_campaign

),

jd_acq_union_campaign AS (

  SELECT * 
  
  FROM jd_acq_campaign_agg

),

jd_acq_entrypage_agg AS (

  SELECT 
    session_date AS analysis_date,
    'entry_page' AS entity_type,
    COALESCE(entry_page, 'unknown') AS entity_key,
    COUNT(*) AS sessions,
    SUM(ordered_flag) AS conversions,
    SUM(CASE
      WHEN pageview_count = 1
        THEN 1
      ELSE 0
    END) AS bounces,
    SUM(COALESCE(gross_revenue_usd, 0.0)) AS total_gross_revenue,
    SUM(COALESCE(net_revenue_usd, 0.0)) AS total_net_revenue
  
  FROM jd_acq_joined
  
  GROUP BY 
    session_date, entry_page

),

jd_acq_union_entrypage AS (

  SELECT * 
  
  FROM jd_acq_entrypage_agg

),

jd_acq_channel_agg AS (

  SELECT 
    session_date AS analysis_date,
    'channel' AS entity_type,
    COALESCE(utm_source, 'direct') AS entity_key,
    COUNT(*) AS sessions,
    SUM(ordered_flag) AS conversions,
    SUM(CASE
      WHEN pageview_count = 1
        THEN 1
      ELSE 0
    END) AS bounces,
    SUM(COALESCE(gross_revenue_usd, 0.0)) AS total_gross_revenue,
    SUM(COALESCE(net_revenue_usd, 0.0)) AS total_net_revenue
  
  FROM jd_acq_joined
  
  GROUP BY 
    session_date, utm_source

),

jd_acq_union_channel AS (

  SELECT * 
  
  FROM jd_acq_channel_agg

),

jd_acq_combined AS (

  SELECT * 
  
  FROM jd_acq_union_channel
  
  UNION ALL
  
  SELECT * 
  
  FROM jd_acq_union_campaign
  
  UNION ALL
  
  SELECT * 
  
  FROM jd_acq_union_entrypage

),

jd_acq_metrics AS (

  SELECT 
    analysis_date,
    entity_type,
    entity_key,
    sessions,
    conversions,
    bounces,
    total_gross_revenue,
    total_net_revenue,
    CASE
      WHEN sessions > 0
        THEN (bounces * 1.0) / sessions
      ELSE 0.0
    END AS bounce_rate,
    CASE
      WHEN sessions > 0
        THEN total_gross_revenue / sessions
      ELSE 0.0
    END AS revenue_per_session,
    CASE
      WHEN sessions > 0
        THEN total_net_revenue / sessions
      ELSE 0.0
    END AS net_revenue_per_session
  
  FROM jd_acq_combined

),

jd_acq_severity AS (

  SELECT 
    analysis_date,
    entity_type,
    entity_key,
    sessions,
    conversions,
    bounce_rate,
    revenue_per_session,
    net_revenue_per_session,
    -- Severity: normalized score 0-100 based on volume and underperformance
    -- High bounce + low conversion + meaningful volume = high severity
    LEAST(
      100.0, 
      (bounce_rate * 30.0)
      + (
          (
            1.0 - CASE
                WHEN sessions > 0
                  THEN (conversions * 1.0) / sessions
                ELSE 0.0
              END
          )
          * 40.0
        )
      + (LEAST(sessions, 1000) / 1000.0 * 30.0)) AS severity_score
  
  FROM jd_acq_metrics

),

jd_acq_with_action AS (

  SELECT 
    analysis_date,
    entity_type,
    entity_key,
    sessions,
    conversions,
    bounce_rate,
    revenue_per_session,
    net_revenue_per_session,
    severity_score,
    CASE
      WHEN bounce_rate > 0.7 AND entity_type = 'entry_page'
        THEN 'Redesign landing page to improve engagement'
      WHEN bounce_rate > 0.7 AND entity_type = 'channel'
        THEN 'Review ad targeting and audience match'
      WHEN (conversions * 1.0 / NULLIF(sessions, 0)) < 0.01 AND sessions > 100
        THEN 'Investigate conversion blockers'
      WHEN revenue_per_session < 0.5 AND sessions > 100
        THEN 'Optimize for higher-value conversions'
      ELSE NULL
    END AS recommended_action
  
  FROM jd_acq_severity

),

jd_acq_final AS (

  SELECT 
    CAST(analysis_date AS DATE) AS analysis_date,
    CAST(entity_type AS STRING) AS entity_type,
    CAST(entity_key AS STRING) AS entity_key,
    CAST(sessions AS BIGINT) AS sessions,
    CAST(conversions AS BIGINT) AS conversions,
    CAST(bounce_rate AS DOUBLE) AS bounce_rate,
    CAST(revenue_per_session AS DOUBLE) AS revenue_per_session,
    CAST(net_revenue_per_session AS DOUBLE) AS net_revenue_per_session,
    CAST(severity_score AS DOUBLE) AS severity_score,
    CAST(recommended_action AS STRING) AS recommended_action
  
  FROM jd_acq_with_action

)

SELECT *

FROM jd_acq_final
