{{
  config({    
    "materialized": "table",
    "alias": "diag__acquisition_leak",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH dal_page_sequence AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'int__session_page_sequence') }}

),

dal_bounces AS (

  SELECT 
    website_session_id,
    MAX(page_sequence_number) AS max_page
  
  FROM dal_page_sequence
  
  GROUP BY website_session_id

),

dal_entry_page AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'int__session_entry_page') }}

),

dal_sessions AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'stg__website_sessions') }}

),

dal_order_bridge AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'int__session_order_bridge') }}

),

dal_order_item_net AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'int__order_item_net_value') }}

),

dal_order_net_revenue AS (

  SELECT 
    order_id,
    SUM(net_revenue_usd) AS net_revenue_usd
  
  FROM dal_order_item_net
  
  GROUP BY order_id

),

dal_session_enriched AS (

  SELECT 
    s.website_session_id,
    s.session_date,
    s.utm_source,
    s.utm_campaign,
    s.utm_content,
    s.device_type,
    ep.entry_page,
    ob.ordered_flag,
    ob.gross_revenue_usd,
    onr.net_revenue_usd,
    CASE
      WHEN b.max_page = 1
        THEN 1
      ELSE 0
    END AS is_bounce
  
  FROM dal_sessions AS s
  LEFT JOIN dal_entry_page AS ep
     ON s.website_session_id = ep.website_session_id
  LEFT JOIN dal_order_bridge AS ob
     ON s.website_session_id = ob.website_session_id
  LEFT JOIN dal_order_net_revenue AS onr
     ON ob.order_id = onr.order_id
  LEFT JOIN dal_bounces AS b
     ON s.website_session_id = b.website_session_id

),

dal_entry_metrics AS (

  SELECT 
    session_date AS analysis_date,
    'entry_page' AS entity_type,
    COALESCE(entry_page, 'unknown') AS entity_key,
    COUNT(*) AS sessions,
    SUM(ordered_flag) AS conversions,
    SUM(is_bounce) AS bounces,
    SUM(COALESCE(gross_revenue_usd, 0.0)) AS gross_revenue,
    SUM(COALESCE(net_revenue_usd, 0.0)) AS net_revenue
  
  FROM dal_session_enriched
  
  GROUP BY 
    session_date, COALESCE(entry_page, 'unknown')

),

dal_source_metrics AS (

  SELECT 
    session_date AS analysis_date,
    'traffic_source' AS entity_type,
    COALESCE(utm_source, 'direct') AS entity_key,
    COUNT(*) AS sessions,
    SUM(ordered_flag) AS conversions,
    SUM(is_bounce) AS bounces,
    SUM(COALESCE(gross_revenue_usd, 0.0)) AS gross_revenue,
    SUM(COALESCE(net_revenue_usd, 0.0)) AS net_revenue
  
  FROM dal_session_enriched
  
  GROUP BY 
    session_date, COALESCE(utm_source, 'direct')

),

dal_campaign_metrics AS (

  SELECT 
    session_date AS analysis_date,
    'campaign' AS entity_type,
    COALESCE(utm_campaign, 'none') AS entity_key,
    COUNT(*) AS sessions,
    SUM(ordered_flag) AS conversions,
    SUM(is_bounce) AS bounces,
    SUM(COALESCE(gross_revenue_usd, 0.0)) AS gross_revenue,
    SUM(COALESCE(net_revenue_usd, 0.0)) AS net_revenue
  
  FROM dal_session_enriched
  
  GROUP BY 
    session_date, COALESCE(utm_campaign, 'none')

),

dal_combined AS (

  SELECT * 
  
  FROM dal_source_metrics
  
  UNION ALL
  
  SELECT * 
  
  FROM dal_entry_metrics
  
  UNION ALL
  
  SELECT * 
  
  FROM dal_campaign_metrics

),

dal_daily_benchmark AS (

  SELECT 
    session_date AS analysis_date,
    SUM(ordered_flag) * 1.0 / NULLIF(COUNT(*), 0) AS avg_conversion_rate,
    SUM(COALESCE(gross_revenue_usd, 0.0)) / NULLIF(COUNT(*), 0) AS avg_rps,
    SUM(COALESCE(net_revenue_usd, 0.0)) / NULLIF(COUNT(*), 0) AS avg_net_rps
  
  FROM dal_session_enriched
  
  GROUP BY session_date

),

dal_with_benchmark AS (

  SELECT 
    c.analysis_date,
    c.entity_type,
    c.entity_key,
    c.sessions,
    c.conversions,
    c.bounces,
    c.gross_revenue,
    c.net_revenue,
    b.avg_conversion_rate,
    b.avg_rps,
    b.avg_net_rps
  
  FROM dal_combined AS c
  LEFT JOIN dal_daily_benchmark AS b
     ON c.analysis_date = b.analysis_date

),

dal_scored AS (

  SELECT 
    analysis_date,
    entity_type,
    entity_key,
    sessions,
    conversions,
    CASE
      WHEN sessions > 0
        THEN bounces * 1.0 / sessions
      ELSE 0.0
    END AS bounce_rate,
    CASE
      WHEN sessions > 0
        THEN gross_revenue / sessions
      ELSE 0.0
    END AS revenue_per_session,
    CASE
      WHEN sessions > 0
        THEN net_revenue / sessions
      ELSE 0.0
    END AS net_revenue_per_session,
    -- Severity score: weighted underperformance vs benchmark (0-100 bounded)
    CASE
      WHEN sessions < 10
        THEN 0.0
      ELSE LEAST(
        100.0, 
        GREATEST(
          0.0, 
          50.0
          * (
              1.0
              - (
                  CASE
                    WHEN sessions > 0
                      THEN conversions * 1.0 / sessions
                    ELSE 0.0
                  END
                )
                / NULLIF(avg_conversion_rate, 0.0)
            )
          + 30.0
            * (
                1.0
                - (
                    CASE
                      WHEN sessions > 0
                        THEN gross_revenue / sessions
                      ELSE 0.0
                    END
                  )
                  / NULLIF(avg_rps, 0.0)
              )
          + 20.0
            * (
                CASE
                  WHEN sessions > 0
                    THEN bounces * 1.0 / sessions
                  ELSE 0.0
                END
              )))
    END AS severity_score
  
  FROM dal_with_benchmark

),

dal_final AS (

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
    CAST(CASE
      WHEN severity_score >= 70
        THEN 'Investigate immediately - significant underperformance'
      WHEN severity_score >= 40
        THEN 'Review campaign or landing page'
      WHEN severity_score >= 20
        THEN 'Monitor for trends'
      ELSE NULL
    END AS STRING) AS recommended_action
  
  FROM dal_scored

)

SELECT *

FROM dal_final
