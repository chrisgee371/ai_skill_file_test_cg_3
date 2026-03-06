{{
  config({    
    "materialized": "table",
    "alias": "diag__experiment_findings",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH def_order_bridge AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'int__session_order_bridge') }}

),

def_exp3_baseline_source_table_001 AS (

  SELECT * 
  
  FROM def_order_bridge

),

def_pageviews AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'stg__website_pageviews') }}

),

def_exp3_variant_source_table_000 AS (

  SELECT * 
  
  FROM def_pageviews

),

def_exp3_variant_source_table_001 AS (

  SELECT * 
  
  FROM def_order_bridge

),

def_exp3_variant_from_000 AS (

  SELECT 
    pv.pageview_url,
    ob.website_session_id AS ob_website_session_id,
    pv.website_session_id AS pv_website_session_id,
    pv.page_date,
    ob.ordered_flag
  
  FROM def_exp3_variant_source_table_000 AS pv
  LEFT JOIN def_exp3_variant_source_table_001 AS ob
     ON pv.website_session_id = ob.website_session_id

),

def_exp3_variant_filter_001 AS (

  SELECT * 
  
  FROM def_exp3_variant_from_000
  
  WHERE pageview_url = '/billing-2' AND page_date >= DATE('2012-09-10') AND page_date <= DATE('2013-01-05')

),

def_entry_page AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'int__session_entry_page') }}

),

def_page_sequence AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'int__session_page_sequence') }}

),

def_bounces AS (

  SELECT 
    website_session_id,
    MAX(page_sequence_number) AS max_page
  
  FROM def_page_sequence
  
  GROUP BY website_session_id

),

def_sessions AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'stg__website_sessions') }}

),

def_session_landing AS (

  SELECT 
    s.website_session_id,
    s.session_date,
    ep.entry_page,
    COALESCE(ob.ordered_flag, 0) AS ordered_flag,
    CASE
      WHEN b.max_page = 1
        THEN 1
      ELSE 0
    END AS is_bounce
  
  FROM def_sessions AS s
  INNER JOIN def_entry_page AS ep
     ON s.website_session_id = ep.website_session_id
  LEFT JOIN def_order_bridge AS ob
     ON s.website_session_id = ob.website_session_id
  LEFT JOIN def_bounces AS b
     ON s.website_session_id = b.website_session_id

),

def_exp1_baseline_source_table_000 AS (

  SELECT * 
  
  FROM def_session_landing

),

def_exp1_baseline_from_000 AS (

  SELECT 
    ordered_flag,
    entry_page,
    session_date
  
  FROM def_exp1_baseline_source_table_000

),

def_exp2_baseline_source_table_000 AS (

  SELECT * 
  
  FROM def_session_landing

),

def_exp2_baseline_from_000 AS (

  SELECT 
    ordered_flag,
    entry_page,
    session_date
  
  FROM def_exp2_baseline_source_table_000

),

def_exp2_baseline_filter_001 AS (

  SELECT * 
  
  FROM def_exp2_baseline_from_000
  
  WHERE entry_page = '/home' AND session_date >= DATE('2013-01-14') AND session_date <= DATE('2014-12-27')

),

def_exp2_baseline_projection_002 AS (

  SELECT 
    COUNT(*) AS baseline_sessions,
    SUM(ordered_flag) AS baseline_conversions
  
  FROM def_exp2_baseline_filter_001

),

def_exp2_variant_source_table_000 AS (

  SELECT * 
  
  FROM def_session_landing

),

def_exp2_variant_from_000 AS (

  SELECT 
    ordered_flag,
    is_bounce,
    entry_page,
    session_date
  
  FROM def_exp2_variant_source_table_000

),

def_exp2_variant_filter_001 AS (

  SELECT * 
  
  FROM def_exp2_variant_from_000
  
  WHERE entry_page = '/lander-2'
        AND session_date >= DATE('2013-01-14')
        AND session_date <= DATE('2014-12-27')

),

def_exp2_variant_groupBy_002 AS (

  SELECT 
    'landing_page' AS experiment_family,
    DATE('2013-01-14') AS comparison_window_start,
    DATE('2014-12-27') AS comparison_window_end,
    '/lander-2' AS variant_key,
    '/home' AS baseline_key,
    COUNT(*) AS sessions,
    SUM(ordered_flag) AS conversions,
    SUM(is_bounce) AS bounces
  
  FROM def_exp2_variant_filter_001
  
  GROUP BY 
    1, 2, 3, 4, 5

),

def_exp1_baseline_filter_001 AS (

  SELECT * 
  
  FROM def_exp1_baseline_from_000
  
  WHERE entry_page = '/home' AND session_date >= DATE('2012-06-19') AND session_date <= DATE('2013-03-10')

),

def_exp1_baseline_projection_002 AS (

  SELECT 
    COUNT(*) AS baseline_sessions,
    SUM(ordered_flag) AS baseline_conversions
  
  FROM def_exp1_baseline_filter_001

),

def_exp3_baseline_source_table_000 AS (

  SELECT * 
  
  FROM def_pageviews

),

def_exp3_baseline_from_000 AS (

  SELECT 
    ob.website_session_id AS ob_website_session_id,
    pv.website_session_id AS pv_website_session_id,
    ob.ordered_flag,
    pv.pageview_url,
    pv.page_date
  
  FROM def_exp3_baseline_source_table_000 AS pv
  LEFT JOIN def_exp3_baseline_source_table_001 AS ob
     ON pv.website_session_id = ob.website_session_id

),

def_exp3_baseline_filter_001 AS (

  SELECT * 
  
  FROM def_exp3_baseline_from_000
  
  WHERE pageview_url = '/billing' AND page_date >= DATE('2012-09-10') AND page_date <= DATE('2013-01-05')

),

def_exp3_baseline_projection_002 AS (

  SELECT 
    DISTINCT COUNT(DISTINCT pv_website_session_id) AS baseline_sessions,
    SUM(COALESCE(ordered_flag, 0)) AS baseline_conversions
  
  FROM def_exp3_baseline_filter_001

),

def_exp1_variant_source_table_000 AS (

  SELECT * 
  
  FROM def_session_landing

),

def_exp1_variant_from_000 AS (

  SELECT 
    ordered_flag,
    is_bounce,
    entry_page,
    session_date
  
  FROM def_exp1_variant_source_table_000

),

def_exp1_variant_filter_001 AS (

  SELECT * 
  
  FROM def_exp1_variant_from_000
  
  WHERE entry_page = '/lander-1'
        AND session_date >= DATE('2012-06-19')
        AND session_date <= DATE('2013-03-10')

),

def_exp1_variant_groupBy_002 AS (

  SELECT 
    'landing_page' AS experiment_family,
    DATE('2012-06-19') AS comparison_window_start,
    DATE('2013-03-10') AS comparison_window_end,
    '/lander-1' AS variant_key,
    '/home' AS baseline_key,
    COUNT(*) AS sessions,
    SUM(ordered_flag) AS conversions,
    SUM(is_bounce) AS bounces
  
  FROM def_exp1_variant_filter_001
  
  GROUP BY 
    1, 2, 3, 4, 5

),

def_exp2_combined AS (

  SELECT 
    v.experiment_family,
    v.comparison_window_start,
    v.comparison_window_end,
    v.variant_key,
    v.baseline_key,
    v.sessions,
    v.conversions,
    v.bounces,
    b.baseline_sessions,
    b.baseline_conversions
  
  FROM def_exp2_variant_groupBy_002 AS v
  CROSS JOIN def_exp2_baseline_projection_002 AS b

),

def_exp1_combined AS (

  SELECT 
    v.experiment_family,
    v.comparison_window_start,
    v.comparison_window_end,
    v.variant_key,
    v.baseline_key,
    v.sessions,
    v.conversions,
    v.bounces,
    b.baseline_sessions,
    b.baseline_conversions
  
  FROM def_exp1_variant_groupBy_002 AS v
  CROSS JOIN def_exp1_baseline_projection_002 AS b

),

def_exp3_variant_groupBy_002 AS (

  SELECT 
    'checkout_page' AS experiment_family,
    DATE('2012-09-10') AS comparison_window_start,
    DATE('2013-01-05') AS comparison_window_end,
    '/billing-2' AS variant_key,
    '/billing' AS baseline_key,
    COUNT(DISTINCT pv_website_session_id) AS sessions,
    SUM(COALESCE(ordered_flag, 0)) AS conversions,
    0 AS bounces
  
  FROM def_exp3_variant_filter_001
  
  GROUP BY 
    1, 2, 3, 4, 5

),

def_exp3_combined AS (

  SELECT 
    v.experiment_family,
    v.comparison_window_start,
    v.comparison_window_end,
    v.variant_key,
    v.baseline_key,
    v.sessions,
    v.conversions,
    v.bounces,
    b.baseline_sessions,
    b.baseline_conversions
  
  FROM def_exp3_variant_groupBy_002 AS v
  CROSS JOIN def_exp3_baseline_projection_002 AS b

),

def_all_experiments AS (

  SELECT * 
  
  FROM def_exp1_combined
  
  UNION ALL
  
  SELECT * 
  
  FROM def_exp2_combined
  
  UNION ALL
  
  SELECT * 
  
  FROM def_exp3_combined

),

def_final AS (

  SELECT 
    CAST(experiment_family AS STRING) AS experiment_family,
    CAST(comparison_window_start AS DATE) AS comparison_window_start,
    CAST(comparison_window_end AS DATE) AS comparison_window_end,
    CAST(variant_key AS STRING) AS variant_key,
    CAST(baseline_key AS STRING) AS baseline_key,
    CAST(sessions AS BIGINT) AS sessions,
    CAST(conversions AS BIGINT) AS conversions,
    CAST(bounces AS BIGINT) AS bounces,
    CAST(CASE
      WHEN sessions > 0
        THEN CAST(conversions AS DOUBLE) / sessions
      ELSE 0.0
    END AS DOUBLE) AS session_conversion_rate,
    CAST(CASE
      WHEN sessions > 0
        THEN CAST(bounces AS DOUBLE) / sessions
      ELSE 0.0
    END AS DOUBLE) AS bounce_rate,
    CAST(CASE
      WHEN baseline_sessions > 0 AND baseline_conversions > 0
        THEN (
          (CAST(conversions AS DOUBLE) / NULLIF(sessions, 0))
          - (CAST(baseline_conversions AS DOUBLE) / baseline_sessions)
        )
        / (CAST(baseline_conversions AS DOUBLE) / baseline_sessions)
      ELSE NULL
    END AS DOUBLE) AS relative_lift,
    CAST(CASE
      WHEN baseline_sessions > 0 AND sessions > 0
        THEN CASE
          WHEN (CAST(conversions AS DOUBLE) / sessions) > (CAST(baseline_conversions AS DOUBLE) / baseline_sessions) * 1.05
            THEN 'variant_better'
          WHEN (CAST(conversions AS DOUBLE) / sessions) < (CAST(baseline_conversions AS DOUBLE) / baseline_sessions) * 0.95
            THEN 'baseline_better'
          ELSE 'no_clear_winner'
        END
      ELSE NULL
    END AS STRING) AS finding_direction
  
  FROM def_all_experiments

)

SELECT *

FROM def_final
