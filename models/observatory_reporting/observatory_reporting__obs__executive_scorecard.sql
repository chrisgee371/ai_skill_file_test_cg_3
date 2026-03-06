{{
  config({    
    "materialized": "table",
    "alias": "obs__executive_scorecard",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH oes_basket AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__basket_leak') }}

),

oes_bsk_counts_source_table_000 AS (

  SELECT * 
  
  FROM oes_basket

),

oes_bsk_counts_from_000 AS (

  SELECT 
    leak_severity,
    estimated_leaked_cross_sell_revenue_usd
  
  FROM oes_bsk_counts_source_table_000

),

oes_bsk_counts_filter_001 AS (

  SELECT * 
  
  FROM oes_bsk_counts_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

oes_bsk_counts_projection_002 AS (

  SELECT 
    COUNT(*) AS cnt,
    SUM(CASE
      WHEN leak_severity IN ('critical', 'warning')
        THEN 1
      ELSE 0
    END) AS high_cnt,
    SUM(
      CASE
        WHEN leak_severity = 'critical'
          THEN 90
        WHEN leak_severity = 'warning'
          THEN 70
        WHEN leak_severity = 'moderate'
          THEN 40
        ELSE 0
      END) AS sev_sum,
    SUM(COALESCE(estimated_leaked_cross_sell_revenue_usd, 0)) AS leak_val
  
  FROM oes_bsk_counts_filter_001

),

oes_acquisition AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__acquisition_leak') }}

),

oes_journey AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__journey_leak') }}

),

oes_jny_counts_source_table_000 AS (

  SELECT * 
  
  FROM oes_journey

),

oes_refund AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__refund_leak') }}

),

oes_rfd_counts_source_table_000 AS (

  SELECT * 
  
  FROM oes_refund

),

oes_jny_counts_from_000 AS (

  SELECT 
    leak_severity,
    estimated_leaked_revenue_usd
  
  FROM oes_jny_counts_source_table_000

),

oes_jny_counts_filter_001 AS (

  SELECT * 
  
  FROM oes_jny_counts_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

oes_jny_counts_projection_002 AS (

  SELECT 
    COUNT(*) AS cnt,
    SUM(CASE
      WHEN leak_severity IN ('critical', 'warning')
        THEN 1
      ELSE 0
    END) AS high_cnt,
    SUM(
      CASE
        WHEN leak_severity = 'critical'
          THEN 90
        WHEN leak_severity = 'warning'
          THEN 70
        WHEN leak_severity = 'moderate'
          THEN 40
        ELSE 0
      END) AS sev_sum,
    SUM(COALESCE(estimated_leaked_revenue_usd, 0)) AS leak_val
  
  FROM oes_jny_counts_filter_001

),

oes_rfd_counts_from_000 AS (

  SELECT 
    leak_severity,
    estimated_excess_refund_usd
  
  FROM oes_rfd_counts_source_table_000

),

oes_rfd_counts_filter_001 AS (

  SELECT * 
  
  FROM oes_rfd_counts_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

oes_rfd_counts_projection_002 AS (

  SELECT 
    COUNT(*) AS cnt,
    SUM(CASE
      WHEN leak_severity IN ('critical', 'warning')
        THEN 1
      ELSE 0
    END) AS high_cnt,
    SUM(
      CASE
        WHEN leak_severity = 'critical'
          THEN 90
        WHEN leak_severity = 'warning'
          THEN 70
        WHEN leak_severity = 'moderate'
          THEN 40
        ELSE 0
      END) AS sev_sum,
    SUM(COALESCE(estimated_excess_refund_usd, 0)) AS leak_val
  
  FROM oes_rfd_counts_filter_001

),

oes_launch AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__launch_leak') }}

),

oes_lch_counts_source_table_000 AS (

  SELECT * 
  
  FROM oes_launch

),

oes_lch_counts_from_000 AS (

  SELECT 
    leak_severity,
    estimated_launch_leak_usd
  
  FROM oes_lch_counts_source_table_000

),

oes_lch_counts_filter_001 AS (

  SELECT * 
  
  FROM oes_lch_counts_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

oes_lch_counts_projection_002 AS (

  SELECT 
    COUNT(*) AS cnt,
    SUM(CASE
      WHEN leak_severity IN ('critical', 'warning')
        THEN 1
      ELSE 0
    END) AS high_cnt,
    SUM(
      CASE
        WHEN leak_severity = 'critical'
          THEN 90
        WHEN leak_severity = 'warning'
          THEN 70
        WHEN leak_severity = 'moderate'
          THEN 40
        ELSE 0
      END) AS sev_sum,
    SUM(COALESCE(estimated_launch_leak_usd, 0)) AS leak_val
  
  FROM oes_lch_counts_filter_001

),

oes_acq_counts_source_table_000 AS (

  SELECT * 
  
  FROM oes_acquisition

),

oes_acq_counts_from_000 AS (

  SELECT 
    leak_severity,
    estimated_leaked_revenue_usd
  
  FROM oes_acq_counts_source_table_000

),

oes_acq_counts_filter_001 AS (

  SELECT * 
  
  FROM oes_acq_counts_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

oes_acq_counts_projection_002 AS (

  SELECT 
    COUNT(*) AS cnt,
    SUM(CASE
      WHEN leak_severity IN ('critical', 'warning')
        THEN 1
      ELSE 0
    END) AS high_cnt,
    SUM(
      CASE
        WHEN leak_severity = 'critical'
          THEN 90
        WHEN leak_severity = 'warning'
          THEN 70
        WHEN leak_severity = 'moderate'
          THEN 40
        ELSE 0
      END) AS sev_sum,
    SUM(COALESCE(estimated_leaked_revenue_usd, 0)) AS leak_val
  
  FROM oes_acq_counts_filter_001

),

oes_totals AS (

  SELECT 
    CURRENT_DATE() AS period_start,
    a.cnt + j.cnt + b.cnt + r.cnt + l.cnt AS total_leak_findings,
    a.high_cnt + j.high_cnt + b.high_cnt + r.high_cnt + l.high_cnt AS high_severity_findings,
    (a.sev_sum + j.sev_sum + b.sev_sum + r.sev_sum + l.sev_sum)
    / NULLIF(a.cnt + j.cnt + b.cnt + r.cnt + l.cnt, 0) AS average_severity_score,
    a.leak_val + j.leak_val + b.leak_val + r.leak_val + l.leak_val AS projected_value_recovery_usd
  
  FROM oes_acq_counts_projection_002 AS a
  CROSS JOIN oes_jny_counts_projection_002 AS j
  
  CROSS JOIN oes_bsk_counts_projection_002 AS b
  
  CROSS JOIN oes_rfd_counts_projection_002 AS r
  
  CROSS JOIN oes_lch_counts_projection_002 AS l

),

oes_final AS (

  SELECT 
    CAST(period_start AS DATE) AS period_start,
    CAST(total_leak_findings AS BIGINT) AS total_leak_findings,
    CAST(high_severity_findings AS BIGINT) AS high_severity_findings,
    CAST(COALESCE(average_severity_score, 0) AS DOUBLE) AS average_severity_score,
    CAST(projected_value_recovery_usd AS DOUBLE) AS projected_value_recovery_usd
  
  FROM oes_totals

)

SELECT *

FROM oes_final
