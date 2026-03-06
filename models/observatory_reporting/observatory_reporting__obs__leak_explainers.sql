{{
  config({    
    "materialized": "table",
    "alias": "obs__leak_explainers",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH ole_refund AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__refund_leak') }}

),

ole_rfd_explained_source_table_000 AS (

  SELECT * 
  
  FROM ole_refund

),

ole_rfd_explained_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    estimated_excess_refund_usd,
    leak_dimension AS entity_type,
    items_refunded,
    items_sold,
    refund_rate_pct,
    dimension_value,
    benchmark_refund_rate_pct,
    CURRENT_DATE() AS period_start,
    'refund_leak' AS leak_type,
    CONCAT(
      'Product "', 
      dimension_value, 
      '" has elevated refunds: ', 
      CAST(items_refunded AS STRING), 
      ' of ', 
      CAST(items_sold AS STRING), 
      ' items refunded (', 
      CAST(ROUND(CAST(refund_rate_pct AS DOUBLE), 1) AS STRING), 
      '%). ', 
      'vs benchmark ', 
      CAST(ROUND(CAST(benchmark_refund_rate_pct AS DOUBLE), 1) AS STRING), 
      '%. ', 
      'Estimated excess refund cost: $', 
      CAST(ROUND(COALESCE(estimated_excess_refund_usd, 0), 2) AS STRING), 
      '.') AS explainer_text,
    CONCAT(
      '{"items_sold":', 
      CAST(items_sold AS STRING), 
      ',"items_refunded":', 
      CAST(items_refunded AS STRING), 
      ',"refund_rate_pct":', 
      CAST(COALESCE(refund_rate_pct, 0) AS STRING), 
      ',"estimated_excess_refund_usd":', 
      CAST(COALESCE(estimated_excess_refund_usd, 0) AS STRING), 
      '}') AS supporting_metrics,
    '["diag__refund_leak"]' AS upstream_model_refs,
    CASE
      WHEN items_sold >= 50
        THEN 'High confidence based on 50+ items'
      WHEN items_sold >= 10
        THEN 'Moderate confidence based on 10+ items'
      ELSE 'Low confidence - fewer than 10 items'
    END AS confidence_note
  
  FROM ole_rfd_explained_source_table_000

),

ole_rfd_explained_filter_001 AS (

  SELECT * 
  
  FROM ole_rfd_explained_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

ole_acquisition AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__acquisition_leak') }}

),

ole_acq_explained_source_table_000 AS (

  SELECT * 
  
  FROM ole_acquisition

),

ole_acq_explained_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    benchmark_conversion_rate_pct,
    leak_dimension AS entity_type,
    sessions,
    leak_dimension,
    conversions,
    estimated_leaked_revenue_usd,
    dimension_value,
    conversion_rate_pct,
    CURRENT_DATE() AS period_start,
    'acquisition_leak' AS leak_type,
    CONCAT(
      'Traffic from ', 
      leak_dimension, 
      ' "', 
      dimension_value, 
      '" shows ', 
      CASE
        WHEN leak_severity = 'critical'
          THEN 'critical underperformance'
        WHEN leak_severity = 'warning'
          THEN 'significant underperformance'
        WHEN leak_severity = 'moderate'
          THEN 'moderate underperformance'
        ELSE 'some performance concerns'
      END, 
      ' with conversion rate of ', 
      CAST(ROUND(CAST(conversion_rate_pct AS DOUBLE), 1) AS STRING), 
      '%', 
      ' vs benchmark ', 
      CAST(ROUND(CAST(benchmark_conversion_rate_pct AS DOUBLE), 1) AS STRING), 
      '%.', 
      ' Estimated leaked revenue: $', 
      CAST(ROUND(COALESCE(estimated_leaked_revenue_usd, 0), 2) AS STRING), 
      '.') AS explainer_text,
    CONCAT(
      '{"sessions":', 
      CAST(sessions AS STRING), 
      ',"conversions":', 
      CAST(conversions AS STRING), 
      ',"conversion_rate_pct":', 
      CAST(COALESCE(conversion_rate_pct, 0) AS STRING), 
      ',"estimated_leaked_revenue_usd":', 
      CAST(COALESCE(estimated_leaked_revenue_usd, 0) AS STRING), 
      '}') AS supporting_metrics,
    '["diag__acquisition_leak"]' AS upstream_model_refs,
    CASE
      WHEN sessions >= 100
        THEN 'High confidence based on 100+ sessions'
      WHEN sessions >= 20
        THEN 'Moderate confidence based on 20+ sessions'
      ELSE 'Low confidence - fewer than 20 sessions'
    END AS confidence_note
  
  FROM ole_acq_explained_source_table_000

),

ole_basket AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__basket_leak') }}

),

ole_bsk_explained_source_table_000 AS (

  SELECT * 
  
  FROM ole_basket

),

ole_journey AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__journey_leak') }}

),

ole_jny_explained_source_table_000 AS (

  SELECT * 
  
  FROM ole_journey

),

ole_jny_explained_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    leak_dimension AS entity_type,
    drop_off_rate_pct,
    estimated_leaked_revenue_usd,
    sessions_dropped,
    dimension_value,
    sessions_entering,
    CURRENT_DATE() AS period_start,
    'journey_leak' AS leak_type,
    CONCAT(
      'Funnel step "', 
      dimension_value, 
      '" is losing ', 
      CAST(sessions_dropped AS STRING), 
      ' sessions (', 
      CAST(ROUND(CAST(drop_off_rate_pct AS DOUBLE), 1) AS STRING), 
      '% dropoff). ', 
      CASE
        WHEN leak_severity = 'critical'
          THEN 'This represents a critical bottleneck.'
        WHEN leak_severity = 'warning'
          THEN 'This requires attention.'
        ELSE 'Monitor for trends.'
      END, 
      ' Estimated leaked revenue: $', 
      CAST(ROUND(COALESCE(estimated_leaked_revenue_usd, 0), 2) AS STRING), 
      '.') AS explainer_text,
    CONCAT(
      '{"sessions_entering":', 
      CAST(sessions_entering AS STRING), 
      ',"sessions_dropped":', 
      CAST(sessions_dropped AS STRING), 
      ',"drop_off_rate_pct":', 
      CAST(COALESCE(drop_off_rate_pct, 0) AS STRING), 
      ',"estimated_leaked_revenue_usd":', 
      CAST(COALESCE(estimated_leaked_revenue_usd, 0) AS STRING), 
      '}') AS supporting_metrics,
    '["diag__journey_leak"]' AS upstream_model_refs,
    CASE
      WHEN sessions_entering >= 100
        THEN 'High confidence based on 100+ sessions'
      WHEN sessions_entering >= 20
        THEN 'Moderate confidence based on 20+ sessions'
      ELSE 'Low confidence - fewer than 20 sessions'
    END AS confidence_note
  
  FROM ole_jny_explained_source_table_000

),

ole_jny_explained_filter_001 AS (

  SELECT * 
  
  FROM ole_jny_explained_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

ole_jny_explained_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'journey_leak' AS leak_type,
    explainer_text,
    supporting_metrics,
    '["diag__journey_leak"]' AS upstream_model_refs,
    confidence_note
  
  FROM ole_jny_explained_filter_001

),

ole_launch AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'diag__launch_leak') }}

),

ole_lch_explained_source_table_000 AS (

  SELECT * 
  
  FROM ole_launch

),

ole_lch_explained_from_000 AS (

  SELECT 
    refund_rate_lifetime_pct,
    dimension_value AS entity_key,
    refunds_first_30d,
    items_sold_first_30d,
    leak_severity,
    refund_rate_first_30d_pct,
    leak_dimension AS entity_type,
    product_launch_date AS period_start,
    estimated_launch_leak_usd,
    dimension_value,
    'launch_leak' AS leak_type,
    CONCAT(
      'Product "', 
      dimension_value, 
      '" launch shows quality concerns: ', 
      'first 30 days had ', 
      CAST(ROUND(CAST(refund_rate_first_30d_pct AS DOUBLE), 1) AS STRING), 
      '% refund rate ', 
      'vs lifetime ', 
      CAST(ROUND(CAST(refund_rate_lifetime_pct AS DOUBLE), 1) AS STRING), 
      '%. ', 
      CASE
        WHEN refund_rate_lifetime_pct > refund_rate_first_30d_pct
          THEN 'Refund rate is worsening over time. '
        ELSE ''
      END, 
      'Estimated launch leak: $', 
      CAST(ROUND(COALESCE(estimated_launch_leak_usd, 0), 2) AS STRING), 
      '.') AS explainer_text,
    CONCAT(
      '{"items_sold_first_30d":', 
      CAST(items_sold_first_30d AS STRING), 
      ',"refunds_first_30d":', 
      CAST(refunds_first_30d AS STRING), 
      ',"refund_rate_first_30d_pct":', 
      CAST(COALESCE(refund_rate_first_30d_pct, 0) AS STRING), 
      ',"estimated_launch_leak_usd":', 
      CAST(COALESCE(estimated_launch_leak_usd, 0) AS STRING), 
      '}') AS supporting_metrics,
    '["diag__launch_leak"]' AS upstream_model_refs,
    CASE
      WHEN items_sold_first_30d >= 50
        THEN 'High confidence based on 50+ items in launch window'
      WHEN items_sold_first_30d >= 10
        THEN 'Moderate confidence based on 10+ items'
      ELSE 'Low confidence - fewer than 10 items in launch window'
    END AS confidence_note
  
  FROM ole_lch_explained_source_table_000

),

ole_acq_explained_filter_001 AS (

  SELECT * 
  
  FROM ole_acq_explained_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

ole_acq_explained_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'acquisition_leak' AS leak_type,
    explainer_text,
    supporting_metrics,
    '["diag__acquisition_leak"]' AS upstream_model_refs,
    confidence_note
  
  FROM ole_acq_explained_filter_001

),

ole_lch_explained_filter_001 AS (

  SELECT * 
  
  FROM ole_lch_explained_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

ole_lch_explained_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'launch_leak' AS leak_type,
    explainer_text,
    supporting_metrics,
    '["diag__launch_leak"]' AS upstream_model_refs,
    confidence_note
  
  FROM ole_lch_explained_filter_001

),

ole_rfd_explained_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'refund_leak' AS leak_type,
    explainer_text,
    supporting_metrics,
    '["diag__refund_leak"]' AS upstream_model_refs,
    confidence_note
  
  FROM ole_rfd_explained_filter_001

),

ole_bsk_explained_from_000 AS (

  SELECT 
    dimension_value AS entity_key,
    leak_severity,
    benchmark_cross_sell_rate_pct,
    leak_dimension AS entity_type,
    cross_sell_rate_pct,
    estimated_leaked_cross_sell_revenue_usd,
    orders,
    dimension_value,
    CURRENT_DATE() AS period_start,
    'basket_leak' AS leak_type,
    CONCAT(
      'Product "', 
      dimension_value, 
      '" has weak cross-sell performance: ', 
      'cross-sell rate of ', 
      CAST(ROUND(CAST(cross_sell_rate_pct AS DOUBLE), 1) AS STRING), 
      '% ', 
      'vs benchmark ', 
      CAST(ROUND(CAST(benchmark_cross_sell_rate_pct AS DOUBLE), 1) AS STRING), 
      '%. ', 
      'Estimated leaked cross-sell revenue: $', 
      CAST(ROUND(COALESCE(estimated_leaked_cross_sell_revenue_usd, 0), 2) AS STRING), 
      '.') AS explainer_text,
    CONCAT(
      '{"orders":', 
      CAST(orders AS STRING), 
      ',"cross_sell_rate_pct":', 
      CAST(COALESCE(cross_sell_rate_pct, 0) AS STRING), 
      ',"estimated_leaked_cross_sell_revenue_usd":', 
      CAST(COALESCE(estimated_leaked_cross_sell_revenue_usd, 0) AS STRING), 
      '}') AS supporting_metrics,
    '["diag__basket_leak"]' AS upstream_model_refs,
    CASE
      WHEN orders >= 50
        THEN 'High confidence based on 50+ orders'
      WHEN orders >= 10
        THEN 'Moderate confidence based on 10+ orders'
      ELSE 'Low confidence - fewer than 10 orders'
    END AS confidence_note
  
  FROM ole_bsk_explained_source_table_000

),

ole_bsk_explained_filter_001 AS (

  SELECT * 
  
  FROM ole_bsk_explained_from_000
  
  WHERE leak_severity IN ('critical', 'warning', 'moderate')

),

ole_bsk_explained_projection_002 AS (

  SELECT 
    period_start,
    entity_type,
    entity_key,
    'basket_leak' AS leak_type,
    explainer_text,
    supporting_metrics,
    '["diag__basket_leak"]' AS upstream_model_refs,
    confidence_note
  
  FROM ole_bsk_explained_filter_001

),

ole_combined AS (

  SELECT * 
  
  FROM ole_acq_explained_projection_002 AS ole_acq_explained
  
  UNION ALL
  
  SELECT * 
  
  FROM ole_jny_explained_projection_002 AS ole_jny_explained
  
  UNION ALL
  
  SELECT * 
  
  FROM ole_bsk_explained_projection_002 AS ole_bsk_explained
  
  UNION ALL
  
  SELECT * 
  
  FROM ole_rfd_explained_projection_002 AS ole_rfd_explained
  
  UNION ALL
  
  SELECT * 
  
  FROM ole_lch_explained_projection_002 AS ole_lch_explained

),

ole_final AS (

  SELECT 
    CAST(period_start AS DATE) AS period_start,
    CAST(entity_type AS STRING) AS entity_type,
    CAST(entity_key AS STRING) AS entity_key,
    CAST(leak_type AS STRING) AS leak_type,
    CAST(explainer_text AS STRING) AS explainer_text,
    CAST(supporting_metrics AS STRING) AS supporting_metrics,
    CAST(upstream_model_refs AS STRING) AS upstream_model_refs,
    CAST(confidence_note AS STRING) AS confidence_note
  
  FROM ole_combined

)

SELECT *

FROM ole_final
