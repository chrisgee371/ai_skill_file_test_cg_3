{{
  config({    
    "materialized": "table",
    "alias": "observatory_reporting__obs__priority_actions",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH obs_pra_journey_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'journey_diagnostics__diag__journey_leak') }}

),

obs_pra_launch_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'product_value_diagnostics__diag__launch_leak') }}

),

obs_pra_launch_actions_source_table_000 AS (

  SELECT * 
  
  FROM obs_pra_launch_leak

),

obs_pra_journey_actions_source_table_000 AS (

  SELECT * 
  
  FROM obs_pra_journey_leak

),

obs_pra_journey_actions_from_000 AS (

  SELECT 
    sessions_entering AS volume_metric,
    entity_type,
    severity_score,
    recommended_action,
    entity_key,
    'journey_leak' AS leak_family,
    'product' AS owner_team
  
  FROM obs_pra_journey_actions_source_table_000

),

obs_pra_journey_actions_filter_001 AS (

  SELECT * 
  
  FROM obs_pra_journey_actions_from_000
  
  WHERE recommended_action IS NOT NULL

),

obs_pra_refund_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'product_value_diagnostics__diag__refund_leak') }}

),

obs_pra_refund_actions_source_table_000 AS (

  SELECT * 
  
  FROM obs_pra_refund_leak

),

obs_pra_refund_actions_from_000 AS (

  SELECT 
    entity_type,
    items_sold AS volume_metric,
    severity_score,
    recommended_action,
    entity_key,
    'refund_leak' AS leak_family,
    'operations' AS owner_team
  
  FROM obs_pra_refund_actions_source_table_000

),

obs_pra_refund_actions_filter_001 AS (

  SELECT * 
  
  FROM obs_pra_refund_actions_from_000
  
  WHERE recommended_action IS NOT NULL

),

obs_pra_basket_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'product_value_diagnostics__diag__basket_leak') }}

),

obs_pra_basket_actions_source_table_000 AS (

  SELECT * 
  
  FROM obs_pra_basket_leak

),

obs_pra_basket_actions_from_000 AS (

  SELECT 
    entity_type,
    severity_score,
    recommended_action,
    total_orders AS volume_metric,
    entity_key,
    'basket_leak' AS leak_family,
    'product' AS owner_team
  
  FROM obs_pra_basket_actions_source_table_000

),

obs_pra_basket_actions_filter_001 AS (

  SELECT * 
  
  FROM obs_pra_basket_actions_from_000
  
  WHERE recommended_action IS NOT NULL

),

obs_pra_basket_actions_projection_002 AS (

  SELECT 
    'basket_leak' AS leak_family,
    entity_type,
    entity_key,
    volume_metric,
    severity_score,
    recommended_action,
    'product' AS owner_team
  
  FROM obs_pra_basket_actions_filter_001

),

obs_pra_acquisition_leak AS (

  SELECT * 
  
  FROM {{ source('chris_demos.demos', 'journey_diagnostics__diag__acquisition_leak') }}

),

obs_pra_acquisition_actions_source_table_000 AS (

  SELECT * 
  
  FROM obs_pra_acquisition_leak

),

obs_pra_acquisition_actions_from_000 AS (

  SELECT 
    sessions AS volume_metric,
    entity_type,
    severity_score,
    recommended_action,
    entity_key,
    'acquisition_leak' AS leak_family,
    CASE
      WHEN entity_type = 'channel'
        THEN 'marketing'
      WHEN entity_type = 'campaign'
        THEN 'marketing'
      WHEN entity_type = 'entry_page'
        THEN 'product'
      ELSE 'operations'
    END AS owner_team
  
  FROM obs_pra_acquisition_actions_source_table_000

),

obs_pra_acquisition_actions_filter_001 AS (

  SELECT * 
  
  FROM obs_pra_acquisition_actions_from_000
  
  WHERE recommended_action IS NOT NULL

),

obs_pra_acquisition_actions_projection_002 AS (

  SELECT 
    'acquisition_leak' AS leak_family,
    entity_type,
    entity_key,
    volume_metric,
    severity_score,
    recommended_action,
    owner_team
  
  FROM obs_pra_acquisition_actions_filter_001

),

obs_pra_journey_actions_projection_002 AS (

  SELECT 
    'journey_leak' AS leak_family,
    entity_type,
    entity_key,
    volume_metric,
    severity_score,
    recommended_action,
    'product' AS owner_team
  
  FROM obs_pra_journey_actions_filter_001

),

obs_pra_refund_actions_projection_002 AS (

  SELECT 
    'refund_leak' AS leak_family,
    entity_type,
    entity_key,
    volume_metric,
    severity_score,
    recommended_action,
    'operations' AS owner_team
  
  FROM obs_pra_refund_actions_filter_001

),

obs_pra_launch_actions_from_000 AS (

  SELECT 
    launch_items_sold AS volume_metric,
    entity_type,
    severity_score,
    recommended_action,
    entity_key,
    'launch_leak' AS leak_family,
    'product' AS owner_team
  
  FROM obs_pra_launch_actions_source_table_000

),

obs_pra_launch_actions_filter_001 AS (

  SELECT * 
  
  FROM obs_pra_launch_actions_from_000
  
  WHERE recommended_action IS NOT NULL

),

obs_pra_launch_actions_projection_002 AS (

  SELECT 
    'launch_leak' AS leak_family,
    entity_type,
    entity_key,
    volume_metric,
    severity_score,
    recommended_action,
    'product' AS owner_team
  
  FROM obs_pra_launch_actions_filter_001

),

obs_pra_combined AS (

  SELECT * 
  
  FROM obs_pra_acquisition_actions_projection_002 AS obs_pra_acquisition_actions
  
  UNION ALL
  
  SELECT * 
  
  FROM obs_pra_journey_actions_projection_002 AS obs_pra_journey_actions
  
  UNION ALL
  
  SELECT * 
  
  FROM obs_pra_basket_actions_projection_002 AS obs_pra_basket_actions
  
  UNION ALL
  
  SELECT * 
  
  FROM obs_pra_refund_actions_projection_002 AS obs_pra_refund_actions
  
  UNION ALL
  
  SELECT * 
  
  FROM obs_pra_launch_actions_projection_002 AS obs_pra_launch_actions

),

obs_pra_ranked AS (

  SELECT 
    leak_family,
    entity_type,
    entity_key,
    volume_metric,
    severity_score,
    recommended_action,
    owner_team,
    ROW_NUMBER() OVER (ORDER BY severity_score DESC, volume_metric DESC) AS priority_rank
  
  FROM obs_pra_combined

),

obs_pra_final AS (

  SELECT 
    CAST(priority_rank AS BIGINT) AS priority_rank,
    CAST(leak_family AS STRING) AS leak_family,
    CAST(entity_type AS STRING) AS entity_type,
    CAST(entity_key AS STRING) AS entity_key,
    CAST(volume_metric AS BIGINT) AS volume_metric,
    CAST(severity_score AS DOUBLE) AS severity_score,
    CAST(recommended_action AS STRING) AS recommended_action,
    CAST(owner_team AS STRING) AS owner_team
  
  FROM obs_pra_ranked

)

SELECT *

FROM obs_pra_final
