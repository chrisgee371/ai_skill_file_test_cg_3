{{
  config({    
    "materialized": "table",
    "alias": "observatory_reporting__obs__leak_explainers",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH obs_exp_leak_definitions_from_001 AS (

  SELECT 
    'journey_leak' AS leak_family,
    'Revenue lost due to customer drop-off in the purchase funnel' AS definition,
    'Low step-through rates, high drop-off at specific stages' AS symptoms,
    'Cart abandonment, checkout friction, conversion loss' AS business_impact,
    'funnel_step' AS entity_types,
    'product, ux' AS owner_teams

),

obs_exp_final AS (

  SELECT 
    CAST(leak_family AS STRING) AS leak_family,
    CAST(definition AS STRING) AS definition,
    CAST(symptoms AS STRING) AS symptoms,
    CAST(business_impact AS STRING) AS business_impact,
    CAST(entity_types AS STRING) AS entity_types,
    CAST(owner_teams AS STRING) AS owner_teams
  
  FROM obs_exp_leak_definitions_from_001 AS obs_exp_leak_definitions

)

SELECT *

FROM obs_exp_final
