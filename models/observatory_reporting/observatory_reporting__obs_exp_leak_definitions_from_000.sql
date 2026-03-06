{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH obs_exp_leak_definitions_from_000 AS (

  SELECT 
    'acquisition_leak' AS leak_family,
    'Revenue lost at the point of customer acquisition' AS definition,
    'High bounce rates, low conversion, poor traffic quality' AS symptoms,
    'Marketing spend inefficiency, missed customer acquisition' AS business_impact,
    'channel, campaign, entry_page' AS entity_types,
    'marketing, product' AS owner_teams

)

SELECT *

FROM obs_exp_leak_definitions_from_000
