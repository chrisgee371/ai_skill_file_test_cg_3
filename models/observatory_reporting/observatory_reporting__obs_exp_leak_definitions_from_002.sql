{{
  config({    
    "materialized": "ephemeral",
    "database": "chris_demos",
    "schema": "demos"
  })
}}

WITH obs_exp_leak_definitions_from_002 AS (

  SELECT 
    'basket_leak' AS leak_family,
    'Revenue lost from missed cross-sell and upsell opportunities' AS definition,
    'Low cross-sell rates, small basket sizes, no product bundling' AS symptoms,
    'Reduced average order value, missed incremental revenue' AS business_impact,
    'primary_product' AS entity_types,
    'product, merchandising' AS owner_teams

)

SELECT *

FROM obs_exp_leak_definitions_from_002
