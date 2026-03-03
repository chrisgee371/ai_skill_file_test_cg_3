# Prophecy naming

## Physical model naming pattern

Use:

`<pipeline_name>__<model_shortname>`

Examples:
- `revenue_leak_observatory__src__website_sessions`
- `revenue_leak_observatory__int__session_order_bridge`
- `revenue_leak_observatory__obs__leak_registry`

## Alignment rule

The following should all match the same physical name:
- SQL file name
- model name / modelName
- schema.yml model entry
- `ref()` target

## CTE prefix rule

Derive CTE prefixes from the shortname to reduce collision risk.
