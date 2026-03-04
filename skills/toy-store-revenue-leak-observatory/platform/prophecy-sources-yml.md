# sources.yml guidance

If a model is materialized as a persistent table or view and is then reused downstream, create a matching `sources.yml` entry.

Why: downstream pipeline gems should consume the source entry instead of re-executing or duplicating the model logic.

## This is mandatory for cross-pipeline handoffs in this skill

This skill uses several smaller pipelines. The handoff rule is:

1. upstream pipeline materializes the handoff model as a Databricks table or view
2. create the `sources.yml` entry for that handoff
3. downstream pipeline reads it with `{{ source() }}`

Use this pattern for:
- `commerce_foundation` -> `journey_diagnostics`
- `commerce_foundation` -> `product_value_diagnostics`
- `journey_diagnostics` -> `observatory_reporting`
- `product_value_diagnostics` -> `observatory_reporting`

Do not use cross-pipeline `ref()` for these boundaries.
