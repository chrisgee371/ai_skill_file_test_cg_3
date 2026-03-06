# Naming conventions

## 1. Logical shortnames used in this bundle

The stage plans, prompts, and contracts use **logical shortnames** such as:

- `src__website_sessions`
- `stg__website_sessions`
- `int__session_order_bridge`
- `diag__acquisition_leak`
- `obs__leak_registry`

These shortnames express semantic intent only.

## 2. Physical Prophecy model names are required

When implementing in Prophecy, use this physical naming pattern everywhere:

- `<pipeline_name>__<model_shortname>`

Examples:

- `commerce_foundation__src__website_sessions`
- `commerce_foundation__int__session_order_bridge`
- `journey_diagnostics__diag__acquisition_leak`
- `product_value_diagnostics__diag__refund_leak`
- `observatory_reporting__obs__leak_registry`

Use the same physical name in:
- SQL filenames
- model `name` / `modelName`
- schema.yml entries
- `{{ ref('...') }}` calls

## 3. Approved pipeline names for this skill

Use these pipeline names exactly:

- `commerce_foundation`
- `journey_diagnostics`
- `product_value_diagnostics`
- `observatory_reporting`

Pipeline names must be lowercase and use only alphanumeric characters plus underscore.

## 4. CTE naming pattern

CTE names must be globally unique across the whole project. Prefix them with the model shortname or a stable abbreviation derived from it.

Examples:

- good: `sess_entry_source`, `sess_entry_ranked`, `obs_leak_scored`
- bad: `source_data`, `filtered`, `final`, `aggregated`

## 5. Additional rules

- Use lowercase snake_case after the logical prefix.
- Prefer stable business nouns over generic names like `final_table`.
- Do not create duplicate semantic models under different names.
- Do not use date-suffixed model names unless the model itself is snapshot-like by design.
- If a model becomes a cross-pipeline handoff point, materialize it as a Databricks table or view and create the matching `sources.yml` entry.
