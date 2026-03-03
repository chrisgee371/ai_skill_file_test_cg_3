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

- `revenue_leak_observatory__src__website_sessions`
- `revenue_leak_observatory__stg__website_sessions`
- `revenue_leak_observatory__int__session_order_bridge`
- `revenue_leak_observatory__obs__leak_registry`

Use the same physical name in:
- SQL filenames
- model `name` / `modelName`
- schema.yml entries
- `{{ ref('...') }}` calls

## 3. CTE naming pattern

CTE names must be globally unique across the whole project. Prefix them with the model shortname or a stable abbreviation derived from it.

Examples:

- good: `sess_entry_source`, `sess_entry_ranked`, `obs_leak_scored`
- bad: `source_data`, `filtered`, `final`, `aggregated`

## 4. Additional rules

- Use lowercase snake_case after the logical prefix.
- Prefer stable business nouns over generic names like `final_table`.
- Do not create duplicate semantic models under different names.
- Do not use date-suffixed model names unless the model itself is snapshot-like by design.
- If a model is materialized as a table or view and reused downstream on the canvas, create the matching `sources.yml` entry as described in `platform/prophecy-sources-yml.md`.
