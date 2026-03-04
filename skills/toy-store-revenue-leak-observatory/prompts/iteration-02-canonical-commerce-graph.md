# Iteration 02 - Canonical commerce graph

## Target pipeline
- `commerce_foundation`

## Objective

Build the stable internal layer that the diagnostic pipelines will depend on.

## Read these resources first

- `skills/toy-store-revenue-leak-observatory/docs/pipeline-topology.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-workflow.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-sql-rules.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-naming.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-server-behavior.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-model-patterns.md`
- `chris_demos.demos.phase_plan`
- `chris_demos.demos.model_registry`
- `chris_demos.demos.join_contracts`
- `chris_demos.demos.acceptance_tests`
- `chris_demos.demos.model_output_schemas`

## Inputs

Use the stage-1 source models inside `commerce_foundation` with one of these patterns:

- **Preferred:** make the consuming model self-contained by reading the required Databricks source tables with `{{ source() }}` and keeping `input_ports = None`
- **If you must reuse stage-1 model output inside the same pipeline:** use `{{ ref('src__model_name') }}` and ensure the consuming model has matching `input_ports` and `>>` connections in `pipeline.py`

Do not use orphaned `ref()` calls.

## What to build in this iteration

- `stg__website_sessions`
- `stg__website_pageviews`
- `stg__orders`
- `stg__order_items`
- `stg__order_item_refunds`
- `dim__products`
- `int__session_entry_page`
- `int__session_page_sequence`
- `int__session_order_bridge`
- `int__order_item_net_value`

## Design intent

- `stg__website_sessions` should normalize traffic fields and add a session_date.
- `stg__website_pageviews` should normalize page fields and add a pageview_date.
- `int__session_entry_page` should derive the first pageview per session.
- `int__session_page_sequence` should impose explicit page order within each session.
- `int__session_order_bridge` should collapse session-level conversion and order-value facts to one row per session.
- `int__order_item_net_value` should attach refunds and compute net value at order-item grain.

## Datatype requirements

- Make every final projected column in this iteration match `chris_demos.demos.model_output_schemas`.
- Do not rely on inferred widths for ids, counts, dates, timestamps, or money columns.
- Use explicit casts in the final projection of each model.

## Explicit constraints

- Keep the intermediate layer narrow and reusable.
- Preserve one clear grain per model.
- Avoid building metrics marts prematurely.
- Validate every join path against `chris_demos.demos.join_contracts`.
- If you split logic across models, verify the corresponding pipeline connections survive compile.
- At the end of this iteration, materialize the handoff outputs that downstream pipelines will consume and create the matching `sources.yml` entries.

## Completion standard

This iteration is complete only when `commerce_foundation` provides stable handoff outputs that `journey_diagnostics` and `product_value_diagnostics` can consume via `{{ source() }}` rather than cross-pipeline `ref()`.

## Prophecy implementation reminder

- The model names listed above are logical shortnames.
- Physical model names and filenames must follow `<pipeline_name>__<model_shortname>`.
- For this iteration, the physical pipeline name is `commerce_foundation`.
- Downstream pipelines must consume this iteration's reusable outputs through materialized Databricks tables/views plus `sources.yml`.
- Keep CTE names globally unique across the project.
- After editing files, call `update_files()`, inspect the surviving files, and continue from the post-compile state.
