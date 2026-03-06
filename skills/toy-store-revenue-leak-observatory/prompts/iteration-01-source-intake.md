# Iteration 01 - Source intake and profiling

## Target pipeline
- `commerce_foundation`

## Objective

Create the source-entry layer for the supplied Databricks source tables and verify that the real schema matches the provided support tables.

## Read these resources first

- `skills/toy-store-revenue-leak-observatory/docs/pipeline-topology.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-workflow.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-sql-rules.md`
- `skills/toy-store-revenue-leak-observatory/platform/prophecy-naming.md`
- `chris_demos.demos.table_profiles`
- `chris_demos.demos.key_integrity`
- `chris_demos.demos.source_manifest`
- `chris_demos.demos.acceptance_tests`
- `chris_demos.demos.model_output_schemas`


## SQL pattern library preflight

Before you author any non-trivial SQL in this iteration:

- read `chris_demos.demos.databricks_sql_pattern_index` or `skills/toy-store-revenue-leak-observatory/support_files/databricks_sql_pattern_index.json`
- shortlist relevant pattern families and open the referenced raw pack files under `skills/toy-store-revenue-leak-observatory/support_files/databricks_sql_pattern_packs/`
- read `chris_demos.demos.databricks_sql_pattern_registry` and `chris_demos.demos.databricks_sql_pattern_composition_rules`, or their JSON fallbacks under `support_files/`
- prefer entries marked `preferred` or `allowed`
- do not emit entries marked `suitable_for_model_production = false`, `negative_control = true`, or `prophecy_skill_status = 'forbidden'`

Use the library for structure guidance, but keep the current iteration contract and datatype requirements as the final source of truth.

## What to build in this iteration

Create exactly these source models inside `commerce_foundation`:

- `src__website_sessions`
- `src__website_pageviews`
- `src__orders`
- `src__order_items`
- `src__order_item_refunds`
- `src__products`

Do not create staging, marts, or diagnostic layers yet.

## Required behavior

- Map each raw Databricks source table cleanly into a source model.
- Preserve all source columns unless there is a documented reason to drop or rename something.
- Use safe typing for timestamps, numeric values, and identifiers.
- Match the declared source model datatypes in `chris_demos.demos.model_output_schemas`.
- Confirm row counts and primary keys.
- Document any mismatch between the manifest table and the discovered table structure.

## Explicit constraints

- Do not guess columns.
- Do not infer new business fields yet.
- Do not create a data mart.
- Do not skip quality checks because the source tables “look clean”.

## Completion standard

This iteration is complete only when the source layer matches the supplied Databricks tables exactly and the source-level acceptance tests can be satisfied.

## Prophecy implementation reminder

- The model names listed above are logical shortnames.
- Physical model names and filenames must follow `<pipeline_name>__<model_shortname>`.
- For this iteration, the physical pipeline name is `commerce_foundation`.
- Keep CTE names globally unique across the project.
- After editing files, call `update_files()`, inspect the surviving files, and continue from the post-compile state.
