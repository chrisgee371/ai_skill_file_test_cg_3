---
name: toy-store-revenue-leak-observatory
description: Multi-iteration, multi-pipeline Prophecy build for the Maven Fuzzy Factory dataset. Use when building Databricks SQL models, funnel diagnostics, landing-page experiments, refund-aware revenue analysis, or a staged observatory over Databricks source tables.
argument-hint: [iteration-number or task description]
---

# Toy Store Revenue Leak Observatory Skill

You are building a Prophecy **solution as several smaller pipelines**, not one giant pipeline, over Databricks-backed source tables.

## Mission

Build an explainable **Revenue Leak Observatory** over the Maven Fuzzy Factory data. The observatory should identify where commercial value is leaking out of the system across the full path from traffic to refund.

Leak families:
- acquisition leak
- journey leak
- basket leak
- refund leak
- launch leak

The outcome should be a stable, lineage-safe Prophecy implementation made of several logic-grouped pipelines, not a one-off dashboard or a giant denormalized query.

## Required pipeline topology

Build and maintain these pipelines:

1. `commerce_foundation`
   - source intake
   - staging
   - reusable intermediate commerce graph
2. `journey_diagnostics`
   - traffic, landing page, funnel, and experiment diagnostics
3. `product_value_diagnostics`
   - product, basket, refund, and launch diagnostics
4. `observatory_reporting`
   - final leak registry, priority actions, executive scorecard, and explainers

Read `docs/pipeline-topology.md` before generating code.

## Read these platform docs before generating SQL

- `platform/prophecy-workflow.md`
- `platform/prophecy-sql-rules.md`
- `platform/prophecy-naming.md`
- `platform/prophecy-server-behavior.md`
- `platform/prophecy-error-triage.md`
- `platform/prophecy-model-patterns.md`
- `platform/prophecy-analysis.md` when Analysis exposure is requested
- `platform/prophecy-schema-yml.md` and `platform/prophecy-sources-yml.md` when models are materialized or made reusable outside the immediate build step

## Non-negotiable rules

1. **Databricks only**
   - Use Databricks-compatible SQL patterns only.
   - Do not emit Spark, T-SQL, BigQuery, or warehouse-specific syntax that is not also valid for Databricks SQL in this project.
   - This project should target Databricks SQL models.

2. **Read structured inputs before reasoning from memory**
   - Read `chris_demos.demos.phase_plan`, the current stage IR table, `chris_demos.demos.source_manifest`, `docs/pipeline-topology.md`, the Databricks SQL pattern library, and the profile/support tables before generating code.
   - If a referenced Databricks support table is missing, read the matching JSON file under `support_files/` instead of guessing.
   - When structured resources answer the question, trust them over memory.

3. **No silent assumptions**
   - Do not invent columns, tables, time windows, joins, pipeline boundaries, or business logic.
   - If an ambiguity remains after reading the supplied resources, ask an explicit question instead of guessing.

4. **Pipeline topology discipline**
   - Use the four named pipelines in this skill.
   - Keep each pipeline focused on its logic group.
   - Do not collapse all stages into one giant Prophecy pipeline.
   - Build downstream pipelines only after the upstream handoff outputs are stable and materialized.

5. **Same-pipeline lineage rule**
   - When a model needs data from an upstream model **inside the same pipeline**, choose one of these approaches:

     **Option A — Self-contained model (preferred when feasible):**
     - Read directly from source tables using `{{ source() }}`
     - Set `input_ports = None` in `pipeline.py`
     - No pipeline connection is required
     - Use this when you do not need to reuse transformation logic from another model

     **Option B — `ref()` with pipeline connection (required when reusing model output):**
     - Use `{{ ref('model_name') }}` in SQL
     - Ensure the consuming model has `input_ports` defined in `pipeline.py`
     - Add the matching `>>` connection in `pipeline.py`: `upstream._out(0) >> consuming._in(N)`
     - All three must be present: `ref()` in SQL, `input_ports` on the consumer, and the connection between them

     Using `ref()` without the corresponding pipeline connection will cause the server to replace the `ref()` with an unresolved placeholder, leading to compilation failure.

6. **Cross-pipeline handoff rule**
   - When one pipeline needs outputs from another pipeline, **do not** use cross-pipeline `ref()`.
   - Instead:
     - materialize the handoff model as a Databricks table or view
     - create or update the matching `sources.yml` entry
     - consume the handoff in the downstream pipeline with `{{ source() }}`
   - This rule applies to the handoff from `commerce_foundation` into `journey_diagnostics` and `product_value_diagnostics`, and from those diagnostic pipelines into `observatory_reporting`.

7. **No direct gem reuse across pipeline boundaries**
   - Do not reference prior canvas gems directly when the intent is to continue the solution in a different pipeline.
   - Cross-pipeline reuse must happen through materialized Databricks handoff tables/views and `sources.yml`.
   - Direct gem reuse is only relevant inside the same pipeline and only when the pipeline connections exist.

8. **Prophecy physical naming is mandatory**
   - The model names in this skill are logical shortnames.
   - Physical model names, filenames, schema.yml model names, and modelName references must follow:
     `<pipeline_name>__<model_shortname>`
   - The SQL file name and the model name must match exactly.
   - Pipeline names must be lowercase alphanumeric plus underscore only.

9. **Project-wide SQL hygiene is mandatory**
   - CTE names must be globally unique across the entire project, not just within a single model.
   - `UNION`, `INTERSECT`, and `EXCEPT` should operate on pre-shaped CTEs using `SELECT *` only.
   - Keep one major SQL clause per CTE whenever possible.

10. **Datatype discipline for all named outputs**
   - Do not rely on implicit inference for any named output field in this project.
   - Every named output field must match the declared type in:
     - `chris_demos.demos.model_output_schemas` (support table), or
     - `contracts/model_output_schemas.json` (repo file).
   - The flat compatibility mirror also exists at:
     - `chris_demos.demos.expected_column_types` (support table), or
     - `contracts/expected_column_types.json` (repo file).
   - Count-style outputs must be explicitly cast to `Long` / `BIGINT`.
   - Rate, score, money, date, timestamp, and string outputs must also be explicitly cast to their declared types in the final projection.
   - Use STRING-encoded JSON for `supporting_metrics` and `upstream_model_refs` unless a later platform-safe nested type contract is introduced.

11. **Respect the Prophecy compile loop**
   - After every create or change set, call `update_files()`.
   - If `update_files()` returns failure, treat the workspace edits as rejected until they are re-applied.
   - Re-open the surviving files after each successful compile because the server may consolidate or split models.
   - Finish and validate one pipeline handoff boundary before starting the next pipeline.

12. **Keep the idea original**
   - Do not collapse this into a generic marketing dashboard.
   - Build the leak taxonomy, experiment-aware diagnostics, and final action layer as defined in the skill docs.



## Global Databricks SQL pattern library

Before you author any non-trivial SQL model in this skill:

1. Read `chris_demos.demos.databricks_sql_pattern_index` or `support_files/databricks_sql_pattern_index.json`.
2. Shortlist candidate patterns by `selection_tags`, `pattern_shape`, `pattern_role`, and `prophecy_skill_status`.
3. Read `chris_demos.demos.databricks_sql_pattern_registry` and `chris_demos.demos.databricks_sql_pattern_composition_rules`, or the mirrored JSON files in `support_files/`.
4. Open the referenced raw pack files under `support_files/databricks_sql_pattern_packs/` when you need the original pack context.
5. Prefer `preferred` or `allowed` entries first.
6. Use `caution` entries only intentionally.
7. Do not emit any entry where `suitable_for_model_production = false`, `negative_control = true`, or `prophecy_skill_status = 'forbidden'`.

The pattern library is generic and broader than the immediate toy-store task on purpose. Use it to avoid improvising fragile advanced structures when the platform already has a safer known-good pattern available.

## Build algorithm

For every iteration:

1. Read `chris_demos.demos.phase_plan`.
2. Read `docs/pipeline-topology.md`.
3. Read `chris_demos.demos.model_output_schemas`.
4. Read `chris_demos.demos.databricks_sql_pattern_index`.
5. Read `chris_demos.demos.databricks_sql_pattern_composition_rules`.
6. Read the current iteration prompt in `prompts/`.
7. Read the current stage IR table in Databricks.
8. Shortlist relevant pattern families, then open the matching registry entries and raw pattern packs from disk.
9. Re-read any previous stage outputs, profile/support tables, and relevant platform docs.
10. Identify the **target pipeline name** for the current stage.
11. List the models to be created, including grain, upstream dependencies, physical model names, whether each dependency is same-pipeline or cross-pipeline, and which library pattern family each model will start from.
12. Generate the models using Databricks-safe SQL and Prophecy-safe naming / CTE rules.
13. Make the final projection of every model conform to `chris_demos.demos.model_output_schemas`.
14. If a stage creates downstream handoff outputs, materialize them and update `sources.yml` before moving on.
15. Update any required `schema.yml` or `sources.yml` entries.
16. Call `update_files()`.
17. Validate against `chris_demos.demos.acceptance_tests`, `chris_demos.demos.model_output_schemas`, the pattern composition rules, and the platform quality gates.
18. Report what was created, which pipeline was touched, which handoff tables or views were created, which source entries were added, whether each internal dependency uses self-contained `source()` or `ref()` plus a matching pipeline connection, which pattern families were used, whether each output model matches the declared datatype contract, which files survived compilation, and which quality gates were satisfied.

## Required source tables

The source tables are expected in Databricks under:

- `chris_demos.demos.website_sessions`
- `chris_demos.demos.website_pageviews`
- `chris_demos.demos.orders`
- `chris_demos.demos.order_items`
- `chris_demos.demos.order_item_refunds`
- `chris_demos.demos.products`
- `chris_demos.demos.maven_fuzzy_factory_data_dictionary`

## Required support tables

The support tables are expected in Databricks under:

- `chris_demos.demos.phase_plan`
- `chris_demos.demos.source_manifest`
- `chris_demos.demos.model_registry`
- `chris_demos.demos.metric_catalog`
- `chris_demos.demos.join_contracts`
- `chris_demos.demos.acceptance_tests`
- `chris_demos.demos.dataset_summary`
- `chris_demos.demos.table_profiles`
- `chris_demos.demos.key_integrity`
- `chris_demos.demos.page_catalog`
- `chris_demos.demos.traffic_catalog`
- `chris_demos.demos.chronology_summary`
- `chris_demos.demos.expected_column_types`
- `chris_demos.demos.model_output_schemas`
- `chris_demos.demos.stage_01`
- `chris_demos.demos.stage_02`
- `chris_demos.demos.stage_03`
- `chris_demos.demos.stage_04`
- `chris_demos.demos.stage_05`
- `chris_demos.demos.databricks_sql_pattern_index`
- `chris_demos.demos.databricks_sql_pattern_registry`
- `chris_demos.demos.databricks_sql_pattern_composition_rules`

## What good looks like

A good implementation:
- builds the solution as four smaller pipelines with clear handoff boundaries
- keeps `commerce_foundation` reusable and stable
- keeps diagnostic logic split into journey and product-value domains
- uses materialized Databricks handoff tables plus `sources.yml` between pipelines
- avoids a giant all-in-one canvas
- ends with a unified observatory and ranked action layer
