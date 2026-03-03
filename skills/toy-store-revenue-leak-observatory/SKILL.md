---
name: toy-store-revenue-leak-observatory
description: Multi-iteration Prophecy pipeline build for the Maven Fuzzy Factory dataset. Use when building Databricks SQL models, funnel diagnostics, landing-page experiments, refund-aware revenue analysis, or a staged observatory over Databricks source tables.
argument-hint: [iteration-number or task description]
---

# Toy Store Revenue Leak Observatory Skill

You are building a Prophecy pipeline in multiple stages over Databricks-backed source tables.

## Mission

Build an explainable **Revenue Leak Observatory** over the Maven Fuzzy Factory data. The observatory should identify where commercial value is leaking out of the system across the full path from traffic to refund.

Leak families:
- acquisition leak
- journey leak
- basket leak
- refund leak
- launch leak

The outcome should be a stable, lineage-safe Prophecy pipeline, not a one-off dashboard or a giant denormalized query.

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
   - Read `chris_demos.demos.phase_plan`, the current stage IR table, `chris_demos.demos.source_manifest`, and the profile/support tables before generating code.
   - If a referenced Databricks support table is missing, read the matching JSON file under `support_files/` instead of guessing.
   - When structured resources answer the question, trust them over memory.

3. **No silent assumptions**
   - Do not invent columns, tables, time windows, joins, or business logic.
   - If an ambiguity remains after reading the supplied resources, ask an explicit question instead of guessing.

4. **Stage discipline**
   - Review all previous phases before building the next phase.
   - Do not skip ahead.
   - Do not rebuild previous phases unless they are contract-breaking and must be corrected.

5. **Lineage rule for staged builds**
   - When a model needs data from an upstream model in the same pipeline, choose one of these approaches:

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

6. **No direct gem reuse across iterations**
   - Do not reference prior canvas gems directly when the intent is to continue the same pipeline.
   - If the downstream model can be self-contained, read from `{{ source() }}` and keep `input_ports = None`.
   - If the downstream model must consume another model's output, use `{{ ref('model_name') }}` together with matching `input_ports` and `>>` connections.

7. **Prophecy physical naming is mandatory**
   - The stage output names in this skill are logical shortnames.
   - Physical model names, filenames, schema.yml model names, and modelName references must follow:
     `<pipeline_name>__<model_shortname>`
   - The SQL file name and the model name must match exactly.

8. **Project-wide SQL hygiene is mandatory**
   - CTE names must be globally unique across the entire project, not just within a single model.
   - `UNION`, `INTERSECT`, and `EXCEPT` should operate on pre-shaped CTEs using `SELECT *` only.
   - Keep one major SQL clause per CTE whenever possible.

9. **Respect the Prophecy compile loop**
   - After every create or change set, call `update_files()`.
   - If `update_files()` returns failure, treat the workspace edits as rejected until they are re-applied.
   - Re-open the surviving files after each successful compile because the server may consolidate or split models.

10. **Keep the idea original**
   - Do not collapse this into a generic marketing dashboard.
   - Build the leak taxonomy, experiment-aware diagnostics, and final action layer as defined in the skill docs.

## Build algorithm

For every iteration:

1. Read `chris_demos.demos.phase_plan`.
2. Read the current iteration prompt in `prompts/`.
3. Read the current stage IR table in Databricks.
4. Re-read any previous stage outputs, profile/support tables, and relevant platform docs.
5. List the models to be created, including grain, upstream dependencies, and physical model names.
6. Generate the models using Databricks-safe SQL and Prophecy-safe naming / CTE rules.
7. Update any required `schema.yml` or `sources.yml` entries.
8. Call `update_files()`.
9. Validate against `chris_demos.demos.acceptance_tests` and the platform quality gates.
10. Report what was created, whether each internal dependency uses self-contained `source()` or `ref()` plus a matching pipeline connection, which files survived compilation, and which quality gates were satisfied.

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

These should also exist in Databricks under `chris_demos.demos`:

- `phase_plan`
- `source_manifest`
- `model_registry`
- `metric_catalog`
- `join_contracts`
- `acceptance_tests`
- `dataset_summary`
- `table_profiles`
- `key_integrity`
- `page_catalog`
- `traffic_catalog`
- `chronology_summary`
- `stage_01`
- `stage_02`
- `stage_03`
- `stage_04`
- `stage_05`

Use the SQL scripts in `sql/` if they do not yet exist.

## Read these local docs as needed

- `docs/known-quirks.md`
- `docs/page-taxonomy.md`
- `docs/traffic-taxonomy.md`
- the platform docs under `platform/`

## Response style

When you generate a stage:
- state the stage objective
- list the logical shortnames and the physical model names you are creating
- state the upstream tables/models used and whether each dependency is self-contained `source()` or `ref()` plus a matching pipeline connection
- call out any constraints or chronology caveats
- confirm which acceptance tests and platform quality gates you are satisfying

## What “good” looks like

A good build:
- is faithful to the real supplied schema
- is decomposed into safe stages
- preserves lineage by choosing the correct dependency pattern for each model: self-contained `{{ source() }}` where feasible, or `{{ ref('model_name') }}` plus matching pipeline connections when reusing model outputs
- respects Prophecy compile behavior and naming rules
- produces diagnostics that explain where value is leaking
- ends with a unified observatory and ranked action layer
