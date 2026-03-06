# Toy Store Revenue Leak Observatory

This bundle is a repo-ready, multi-iteration Prophecy skill package for building a Databricks-backed **multi-pipeline** solution over the Maven Fuzzy Factory dataset.

## What is included

- `skills/toy-store-revenue-leak-observatory/` - the skill, prompts, IR, contracts, and supporting docs
- `skills/toy-store-revenue-leak-observatory/platform/` - Prophecy platform-specific build rules and workflow guidance
- `skills/toy-store-revenue-leak-observatory/support_files/` - JSON artefacts that can be uploaded to a Unity Catalog volume and materialized as Databricks support tables
- `skills/toy-store-revenue-leak-observatory/support_files/databricks_sql_pattern_packs/` - the full raw Databricks SQL pattern-pack library built from all delivered iteration outputs
- `skills/toy-store-revenue-leak-observatory/sql/` - Databricks SQL scripts for creating the missing support tables and validating that the required inputs exist
- references to Databricks source and support tables under `chris_demos.demos`

## Installation

Transport format is a single zip archive.

Runtime expectation is **an unpacked skill directory on disk plus Databricks tables**. Extract this archive at repo root so that this path exists:

- `skills/toy-store-revenue-leak-observatory/`

Then do both of the following:

1. Make the raw Maven source tables available in Databricks under `chris_demos.demos`.
2. Upload the JSON files in `support_files/` to a Unity Catalog volume and run the SQL scripts in `sql/` so the missing support tables also exist under `chris_demos.demos`.

## Required Databricks tables

### Raw source tables

- `chris_demos.demos.website_sessions`
- `chris_demos.demos.website_pageviews`
- `chris_demos.demos.orders`
- `chris_demos.demos.order_items`
- `chris_demos.demos.order_item_refunds`
- `chris_demos.demos.products`
- `chris_demos.demos.maven_fuzzy_factory_data_dictionary`

### Support tables created from `support_files/`

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

## Global Databricks SQL pattern library

This bundle now carries a **generic, non-task-specific Databricks SQL pattern library** intended to reduce advanced-structure guesswork during Prophecy builds.

The library is split into four layers:

1. raw pattern packs under `support_files/databricks_sql_pattern_packs/`
2. `contracts/databricks_sql_pattern_index.json` and the mirrored support-file copy
3. `contracts/databricks_sql_pattern_registry.json` and the mirrored support-file copy
4. `contracts/databricks_sql_pattern_composition_rules.json` and the mirrored support-file copy

Use the library this way:

1. read the pattern index first
2. shortlist candidate pattern families by tags, shape, and `prophecy_skill_status`
3. open the referenced raw pattern packs from disk
4. use the registry to select the exact reusable SQL template
5. use the composition rules before combining base queries, modifiers, wrappers, and follow-on DDL
6. never emit entries marked `suitable_for_model_production = false`

This library is intentionally broader than the initial toy-store task. It exists so the agent has a governed pattern set available even when it chooses a different valid advanced structure than expected at the outset.

## Target pipeline topology

This bundle deliberately avoids one giant Prophecy pipeline. Instead, it uses **four smaller pipelines** divided by logic grouping:

1. `commerce_foundation`
   - source intake
   - clean staging
   - reusable commerce intermediates
2. `journey_diagnostics`
   - traffic, landing, funnel, and experiment diagnostics
3. `product_value_diagnostics`
   - product, basket, refund, and launch diagnostics
4. `observatory_reporting`
   - unified leak registry, scorecards, actions, and explainers

Read `docs/pipeline-topology.md` before build work begins.

## Cross-pipeline handoff rule

Do **not** wire these four pipelines together as one giant canvas.

When a downstream pipeline needs outputs from an earlier pipeline:

1. materialize the handoff model as a Databricks table or view
2. create the matching `sources.yml` entry
3. consume it in the downstream pipeline with `{{ source() }}`

Use `{{ ref() }}` only for dependencies **inside the same pipeline**.

## Recommended read order for the agent

1. `SKILL.md`
2. `docs/pipeline-topology.md`
3. `platform/prophecy-workflow.md`
4. `platform/prophecy-sql-rules.md`
5. `platform/prophecy-naming.md`
6. `platform/prophecy-model-patterns.md`
7. `chris_demos.demos.databricks_sql_pattern_index`
8. `chris_demos.demos.databricks_sql_pattern_composition_rules`
9. `chris_demos.demos.phase_plan`
10. `chris_demos.demos.source_manifest`
11. `chris_demos.demos.table_profiles`
12. `chris_demos.demos.model_output_schemas`
13. the current prompt file and current IR table
14. the raw pattern packs referenced by the index when deeper examples are needed

## Important implementation notes

- The stage output names shown in the phase plan and contracts are **logical shortnames**.
- `chris_demos.demos.model_output_schemas` is the authoritative datatype contract for all named output fields.
- `chris_demos.demos.expected_column_types` is the flat compatibility mirror of that same contract.
- The pattern index is for fast discovery, the registry is for exact template selection, and the raw pattern packs are the disk-backed source examples.
- In Prophecy, physical SQL model names and filenames should follow:
  - `<pipeline_name>__<model_shortname>`
- Pipeline names in this bundle are restricted to lowercase alphanumeric plus underscore.
- Cross-pipeline reuse should happen through materialized Databricks tables/views and `sources.yml`, not through giant cross-canvas dependency chains.
- CTE names must be globally unique across the entire project, not just within one model.
- After every file change, call `update_files()`, inspect the compilation result, and re-open the surviving files before continuing.
- If a Databricks support table is temporarily missing, the corresponding JSON file in `support_files/` is the fallback source of truth.

## Why this bundle exists

The goal is not to build a generic e-commerce dashboard. The goal is to build an explainable **Revenue Leak Observatory** that identifies where value is being lost across:

- acquisition
- session journey
- checkout
- basket structure
- refunds
- product launches

This bundle uses a **smaller-pipelines architecture** because Prophecy server behavior is safer and easier to manage when logic groups are separated by stable handoff boundaries.

It also now carries a full Databricks SQL pattern library so advanced model structures can be selected from governed templates instead of improvised from memory.

## Dataset baseline

- sessions: 472,871
- pageviews: 1,188,124
- orders: 32,313
- order items: 40,025
- refunded order items: 1,731
- products: 4
- activity window: 2012-03-19T08:04:16 to 2015-04-01T18:11:08
