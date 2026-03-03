# Toy Store Revenue Leak Observatory

This bundle is a repo-ready, multi-iteration Prophecy skill package for building a Databricks-backed pipeline over the Maven Fuzzy Factory dataset.

## What is included

- `skills/toy-store-revenue-leak-observatory/` - the skill, prompts, IR, contracts, and supporting docs
- `skills/toy-store-revenue-leak-observatory/platform/` - Prophecy platform-specific build rules and workflow guidance
- `skills/toy-store-revenue-leak-observatory/support_files/` - JSON artefacts that can be uploaded to a Unity Catalog volume and materialized as Databricks support tables
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
- `chris_demos.demos.stage_01`
- `chris_demos.demos.stage_02`
- `chris_demos.demos.stage_03`
- `chris_demos.demos.stage_04`
- `chris_demos.demos.stage_05`

## Recommended read order for the agent

1. `SKILL.md`
2. `platform/prophecy-workflow.md`
3. `platform/prophecy-sql-rules.md`
4. `platform/prophecy-naming.md`
5. `chris_demos.demos.phase_plan`
6. `chris_demos.demos.source_manifest`
7. `chris_demos.demos.table_profiles`
8. the current prompt file and current IR table

## Important implementation notes

- The stage output names shown in the phase plan and contracts are **logical shortnames**.
- In Prophecy, physical SQL model names and filenames should follow:
  - `<pipeline_name>__<model_shortname>`
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

## Dataset baseline

- sessions: 472,871
- pageviews: 1,188,124
- orders: 32,313
- order items: 40,025
- refunded order items: 1,731
- products: 4
- activity window: 2012-03-19T08:04:16 to 2015-04-01T18:11:08
