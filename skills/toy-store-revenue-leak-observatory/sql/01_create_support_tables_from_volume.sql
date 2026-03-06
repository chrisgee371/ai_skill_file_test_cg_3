-- Create or use a managed Unity Catalog volume first if needed.
-- Example:
-- CREATE VOLUME IF NOT EXISTS chris_demos.demos.ai_skill_support;
--
-- Upload the JSON files from skills/toy-store-revenue-leak-observatory/support_files/
-- into this folder (or change the path prefix below):
-- /Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/
--
-- The raw pattern packs under support_files/databricks_sql_pattern_packs/
-- are kept as disk-backed runtime assets and are not loaded into separate
-- Databricks tables by this script.

CREATE SCHEMA IF NOT EXISTS chris_demos.demos;

CREATE OR REPLACE TABLE chris_demos.demos.phase_plan AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/phase_plan.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.source_manifest AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/source_manifest.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.model_registry AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/model_registry.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.metric_catalog AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/metric_catalog.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.join_contracts AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/join_contracts.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.acceptance_tests AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/acceptance_tests.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.dataset_summary AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/dataset_summary.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.table_profiles AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/table_profiles.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.key_integrity AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/key_integrity.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.page_catalog AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/page_catalog.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.traffic_catalog AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/traffic_catalog.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.chronology_summary AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/chronology_summary.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.expected_column_types AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/expected_column_types.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.model_output_schemas AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/model_output_schemas.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.stage_01 AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/stage_01.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.stage_02 AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/stage_02.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.stage_03 AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/stage_03.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.stage_04 AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/stage_04.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.stage_05 AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/stage_05.json',
  format => 'json',
  multiLine => true
);

CREATE OR REPLACE TABLE chris_demos.demos.databricks_sql_pattern_index AS
WITH raw_pattern_index AS (
  SELECT *
  FROM read_files(
    '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/databricks_sql_pattern_index.json',
    format => 'json',
    multiLine => true
  )
)
SELECT
  schema_version,
  contract_type,
  library_name,
  library_scope,
  generated_from_skill_bundle,
  template_conventions.default_external_source_pattern AS default_external_source_pattern,
  template_conventions.same_pipeline_reference_pattern AS same_pipeline_reference_pattern,
  template_conventions.generic_placeholder_style AS generic_placeholder_style,
  pattern.pattern_id AS pattern_id,
  pattern.group_id AS group_id,
  pattern.group_name AS group_name,
  pattern.pattern_family AS pattern_family,
  pattern.item_id AS item_id,
  pattern.item_name AS item_name,
  pattern.pack_id AS pack_id,
  pattern.pack_file AS pack_file,
  pattern.relative_disk_path AS relative_disk_path,
  pattern.suitable_for_model_production AS suitable_for_model_production,
  pattern.pattern_shape AS pattern_shape,
  pattern.pattern_role AS pattern_role,
  pattern.composition_mode AS composition_mode,
  pattern.prophecy_skill_status AS prophecy_skill_status,
  pattern.selection_tags AS selection_tags,
  pattern.binding_strategy AS binding_strategy,
  pattern.requires_sources_yml AS requires_sources_yml,
  pattern.requires_schema_yml_when_materialized AS requires_schema_yml_when_materialized,
  pattern.pipeline_connection_rule AS pipeline_connection_rule,
  pattern.cross_pipeline_reuse_mode AS cross_pipeline_reuse_mode,
  pattern.preferred_materialization_mode AS preferred_materialization_mode,
  pattern.negative_control AS negative_control
FROM raw_pattern_index
LATERAL VIEW explode(patterns) exploded_patterns AS pattern;

CREATE OR REPLACE TABLE chris_demos.demos.databricks_sql_pattern_registry AS
WITH raw_pattern_registry AS (
  SELECT *
  FROM read_files(
    '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/databricks_sql_pattern_registry.json',
    format => 'json',
    multiLine => true
  )
)
SELECT
  schema_version,
  contract_type,
  library_name,
  library_scope,
  generated_from_skill_bundle,
  template_conventions.default_external_source_pattern AS default_external_source_pattern,
  template_conventions.same_pipeline_reference_pattern AS same_pipeline_reference_pattern,
  template_conventions.generic_placeholder_style AS generic_placeholder_style,
  pattern.pattern_id AS pattern_id,
  pattern.group_id AS group_id,
  pattern.group_name AS group_name,
  pattern.pattern_family AS pattern_family,
  pattern.item_id AS item_id,
  pattern.item_name AS item_name,
  pattern.pack_id AS pack_id,
  pattern.pack_file AS pack_file,
  pattern.relative_disk_path AS relative_disk_path,
  pattern.suitable_for_model_production AS suitable_for_model_production,
  pattern.source_list_suitable_for_model_production AS source_list_suitable_for_model_production,
  pattern.reclassified_from_source_list AS reclassified_from_source_list,
  pattern.reclassification_note AS reclassification_note,
  pattern.pattern_shape AS pattern_shape,
  pattern.pattern_role AS pattern_role,
  pattern.composition_mode AS composition_mode,
  pattern.prophecy_skill_status AS prophecy_skill_status,
  pattern.selection_tags AS selection_tags,
  pattern.binding_strategy AS binding_strategy,
  pattern.requires_sources_yml AS requires_sources_yml,
  pattern.requires_schema_yml_when_materialized AS requires_schema_yml_when_materialized,
  pattern.pipeline_connection_rule AS pipeline_connection_rule,
  pattern.cross_pipeline_reuse_mode AS cross_pipeline_reuse_mode,
  pattern.preferred_materialization_mode AS preferred_materialization_mode,
  pattern.negative_control AS negative_control,
  pattern.sql_template AS sql_template,
  pattern.sql_template_line_count AS sql_template_line_count,
  pattern.authoring_notes AS authoring_notes,
  pattern.template_placeholders AS template_placeholders
FROM raw_pattern_registry
LATERAL VIEW explode(patterns) exploded_patterns AS pattern;

CREATE OR REPLACE TABLE chris_demos.demos.databricks_sql_pattern_composition_rules AS
WITH raw_pattern_rules AS (
  SELECT *
  FROM read_files(
    '/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/databricks_sql_pattern_composition_rules.json',
    format => 'json',
    multiLine => true
  )
)
SELECT
  schema_version,
  contract_type,
  library_name,
  library_scope,
  generated_from_skill_bundle,
  template_conventions.default_external_source_pattern AS default_external_source_pattern,
  template_conventions.same_pipeline_reference_pattern AS same_pipeline_reference_pattern,
  template_conventions.generic_placeholder_style AS generic_placeholder_style,
  rule.rule_id AS rule_id,
  rule.priority AS priority,
  rule.category AS category,
  rule.applies_when AS applies_when,
  rule.required_action AS required_action,
  rule.disallowed_action AS disallowed_action,
  rule.related_pattern_shapes AS related_pattern_shapes,
  rule.related_policy_statuses AS related_policy_statuses,
  rule.notes AS notes
FROM raw_pattern_rules
LATERAL VIEW explode(rules) exploded_rules AS rule;
