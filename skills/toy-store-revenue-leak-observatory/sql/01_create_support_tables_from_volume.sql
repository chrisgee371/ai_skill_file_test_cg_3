-- Create or use a managed Unity Catalog volume first if needed.
-- Example:
-- CREATE VOLUME IF NOT EXISTS chris_demos.demos.ai_skill_support;
--
-- Upload the JSON files from skills/toy-store-revenue-leak-observatory/support_files/
-- into this folder (or change the path prefix below):
-- /Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/

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
