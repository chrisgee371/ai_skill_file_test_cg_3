# Support files

These JSON files are the machine-readable support artefacts that the skill refers to as Databricks tables under `chris_demos.demos`.

## Upload target

Upload these files to a Unity Catalog volume directory such as:

`/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/`

Then run the SQL scripts in `sql/` to create the Databricks tables.

## File-to-table mapping

- `phase_plan.json` -> `chris_demos.demos.phase_plan`
- `source_manifest.json` -> `chris_demos.demos.source_manifest`
- `model_registry.json` -> `chris_demos.demos.model_registry`
- `metric_catalog.json` -> `chris_demos.demos.metric_catalog`
- `join_contracts.json` -> `chris_demos.demos.join_contracts`
- `acceptance_tests.json` -> `chris_demos.demos.acceptance_tests`
- `dataset_summary.json` -> `chris_demos.demos.dataset_summary`
- `table_profiles.json` -> `chris_demos.demos.table_profiles`
- `key_integrity.json` -> `chris_demos.demos.key_integrity`
- `page_catalog.json` -> `chris_demos.demos.page_catalog`
- `traffic_catalog.json` -> `chris_demos.demos.traffic_catalog`
- `chronology_summary.json` -> `chris_demos.demos.chronology_summary`
- `stage_01.json` -> `chris_demos.demos.stage_01`
- `stage_02.json` -> `chris_demos.demos.stage_02`
- `stage_03.json` -> `chris_demos.demos.stage_03`
- `stage_04.json` -> `chris_demos.demos.stage_04`
- `stage_05.json` -> `chris_demos.demos.stage_05`

`project_manifest.json` is included for completeness but is not required as a Databricks table by the skill.


These support files now encode the v5 multi-pipeline topology (commerce_foundation, journey_diagnostics, product_value_diagnostics, observatory_reporting) in addition to the original stage plan.
