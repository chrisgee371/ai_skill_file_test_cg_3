# Databricks support tables

The skill expects the following non-raw support tables to exist under `chris_demos.demos`:

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
- `expected_column_types`
- `model_output_schemas`
- `stage_01`
- `stage_02`
- `stage_03`
- `stage_04`
- `stage_05`

The JSON files used to create them live in `support_files/`.

Run `sql/01_create_support_tables_from_volume.sql` after uploading those JSON files to a Unity Catalog volume.
