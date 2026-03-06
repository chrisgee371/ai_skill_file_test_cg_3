# Databricks SQL setup

## Goal

These scripts create the missing Databricks support tables that the skill references.

## Expected workflow

1. Create or use a Unity Catalog volume.
2. Upload the JSON files from `support_files/` into a folder such as:
   `/Volumes/chris_demos/demos/ai_skill_support/toy-store-revenue-leak-observatory/support_files/`
3. Run `01_create_support_tables_from_volume.sql` in the Databricks SQL editor.
4. Optionally run `02_validate_required_tables.sql`.
5. If you have not already created the raw CSV source tables, run `03_create_source_tables_from_volume.sql` after uploading the raw CSV files to a volume.

## Notes

- The support-table script assumes the JSON filenames in `support_files/` are uploaded unchanged.
- `model_output_schemas.json` is the authoritative datatype contract for all named output fields.
- `expected_column_types.json` is the flat compatibility mirror of that same datatype contract.
- `databricks_sql_pattern_index.json`, `databricks_sql_pattern_registry.json`, and `databricks_sql_pattern_composition_rules.json` are also loaded into Databricks support tables by `01_create_support_tables_from_volume.sql`.
- The raw pattern packs in `support_files/databricks_sql_pattern_packs/` remain disk-backed runtime artefacts and are not loaded into separate Databricks tables by default.
- If you use a different volume path, replace the path prefix in the SQL scripts.
- The source-table script is optional if those raw tables already exist.
