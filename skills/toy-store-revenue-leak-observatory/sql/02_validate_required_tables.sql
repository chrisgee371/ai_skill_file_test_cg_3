SELECT 'phase_plan' AS table_name, COUNT(*) AS row_count FROM chris_demos.demos.phase_plan
UNION ALL SELECT 'source_manifest', COUNT(*) FROM chris_demos.demos.source_manifest
UNION ALL SELECT 'model_registry', COUNT(*) FROM chris_demos.demos.model_registry
UNION ALL SELECT 'metric_catalog', COUNT(*) FROM chris_demos.demos.metric_catalog
UNION ALL SELECT 'join_contracts', COUNT(*) FROM chris_demos.demos.join_contracts
UNION ALL SELECT 'acceptance_tests', COUNT(*) FROM chris_demos.demos.acceptance_tests
UNION ALL SELECT 'dataset_summary', COUNT(*) FROM chris_demos.demos.dataset_summary
UNION ALL SELECT 'table_profiles', COUNT(*) FROM chris_demos.demos.table_profiles
UNION ALL SELECT 'key_integrity', COUNT(*) FROM chris_demos.demos.key_integrity
UNION ALL SELECT 'page_catalog', COUNT(*) FROM chris_demos.demos.page_catalog
UNION ALL SELECT 'traffic_catalog', COUNT(*) FROM chris_demos.demos.traffic_catalog
UNION ALL SELECT 'chronology_summary', COUNT(*) FROM chris_demos.demos.chronology_summary
UNION ALL SELECT 'expected_column_types', COUNT(*) FROM chris_demos.demos.expected_column_types
UNION ALL SELECT 'stage_01', COUNT(*) FROM chris_demos.demos.stage_01
UNION ALL SELECT 'stage_02', COUNT(*) FROM chris_demos.demos.stage_02
UNION ALL SELECT 'stage_03', COUNT(*) FROM chris_demos.demos.stage_03
UNION ALL SELECT 'stage_04', COUNT(*) FROM chris_demos.demos.stage_04
UNION ALL SELECT 'stage_05', COUNT(*) FROM chris_demos.demos.stage_05
ORDER BY table_name;
