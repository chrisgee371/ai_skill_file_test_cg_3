# Mandatory Prophecy workflow

1. Create or update files (`pipeline.py`, SQL models, `schema.yml`, `sources.yml` as needed).
2. Call `update_files()`.
3. Check the compilation result.
4. Fix blocking errors.
5. Re-run `update_files()`.
6. Re-open the surviving files because the server may have rewritten or consolidated them.
7. Continue only from the post-compile state.

## Important failure rule

If `update_files()` returns `success=false`, treat the attempted edit as rejected. Re-apply the fix from the last surviving state.

## Error handling rule

Ignore only known platform noise such as `connection not found`.
Fix immediately:
- syntax errors
- missing columns
- type mismatches
- duplicate CTE names
- UNION shape mismatches
- naming / file alignment issues
