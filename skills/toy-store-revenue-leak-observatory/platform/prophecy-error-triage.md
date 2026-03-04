# Error triage

## Usually safe to ignore

- `connection not found`

## Must fix immediately

- SQL syntax errors
- missing columns
- type mismatches
- duplicate CTE names
- UNION / INTERSECT / EXCEPT column mismatches
- file name vs model name mismatch
- 0-row outputs caused by broken joins or incorrect chronology filters

## 0-row debugging rule

When an output unexpectedly drops to zero rows, trace from the nearest healthy upstream model forward. Do not assume the visible canvas shape matches the post-compile SQL model shape.
