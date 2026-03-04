# Prophecy server behavior

## Auto-consolidation can happen

The server may consolidate a linear chain of models into fewer files or nodes.

## Auto-splitting can happen

If a CTE is reused or the compiler sees an opportunity to reshape the graph, it may split logic into separate generated models.


## JOIN CTE splitting (critical pattern)

Treat any CTE that contains a `JOIN` as split-prone. In practice, the server may extract one or more joined inputs into separate generated models and replace the original reference with an unresolved placeholder if the wiring is incomplete.

### What happens

When you write SQL shaped like this:

```sql
WITH products AS (
    SELECT * FROM {{ source('db.schema', 'products') }}
),
orders AS (
    SELECT * FROM {{ source('db.schema', 'orders') }}
),
joined_data AS (
    SELECT *
    FROM orders o
    LEFT JOIN products p ON o.product_id = p.product_id
)
SELECT * FROM joined_data
```

the server may:

1. extract `products`, `orders`, or the join CTE into a separate model
2. replace the original reference with an unresolved placeholder or empty backticks
3. expect you to wire the dependency through `input_ports`, `>>` connections, and matching SQL references

### Prevention strategies

#### Strategy A - Prefer self-contained logic when feasible

If the join only exists to enrich one attribute, consider keeping the model self-contained by reading directly from source tables in one SQL unit, or reduce the enrichment to a scalar subquery when that keeps the model simpler.

```sql
SELECT
    m.product_id,
    (
        SELECT product_name
        FROM {{ source('db.schema', 'products') }} p
        WHERE p.product_id = m.product_id
        LIMIT 1
    ) AS product_name
FROM main_table m
```

#### Strategy B - Accept the split and wire it properly

If the server splits the model, you must add all three of these:

1. `{{ ref('split_model_name') }}` or the server-generated temporary source reference in SQL
2. `input_ports = ["in_0", ...]` on the consuming `Process`
3. the matching connection in `pipeline.py`, for example `split_model._out(0) >> consuming_model._in(0)`

With all three present, the server may either keep the split with proper wiring or inline it again during a later compile.

#### Strategy C - Inline the joined source subquery

Instead of a separate upstream CTE, inline the joined source directly inside the consuming query.

```sql
SELECT *
FROM {{ source('db.schema', 'orders') }} o
LEFT JOIN (
    SELECT product_id, product_name
    FROM {{ source('db.schema', 'products') }}
) p
    ON o.product_id = p.product_id
```

This can still be split by the server, but it reduces the number of separate named units the server can rewrite.

### Recovery from empty-reference state

If you see `FROM `` AS model_name` or another unresolved placeholder in generated SQL:

1. find the split model the server created
2. add the explicit SQL dependency the server expects, using `{{ ref('split_model_name') }}` or the generated temporary source reference
3. add `input_ports` on the consumer and the matching `>>` connection in `pipeline.py`
4. call `update_files()` again and re-check whether the server kept the split or inlined it

## Model deletion can happen

If a SQL file is not actually referenced by the pipeline, or if the model name and the file name drift apart, the server can remove the model during compile/update.

## Practical rule

After every `update_files()` call:

1. inspect the surviving files
2. inspect the surviving graph shape
3. verify that any `{{ ref() }}` calls have matching `>>` connections in `pipeline.py`
4. adjust connections, refs, and expectations to the server's final structure

Do not assume the pre-compile local shape survived unchanged.

## Port and connection consistency

The server enforces strict consistency between:

- `input_ports` declarations in `pipeline.py`
- `>>` connection statements in `pipeline.py`
- `{{ ref() }}` calls in SQL models

If a model uses `{{ ref() }}` but no corresponding `>>` connection exists, the server will:

1. replace the `ref()` with an unresolved placeholder
2. create `input_ports` expecting data that never arrives
3. lock the model into an unrecoverable error state

To avoid this:

- whenever you add a `ref()`, also add the matching connection
- whenever the server creates `input_ports`, verify that connections exist to feed them

## Recovery when the server enforces inconsistent state

Sometimes the server's internal graph state becomes inconsistent. Symptoms include:

- unresolved placeholders or empty backticks in generated SQL
- the server reverts your changes back to a broken state
- the server recreates deleted models with the same broken structure
- the server removes connections you add

Recovery options, in order of preference:

1. Make the model self-contained by reading from `{{ source() }}` and removing the port dependency.
2. Rename the model if the server appears to have corrupted state tied to one model name.
3. Delete and recreate the affected pipeline if corruption is widespread.
4. Escalate to Prophecy support if the server continues to enforce the broken state.
