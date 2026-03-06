# Pattern-library driven model authoring

## Mandatory preflight

Before writing non-trivial SQL in this skill:

1. read `chris_demos.demos.databricks_sql_pattern_index`
2. shortlist candidate patterns by tags, shape, role, and `prophecy_skill_status`
3. read the matching entries in `chris_demos.demos.databricks_sql_pattern_registry`
4. read `chris_demos.demos.databricks_sql_pattern_composition_rules`
5. open the referenced raw pattern packs under `support_files/databricks_sql_pattern_packs/` when you need the original delivered pack context

The library is intentionally broad. Use it to select a safe advanced structure even when the final business model does not match the structure you expected at first.

## Pattern roles

### Base query
A reusable transformation body such as:
- joins
- windows
- aggregations
- set logic
- semi-structured shaping
- built-in function usage
- AI-native transformation calls

Use a base query when the main job is to transform rows.

### Read pattern
A reusable read structure such as:
- time travel
- CDF / `table_changes`
- federation reads
- `information_schema`
- TVF-driven or generator-heavy reads

Use a read pattern when the important part is **how the data is being read**. Land the read in its own named CTE first.

### Clause modifier
A decorator on a base query, such as:
- `ORDER BY`
- `LIMIT`
- `DISTINCT`
- query hints
- `SORT BY`
- `DISTRIBUTE BY`
- `CLUSTER BY`

Treat these as attachable patterns, not as the whole model.

### Materialization wrapper
A create/replace statement that wraps a finished inner query, such as:
- CTAS
- view creation
- materialized view creation
- full-refresh replace workflows
- table-property wrappers

Use wrappers only when a materialized target is intentionally required.

### Post-create augmentation
A follow-on DDL step such as `SET TAG`. Keep it separate from the main transformation body.

### Negative control
A non-suitable pattern retained only so the agent knows what **not** to synthesize as a standalone model.

## Composition order

For most non-trivial models, compose in this order:

1. choose a base query or read pattern
2. adapt it to the required grain and datatype contract
3. attach any necessary modifiers
4. wrap it only if a table/view/materialized-view/full-refresh target is intentionally needed
5. apply any follow-on augmentation DDL
6. check the result against the composition rules and forbidden-pattern guidance

## Split into multiple models when

- the data branches to different downstream targets
- an intermediate dataset must be exposed through Visualize
- different materializations are required
- a step is easier to validate as an independent contract point
- a chosen advanced pattern becomes clearer when isolated as its own contract boundary

## Keep one model when

- the logic is a simple linear chain
- the user wants a compact DAG
- there is no branching or reuse pressure
- the chosen pattern is already clear and reviewable inside one model body

Prefer the smallest number of models that still keeps lineage, debugging, and server behavior manageable.

## Prefer several smaller pipelines for this skill

This skill deliberately uses **four pipelines** instead of one giant Prophecy canvas:

- `commerce_foundation`
- `journey_diagnostics`
- `product_value_diagnostics`
- `observatory_reporting`

Use a new pipeline when:
- the logic belongs to a different business domain
- a stable handoff point is needed for downstream reuse
- the upstream outputs should be materialized and source-declared
- keeping everything together would create an oversized or fragile graph

## Prefer self-contained models when feasible

When possible, keep models self-contained by reading from `{{ source() }}` with `input_ports = None`. This avoids the `ref()` + connection coordination requirement and is more resilient to server restructuring.

Self-contained pattern example:

```sql
WITH cf_orders_source AS (
  SELECT *
  FROM {{ source('chris_demos.demos', 'orders') }}
),
cf_orders_typed AS (
  SELECT
    CAST(order_id AS BIGINT) AS order_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(price_usd AS DOUBLE) AS price_usd
  FROM cf_orders_source
)
SELECT *
FROM cf_orders_typed
```

## If you split models inside one pipeline: ensure the connection exists

When a downstream model references an upstream model in the **same pipeline**:

- the downstream model needs `input_ports` in `pipeline.py`
- a `>>` connection must link the upstream output to the downstream input
- the SQL must use `{{ ref('pipeline_name__upstream_model_shortname') }}`

All three must be present.

Same-pipeline connection example:

```python
orders_enriched = Process(
    name="commerce_foundation__int__orders_enriched",
    properties=ModelTransform(modelName="commerce_foundation__int__orders_enriched")
)

order_day = Process(
    name="commerce_foundation__mart__order_day",
    properties=ModelTransform(modelName="commerce_foundation__mart__order_day"),
    input_ports=["in_0"]
)

orders_enriched >> order_day._in(0)
```

```sql
WITH cf_order_day_source AS (
  SELECT *
  FROM {{ ref('commerce_foundation__int__orders_enriched') }}
),
cf_order_day_agg AS (
  SELECT
    CAST(order_date AS DATE) AS order_date,
    CAST(COUNT(*) AS BIGINT) AS orders
  FROM cf_order_day_source
  GROUP BY order_date
)
SELECT *
FROM cf_order_day_agg
```

## If you split work across pipelines: use materialized handoffs

When a downstream pipeline depends on an upstream pipeline:

1. materialize the upstream handoff model as a table or view
2. create the matching `sources.yml` entry
3. consume it downstream with `{{ source() }}`

Do not use cross-pipeline `ref()`.

Cross-pipeline handoff example:

- upstream physical model: `commerce_foundation__int__session_order_bridge`
- materialized in Databricks as a table or view
- declared in `sources.yml`
- consumed downstream in `journey_diagnostics` or `product_value_diagnostics` through `{{ source() }}`

## Pattern status guidance

- `preferred` - strong default pattern for the feature family
- `allowed` - valid reusable pattern when it fits
- `caution` - valid, but feature availability or behavior may need explicit confirmation
- `avoid` - valid syntax, but usually a worse choice than a clearer alternative
- `forbidden` - retained only as negative guidance; do not emit it as a standalone model

If several patterns fit, start with `preferred`, then `allowed`, and only then consider `caution`.
