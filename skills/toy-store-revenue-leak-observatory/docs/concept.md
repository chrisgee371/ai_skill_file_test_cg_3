# Concept

## Project name

Toy Store Revenue Leak Observatory

## What the pipeline is trying to do

The pipeline is trying to identify where commercial value is being lost across the full e-commerce journey, from session acquisition through checkout and into post-purchase refunds.

It treats revenue loss as a set of diagnosable leak families rather than a single KPI problem:

- acquisition leak
- journey leak
- basket leak
- refund leak
- launch leak

## Why this idea is stronger than a normal dashboard

A normal e-commerce build would stop at conversion rate, revenue, and maybe product sales. This build goes further. It creates an observatory that:
- preserves lineage across multiple iterations
- compares experiments only in valid windows
- separates gross success from net-value erosion
- ends with ranked actions rather than isolated charts

## Why this dataset fits

The supplied data is strong enough for this design because it contains:
- 472,871 sessions
- 1,188,124 pageviews
- 32,313 orders
- 1,731 refunded order items
- multiple landing-page variants
- two billing-page variants
- four products with known launch timestamps

## Signature final output

The signature final model is `obs__leak_registry`, which should contain one row per period, entity, and leak family, including:
- severity_score
- supporting_metrics
- confidence_note
- recommended_action
- upstream_model_refs

## Innovation point

The innovative part is not the use of funnel data alone. It is the combination of:
- variant-aware experimentation
- refund-aware net economics
- launch-window diagnostics
- staged lineage-safe generation
- an explainable action layer
