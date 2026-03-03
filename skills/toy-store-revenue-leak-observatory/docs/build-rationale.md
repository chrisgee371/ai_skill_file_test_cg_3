# Build rationale

This bundle uses the supplied sample skill as a loose base, but it intentionally changes the design where the sample is weak for this task.

## What was reused from the sample approach

- file-based skill decomposition
- supporting docs rather than a single giant instruction file
- iterative workflow discipline
- a strong emphasis on transparency and guardrails

## What was changed

### 1. Pipeline-first, not dashboard-first

The sample skill is oriented toward dashboards and domain walkthroughs. This build is oriented toward stable pipeline generation first, with analytics layers built on top of that foundation.

### 2. Databricks-native, data-first IR

The sample prompt fragment was built around translating foreign code into SQL via JSON IR. That is not the best fit here. This bundle uses a Databricks-native IR built around:
- source manifests
- stage plans
- model contracts
- quality gates
- forbidden patterns

### 3. Machine-readable support objects

The sample relies heavily on prose. This build adds machine-readable support objects that can be loaded as Databricks tables to narrow the model's search space and reduce drift:
- `chris_demos.demos.source_manifest`
- `chris_demos.demos.model_registry`
- `chris_demos.demos.metric_catalog`
- `chris_demos.demos.join_contracts`
- `chris_demos.demos.acceptance_tests`

### 4. Packaging aligned to the current repo layout

The supplied sample archive needed repo-layout cleanup. This build preserves the repo-visible `skills/` root and expects the structured source and support inputs to be available in Databricks under `chris_demos.demos`.

### 5. Packaging hygiene

The sample archive contains macOS clutter such as `__MACOSX` and `.DS_Store`. This build does not ship those files.

## Why these changes matter

The goal is not only to give the model instructions. The goal is to make it harder for the model to fail in known ways:
- duplicating upstream stages
- breaking lineage
- guessing joins
- forgetting chronology
- over-generalizing from prose
