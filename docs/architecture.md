# FSL Analytics App Architecture

## Goals

- Preserve the repo's existing calculation logic instead of replacing it.
- Let non-technical users load local files and explore outcomes interactively.
- Keep every original source file untouched and write app outputs separately.

## Design choices

### 1. Reuse existing source-of-truth logic first

The app prefers these existing layers in order:

1. `output/current_snapshot_metrics/run_*`
2. `output/enhanced_metrics/run_*`
3. `data/processed/*.csv` plus `output/metrics/*.csv`
4. New uploaded sessions built from raw academic + roster files

That means graduation, retention, GPA-change, standing, and estimated-hours logic come from the repo's existing builders whenever those outputs exist.

### 2. Add a canonical analysis model on top

The app standardizes each bundle into:

- one student-level summary table
- one longitudinal term-level table

This gives the UI one consistent schema for filters, rankings, controls, and charts without changing the legacy builders.

### 3. Centralize metrics

Metric metadata lives in `config/metric_catalog.json` and includes:

- display name
- source table
- numerator / denominator
- logic source
- notes / limitations
- minimum sample-size guidance

The actual aggregation engine is in `app/metrics_engine.py`, so formulas are not scattered through the UI.

### 4. Separate staging from source data

Uploaded files are copied into `data/processed/app_sessions/<session>/uploads/`.
Derived CSV/parquet outputs are written to `data/processed/app_sessions/<session>/processed/`.

Original user files are never modified in place.

### 5. Backward compatibility

- Existing `src/` files and run scripts remain intact.
- The app reads legacy outputs but does not rewrite them.
- App-only derived measures such as the completeness score are labeled separately from legacy production metrics.

## Main modules

- `app/legacy_bridge.py`: dataset discovery, uploaded-session staging, and legacy bundle loading
- `app/standardize.py`: canonical student/term model
- `app/metrics_engine.py`: metric registry execution
- `app/analysis.py`: filters, grouping, ranking, comparisons, trends
- `app/charts.py`: Plotly chart builders
- `app/exports.py`: CSV/XLSX/HTML/PNG exports
- `app/main.py`: Streamlit UI

## Known limits

- Snapshot augmentation inside the app currently expects enhanced-style summary + longitudinal tables.
- Campus baseline comparisons only appear when non-FSL students exist in the selected bundle.
- Some controls such as Pell and transfer are only available when the source data actually contains them.

