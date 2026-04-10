# FSL Analytics App Architecture

## Goals

- Preserve the repo's existing calculation logic instead of replacing it
- Let non-technical users explore prepared analytics outputs interactively
- Keep preprocessing outside the app and dashboard analysis inside the app
- Never require source-file uploads inside the interface

## Design choices

### 1. Preloaded local data workflow

The app now assumes datasets are prepared before launch.

Operational split:

- preprocessing / Excel-driven prep happens outside the app
- prepared outputs are written into the repo's local folders
- the app scans those folders at startup and auto-loads the best valid dataset

This keeps the UI focused on analysis rather than file handling.

### 2. Reuse existing source-of-truth logic first

The app prefers these existing local outputs in order:

1. `output/current_snapshot_metrics/run_*`
2. `output/enhanced_metrics/run_*`
3. `data/processed/*.csv` plus `output/metrics/*.csv`

That means graduation, retention, GPA-change, standing, and estimated-hours logic come from the repo's existing builders whenever those outputs exist.

The source order and required files are defined in:

- `config/dataset_manifest.json`

### 3. Add a canonical analysis model on top

The app standardizes each loaded bundle into:

- one student-level summary table
- one longitudinal term-level table

This gives the UI one consistent schema for filters, rankings, controls, and charts without changing the legacy builders.

### 4. Centralize metrics

Metric metadata lives in `config/metric_catalog.json` and includes:

- display name
- source table
- numerator / denominator
- logic source
- notes / limitations
- minimum sample-size guidance

The actual aggregation engine is in `app/metrics_engine.py`, so formulas are not scattered through the UI.

### 5. Validate and surface data status

Startup validation is handled before the dashboard renders.

The app checks:

- whether the expected prepared files exist
- whether required tables are present
- whether key required columns exist

The UI exposes a `Data Status` panel showing:

- active dataset source
- loaded files
- timestamps
- row counts
- validation warnings
- discovered local sources and expected files

### 6. Backward compatibility

- Existing `src/` files and run scripts remain intact
- The app reads legacy outputs but does not rewrite them
- App-only derived measures such as completeness are labeled separately from legacy production metrics

## Main modules

- `app/legacy_bridge.py`: dataset discovery, manifest-based validation, and legacy bundle loading
- `app/standardize.py`: canonical student/term model
- `app/metrics_engine.py`: metric registry execution
- `app/analysis.py`: filters, grouping, ranking, comparisons, trends
- `app/charts.py`: Plotly chart builders
- `app/exports.py`: CSV/XLSX/HTML/PNG exports
- `app/main.py`: Streamlit UI

## Known limits

- Snapshot augmentation inside the app currently expects enhanced-style summary + longitudinal tables
- Campus baseline comparisons only appear when non-FSL students exist in the selected bundle
- Some controls such as Pell and transfer are only available when the source data actually contains them
