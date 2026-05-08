# FSL Analytics App Architecture

## Goals

- Center all analytics on one canonical source-of-truth bundle
- Keep preprocessing outside the app and analysis inside the app
- Remove fallback app-loading branches so the dashboard only reads canonical runs
- Avoid app-side recalculation of already standardized canonical tables
- Allow persistent manual roster corrections without editing raw source files

## Active architecture

### 1. Canonical-first workflow

The app now expects the pipeline to build one canonical analytics run before launch.

Authoritative inputs:

- `output/canonical/run_*/roster_term.csv`
- `output/canonical/run_*/academic_term.csv`
- `output/canonical/run_*/master_longitudinal.csv`
- `output/canonical/run_*/student_summary.csv`
- `output/canonical/run_*/cohort_metrics.csv`
- `output/canonical/run_*/qa_checks.csv`
- `output/canonical/run_*/canonical_schema.json`

Optional exception tables:

- `identity_exceptions.csv`
- `term_exceptions.csv`
- `status_exceptions.csv`
- `chapter_conflicts.csv`
- `outcome_exceptions.csv`
- `missing_evidence_cases.csv`

The active dataset source order is defined in:

- `config/dataset_manifest.json`

That manifest is now canonical-only.

### 2. Canonical source of truth

The app treats these as the only authoritative analytics tables:

- `roster_term`
- `academic_term`
- `master_longitudinal`
- `student_summary`
- `cohort_metrics`
- `qa_checks`

Everything else is downstream presentation output.

### 3. No required app-side re-standardization for canonical data

When the canonical bundle is loaded:

- the app reads the canonical tables directly
- no additional summary standardization is required
- no longitudinal rollup merge is required

App-side fallback standardization has been removed. Any older bundle must be rebuilt through the canonical pipeline before it can be used by the app.

### 4. Metric execution model

Metric metadata still lives in:

- `config/metric_catalog.json`

Interactive calculations and regrouping live in:

- `app/metrics_engine.py`

Those app-side calculations are downstream consumers of canonical `student_summary` and `master_longitudinal`; they are not a competing source of truth.

### 5. Validation and status

The app validates:

- presence of the canonical run folder
- presence of required canonical files
- required columns in canonical tables

The UI exposes dataset status, file presence, timestamps, row counts, QA warnings, and exception-table availability.

### 6. Removed fallback app paths

The app no longer loads enhanced, snapshot-augmented, or processed bundles directly. This keeps the denominator, graduation, and current-active rules in one canonical source of truth.

### 7. Manual correction ledger

The app can write roster corrections to:

- `config/manual_roster_corrections.csv`

Those corrections are not applied by editing raw Excel/PDF files. The canonical pipeline reads the ledger during source preparation, applies matching chapter/status/new-member/removal corrections before dedupe and conflict resolution, and includes the correction file in cache invalidation. Refreshing source caches does not erase the ledger.

## Main modules

- `app/data_loader.py`: dataset discovery, manifest validation, and canonical bundle loading
- `app/status_framework.py`: outcome-resolution classification utilities
- `app/metrics_engine.py`: metric execution on canonical app tables
- `app/analysis.py`: filtering, grouping, ranking, comparisons, and trends
- `app/charts.py`: chart builders
- `app/exports.py`: CSV/XLSX/HTML/PNG exports
- `app/main.py`: Streamlit UI

## Known limits

- Runtime validation still depends on the presence of a built canonical run
- Older non-canonical outputs must be regenerated through `run_canonical_pipeline.py` before use in the app
- Any metric quality issues inherited from source data still need to be handled through canonical QA and exception tables
- Manual corrections saved in the app require a canonical pipeline rerun before all metrics/charts reflect them
