# FSL Analytics App Architecture

## Goals

- Center all analytics on one canonical source-of-truth bundle
- Keep preprocessing outside the app and analysis inside the app
- Preserve legacy outputs only for backward compatibility and manual review
- Avoid app-side recalculation of already standardized canonical tables

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

Everything else is downstream presentation or backward-compatibility output.

### 3. No required app-side re-standardization for canonical data

When the canonical bundle is loaded:

- the app reads the canonical tables directly
- no additional summary standardization is required
- no longitudinal rollup merge is required

Legacy standardization code remains available only for manual review of older bundles.

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

### 6. Backward compatibility

Legacy bundles such as enhanced or snapshot-augmented outputs are preserved only for:

- manual review
- historical comparison
- troubleshooting

They are not the active default dataset path once a canonical bundle exists.

## Main modules

- `app/legacy_bridge.py`: dataset discovery, manifest validation, and canonical bundle loading
- `app/standardize.py`: legacy-only standardization helpers retained for backward compatibility
- `app/status_framework.py`: outcome-resolution classification utilities
- `app/metrics_engine.py`: metric execution on canonical app tables
- `app/analysis.py`: filtering, grouping, ranking, comparisons, and trends
- `app/charts.py`: chart builders
- `app/exports.py`: CSV/XLSX/HTML/PNG exports
- `app/main.py`: Streamlit UI

## Known limits

- Runtime validation still depends on the presence of a built canonical run
- Legacy loaders remain in code for compatibility, even though the active manifest is canonical-only
- Any metric quality issues inherited from source data still need to be handled through canonical QA and exception tables
