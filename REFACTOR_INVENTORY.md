# Refactor Inventory

## Supported system

The supported workflow is now:

1. Run `py run_canonical_pipeline.py`.
2. Open `py run_local_analytics_app.py` or `py -m streamlit run app/main.py`.
3. Use the application dashboards and `analytics_export.xlsx` for review/export work.

## Required runtime entry points

- `run_canonical_pipeline.py`
- `run_local_analytics_app.py`

## Required source modules

- `src/build_canonical_pipeline.py`: canonical ETL, normalization, QA, and bundle writing.
- `src/build_master_roster.py`: roster parsing helpers used by the canonical pipeline.
- `src/shared_utils.py`: shared text/numeric/chapter helpers.
- `src/excel_utils.py`: shared Excel formatting helpers used by retained code/tests.
- `app/data_loader.py`: canonical bundle discovery, validation, and loading.
- `app/main.py`: Streamlit UI.
- `app/analysis.py`, `app/charts.py`, `app/config_loader.py`, `app/exports.py`, `app/io_utils.py`, `app/metrics_engine.py`, `app/models.py`, `app/presets.py`, `app/status_framework.py`: app support modules.

## Required inputs

- `data/inbox/rosters/`
- `data/inbox/academic/`
- `data/inbox/transcript_text/`
- `data/inbox/graduation/`
- `data/inbox/reference_data/`
- `config/app_settings.json`
- `config/canonical_schema.json`
- `config/dataset_manifest.json`
- `config/metric_catalog.json`
- `config/status_code_map.json`
- `config/column_aliases.json`
- `config/transcript_text_manifest.csv`
- `config/manual_chapter_assignments.csv`
- `config/chapter_groups.csv`

## Required outputs

- Canonical run folders under `output/canonical/run_*`
- Latest canonical mirror under `output/canonical/latest`
- Canonical QA/audit CSVs
- App dashboards and app-generated export workbook

## Old standalone spreadsheet workflows

The old standalone workbook/report scripts and builder modules have been retired. Their information is now surfaced through:

- Data & Export filtered student/longitudinal tables
- Chapter Health dashboard
- Advisor Help intervention queue
- Audit tab and graduation evidence audit
- Persistence & Graduation landing page
- Rankings, comparisons, trends, and app workbook export

## Non-negotiable logic

- Do not count graduation without explicit graduation evidence.
- Do not infer graduation from disappearance, hours, or last observed term.
- Current active means present on the most recent roster only.
- Historical rosters remain useful for cohorts/trends but cannot inflate current-active counts.
- Unknown outcomes must stay visible and auditable.
