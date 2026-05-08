# Refactor Plan

## Current target architecture

The project should now stay centered on two supported surfaces:

1. `src/build_canonical_pipeline.py`
   - Ingests rosters, grades, transcript text, graduation evidence, snapshots, reference data, and config.
   - Produces the canonical CSV bundle and QA/audit outputs.
   - Owns normalization, student matching, chapter assignment, current-active tagging, graduation evidence gating, cohorts, and metrics.

2. `app/`
   - Loads only the canonical bundle through `app/data_loader.py`.
   - Provides dashboards, rankings, audit tables, Advisor Help, Chapter Health, and export workbooks.
   - Replaces the old standalone spreadsheet/report builders.

## Files that should remain

- `run_canonical_pipeline.py`
- `run_local_analytics_app.py`
- `src/build_canonical_pipeline.py`
- `src/build_master_roster.py` as roster parsing/helper utilities only
- `src/shared_utils.py`
- `src/excel_utils.py`
- `app/*.py`
- `config/*.json` and required config CSVs
- `tests/*.py`
- `data/inbox/**/.gitkeep` and source-folder documentation

## Files intentionally removed

The standalone workbook/report builders were removed because their review workflows now live in the app and app export workbook. The app and canonical pipeline are the supported path.

## Operations that should happen once

- Term normalization
- Status taxonomy resolution
- Graduation evidence gating
- Current-active tagging from the most recent roster
- Chapter mapping and chapter provenance resolution
- Reference inventory parsing
- Transcript text parsing
- Metric table preparation

## Outputs that must be preserved

- Canonical CSV bundle under `output/canonical/run_*`
- `output/canonical/latest`
- Canonical QA/audit CSVs
- Streamlit app dashboards and downloadable `analytics_export.xlsx`

## Future cleanup checklist

- Split the large canonical pipeline into focused modules only when tests can prove output parity.
- Keep app loading canonical-only.
- Do not reintroduce standalone report builders unless there is a required output the app cannot replace.
- Keep graduation explicit-evidence-only and current-active latest-roster-only.
