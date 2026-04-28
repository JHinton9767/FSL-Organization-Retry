# Refactor Inventory

## Repository purpose

This project ingests Fraternity and Sorority Life roster files, academic records, optional current snapshot files, optional graduation lists, optional transcript-text exports, and optional reference-data workbooks. It normalizes those inputs into a canonical analytics bundle, then builds downstream workbooks, exports, and a Streamlit analytics app.

## Major tasks the code currently performs

- Read messy roster files from `Copy of Rosters/` and `data/inbox/rosters/`, including nested folders, workbook sheets, revised/final naming priority, and some PDFs.
- Read academic term files from `data/inbox/academic/`.
- Read one-row-per-student current snapshot files from `data/inbox/academic/` when filenames look like `New Member (X)`.
- Read transcript-like text exports from `data/inbox/transcript_text/`.
- Read graduation list files from `data/inbox/graduation/`.
- Read mixed reference data from `data/inbox/reference_data/` and specialized reference folders.
- Normalize student identity keys, term codes, chapter names, roster status values, academic standing, GPA fields, and hours fields.
- Resolve missing student IDs by email or name.
- Resolve missing chapter assignments by cross-row matching, file/sheet-name inference, and manual overrides.
- Remove or deprioritize secondary organizations when assigning a primary chapter.
- Deduplicate roster and academic rows.
- Resolve same-term roster conflicts using source priority, month priority, status priority, and RS/RV handling.
- Determine observed organization entry term and observed school-entry proxies.
- Build canonical tables:
  - `roster_term`
  - `academic_term`
  - `master_longitudinal`
  - `student_summary`
  - `cohort_metrics`
  - `qa_checks`
- Build exception and audit tables:
  - `identity_exceptions`
  - `term_exceptions`
  - `status_exceptions`
  - `chapter_conflicts`
  - `outcome_exceptions`
  - `missing_evidence_cases`
  - `unresolved_chapter_review`
  - `graduation_status_audit`
  - transcript parse audit outputs
  - reference validation outputs
- Build downstream exports from the canonical bundle:
  - `Master_Roster_Grades.xlsx`
  - `Member_Tenure_Report.xlsx`
  - yearly chapter roster workbooks
  - per-chapter history workbooks
  - full academic record priority workbook and CSV
  - unresolved outcome by year workbook
  - executive report workbook, slide-data workbook, markdown summary, README
- Load the canonical bundle into a Streamlit app for ranking, filtering, comparison, trend, audit, and export workflows.
- Preserve legacy loaders and standardizers for older enhanced/snapshot/processed bundles.

## Primary entry points

- `run_canonical_pipeline.py`
- `run_local_analytics_app.py`
- `run_master_roster.py`
- `run_master_roster_grades.py`
- `run_member_tenure_report.py`
- `run_yearly_chapter_rosters.py`
- `run_chapter_history_workbooks.py`
- `run_full_record_priority_list.py`
- `run_unresolved_outcome_year_report.py`
- `run_executive_report.py`
- `run_current_snapshot_analytics.py`
- `run_enhanced_org_analytics.py`
- `run_pipeline.py`

## Core modules and current responsibilities

- `src/build_canonical_pipeline.py`
  - Monolithic orchestrator for ingestion, normalization, identity resolution, chapter inference, deduplication, outcome logic, caches, QA, transcript parsing, reference parsing, exports, and performance reporting.
- `src/build_master_roster.py`
  - Legacy-style roster ingestion and workbook export, plus reusable helpers that other modules currently import.
- `src/canonical_bundle.py`
  - Load the latest canonical run and required/optional tables.
- `app/legacy_bridge.py`
  - Dataset discovery, legacy-bundle loading, app bundle assembly, compatibility validation.
- `app/standardize.py`
  - Standardize legacy enhanced/snapshot/processed tables into app-ready shape.
- `app/status_framework.py`
  - Shared outcome taxonomy, evidence gating, resolved-only logic, and denominator helpers.
- `app/metrics_engine.py`
  - Metric execution and population-view logic.
- `app/analysis.py`
  - Grouping, ranking, comparisons, distributions, trends, scatter prep.
- `app/main.py`
  - Streamlit interface and audit displays.

## Inputs the project currently reads

- `Copy of Rosters/`
- `data/inbox/rosters/`
- `data/inbox/academic/`
- `data/inbox/transcript_text/`
- `data/inbox/graduation/`
- `data/inbox/reference_data/`
- `data/inbox/membership_reference/`
- `data/inbox/gpa_reference/`
- `data/inbox/gpa_benchmark_reference/`
- `config/app_settings.json`
- `config/canonical_schema.json`
- `config/dataset_manifest.json`
- `config/metric_catalog.json`
- `config/status_code_map.json`
- `config/column_aliases.json`
- `config/transcript_text_manifest.csv`
- `config/manual_chapter_assignments.csv`
- `config/chapter_groups.csv` when present
- `config/chapter_groups.example.csv`

## Outputs the project currently creates

- Canonical run folders under `output/canonical/run_*/`
- Canonical latest copies under `output/canonical/latest/`
- Source-cache folders under `output/canonical/_source_cache/`
- Performance reports:
  - `performance_report.csv`
  - `performance_report.json`
- Reference validation CSVs
- Transcript parse CSVs
- `Master_FSL_Roster.xlsx`
- `Master_FSL_Roster_New_Members.xlsx`
- `Master_FSL_Roster_Active_Members.xlsx`
- `Master_FSL_Roster_Unique_Banner_IDs.xlsx`
- `Master_Roster_Grades.xlsx`
- `Member_Tenure_Report.xlsx`
- `Yearly/*.xlsx`
- `output/chapter_history/run_*/...`
- `output/record_priority/run_*/...`
- `output/unresolved_outcomes/run_*/...`
- `output/presentation_ready/run_*/...`
- `output/current_snapshot_metrics/run_*/...`
- `output/enhanced_metrics/run_*/...`

## Transformations currently applied

- Header canonicalization and alias matching for spreadsheets.
- Term parsing from folder names, filenames, sheet names, raw term labels, and coded values.
- Student ID normalization, including missing `A0` handling and numeric-string cleanup.
- Snapshot column alias resolution.
- Graduation list header alias resolution.
- Transcript-text parsing into term summary, course detail, and audit rows.
- Reference workbook classification into membership counts, new-member counts, chapter GPA rows, benchmark GPA rows, retention rows, or unknown rows.
- Preferred roster row selection based on file version and month priority.
- Secondary-organization suppression during primary chapter assignment.
- Manual chapter override application.
- Deduplication by identity and term keys.
- Same-term roster conflict ranking.
- Organization entry term assignment.
- Longitudinal merge of roster and academic rows.
- Student-summary derivation for current activity, GPA, hours, measurable windows, retention, graduation, and outcome flags.
- Outcome reclassification into standardized groups.
- QA aggregation and exception output generation.
- Workbook rendering for multiple downstream audiences.

## Caches and intermediate files currently used

- Source-stage caches under `output/canonical/_source_cache/`:
  - `roster_sources`
  - `academic_sources`
  - `snapshot_sources`
  - `transcript_text_sources`
  - `graduation_sources`
  - `reference_sources`
- Staged downstream caches:
  - `reference_derivatives`
  - `prepared_sources`
  - `canonical_core`
- Latest-run mirror folder under `output/canonical/latest/`
- Multiple downstream run folders that preserve history.

## Duplicated or near-duplicated logic

- `clean_text` appears in multiple modules:
  - `src/build_master_roster.py`
  - `src/build_member_tenure_report.py`
  - `src/build_master_roster_grades.py`
  - `src/build_yearly_chapter_rosters.py`
  - plus imports from builder modules used as utility libraries elsewhere
- `coerce_numeric` appears in:
  - `src/build_canonical_pipeline.py`
  - `src/build_member_tenure_report.py`
  - multiple local lambdas around `pd.to_numeric`
- `yes_mask`, `mean_or_blank`, `rate`, and adjusted graduation-rate logic are repeated in:
  - `src/build_executive_report.py`
  - `src/build_chapter_history_workbooks.py`
  - `src/build_current_snapshot_analytics.py`
- Workbook formatting helpers are repeated or imported sideways:
  - `autosize_columns`
  - `style_header`
  - sheet-title helpers
  - safe sheet/file-name helpers
- Term parsing logic is implemented more than once:
  - `src/build_canonical_pipeline.py`
  - `src/greek_life_pipeline.py`
  - `app/io_utils.py`
  - ad hoc `extract_year()` helpers in multiple builders
- Snapshot loading and “choose best row” logic exists in both:
  - `src/build_canonical_pipeline.py`
  - `src/build_current_snapshot_analytics.py`
- Graduation/status logic is implemented in more than one layer:
  - `src/build_canonical_pipeline.py`
  - `app/status_framework.py`
  - `app/standardize.py`
  - `src/build_enhanced_org_analytics.py`
  - `src/build_current_snapshot_analytics.py`
- Dataset-loading and validation logic exists in both:
  - `src/canonical_bundle.py`
  - `app/legacy_bridge.py`
- Cross-module utility imports create tangled dependencies:
  - report builders import helpers from other report builders
  - app standardization imports `bucket_30_hours` from a downstream snapshot builder
  - canonical pipeline imports basic text helpers from the roster export builder

## Slow sections or repeated scans

- `src/build_canonical_pipeline.py` still scans many file groups independently and performs multiple large build phases in one file.
- Reference data is classified and re-sliced into multiple derivative tables after a wide inventory read.
- Transcript parsing, snapshot parsing, graduation parsing, and reference parsing all live in the same pipeline module and are rebuilt via separate cache bundles.
- Downstream builders often re-aggregate from `student_summary` or `master_longitudinal` separately instead of sharing a compact report-support layer.
- Legacy app paths keep compatibility code for enhanced, current-snapshot, and processed bundles even though the manifest defaults to canonical only.

## Dependencies that appear unnecessary or overly coupled

- `app/legacy_bridge.py` still imports and supports legacy processed/enhanced/snapshot paths even though `config/dataset_manifest.json` is canonical-first.
- `src/greek_life_pipeline.py` represents an older pipeline with overlapping normalization and metrics logic.
- Several downstream builders depend on functions from other builders rather than a shared utility module.
- `powerquery/*.pq` scripts duplicate some folder-transform assumptions outside the Python pipeline.

## Places where the same logic is implemented more than once

- Status normalization
- Term parsing
- Graduation evidence checks
- Active/current-member labeling
- Workbook column auto-sizing and header styling
- Rate calculations with measurable-window filtering
- DataFrame export-sheet writing
- Dataset discovery and latest-run resolution

## Parts better suited to a different structure

- Messy workbook and PDF ingestion should stay in pandas/openpyxl-plus-parser code.
- Canonical term/identity/chapter normalization should happen once early in the canonical pipeline and be reused everywhere downstream.
- Large joins, dedupes, and cohort aggregations are currently manageable in pandas, but the logical structure should be split into focused modules rather than one giant file.
- Repeated report-table aggregation is best handled by shared pandas helper functions, not repeated workbook-local implementations.
- Legacy compatibility should be isolated behind a small adapter layer, not spread across the app and builders.

## Minimum behavior that must survive refactor

- Canonical pipeline inputs, outputs, and schema
- Current active logic based on most recent roster only
- Graduation logic gated by explicit roster graduation evidence
- Transcript-text support
- Reference-data support
- Manual chapter override support
- Downstream workbook/report generation
- Streamlit app loading the canonical bundle
- Exception and QA output generation
