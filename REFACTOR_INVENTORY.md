# Refactor Inventory

## Current Supported Workflow

The project now has two supported runtime surfaces:

1. `py run_canonical_pipeline.py`
   - Reads raw roster, academic, transcript-text, graduation, snapshot, reference, and config inputs.
   - Builds normalized source caches, canonical Parquet/CSV tables, QA/audit tables, and performance reports under `output/canonical/`.
   - Applies manual chapter and roster corrections from `config/` without editing raw files.

2. `py run_local_analytics_app.py` or `py -m streamlit run app/main.py`
   - Loads the latest canonical run through `app/data_loader.py`.
   - Renders dashboards, rankings, persistence/graduation views, Chapter Health, Advisor Help, Manual Corrections, audit tables, and exports.

## File-by-File Behavior Inventory

| File | Purpose | Inputs | Outputs / Side Effects | Required? | Notes |
|---|---|---|---|---|---|
| `run_canonical_pipeline.py` | Thin CLI entry point into `src.build_canonical_pipeline.main` | CLI args, configured input folders | Canonical output run | Yes | Keep as the main pipeline command. |
| `run_local_analytics_app.py` | Thin Streamlit launcher | `app/main.py` | Starts Streamlit app | Yes | Useful for nontechnical users. |
| `Start_FSL_Analytics_App.bat` | Windows double-click app launcher | Local Python/uv environment | Starts app | Yes | Helper-facing convenience. |
| `Start_Manual_Corrections_App.bat` | Windows launcher for manual-corrections mode | Local canonical run | Starts app in correction mode | Yes | Important for shared helper workflow. |
| `src/build_canonical_pipeline.py` | Main ETL, caching, normalization, canonical table generation, QA | `data/inbox/**`, `config/**`, manual ledgers | `output/canonical/run_*`, `latest`, Parquet/CSV tables, QA, manifests | Essential | Large and procedural; must be split only with parity tests. |
| `src/build_master_roster.py` | Roster/PDF/Excel parsing helpers, chapter/status inference | Roster workbook/PDF paths and rows | Parsed roster records and helper metadata | Essential | Used directly by canonical pipeline. |
| `src/shared_utils.py` | Shared text, chapter, numeric, and outcome helper constants | DataFrames/Series/text | Helper values | Essential | Some old formatting/rate helpers are unused and safe to remove. |
| `src/excel_utils.py` | Removed legacy Excel formatting helpers | openpyxl worksheets | Styled worksheets | No | Removed after confirming no active imports in code or tests. |
| `app/main.py` | Streamlit UI and app orchestration | Canonical bundle, config, session state | Dashboards, manual correction CSV edits, downloads | Essential | Still too large; best future split is dashboard modules plus manual-correction module. |
| `app/data_loader.py` | Canonical run discovery, validation, table loading | `output/canonical`, manifest | `AnalysisBundle` | Essential | Correctly canonical-only; minor duplication in status-list builders. |
| `app/config_loader.py` | Settings, metric config, chapter groups, manual correction ledgers/packages | `config/*.json`, `config/*.csv`, transcript folder | DataFrames, package ZIPs, correction CSV writes | Essential | Manual-correction normalization now centralized here. |
| `app/analysis.py` | App-side aggregations for dashboards, rankings, persistence, health, advisor queue | Canonical summary/longitudinal tables | Display-ready DataFrames | Essential | Large but pure; refactor by dashboard family later. |
| `app/charts.py` | Plotly chart constructors | Display DataFrames | Plotly figures | Essential | Small and clean. |
| `app/exports.py` | CSV/XLSX/PNG/HTML export bytes; Excel sheet chunking | App display DataFrames/figures | Download bytes | Essential | Handles Excel row limits. |
| `app/io_utils.py` | App text, term, file-read, slug helpers | Paths/text | Parsed terms and loaded tabular files | Essential | Several unused legacy helpers are safe to remove. |
| `app/metrics_engine.py` | Metric definition execution and denominator views | Summary/longitudinal frames, metric catalog | Metric result dictionaries | Essential | Central app metric engine. |
| `app/status_framework.py` | Outcome taxonomy and explicit-graduation evidence gating | Canonical summary fields | Outcome flags and population masks | Essential | Must preserve explicit-graduation-only behavior. |
| `app/models.py` | Dataclasses for bundles/status/metrics | n/a | Typed containers | Essential | Small and clean. |
| `app/presets.py` | Save/load UI filter presets | `config/analysis_presets/*.json` | Preset JSON | Optional but used by app | Keep. |
| `tests/*.py` | Regression and smoke tests | In-memory data/temp dirs | Test validation | Essential | Must remain. |
| `docs/*.md` | Architecture/status denominator docs | n/a | Human documentation | Required support docs | Keep updated. |
| `powerquery/*.pq` | Old PowerQuery source transformations | Raw folders | Excel/PowerQuery outputs | Optional/legacy | Not used by Python pipeline or app. Preserve unless user confirms removal. |
| `config/*.json`, `config/*.csv` | Settings, schema, manifests, ledgers, mappings | Edited by user/app | Runtime behavior | Essential | Do not delete. |
| `data/inbox/**` | Raw input folder placeholders/docs | User-supplied raw files | Pipeline source data | Essential | Do not overwrite raw files. |

## Required Inputs

- `data/inbox/rosters/`
- `data/inbox/academic/`
- `data/inbox/transcript_text/`
- `data/inbox/graduation/`
- `data/inbox/current_snapshot/`
- `data/inbox/reference_data/`
- Optional reference roots: membership, GPA, GPA benchmark references
- `config/app_settings.json`
- `config/canonical_schema.json`
- `config/dataset_manifest.json`
- `config/metric_catalog.json`
- `config/status_code_map.json`
- `config/column_aliases.json`
- `config/chapter_groups.csv`
- `config/manual_chapter_assignments.csv`
- `config/manual_roster_corrections.csv`
- `config/transcript_text_manifest.csv`

## Required Outputs

- `output/canonical/run_*/roster_term.{parquet,csv}`
- `output/canonical/run_*/academic_term.{parquet,csv}`
- `output/canonical/run_*/master_longitudinal.{parquet,csv}`
- `output/canonical/run_*/student_summary.{parquet,csv}`
- `output/canonical/run_*/cohort_metrics.{parquet,csv}`
- `output/canonical/run_*/qa_checks.{parquet,csv}`
- `output/canonical/run_*/canonical_schema.json`
- `output/canonical/run_*/performance_report.{parquet,csv,json}`
- Optional exception/audit tables including identity, term, status, chapter conflicts, outcome exceptions, missing evidence, unresolved chapter review, graduation status audit, transcript parse outputs, and reference validation outputs
- `output/canonical/latest/` mirror
- App-generated CSV/XLSX/HTML/PNG downloads

## Major Transformations

- Source discovery and source-cache invalidation by file signatures and code token.
- Roster parsing from Excel, CSV, and PDF with folder/file/sheet priority.
- Academic grade parsing from normal grade files, Copy of Grades, LOGI-style reports, CSVs, and transcript text.
- Transcript text parsing into term, course, audit, issue, and academic rows.
- Manual chapter corrections and manual roster corrections.
- Roster conflict resolution by source priority, secondary organization handling, month/version/final/updated/revised ranking.
- Chapter assignment inference and unresolved chapter review.
- Student identity resolution by ID, email, and name fallback.
- Current-active tagging from most recent roster only.
- Graduation outcome classification using explicit graduation evidence only.
- Unknown/active/resolved outcome taxonomy.
- Cohort metrics, retention/GPA outputs, QA checks, and reference validations.
- App-side filtering, grouping, charts, rankings, persistence/graduation display, Chapter Health, Advisor Help, manual correction workflow, and exports.

## Caches and Intermediate Files

- `output/canonical/_source_cache/`: normalized source-table caches with manifests.
- Downstream canonical-core caches inside the same cache root for slow post-ingest stages.
- Parquet siblings for all canonical CSV outputs.
- `config/manual_review_queue.csv`: helper assignment state.
- `data/inbox/transcript_text/Transcripts/*.txt`: manual transcript paste-in files.
- `config/analysis_presets/*.json`: app filter presets.

## Redundancy and Performance Findings

- `src/build_canonical_pipeline.py` is the largest complexity center and mixes source ingestion, normalization, business rules, metrics, QA, and output writing.
- `app/main.py` is the largest app complexity center and mixes many unrelated screens plus manual correction operations.
- Term parsing exists in both `app/io_utils.py` and `src/build_canonical_pipeline.py`; centralizing later would reduce duplicate behavior but is risky without broader parity tests.
- Chapter normalization exists in `src/shared_utils.py`, `src/build_master_roster.py`, and `src/build_canonical_pipeline.py`; centralizing later is desirable but high risk because parsing behavior is source-specific.
- Manual correction normalization is centralized in `app/config_loader.py`, but pipeline application logic remains in `src/build_canonical_pipeline.py` because it mutates roster rows.
- `app/io_utils.py` contains unused legacy helper functions that no active code imports.
- `src/excel_utils.py` was not imported anywhere in active code or tests and has been removed.
- `app/io_utils.py` had unused cache/flag/list helpers left from earlier app designs. These were removed while preserving term parsing, file loading, slugging, and header canonicalization.
- `src/shared_utils.py` had unused report-format and simple-rate helpers left from old spreadsheet-style reporting. These were removed while preserving text cleaning, chapter normalization, chapter mapping override behavior, numeric coercion, and 30-hour bucket logic.
- `powerquery/*.pq` files are not part of the Python/app runtime but may be historical reference artifacts.
- The pipeline still uses some row-wise loops for messy Excel/PDF parsing and student summary construction. Some loops are necessary because sources are irregular; high-value future refactors should target summary construction and chapter inference after adding output-parity fixtures.

## Minimum Necessary System

The minimum supported system is:

- One canonical ETL command.
- One canonical app loader.
- One Streamlit app.
- Config-driven metrics/status/settings.
- Manual correction ledgers.
- Tests covering normalization, explicit graduation, current-active semantics, Copy of Grades, transcript parsing, app analytics, exports, and Parquet loading.

Everything outside that set should either be documentation, raw input placeholders, or explicitly marked legacy.

## Cleanup Applied In This Pass

- Removed `src/excel_utils.py` because it was not imported by the canonical pipeline, app, or tests.
- Removed unused helper functions from `app/io_utils.py`: `write_dataframe_cache`, `bool_from_flag`, `category_from_bool`, `first_non_empty`, `first_non_null_numeric`, and `unique_values`.
- Removed unused helper functions from `src/shared_utils.py`: `yes_mask`, `mean_or_blank`, `unique_non_blank_count`, `extract_year_from_text`, `simple_rate`, `adjusted_grad_rate`, `percent_text`, `decimal_text`, and `count_text`.
- Left the large canonical pipeline and Streamlit app structure intact because splitting them is a higher-risk refactor that needs table-level output parity fixtures.

## Non-Negotiable Rules

- Do not count a student as graduated unless graduation is explicit.
- Do not treat disappearance from rosters as graduation.
- Do not treat last observed term, high hours, GPA, or transcript length as graduation.
- Current active students must come from the most recent roster only.
- Historical rosters remain available for trend/cohort analysis.
- Unknown outcomes must remain visible and auditable.
- Manual corrections must persist across cache refreshes.
- Raw source files must not be overwritten.
- App outputs must load from canonical Parquet/CSV bundles, not legacy processed folders.
