# Greek Life Academic Analytics

This repository now supports two complementary workflows:

1. The existing Python report builders and versioned analytics outputs
2. A new local Streamlit analytics application for dynamic filtering, controls, rankings, comparisons, charts, and exports

The app is built specifically for Fraternity / Sorority Life academic outcomes work, with emphasis on:

- graduation rates
- retention and continuation
- GPA outcomes
- chapter comparisons
- subgroup controls
- non-technical, presentation-ready outputs

## What was already in the repo

The repo already contained working logic for:

- column alias mapping and schema normalization
- term parsing and ordering
- raw academic + roster ingestion
- master dataset creation
- enhanced longitudinal student / cohort analytics
- current snapshot augmentation
- executive reporting outputs

Those existing calculations were preserved. The Streamlit app reads those outputs first when available instead of replacing them with new formulas.

## What the new app adds

The Streamlit app gives you an interactive local workspace where you can:

- load processed, enhanced, snapshot-augmented, or newly uploaded datasets
- segment by chapter, chapter groups, council, fraternity/sorority, major, Pell, transfer, join term/year, graduation year, size bands, estimated join stage, and other available controls
- compare selected chapters or groups side by side
- rank top and bottom performers with minimum-N rules
- view trends over join cohorts and observed terms
- inspect stacked distributions, histograms, boxplots, and scatterplots
- export filtered tables, summary tables, and chart files
- save and reload analysis presets

## Repo structure

Existing logic remains in `src/`.

New app-specific components:

```text
app/
  main.py
  legacy_bridge.py
  standardize.py
  metrics_engine.py
  analysis.py
  charts.py
  exports.py
  presets.py
config/
  app_settings.json
  metric_catalog.json
  status_code_map.json
  chapter_groups.example.csv
docs/
  architecture.md
  templates/
tests/
run_local_analytics_app.py
```

## Preferred data sources

The app prefers sources in this order:

1. `output/current_snapshot_metrics/run_*`
2. `output/enhanced_metrics/run_*`
3. `data/processed/*.csv` plus `output/metrics/*.csv`
4. Uploaded files staged into `data/processed/app_sessions/<session>/`

That lets the app preserve existing repo calculations whenever they already exist.

## Supported inputs

### Raw / uploaded files

- `.csv`
- `.xlsx`
- `.xls`
- `.xlsm`
- `.parquet`

### Common file types

- master roster
- chapter roster files
- academic records
- term-level files
- current snapshot files
- precomputed `student_summary`, `master_longitudinal`, `cohort_metrics`, or snapshot-augmented exports

### Mapping / config files

- chapter group mapping with optional council / org type / family columns

Template files live in:

- `docs/templates/academic_records_template.csv`
- `docs/templates/chapter_roster_template.csv`
- `docs/templates/current_snapshot_template.csv`
- `docs/templates/chapter_groups_template.csv`

## Setup

```powershell
python -m pip install -r requirements.txt
```

## Run the app

Either run Streamlit directly:

```powershell
python -m streamlit run app/main.py
```

Or use the helper:

```powershell
python run_local_analytics_app.py
```

## Existing pipeline workflows

The existing scripts still work exactly as before.

### Base processed pipeline

```powershell
python run_pipeline.py
```

Writes:

- `data/processed/master_dataset.csv`
- `data/processed/student_summary.csv`
- `output/metrics/*.csv`
- `output/excel/greek_life_master.xlsx`

### Master roster helpers

```powershell
python run_master_roster.py
python run_member_tenure_report.py
python run_master_roster_grades.py
python run_enhanced_org_analytics.py
python run_current_snapshot_analytics.py
python run_executive_report.py
python run_chapter_history_workbooks.py
```

## Streamlit app workflow

### Option 1: analyze an existing run

1. Run the legacy pipeline / enhanced / snapshot builders
2. Open the Streamlit app
3. Choose the dataset version from the sidebar
4. Pick a metric, aggregation level, filters, and controls
5. Export the resulting tables and charts

### Option 2: upload files directly into the app

1. Open the Streamlit app
2. Expand `Create Or Update A Dataset Session`
3. Upload:
   - academic files and roster files, or
   - recognized precomputed tables / workbooks
4. Optionally upload:
   - current snapshot files
   - chapter mapping file
5. Click `Process uploaded files`
6. Analyze the resulting saved session from the dataset selector

Uploaded files are copied into `data/processed/app_sessions/<session>/uploads/`.
Cleaned and intermediate outputs are written to `data/processed/app_sessions/<session>/processed/`.
Original source files are never modified in place.

## Metrics available in the app

The app exposes metrics through `config/metric_catalog.json`. Examples include:

- headcount
- active member count
- observed eventual graduation rate
- observed 4-year / 6-year graduation rate
- next-term / next-fall / one-year retention
- next-fall academic continuation
- average term GPA
- average cumulative GPA
- GPA change
- average cumulative hours
- average hours at join
- average estimated pre-organization hours
- low-GPA and first-year probation risk rates
- snapshot match rate
- data completeness rate

The app shows an `About this metric` section with:

- metric key
- source table
- logic source
- numerator
- denominator
- sample-size guidance
- notes / limitations

## Controls and cohorts

Depending on source availability, the app supports:

- chapter
- chapter group
- custom group
- council / family
- fraternity vs sorority
- join term / join year
- graduation year
- status bucket
- major
- Pell vs non-Pell
- transfer vs non-transfer
- estimated join stage
- current high-hours vs lower-hours
- active vs inactive
- chapter size bands

Fields that do not exist in the selected source degrade to `Unknown` rather than breaking the app.

## Estimated fields and caveats

Some metrics are explicitly estimated or inferred, especially in snapshot-augmented bundles.

Examples:

- estimated pre-organization credit hours
- estimated join stage / hours-at-join bucket
- probable advanced-entry / high-hours proxy interpretations

These are labeled in the app and should not be treated as exact reconstructed history.

## Backward compatibility

To preserve existing logic:

- existing `src/` builders were not removed or rewritten
- existing run scripts still point to the same code
- enhanced / snapshot flags are reused directly when available
- app-only metrics such as completeness are labeled separately from production metrics

If you need the design rationale, see:

- `docs/architecture.md`

## Testing

Run:

```powershell
pytest
```

Current tests cover:

- import / alias mapping
- term parsing
- cohort and group standardization
- metric calculations
- ranking minimum-N rules

## Example analysis workflow

1. Run `python run_enhanced_org_analytics.py`
2. Optionally run `python run_current_snapshot_analytics.py`
3. Open the app with `python -m streamlit run app/main.py`
4. Choose a snapshot-augmented or enhanced dataset version
5. Select `Observed 6-Year Graduation Rate`
6. Group by `Chapter`
7. Compare selected chapters against the FSL-wide average
8. Add controls such as major, Pell, transfer, or estimated join stage when available
9. Export the filtered tables and charts for reporting
