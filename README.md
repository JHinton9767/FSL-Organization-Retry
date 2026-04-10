# Greek Life Academic Analytics

This repository supports two connected layers:

1. The existing Python data-prep and reporting scripts in `src/` and the repo root
2. A Streamlit analytics app that reads the prepared local outputs and lets users explore them interactively

The app is designed for Fraternity / Sorority Life academic outcomes work, with emphasis on:

- graduation rates
- retention and continuation
- GPA outcomes
- chapter comparisons
- fair subgroup controls
- presentation-ready outputs for non-technical stakeholders

## Current workflow

The app is now **preload-only**.

That means:

- you run the Excel/source-file prep workflow outside the app
- you place the finished files in the project folders the repo expects
- you launch the app
- the app automatically scans those folders, validates the prepared files, and loads the best available dataset

The app no longer requires file uploads through the UI.

## Outcome population views

The app now keeps two parallel statistical views for major metrics:

- `All Students`: preserves the existing current/full calculation logic
- `Resolved Outcomes Only`: uses the same formula after excluding students classified as unresolved

By default, `Resolved Outcomes Only` excludes outcome groups such as:

- `Still Active`
- `Unknown`
- `Other / Unmapped`

The exact grouping and exclusion rules are configurable in:

- `config/app_settings.json`

This matters most for final-outcome statistics such as graduation rates. Example:

- All Students: `42 / 100 = 42.0%`
- Resolved Outcomes Only: `42 / 61 = 68.9%`
- Excluded as Still Active / Unknown: `39`

## Existing logic preserved

The repo already contained logic for:

- column alias mapping and schema normalization
- term parsing and ordering
- raw academic + roster ingestion
- master dataset creation
- enhanced longitudinal student / cohort analytics
- current snapshot augmentation
- executive reporting outputs

Those existing calculations were preserved. The app reads those outputs first when available instead of replacing them with new formulas.

## Folder structure

The repo uses the existing folder layout below. The app reads from the prepared output folders, not from ad hoc uploads.

```text
app/
config/
data/
  raw/
    academic/
    rosters/
  inbox/
    academic/
    current_snapshot/
    rosters/
  processed/
    master_dataset.csv
    student_summary.csv
docs/
output/
  metrics/
    *.csv
  enhanced_metrics/
    run_*/
  current_snapshot_metrics/
    run_*/
src/
tests/
run_local_analytics_app.py
```

### What each folder is for

- `data/raw/`: original source files you keep as raw inputs
- `data/inbox/`: optional staging/drop locations for prep workflows
- `data/processed/`: processed pipeline outputs the app can use as a fallback source
- `output/enhanced_metrics/run_*/`: enhanced analytics bundles created by the legacy enhanced workflow
- `output/current_snapshot_metrics/run_*/`: snapshot-augmented bundles created by the current snapshot workflow
- `output/metrics/`: supporting processed metric tables
- `config/`: mappings, metric definitions, thresholds, and dataset loader settings

## Source priority

On startup the app scans local folders and uses the first valid source it finds in this order:

1. `output/current_snapshot_metrics/run_*`
2. `output/enhanced_metrics/run_*`
3. `data/processed/*.csv` plus `output/metrics/*.csv`

This priority is defined in:

- `config/dataset_manifest.json`

That manifest controls:

- expected file names
- required vs optional files
- source priority
- which local outputs count as authoritative prepared datasets

## Expected prepared outputs

### Current snapshot run

Expected under the latest folder in `output/current_snapshot_metrics/`:

- `snapshot_augmented_student_summary.csv`
- `snapshot_augmented_cohort_metrics.csv`
- `snapshot_augmented_chapter_metrics.csv`
- `snapshot_merge_qa.csv`

Optional:

- `methodology.md`
- `organization_entry_snapshot_augmented_*.xlsx`

### Enhanced run

Expected under the latest folder in `output/enhanced_metrics/`:

- `student_summary.csv`
- `cohort_metrics.csv`

Optional:

- `master_longitudinal.csv`
- `metric_definitions.csv`
- `qa_checks.csv`
- `organization_entry_analytics_enhanced_*.xlsx`
- `methodology.md`

### Processed fallback

Expected in fixed project folders:

- `data/processed/student_summary.csv`
- `data/processed/master_dataset.csv`

Optional supporting metrics:

- `output/metrics/graduation_rates.csv`
- `output/metrics/retention_rates.csv`
- `output/metrics/gpa_trends.csv`
- `output/metrics/credit_momentum.csv`
- `output/metrics/standing_distribution.csv`

## External prep order

If you are running the full legacy workflow, this is the typical order:

1. `python run_pipeline.py`
2. `python run_enhanced_org_analytics.py`
3. `python run_current_snapshot_analytics.py`

The app does not require every layer. It will fall back automatically:

- snapshot run if available
- otherwise enhanced run
- otherwise processed pipeline outputs

Other legacy helpers in the repo still work as before, including:

- `python run_master_roster.py`
- `python run_member_tenure_report.py`
- `python run_master_roster_grades.py`
- `python run_executive_report.py`
- `python run_chapter_history_workbooks.py`

## Setup

Install dependencies:

```powershell
py -m pip install -r requirements.txt
```

If `pip` is not available:

```powershell
py -m ensurepip --upgrade
py -m pip install -r requirements.txt
```

## Run the app

Either run Streamlit directly:

```powershell
py -m streamlit run app/main.py
```

Or use the helper:

```powershell
py run_local_analytics_app.py
```

## App startup behavior

When the app launches, it will:

1. scan the configured local folders
2. identify the highest-priority valid prepared dataset
3. validate the required tables and columns
4. build the dashboard automatically

If no valid prepared data is present, the app shows:

- which sources were checked
- which expected files were missing

If prepared files are found but fail validation, the app shows the load error and the file/status panel instead of crashing straight into a traceback.

## Data Status panel

The app includes a `Data Status` section that shows:

- the active dataset source
- which files were loaded
- file paths
- last modified timestamps
- row counts for loaded tables
- validation warnings
- discovered local sources and expected files

Use this panel after every refresh to confirm the app is reading the dataset you intended.

The dashboard also includes a population summary section for the selected metric showing:

- All Students value
- Resolved Outcomes Only value
- numerator
- denominator
- excluded Still Active / Unknown count
- excluded share

## Refresh workflow

When new data arrives:

1. rerun your external Excel or Python prep workflow
2. replace or update the prepared files in the expected project folders
3. relaunch or refresh the Streamlit app

The app does not poll for updates, sync in the background, or require in-app uploads.

## What the app does

The Streamlit app gives you an interactive local workspace where you can:

- segment by chapter, chapter groups, council, fraternity/sorority, major, Pell, transfer, join term/year, graduation year, size bands, estimated join stage, and other available controls
- compare selected chapters or groups side by side
- switch charts and rankings between `All Students` and `Resolved Outcomes Only`
- rank top and bottom performers with minimum-N rules for either population view
- view trends over join cohorts and observed terms under either population view
- inspect stacked distributions, histograms, boxplots, and scatterplots
- export filtered tables, summary tables, and chart files with both population versions where practical
- save and reload analysis presets

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
- population-view interpretation notes
- sample-size guidance
- notes / limitations

Major metric tables and exports now carry both views where practical, including columns such as:

- metric value for `All Students`
- metric value for `Resolved Outcomes Only`
- eligible N for both views
- numerator for both views
- excluded Still Active / Unknown count
- excluded Still Active / Unknown share

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
- outcome resolution group
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

## Configuration files

Key config files:

- `config/dataset_manifest.json`: source discovery and required file definitions
- `config/app_settings.json`: thresholds, defaults, and outcome-resolution grouping rules
- `config/metric_catalog.json`: metric registry
- `config/status_code_map.json`: status normalization
- `config/chapter_groups.csv` or `config/chapter_groups.example.csv`: chapter mapping and grouping

## Backward compatibility

To preserve existing logic:

- existing `src/` builders were not removed or rewritten
- existing run scripts still point to the same code
- enhanced / snapshot flags are reused directly when available
- app-only derived measures such as completeness are labeled separately from legacy production metrics

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
- resolved-outcome classification and parallel metric views
- ranking minimum-N rules
- loader validation edge cases

## Example analysis workflow

1. Run `python run_enhanced_org_analytics.py`
2. Optionally run `python run_current_snapshot_analytics.py`
3. Open the app with `py -m streamlit run app/main.py`
4. Confirm the loaded source in `Data Status`
5. Select `Observed 6-Year Graduation Rate`
6. Choose `All Students` or `Resolved Outcomes Only` as the chart/ranking population view
7. Group by `Chapter`
8. Compare selected chapters against the FSL-wide average
9. Add controls such as major, Pell, transfer, or estimated join stage when available
10. Export the filtered tables and charts for reporting
