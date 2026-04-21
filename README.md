# FSL Academic Analytics

This repository now centers on a single canonical analytics architecture for Fraternity / Sorority Life academic reporting.

## Canonical source of truth

All future analytics are expected to flow from exactly six authoritative tables:

- `roster_term`
- `academic_term`
- `master_longitudinal`
- `student_summary`
- `cohort_metrics`
- `qa_checks`

These are produced by:

```powershell
py run_canonical_pipeline.py
```

The output is written to:

- `output/canonical/run_*/`
- `output/canonical/latest/`

## Canonical workflow

Use this order when rebuilding from source files:

1. Place roster files in `Copy of Rosters/` and/or `data/inbox/rosters/`
2. Place term-level academic files in `data/inbox/academic/`
3. Optionally place graduation lists in `data/inbox/graduation/`
4. Optionally place current one-row snapshot files such as `New Member (1)` in `data/inbox/academic/`
5. Optionally place a single combined workbook such as `Reference Data.xlsx` in `data/inbox/reference_data/`
   The canonical run will scan mixed reference sheets for chapter counts, new-member counts, chapter GPA trends, benchmark GPA trends, and retention-style reference rows.
6. Optionally use the specialized folders instead:
   `data/inbox/membership_reference/`, `data/inbox/gpa_reference/`, and `data/inbox/gpa_benchmark_reference/`
7. Run:

```powershell
py run_canonical_pipeline.py
```

After the canonical bundle exists, any workbook/report builders are downstream exports only.

## Faster reruns

`run_canonical_pipeline.py` now keeps a persistent source cache under `output/canonical/_source_cache/`.

On a normal rerun:

- unchanged roster files reuse cached normalized roster input tables
- unchanged academic files reuse cached normalized academic input tables
- unchanged snapshot, graduation, and reference files do the same

This means code changes in downstream analytics/report logic no longer need to reopen every raw Excel file just to rebuild the canonical outputs.

Use:

```powershell
py run_canonical_pipeline.py
```

If you changed raw parsing logic and want to force the source files to be re-read, use:

```powershell
py run_canonical_pipeline.py --refresh-source-cache
```

If you only changed a downstream builder, rerun only that builder instead of the full chain. For example:

- report formatting only: `py run_executive_report.py`
- chapter workbooks only: `py run_chapter_history_workbooks.py`
- app UI only: `py run_local_analytics_app.py`

## Downstream exports

These scripts now read canonical outputs instead of using old report files as upstream inputs:

- `py run_master_roster_grades.py`
- `py run_member_tenure_report.py`
- `py run_yearly_chapter_rosters.py`
- `py run_chapter_history_workbooks.py`
- `py run_full_record_priority_list.py`
- `py run_unresolved_outcome_year_report.py`
- `py run_executive_report.py`

## App behavior

The local analytics app is preload-only and is expected to load the canonical bundle first.

The app manifest now points to canonical outputs as the preferred prepared dataset source.

## Important interpretation rules

- Do not treat first observed organization entry as true school entry.
- Do not treat disappearance as a confirmed negative outcome.
- Do not calculate long-window graduation rates for non-measurable cohorts.
- Keep unresolved outcomes separate from resolved outcomes.

## Outcome status and denominator rules

The canonical pipeline and app now use one shared outcome taxonomy:

- `Graduated`
- `Resolved Non-Graduate Exit`
- `Still Active`
- `Truly Unknown / Unresolved`
- `Other / Unmapped`

Important distinction:

- `Still Active` means the latest available evidence still points to an active/current student or member.
- `Truly Unknown / Unresolved` means there is no reliable final outcome evidence.
- These are not the same thing and are no longer combined silently.

Graduation-focused views now expose two denominator styles:

- `Full Population`
  - keeps the whole filtered cohort in the denominator
- `Resolved Outcomes Only`
  - excludes `Still Active`, `Truly Unknown / Unresolved`, and `Other / Unmapped`

Use `Resolved Outcomes Only` for most final-outcome interpretation.
Use `Full Population` when you need to show the broader unresolved burden alongside the rate.

## Chapter assignment provenance

Canonical roster rows now preserve how a chapter was assigned:

- `original`
- `manual_override`
- `matched_by_id_name`
- `matched_by_id`
- `inferred_from_file_name`
- `inferred_from_sheet_name`
- `unresolved`

Fallback order:

1. source chapter field or inline chapter label
2. matching student ID + exact name in other roster rows
3. matching student ID in other roster rows
4. source file name clue
5. source sheet name clue
6. unresolved

Secondary organizations ignored for primary-chapter analytics:

- `Phi Delta Chi`
- `Alpha Phi Omega`
- `Delta Sigma Pi`
- `Alpha Kappa Psi`
- `Gamma Sigma Alpha`
- `Rho Lambda`
- `Order of Omega`

These rows are still preserved in the canonical roster outputs, but they are ignored when choosing a student's primary chapter, backfilling missing chapter assignments, detecting same-term chapter conflicts, and preferring a chapter for entry-term analytics.

Same-term double-roster cleanup also prefers a non-`Resigned` / non-`Revoked` chapter row over a `Resigned` or `Revoked` row when both appear for the same student and term. This keeps RS/RV legacy rows from driving chapter-level graduation, retention, or GPA analytics for the student's later active organization.

When multiple roster files exist for the same chapter and term, source-file version priority is:

1. regular roster file, meaning the filename does not contain `Revised`, `Updated`, or `Final`
2. `Revised` or `Updated`
3. both `Revised` and `Updated`
4. `Final`

If a student appears only in the regular file, that row is kept. If the same student appears in later revised/updated/final files for the same chapter and term, the later version wins. This preserves students who disappear from later files while still using the most recent available row when present.

When two files are otherwise at the same version level, month names in the filename are used as the next tie-breaker. Month order runs January through December, so a February file outranks a January file, March outranks February, and so on. Files without a month are treated as earlier than files with a month at the same version level.

Folder names are included in this same ranking logic. For example, a regular file inside a folder named `March`, `Updated`, `Revised`, or `Final` inherits that folder's priority when the roster source is ranked.

Roster PDFs are now supported on a best-effort basis when they contain extractable tables that look like the Excel roster files. PDF ingestion uses `pdfplumber`; if a PDF cannot be read as a table, the canonical run records a `roster_pdf_issue` in the exception outputs instead of silently skipping it.

Persistent manual overrides:

- add or edit rows in `config/manual_chapter_assignments.csv`
- the canonical pipeline will reuse those overrides on future runs
- supported columns:
  - `student_id`
  - `first_name`
  - `last_name`
  - `chapter_override`
  - `notes`

Matching priority for manual overrides:

1. exact `student_id`
2. exact `first_name` + `last_name` when no override ID is supplied

The canonical pipeline also writes `unresolved_chapter_review.csv`, which lists uncertain chapter assignments along with the roster files, academic files, and sheets where each student appears so you can review and add a one-time manual override.

## Exception outputs

The canonical run also writes reviewable exception files when applicable:

- `identity_exceptions.csv`
- `term_exceptions.csv`
- `status_exceptions.csv`
- `chapter_conflicts.csv`
- `outcome_exceptions.csv`
- `missing_evidence_cases.csv`
- `unresolved_chapter_review.csv`

If supplemental membership reference workbooks are provided, the canonical run also writes:

- `membership_reference_counts.csv`
- `membership_reference_validation.csv`
- `new_member_reference_values.csv`
- `new_member_reference_validation.csv`

If supplemental GPA reference workbooks are provided, the canonical run also writes:

- `gpa_reference_values.csv`
- `gpa_reference_validation.csv`

If supplemental benchmark GPA workbooks are provided, the canonical run also writes:

- `gpa_benchmark_reference_values.csv`
- `gpa_benchmark_validation.csv`

The canonical pipeline now also scans `data/inbox/reference_data/` as a shared reference-workbook location, so a single workbook can contain:

- chapter membership counts
- chapter new-member counts
- chapter GPA trends
- benchmark GPA trends
- retention reference rows

Additional reference outputs now include:

- `reference_inventory.csv`
- `reference_unclassified_rows.csv`
- `retention_reference_values.csv`

## Legacy scripts

Older builders remain in the repository only for backward compatibility or manual review. They are no longer the analytical source of truth.

In particular:

- `Member_Tenure_Report.xlsx` is no longer an upstream dependency
- `Master_Roster_Grades.xlsx` is no longer the analytical source of truth
- `data/processed/*.csv` is no longer the preferred analytics source once canonical outputs exist

## Setup

Install dependencies with:

```powershell
py -m pip install -r requirements.txt
```

## Run the app

```powershell
py -m streamlit run app/main.py
```

or:

```powershell
py run_local_analytics_app.py
```
