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

## Downstream exports

These scripts now read canonical outputs instead of using old report files as upstream inputs:

- `py run_master_roster_grades.py`
- `py run_member_tenure_report.py`
- `py run_yearly_chapter_rosters.py`
- `py run_chapter_history_workbooks.py`
- `py run_full_record_priority_list.py`
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

## Exception outputs

The canonical run also writes reviewable exception files when applicable:

- `identity_exceptions.csv`
- `term_exceptions.csv`
- `status_exceptions.csv`
- `chapter_conflicts.csv`
- `outcome_exceptions.csv`
- `missing_evidence_cases.csv`

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
