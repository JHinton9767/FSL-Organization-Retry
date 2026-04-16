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
5. Optionally place chapter-by-term membership reference workbooks in `data/inbox/membership_reference/`
6. Run:

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
