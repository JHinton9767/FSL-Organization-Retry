# sqlCompile New-Member Cohort Workflow

Run these commands from the project root in PowerShell.

## 1. Build the base SQLite file

```powershell
$env:UV_CACHE_DIR=".uv-cache"
uv run --with-requirements requirements.txt python sqlCompile.py
```

This writes:

```text
output\sqlCompile\sqlCompile.sqlite
```

The base table is named `sqlCompile` and has these columns:

```text
Semester | Chapter | Student ID | Status
```

When the same student appears in multiple rosters for the same semester, `sqlCompile` applies the conflict rules before writing the one semester row:

- If `A` or `N` appears with any status outside `A`/`N`, the outside status wins.
- If the only conflict is `A` versus `N`, `N` wins.
- If statuses are in the same class, roster pass order is used as the tie-breaker: Initial first, Updated/Revised after, Final last.

Fresh `sqlCompile` runs also write a side table named `sqlCompile_roster_inventory`. The main `sqlCompile` table still keeps only the four columns above, but the inventory table lets cohort reports detect chapter-wide roster disappearance.

Fresh runs also write a side table named `sqlCompile_student_names`. The main `sqlCompile` table still keeps only the four requested columns, but the dashboard Manual Checker uses the name lookup to show `Student Name` after `Student ID`.

When a chapter appears in earlier roster coverage but not in a later roster pass or later semester while other chapter rosters continue, active/new-member students whose last roster row was with that chapter are resolved as:

```text
Chapter Kicked
```

Those students are counted as known non-graduate exits and appear in the dashboard Manual Checker under the `Chapter Kicked` last-known outcome bucket. Students who disappear individually while their chapter continues still go to manual review.

If a chapter was still recognized but had zero active members, add that period to:

```text
config\sqlCompile_zero_member_periods.csv
```

Those rows prevent roster-gap inference from marking the chapter as `Chapter Kicked`. The current file includes Alpha Kappa Alpha for `Spring 2018` through `Fall 2018`.

## 2. Build a new-member cohort report

Use the semester where the students were listed as `Status = N`.

```powershell
uv run --with-requirements requirements.txt python sqlCompileCohort.py --cohort-semester "Fall 2025"
```

You can also compile and report in one command:

```powershell
uv run --with-requirements requirements.txt python sqlCompile.py --cohort-semester "Fall 2025"
```

To build the same report across every semester that has new members:

```powershell
uv run --with-requirements requirements.txt python sqlCompileCohort.py --all-semesters
```

Or compile the roster database and build every cohort report in one command:

```powershell
uv run --with-requirements requirements.txt python sqlCompile.py --all-semesters
```

The all-semester run writes combined CSVs under:

```text
output\sqlCompile\cohorts\all_new_member_cohorts\
```

The report writes CSVs under:

```text
output\sqlCompile\cohorts\fall_2025\
```

It also writes these tables into `sqlCompile.sqlite`:

```text
new_member_timeline
new_member_outcomes
new_member_form_review
new_member_rate_summary
```

## 3. Review last-known student outcomes

Open:

```text
output\sqlCompile\cohorts\fall_2025\new_member_form_review.csv
```

These are students whose last known status is `A`, which means they disappeared without a resolved final status.

The dashboard also has a **Manual Checker** section for every selected cohort student. It supports searching, cohort/chapter filtering, last-seen semester filtering, P&G last-known outcome bucket filtering, paged row selection, batch status entry, copying the last known semester/chapter into selected rows, previewing completed selected rows, and saving completed decisions directly to the manual status CSV.

For speed, the Manual Checker grid is edited inside a form. Check or edit the visible rows, then click **Update Page Edits** once before using **Apply to Selected** or **Save Selected**. Manual saves do not recalculate the full dashboard immediately. Click **Refresh Dashboard Data** when you want saved statuses reflected in the P&G rates and saved/manual flags.

If you already completed manual checks in the older dashboard, use **Reuse Legacy Manual Decisions** in the Manual Checker section or run a preview first:

```powershell
uv run --with-requirements requirements.txt python import_legacy_manual_to_sql_compile.py --legacy-path . --preview-output output\sqlCompile\legacy_manual_import_preview.csv --dry-run
```

Point `--legacy-path` at the old dashboard project root, its `config` folder, the canonical output folder, or one exported manual-check CSV/XLSX file. The importer reads completed outcome-style decisions from `manual_roster_corrections.csv`, `graduation_evidence.csv`, `outcome_overrides.csv`, `manual_adjustments.csv`, `manual_review_queue.csv`, and `manual_review_actions.csv` or `manual_review_actions.pending_*.csv`. It can also auto-detect exported files with names like `Manual checks form.csv`.

After the preview count looks right, append the translated rows into the new sqlCompile manual status file:

```powershell
uv run --with-requirements requirements.txt python import_legacy_manual_to_sql_compile.py --legacy-path . --manual-status-file config\sqlCompile_manual_status.csv
```

## 4. Add manually researched form rows

Add verified form/status rows to:

```text
config\sqlCompile_manual_status.csv
```

If the file does not exist, the cohort command creates it with this header:

```text
Cohort Semester,Cohort Chapter,Semester,Chapter,Student ID,Status,Notes
```

Example manual row:

```text
Fall 2025,Alpha Sigma Phi,Spring 2026,Alpha Sigma Phi,A01234567,RS,Form found in chapter folder.
```

Then rerun:

```powershell
uv run --with-requirements requirements.txt python sqlCompileCohort.py --cohort-semester "Fall 2025"
```

## 5. Use the rate summary

Open:

```text
output\sqlCompile\cohorts\fall_2025\new_member_rate_summary.csv
```

Rates use the resolved denominator, meaning students still in `new_member_form_review.csv` are excluded from rate calculations until a manual status row is added.

The dashboard milestone bars use milestone eligibility. The 1 Year bar includes every selected new-member cohort. Later bars include only cohorts old enough to be measured at that milestone, so recent joiners are not counted in 6 Year results. Outcome buckets carry forward, so a student resolved as resigned, dropped, graduated, chapter kicked, etc. remains in that bucket in later eligible milestone bars unless a later terminal outcome supersedes it.

## 6. Open the new dashboard

The smaller sqlCompile dashboard shows only persistence rates, graduation rates, outcome mix, and the manual checker.

```powershell
uv run --with-requirements requirements.txt python run_sql_compile_dashboard.py
```

If the main dashboard is already using the default Streamlit port, choose another port:

```powershell
uv run --with-requirements requirements.txt python run_sql_compile_dashboard.py --server.port 8502
```
