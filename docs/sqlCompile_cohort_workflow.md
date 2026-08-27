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

When a chapter appears in earlier roster coverage but not in a later roster pass or later semester while other chapter rosters continue, active/new-member students whose last roster row was with that chapter are resolved as:

```text
Chapter Kicked
```

Those students are removed from the manual checker and counted as known non-graduate exits. Students who disappear individually while their chapter continues still go to manual review.

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

## 3. Review disappeared students

Open:

```text
output\sqlCompile\cohorts\fall_2025\new_member_form_review.csv
```

These are students whose last known status is `A`, which means they disappeared without a resolved final status.

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

## 6. Open the new dashboard

The smaller sqlCompile dashboard shows only persistence rates, graduation rates, outcome mix, and the manual checker.

```powershell
uv run --with-requirements requirements.txt python run_sql_compile_dashboard.py
```

If the main dashboard is already using the default Streamlit port, choose another port:

```powershell
uv run --with-requirements requirements.txt python run_sql_compile_dashboard.py --server.port 8502
```
