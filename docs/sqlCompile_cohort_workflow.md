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
