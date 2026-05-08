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
3. Optionally place transcript-style text exports in `data/inbox/transcript_text/`
   These are parsed into transcript term summaries, transcript course detail, and transcript-backed academic term rows.
4. Optionally place graduation lists in `data/inbox/graduation/`
5. Optionally place current one-row snapshot files such as `New Member (1)` in `data/inbox/academic/`
6. Optionally place a single combined workbook such as `Reference Data.xlsx` in `data/inbox/reference_data/`
   The canonical run will scan mixed reference sheets for chapter counts, new-member counts, chapter GPA trends, benchmark GPA trends, and retention-style reference rows.
7. Optionally use the specialized folders instead:
   `data/inbox/membership_reference/`, `data/inbox/gpa_reference/`, and `data/inbox/gpa_benchmark_reference/`
8. Run:

```powershell
py run_canonical_pipeline.py
```

After the canonical bundle exists, use the application for review, filtering, exports, chapter health, advisor queues, and audit tables.

## Helper manual-correction workflow

For people who only need to make manual roster corrections, use the double-click launcher:

- `Start_Manual_Corrections_App.bat`

That launcher opens the app directly in Manual Corrections Mode, skipping the analytics setup screens. Helpers can:

- search for a student by Banner ID, name, or chapter
- edit the nine-column manual correction row
- save corrections to `config/manual_roster_corrections.csv`
- create/open matching transcript paste-in files under `data/inbox/transcript_text/Transcripts/`
- download `manual_corrections_package.zip` to send corrections and transcript text back

Manual Corrections Mode loads only the lightweight correction tables it needs, so helpers do not have to wait for the full analytics interface to initialize.

For the full analytics app, use:

- `Start_FSL_Analytics_App.bat`

Both launchers prefer `uv run --with-requirements requirements.txt ...` when `uv` is installed, then fall back to `.venv`, then `py`.

## Faster reruns

`run_canonical_pipeline.py` now keeps a persistent source cache under `output/canonical/_source_cache/`.

On a normal rerun:

- unchanged roster files reuse cached normalized roster input tables
- unchanged academic files reuse cached normalized academic input tables
- unchanged snapshot, graduation, and reference files do the same

The pipeline now also keeps staged downstream caches for the slowest post-ingest work:

- reference-derivative tables built from `reference_inventory`
- prepared roster / academic source tables after identity resolution, chapter backfill, deduplication, conflict cleanup, and org-entry assignment
- canonical core outputs after longitudinal construction, student summary generation, current-active assignment, outcome classification, and unresolved chapter review

This means unchanged source files no longer force the pipeline to redo the most expensive student-level rebuild steps on every rerun.

Use:

```powershell
py run_canonical_pipeline.py
```

If you changed raw parsing logic and want to force the source files to be re-read, use:

```powershell
py run_canonical_pipeline.py --refresh-source-cache
```

Each canonical run now also writes a small performance report to:

- `output/canonical/run_*/performance_report.csv`
- `output/canonical/run_*/performance_report.parquet`
- `output/canonical/run_*/performance_report.json`
- `output/canonical/latest/performance_report.csv`
- `output/canonical/latest/performance_report.parquet`
- `output/canonical/latest/performance_report.json`

The report records per-stage timing, cache hit/miss status, and key row counts so you can see where the runtime is going and whether cached stages were reused.

Canonical table outputs are now written in both Parquet and CSV form. The app and intermediate pipeline caches prefer Parquet for faster, smaller reads; CSV files remain as compatibility/review exports for people who want to inspect a table directly.

If you only changed app display code, rerun only the app:

```powershell
py run_local_analytics_app.py
```

## App behavior

The local analytics app is preload-only and is expected to load the canonical bundle first.

The app manifest now points to canonical outputs as the preferred prepared dataset source.

Current active membership is now defined separately from historical activeness:

- `Current Active Members (Most Recent Roster)` uses only the single latest roster term in the canonical `roster_term` table.
- A student is current active only if they appear as active or new member on that most recent roster term.
- Older active rows are still kept for historical participation, cohort, retention, graduation, and trend analysis, but they do not roll forward into the present-day active headcount.
- Current chapter headcounts in the app use the chapter assignment from that same most recent roster term, not a student's historical initial chapter.

## Important interpretation rules

- Do not treat first observed organization entry as true school entry.
- Do not treat disappearance as a confirmed negative outcome.
- Do not treat disappearance as graduation.
- Do not calculate long-window graduation rates for non-measurable cohorts.
- Keep unresolved outcomes separate from resolved outcomes.
- The headcount logic is intentionally unchanged by the graduation-outcome correction.
- Current active counts come only from the most recent roster, not from cumulative historical membership.

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

## Graduation evidence rules

Graduation is now evidence-gated. A student is counted as `Graduated` only when the pipeline has a confirmed manual graduation signal from `Copy of Rosters`, such as:

- a roster status explicitly marked as graduated in `Copy of Rosters`

Graduation lists can still be loaded for audit and comparison, but they no longer mark a student as graduated unless `Copy of Rosters` also shows that student as graduated. The pipeline does not treat disappearance, high cumulative hours, good standing, final observed term, or transcript completion history as graduation evidence. If a student disappears without confirmed graduation or another resolved exit, the outcome remains `Truly Unknown / Unresolved`.

Graduation-rate views keep two denominator definitions:

- `Full Population`: unique students in the eligible filtered population
- `Resolved Outcomes Only`: unique students after excluding `Still Active`, `Truly Unknown / Unresolved`, and `Other / Unmapped`

Graduation metrics are calculated at the unique-student level so repeated term rows cannot inflate the numerator or denominator.

## Transcript Text Support

Transcript-style text files are now supported from:

- `data/inbox/transcript_text/`
- app-created manual correction transcript templates in `data/inbox/transcript_text/Transcripts/`

The canonical pipeline scans `.txt` files in that folder and writes:

- `transcript_term_summary.csv`
- `transcript_course_detail.csv`
- `transcript_parse_audit.csv`
- `transcript_parse_issues.csv`

These transcript files are treated as academic evidence only. They can add term GPA, cumulative GPA, academic standing, earned credits, and course detail, but they do not imply graduation unless the text explicitly states graduation.

Supported transcript patterns include:

- term headers such as `Spring 2024`
- course rows with leading credit tokens such as `3 ...` or `0 (3) ...`
- a `Term at a glance:` block
- `Credits`
- `Credit Comp %`
- `Term GPA`
- `Cum GPA`
- `Academic Standing`
- optional transfer markers such as `[TR]`

Summary values can be pasted either on the next line, such as `Credits:` followed by `13`, or on the same line, such as `Credits: 13`.

Student matching for transcript text runs in this order:

1. `config/transcript_text_manifest.csv` exact filename match
2. student ID parsed from filename
3. first/last name parsed from filename
4. unresolved with an audit warning

The transcript manifest template supports:

- `source_file`
- `student_id`
- `first_name`
- `last_name`
- `notes`

Transcript text does not create graduation evidence unless the file explicitly includes a graduation term, graduation flag, or other direct graduation language.
Labels such as `alumni` or historical participation end states are not treated as institutional graduation by themselves.

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

Same-term double-roster cleanup also prefers a non-`Resigned` / non-`Revoked` chapter row over a `Resigned` or `Revoked` row when both appear for the same student and term. This keeps prior RS/RV rows from driving chapter-level graduation, retention, or GPA analytics for the student's later active organization.

When multiple roster files exist for the same chapter and term, source-file version priority is:

1. regular roster file, meaning the filename does not contain `Revised`, `Updated`, or `Final`
2. `Revised` or `Updated`
3. both `Revised` and `Updated`
4. `Final`

If a student appears only in the regular file, that row is kept. If the same student appears in later revised/updated/final files for the same chapter and term, the later version wins. This preserves students who disappear from later files while still using the most recent available row when present.

When two files are otherwise at the same version level, month names in the filename are used as the next tie-breaker. Month order runs January through December, so a February file outranks a January file, March outranks February, and so on. Files without a month are treated as earlier than files with a month at the same version level.

Folder names are included in this same ranking logic. For example, a regular file inside a folder named `March`, `Updated`, `Revised`, or `Final` inherits that folder's priority when the roster source is ranked.

Roster PDFs are now supported on a best-effort basis when they contain extractable tables that look like the Excel roster files. PDF ingestion uses `pdfplumber`; if a PDF cannot be read as a table, the canonical run records a `roster_pdf_issue` in the exception outputs instead of silently skipping it.

Persistent manual chapter overrides:

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

Persistent manual roster corrections:

- use the app's `Manual Corrections` tab, or edit `config/manual_roster_corrections.csv`
- the canonical pipeline reapplies those corrections on future runs, including runs with `--refresh-source-cache`
- raw roster Excel/PDF files are never modified
- supported correction columns:
  - `student_id`
  - `last_name`
  - `first_name`
  - `student_join_term`
  - `organization_join_term`
  - `organization_name`
  - `leaving_organization_term`
  - `final_status_term`
  - `final_status`

Manual roster correction behavior:

1. exact `student_id` match is preferred
2. exact `first_name` + `last_name` is used when no ID is supplied
3. `organization_join_term` anchors the corrected organization-entry term and can create a manual New Member row
4. `organization_name` changes the chapter assignment for matched roster rows
5. `leaving_organization_term` and `final_status_term` mark existing roster rows between those terms as `Unknown`
6. `student_join_term` and `organization_join_term` mark existing roster rows between those terms as `Unknown`; if `student_join_term` is blank, it defaults to `organization_join_term`
7. `final_status_term` and `final_status` can create or update the final manual status row
8. saving correction rows in the app creates missing transcript paste-in templates under `data/inbox/transcript_text/Transcripts/`
9. the app shows an `x` helper column for deleting saved correction rows, but the CSV itself stays in the nine-column format above

## Exception outputs

The canonical run also writes reviewable exception files when applicable:

- `identity_exceptions.csv`
- `term_exceptions.csv`
- `status_exceptions.csv`
- `chapter_conflicts.csv`
- `outcome_exceptions.csv`
- `missing_evidence_cases.csv`
- `unresolved_chapter_review.csv`
- `graduation_status_audit.csv`
- `transcript_term_summary.csv`
- `transcript_course_detail.csv`
- `transcript_parse_audit.csv`
- `transcript_parse_issues.csv`

Each canonical CSV table listed here is also written as a `.parquet` sibling with the same base filename. The app loads the Parquet file when it is available and falls back to CSV for older runs.

`graduation_status_audit.csv` summarizes confirmed graduation evidence, corrected graduation claims, active/unknown/resolved counts, duplicate student checks, and warning checks for suspiciously high graduation rates.

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

## Canonical-only app loading

The application now loads canonical analytics runs only. Older enhanced, current-snapshot, and processed fallback loaders were removed so the app cannot silently switch to a different denominator or graduation-status implementation.

The old standalone spreadsheet builders have been retired in favor of the app views and app export workbook:

- master roster and roster-grade review -> Data & Export filtered student/longitudinal tables
- member tenure report -> Overview, Trends, and Chapter Health cohort views
- chapter history workbooks -> Chapter Health dashboard and current-active audit tables
- full academic record priority list -> Advisor Help intervention queue
- unresolved outcome year report -> Audit tab and unresolved outcome exports
- executive report -> Persistence & Graduation landing page, comparisons, rankings, and app workbook export

The app export workbook automatically splits any table larger than Excel's single-sheet row limit into numbered sheets and includes an `Export Manifest` sheet with row ranges.

`data/processed/*.csv` is not an app source; rebuild through `py run_canonical_pipeline.py`.

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
