# FSL Academic Analytics

This repository now centers on a single canonical analytics architecture for Fraternity / Sorority Life academic reporting.

## Canonical source of truth

All future analytics are expected to flow from the canonical authoritative tables:

- `roster_term`
- `academic_term`
- `master_longitudinal`
- `student_summary`
- `cohort_status_over_time`
- `cohort_metrics`
- `qa_checks`

These are produced by:

```powershell
py run_canonical_pipeline.py --config config\local_paths.yaml
```

The output is written to:

- `output/canonical/latest/`

By default, the pipeline refreshes only `output/canonical/latest/` for the app.
Use `--archive-run` only when you intentionally want an additional timestamped
`output/canonical/run_*/` snapshot.

## Focused graduation-rate workflow

When the only goal is conservative 4-year, 5-year, and 6-year graduation rates,
use the focused graduation pipeline instead of the broader analytics app:

```powershell
uv run --with-requirements requirements.txt python src\graduation_pipeline\run_pipeline.py --config config\local_paths.yaml
```

This pipeline only tracks valid Banner IDs matching `^A0\d{7}$`. Rows without a
valid A# are written to `output/graduation/invalid_ids.csv` and excluded from
graduation-rate calculations.

Graduation is counted only from explicit evidence. Roster graduation status
(`G`, `Grad`, or `Graduated`) is checked first, then explicit transcript
graduation evidence, then graduation files, then explicit academic graduation
fields. Disappearance, inactivity, GPA, hours, senior standing, or lack of
future records are not used to infer graduation.

Manual corrections live at `data/manual/manual_corrections.csv`. The file is
created if missing and is never overwritten by a pipeline run. Blank rows and
invalid IDs in that file are ignored so the correction workflow does not get
bogged down.

Focused outputs are written to `output/graduation/`:

- `source_manifest.csv`
- `raw_required_fields.csv` and `.parquet` when supported
- `normalized_required_fields.csv` and `.parquet` when supported
- `invalid_ids.csv`
- `student_membership_summary.csv`
- `graduation_evidence.csv`
- `manual_review_queue.csv`
- `manual_corrections_applied.csv`
- `correction_audit.csv`
- `final_student_outcomes.csv`
- `graduation_rates_by_cohort.csv`
- `graduation_rates_by_chapter.csv`
- `graduation_rates_by_council.csv`
- `graduation_rate_qa_summary.csv`

## Canonical workflow

Use this order when rebuilding from source files. Raw student files should live
outside the Git checkout, usually on the shared/community drive, and the local
repo should point to them through `config/local_paths.yaml`.

1. Copy `config/example_paths.yaml` to `config/local_paths.yaml`.
2. Edit `config/local_paths.yaml` so `raw_data_root` points at the shared-drive source-data folder.
3. Keep roster files, grade reports, graduation lists, reference workbooks, and transcript text under that shared raw-data root.
4. Optionally place transcript-style text exports in the configured `transcript_text_root`.
   These are parsed into transcript term summaries, transcript course detail, and transcript-backed academic term rows.
5. Optionally place current one-row snapshot files such as `New Member (1)` in the configured grade-report or snapshot folder.
6. Optionally place a single combined workbook such as `Reference Data.xlsx` in the configured reference folder.
   The canonical run will scan mixed reference sheets for chapter counts, new-member counts, chapter GPA trends, benchmark GPA trends, and retention-style reference rows.
7. Optionally use the specialized folders instead:
   `membership_reference_root`, `gpa_reference_root`, and `gpa_benchmark_root`.
8. Run:

```powershell
py run_canonical_pipeline.py --config config\local_paths.yaml
```

After the canonical bundle exists, use the application for review, filtering, exports, chapter health, advisor queues, and audit tables.

To build a semester-by-semester chapter presence list for reviewing possible chapter kicks / returns, run:

```powershell
$env:UV_CACHE_DIR = ".uv-cache"
uv run --no-sync python scripts\build_chapter_semester_inventory.py --config config\local_paths.yaml
```

This writes these lightweight review files next to the canonical `roster_term.csv`:

- `chapter_semester_inventory.csv` lists every observed chapter alphabetically within each semester, with valid-Banner-ID roster counts and status counts.
- `chapter_semester_matrix.csv` lists one row per chapter and one column per semester, with blanks showing terms where that chapter was not present.
- `chapter_lifecycle_review_template.csv` summarizes first/last seen terms and possible gaps, with blank kicked/returned/note columns for review.
- `chapter_status_event_candidates.csv` lists each possible roster gap or disappearance window. These gaps can classify otherwise unresolved students as inferred `Chapter Kicked` outcomes.

Confirmed chapter removals/returns should go in `config/chapter_status_events.csv` using the columns in `config/chapter_status_events.example.csv`. Rows with `event_type` like `Chapter Kicked`, `confidence` set to `Confirmed`, and `active` not set to `No` make the `Chapter Kicked` evidence explicit; roster gaps remain usable as inferred chapter-kick evidence when no later student roster appearance is found.

## Helper manual-correction workflow

For people who only need to make manual roster corrections, use the double-click launcher:

- `Start_Manual_Corrections_App.bat`

That launcher opens the app directly in Manual Corrections Mode, skipping the analytics setup screens. Helpers can:

- work from an Assignment Queue of unresolved, unknown, inferred, or incomplete records
- claim rows with helper initials and set review status / notes
- check multiple Assignment Queue rows and use one final-status button for common outcomes such as Inactive, Resigned, Revoked, Suspended, Unknown, or Graduated
- search for a student by Banner ID, name, or chapter
- edit or stage manual correction rows
- save corrections to `config/manual_roster_corrections.csv`
- track assignment progress in `config/manual_review_queue.csv`
- create/open matching transcript paste-in files under the configured `transcript_text_root/Transcripts/`
- download `manual_corrections_package.zip` to send corrections, queue progress, and transcript text back
- import returned helper packages and review duplicate/conflicting corrections before the next canonical rebuild

Manual Corrections Mode loads only the lightweight correction tables it needs, so helpers do not have to wait for the full analytics interface to initialize.

For the full analytics app, use:

- `Start_FSL_Analytics_App.bat`

Both launchers prefer `uv run --with-requirements requirements.txt ...` when `uv` is installed, then fall back to `.venv`, then `py`.

## Recommended Windows/shared-drive workflow

The safest setup is to keep Git and raw data separate:

- Work from a short local repo path on each computer, for example `C:\FSL`.
- Keep the raw FSL student data on the community/shared drive.
- Use GitHub to sync code, config templates, tests, and documentation.
- Do not commit roster PDFs, roster spreadsheets, grade reports, graduation files, exports, generated outputs, caches, or student records.
- Create `config/local_paths.yaml` locally on each computer. This file is ignored by Git.
- Point `raw_data_root` and the optional source-folder roots in `config/local_paths.yaml` to the shared-drive raw-data location.
- Run `git config --global core.longpaths true` once on each Windows computer.

First-time setup:

```powershell
git clone <repo-url> C:\FSL
cd C:\FSL
git config --global core.longpaths true
copy config\example_paths.yaml config\local_paths.yaml
notepad config\local_paths.yaml
py run_canonical_pipeline.py --config config\local_paths.yaml
```

You can also set `FSL_PATH_CONFIG` instead of passing `--config`:

```powershell
$env:FSL_PATH_CONFIG = "C:\FSL\config\local_paths.yaml"
py run_canonical_pipeline.py --config config\local_paths.yaml
```

To check whether the repo is clean of raw/private files, run:

```powershell
py scripts\check_repo_hygiene.py --config config\local_paths.yaml
```

If old raw folders are already tracked by Git, do not delete them from disk.
Untrack them after reviewing the `.gitignore` changes:

```powershell
git rm -r --cached "Copy of Rosters"
git rm -r --cached "Rosters"
git rm -r --cached "data/inbox"
git rm -r --cached "output"
```

Then commit only the code/config-template changes. Raw files stay where they
are on the shared drive.

## Banner ID Request Batches

Use `scripts/build_banner_id_batches.py` when you need request lists for future
Academic Reports. The script scans configured source folders and existing
canonical outputs, finds possible Banner ID columns, keeps only strict Banner
IDs matching `^A0\d{7}$`, deduplicates them, sorts them, and writes request
batches of 999 IDs.

Run:

```powershell
uv run --with-requirements requirements.txt python scripts\build_banner_id_batches.py --config config\local_paths.yaml
```

Useful options:

```powershell
uv run --with-requirements requirements.txt python scripts\build_banner_id_batches.py --batch-size 999
uv run --with-requirements requirements.txt python scripts\build_banner_id_batches.py --include-raw
uv run --with-requirements requirements.txt python scripts\build_banner_id_batches.py --include-canonical
uv run --with-requirements requirements.txt python scripts\build_banner_id_batches.py --dry-run --verbose
```

If neither `--include-raw` nor `--include-canonical` is passed, the script scans
both configured raw/source roots and canonical output folders. Supported source
files are `.csv`, `.xlsx`, `.xls`, and `.xlsm`; temporary Excel lock files such
as `~$file.xlsx` are skipped.

Outputs are written to:

- `data/outgoing/banner_id_batches/banner_ids_master.csv`
- `data/outgoing/banner_id_batches/requested_banner_ids_master.csv`
- `data/outgoing/banner_id_batches/banner_ids_batch_001.csv`
- `data/outgoing/banner_id_batches/banner_ids_batch_001.txt`
- `data/outgoing/banner_id_batches/rejected_banner_id_values.csv`
- `data/outgoing/banner_id_batches/banner_id_batch_summary.json`
- `data/outgoing/banner_id_batches/banner_id_batch_summary.csv`
- placeholder future comparison files for returned Academic Reports
- `data/outgoing/banner_id_batches/manual_review_candidates.csv`

Each batch CSV has exactly one column, `Banner ID`; each matching TXT file has
one Banner ID per line with no header for copy/paste request workflows.

The current files under `data/inbox/academic` are examples of what returned
Academic Reports may look like later. Blank early academic values are expected:
they may simply mean a student had not started at the university yet. Future
report comparison should use `requested_banner_ids_master.csv` to identify
returned IDs, missing IDs, first academic appearance, last academic appearance,
and stale unchanged academic records. Stale records should be flagged for review
and must not be treated as graduation evidence unless a source explicitly says
the student graduated.

Academic report ingestion also respects the LOGI count-control notes used in
the grade reports. When column K contains or has an Excel note/comment saying
`Counted`, the GPA/hour values are treated as valid. When column K says
`Not Counted` or `Last Semester`, the row is kept for evidence but the
GPA/hour values are blanked so they do not affect GPA averages or counted GPA
totals. When column K says `Not a student`, the row is excluded from the
academic load.

For Spring 2026, the supported incoming grade-report layout is:

```text
data\inbox\academic\Spring 2026\
  IFC Raw Data\
  PHC Raw Data\
  MGC Raw Data\
  NPHC Raw Data\
```

Each council folder can contain the chapter Excel files. Expected columns match
the LOGI-style raw data export, including `Last Name`, `First Name`, `Banner ID`,
`Email`, `Student Status`, `Major`, `Current Academic Standing`, `Term GPA`,
`Term Passed Hours`, and `TxState Cumulative GPA`. These files are treated as
real grade reports, not as NetID-only raw data.

## Community and Chapter Grade Reports

After adding new LOGI / grade reports and updated rosters, run the canonical
pipeline first so the report builder can use the normalized `academic_term`,
`roster_term`, and `master_longitudinal` tables:

```powershell
uv run --with-requirements requirements.txt python run_canonical_pipeline.py --config config\local_paths.yaml --refresh-source-cache
```

Then build the grade-report workbooks:

```powershell
uv run --with-requirements requirements.txt python scripts\build_grade_reports.py --config config\local_paths.yaml --term "Spring 2025"
```

The report builder writes a community workbook and one chapter workbook per
chapter to:

```text
data/outgoing/grade_reports/<term>/
```

Generated files include:

- `community_grade_report_<term>.xlsx`
- `community_grade_summary_<term>.csv`
- `chapter_reports/<chapter>_grade_report_<term>.xlsx`

The community workbook follows the council summary style of the FSL Community
Grade Report: separate council sheets, new-member GPA/count, initiated-member
GPA/count, overall chapter GPA/count, and previous-term GPA change where prior
term data exists. The chapter workbooks follow the chapter report style:
member detail sections for active members, new members, and members not
enrolled / GPA not counted, plus a summary block with active/new/chapter
averages and membership numbers.

These files are generated outputs and are ignored by Git. If PDFs are needed,
open the workbook in Excel and use `File > Export > Create PDF/XPS` or
`Save As > PDF`.

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
py run_canonical_pipeline.py --config config\local_paths.yaml
```

If you changed raw parsing logic and want to force the source files to be re-read, use:

```powershell
py run_canonical_pipeline.py --config config\local_paths.yaml --refresh-source-cache
```

Each canonical run writes a small performance report to:

- `output/canonical/latest/performance_report.csv`
- `output/canonical/latest/performance_report.parquet`
- `output/canonical/latest/performance_report.json`

If you pass `--archive-run`, the same performance report is also written to the
timestamped `output/canonical/run_*/` snapshot.

The report records per-stage timing, cache hit/miss status, and key row counts so you can see where the runtime is going and whether cached stages were reused.

Canonical table outputs are now written in both Parquet and CSV form. The app and intermediate pipeline caches prefer Parquet for faster, smaller reads; CSV files remain as compatibility/review exports for people who want to inspect a table directly.

If you only changed app display code, rerun only the app:

```powershell
py run_local_analytics_app.py
```

## App behavior

The local analytics app is preload-only and is expected to load the canonical bundle first.

The app manifest now points to canonical outputs as the preferred prepared dataset source.

Recent app additions:

- `Persistence & Graduation` includes a graduation-rate denominator toggle for the selected cohort/council view, with `Resolved Outcomes Only`, `Full Population`, and side-by-side tables.
- `Retention & GPA` separates next-fall organization retention from academic continuation and shows GPA trend coverage so incomplete grade files stay visible.
- `Roster Disappearances` isolates students whose chapter roster coverage disappeared, including resolved `Chapter Kicked` outcomes and unresolved `Roster Dissapeared/Unknown` cases, with chapter rollups, join-term timing, last-observed timing, and downloadable student detail.
- `Chapter Health` keeps roster-disappeared unknowns visible beside resolved graduation rates so disappeared rosters do not silently become graduations.

Current active membership is now defined separately from historical activeness:

- `Current Active Members (Latest Full Roster Marker)` uses the term configured as `latest_full_roster_term` in `config/app_settings.json`; if that term is blank or unavailable, it falls back to the latest roster term in the canonical `roster_term` table.
- Update `latest_full_roster_term` each semester after the full roster set is loaded. For example, keep it as `Spring 2026` until Fall 2026 rosters are complete.
- A student is current active only if they appear as active or new member on that marker term.
- Older active rows are still kept for historical participation, cohort, retention, graduation, and trend analysis, but they do not roll forward into the present-day active headcount.
- Current chapter headcounts in the app use the chapter assignment from that same marker term, not a student's historical initial chapter.

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
The app-level denominator toggle does not change the underlying classifications; it only changes which denominator is highlighted for display.

Retention-focused views also keep denominator definitions explicit:

- `Organization Retention Rate`: students retained on a roster at the next-fall checkpoint divided by students with a measurable next-fall roster checkpoint.
- `Academic Continuation Rate`: students with academic evidence at the next-fall checkpoint divided by students with a measurable next-fall academic checkpoint.

GPA trend views show coverage as `students with term GPA / roster students` for each term/segment so missing grade files do not silently bias the average.

## Graduation evidence rules

Graduation is now evidence-gated. A student is counted as `Graduated` only when the pipeline has a confirmed manual graduation signal from the configured roster source, historically `Copy of Rosters`, such as:

- a roster status explicitly marked as graduated in the configured roster source

Graduation lists can still be loaded for audit and comparison, but they no longer mark a student as graduated unless the configured roster source also shows that student as graduated. The pipeline does not treat disappearance, high cumulative hours, good standing, final observed term, or transcript completion history as graduation evidence. If a student disappears without confirmed graduation or another resolved exit, the outcome remains `Truly Unknown / Unresolved`.

Graduation-rate views keep two denominator definitions:

- `Full Population`: unique students in the eligible filtered population
- `Resolved Outcomes Only`: unique students after excluding `Still Active`, `Truly Unknown / Unresolved`, and `Other / Unmapped`

Graduation metrics are calculated at the unique-student level so repeated term rows cannot inflate the numerator or denominator.

## Transcript Text Support

Transcript-style text files are now supported from:

- configured `transcript_text_root`
- app-created manual correction transcript templates in `transcript_text_root/Transcripts/`

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
  - `organization_join_term`
  - `organization_name`
  - `corrected_organization_name`
  - `leaving_organization_term`
  - `final_status_term`
  - `final_status`
  - `exclude_from_roster_calculations`

Manual roster correction behavior:

1. exact `student_id` match is preferred
2. exact `first_name` + `last_name` is used when no ID is supplied
3. `organization_join_term` anchors the corrected organization-entry term and can create a manual New Member row
4. `organization_name` can identify the existing/wrong chapter to match; if `corrected_organization_name` is blank, it keeps the older behavior and also acts as the corrected chapter
5. `corrected_organization_name` changes the chapter assignment for matched roster rows
6. `leaving_organization_term` and `final_status_term` mark existing roster rows between those terms as `Unknown`
7. `final_status_term` and `final_status` can create or update the final manual status row
8. the app can stage correction rows in memory for fast bulk cleanup, then commit them to `config/manual_roster_corrections.csv` all at once
9. saving or committing correction rows in the app creates missing transcript paste-in templates under the configured `transcript_text_root/Transcripts/`
10. the app shows an `x` helper column for deleting saved or staged correction rows, but the `x` column itself is not written to the CSV
11. `exclude_from_roster_calculations` removes matching raw roster rows from canonical roster-based calculations without modifying the raw source files

Roster exclusion behavior:

1. set `exclude_from_roster_calculations` to `Yes`, `True`, `1`, `Remove`, `Delete`, or `Exclude`
2. match is still by exact `student_id` first, or exact `first_name` + `last_name` if no ID is supplied
3. if `organization_name` is supplied, only matching rows for that organization are removed
4. if `organization_join_term` and `final_status_term` or `leaving_organization_term` are supplied, the inclusive term range is removed
5. if only `organization_join_term` is supplied, only that term is removed
6. if no terms are supplied, all matching roster rows for that student/org are removed
7. the app's `x` column is different: it deletes the correction row itself, not the underlying roster record

Manual helper queue behavior:

1. `config/manual_review_queue.csv` stores helper ownership, review status, transcript-needed flags, and notes
2. the queue is generated from current canonical records with unknown outcomes, unresolved/inferred chapters, low data completeness, or missing IDs
3. saved/imported queue rows are preserved even if a later canonical run no longer auto-generates that exact queue item
4. helper packages include `manual_roster_corrections.csv`, `manual_review_queue.csv`, and transcript `.txt` files
5. importing a returned helper package merges correction rows, imports transcript files without overwriting different existing text, and flags duplicate/conflicting corrections
6. the default correction mode is **Stage changes (fast batch)**, which keeps pending rows in the current app session until **Commit staged changes** is clicked
7. staged rows are not included in canonical rebuilds or downloaded helper packages until they are committed to the CSV
8. the Assignment Queue supports batch selection: check visible rows, click one outcome/exclusion button, and the app stages or saves the selected corrections with one normalized transfer

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

The canonical pipeline now also scans the configured `reference_root` as a shared reference-workbook location, so a single workbook can contain:

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
