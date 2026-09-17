# Read-only office dashboard

Use this option when coworkers only need to view and filter persistence and
graduation rates. Publish one self-contained HTML file to approved shared
storage. Coworkers open it in Edge or Chrome, without installing Python, SQL,
or anything else. No web server, sign-in, or inbound firewall change is needed.

## Publish from the computer with the complete data

1. Save any manual corrections in your existing dashboard.
2. Open PowerShell in your project folder and get the new publisher:

   ```powershell
   git pull
   ```

3. Your previous launch reported a missing zero-member exception file. Restore
   the established Alpha Kappa Alpha exception only if that file is absent:

   ```powershell
   if (-not (Test-Path config\sqlCompile_zero_member_periods.csv)) {
       Copy-Item config\sqlCompile_zero_member_periods.example.csv config\sqlCompile_zero_member_periods.csv
   }
   ```

   Keep any existing exception file and manual decisions. The example contains
   the established Spring 2018 through Fall 2018 exception, not a replacement
   for any additional decisions you have saved.

4. After installing this update, run the compiler once to record skipped-file
   audit information in the database. This retains the initial Fall 2026
   rosters; the reporting cutoff excludes them from published calculations:

   ```powershell
   $env:UV_CACHE_DIR=".uv-cache"
   uv run --with-requirements requirements.txt python sqlCompile.py --all-semesters
   ```

5. Publish and review the printed quality warnings before confirming:

   ```powershell
   .\Publish_Read_Only_Dashboard.bat
   ```

6. The publisher prints the destination. By default it is:

   ```text
   output\sqlCompile\viewer\FSL_Dashboard.html
   ```

7. Put **only this HTML file** in an approved shared SSD/network folder that
   your coworkers can read. Send them the shared file location or a shortcut.
   They do not need the code, roster files, database, or correction files.

The publisher uses your saved sqlCompile database, manual status decisions,
duplicate-name decisions/rechecks, and zero-member exceptions. It respects an
existing `config/sqlCompile_host.json` or `FSL_DASHBOARD_HOST_CONFIG` setting.
It does not recompile Excel rosters or modify the source records. When rosters
change, run `sqlCompile.py --all-semesters` first, as usual.

To use a different existing host configuration or publish directly to a shared
folder, run the equivalent command. Replace the example destination with your
actual approved shared location:

```powershell
$env:UV_CACHE_DIR=".uv-cache"
uv run --with-requirements requirements.txt python publish_sql_compile_viewer.py --output "S:\FSL\FSL_Dashboard.html"
```

Add `--host-config "path\to\host.json"` only if using a non-default host
configuration. This is the same configuration format as the shared dashboard.

## Reporting cutoff and year definitions

The owner confirmed **Spring 2026** as the latest complete roster set. Fall
2026 currently has initial rosters only. The editable dashboard and publisher
therefore start with Spring 2026 from `config/sqlCompile_reporting.example.json`.
An owner-saved `config/sqlCompile_reporting.json` takes precedence and is not
overwritten by Git updates. A custom host can set `reporting_settings` to a
different settings file, which must exist.

Both dashboards say **Years since joining FSL**. These are semester-based
checkpoints from the FSL join cohort, not years since enrolling at TXST. The
existing checkpoint convention is unchanged: Year 1 for Fall 2021 joins is
Spring 2022; Year 1 for Spring 2021 joins is also Spring 2022. The counts
below the bars now explicitly identify **eligible students**. As before, Year
1 includes selected new members through the cutoff even if their first year
is still in progress. Years 2-6 require reaching their complete checkpoint.

Roster rows, chapter inventory, and status corrections dated after the cutoff
are excluded before calculating cohort outcomes. Later records stay in the
database and correction files; nothing is deleted. New-member cohorts after
the cutoff are not offered in the report's semester filters. Future-dated
corrections cannot make later milestones measurable.

When a later semester is complete, the owner can change **Reporting cutoff**
in the editable dashboard sidebar and confirm roster completeness. This
control is not available in shared mode or the offline viewer. Alternatively,
after verifying completeness, explicitly save it while publishing:

```powershell
uv run --with-requirements requirements.txt python publish_sql_compile_viewer.py --cutoff "Fall 2026"
```

Do not run that example until Fall 2026 is complete. The setting will not
advance automatically. A cutoff with no compiled roster records is rejected;
having some records alone does not prove semester completeness. Refresh any
other open editable dashboard sessions after changing the setting.

## Owner's pre-publication check

The check runs during publication, not during coworker filtering. It reports:

- Skipped sheet/file issues from the last compilation. Older databases without
  this audit are labeled **unavailable**, never treated as zero issues. The
  compiler stores details in the owner's SQLite `sqlCompile_issues` table.
- Unresolved observed name mismatches and saved name rechecks. These warnings
  do not change outcome buckets or the existing Manual Checker logic.
- Unrecognized semesters and missing chapter names.
- Potential Spring/Fall coverage gaps for previously observed chapters,
  excluding configured zero-member periods. Actual chapter departures can
  legitimately cause these gaps; the check does not establish whether a
  chapter was kicked off campus or verify chapters absent from all sources.
- Changes of at least 10% in existing semester/chapter cohort sizes or the
  total cohort, and at least 5 percentage points in milestone outcome shares,
  compared with the HTML file being replaced. Advancing the cutoff can cause
  legitimate changes because eligible denominators may differ.
- Counts of later roster and manual rows held back by the cutoff.

The detailed JSON report is saved in `publication_checks` beside the owner's
SQLite database, not alongside a separately located shared HTML file. Share
only the HTML. Keep the database folder restricted to the appropriate staff.

Warnings pause publication and leave the prior viewer intact. An interactive
terminal asks for confirmation after displaying them. Noninteractive runs
stop unless the owner explicitly supplies `--allow-warnings` after review.
Missing source files, invalid cutoffs, or concurrent changes cannot be
overridden with that flag.

To run the check without replacing the office viewer:

```powershell
.\Publish_Read_Only_Dashboard.bat --check-only
```

Use the same `--output` destination for successive publications so the check
can compare against the last published file. A new destination has no prior
baseline; an unrecognized existing HTML file produces a warning instead of
being silently accepted as a comparison.

## Coworker use

Double-click `FSL_Dashboard.html`, or choose **Open with > Microsoft Edge** or
**Google Chrome**. Select any join semesters, chapters, and years 1-6. The
chart can also compare semesters or chapters at one selected year. Each
person's selections are independent and never change anyone else's view.

Future milestones, cohort counts, and eligible-student denominators follow the
existing P&G dashboard calculations. With mixed-age cohorts, future students
are excluded from that year's rate denominator and counted below the bar. A
wholly future group is gray. The optional chart-data table only reflects the
visible chart; there are no student-level lists or editing controls.

## Refreshing the data

This is a **published snapshot**, not a live website. After saving corrections
or compiling new rosters, publish again and replace the shared HTML file.
Coworkers must reopen the file to see the replacement. A previously open tab
keeps its old snapshot until reopened.

The header shows publication time and **Data complete through** the explicit
reporting cutoff. Publishing again does not advance that cutoff.
Your computer and Streamlit window do not need to remain running after the
file has been published to storage that stays available to coworkers.

## Privacy and troubleshooting

- Only aggregated semester/chapter/year/outcome counts are embedded. Student
  IDs, names, manual notes, and raw records are not included. Small groups can
  still reveal sensitive information, so restrict the shared folder to the
  appropriate office audience and do not post the file publicly.
- The file includes its chart library and makes no external network requests.
  Everyone with a copy can inspect its aggregate counts; read-only means it
  cannot edit your authoritative records, not that the HTML is tamper-proof.
- If a required source file is missing, restore the existing file from your
  complete data copy. Do not substitute empty manual-decision files just to
  make publication succeed. Missing optional name-review ledgers remain
  absent; publishing does not create them.
- If records change during publication or the destination is locked, the
  publisher leaves the prior publication intact. Finish saving or close the
  program holding the destination, then publish again.
- A file preview in email or a shared-drive website may not execute the
  dashboard. Open the actual HTML file in a browser. If institutional policy
  blocks local HTML/JavaScript, use an approved IT-provided option; this
  publisher does not disable or circumvent those controls.
- This does not make the blocked `http://147.26.114.103:8502` address reachable.
  It removes the need for coworkers to connect to that address at all.
