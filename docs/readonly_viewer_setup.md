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

4. Publish:

   ```powershell
   .\Publish_Read_Only_Dashboard.bat
   ```

5. The publisher prints the destination. By default it is:

   ```text
   output\sqlCompile\viewer\FSL_Dashboard.html
   ```

6. Put **only this HTML file** in an approved shared SSD/network folder that
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

The header shows both publication time and the latest semester in the source
timeline. Publishing again does not itself add a new semester of observations.
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
