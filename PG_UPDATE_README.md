# Update and run the P&G dashboard on the office computer

The update is delivered through the project's Git repository on `master`.
It contains application code, documentation, tests, and empty example templates.
It does not include office student records, working databases, saved manual
decisions, active local configuration, or the published dashboard.

1. Close the editable dashboard on the office computer.
2. Make a copy of the existing project folder using your approved office backup
   location. Keep the separate original roster archive as well.
3. Open PowerShell in the existing project folder and pull the update:

   ```powershell
   git pull --ff-only origin master
   ```

   If Git reports local changes or a diverged branch, preserve those changes
   and resolve the conflict before continuing. Do not force a reset or delete
   the existing project or its `config`, `data`, or `output` folders.
4. For the November 6 chart repair, start **Start_PG_Dashboard.bat**. The default
   measure is **Persistence and graduation**, with the full set of roster and
   manually corrected outcome categories. A code-only update does not require
   recompiling the rosters. Open http://localhost:8502 if the browser does not open.
5. When roster files have changed, or the office baseline has not yet been
   verified, close the dashboard and run **Verify_PG_Records.bat** first. It
   takes a dated backup, imports the rosters, and runs the accuracy checks.
   Then start the dashboard and review **Record accuracy** using the
   [maintenance guide](docs/pg_record_accuracy.md).
6. When the office verification is complete, run
   **Publish_Read_Only_Dashboard.bat**, review the report, and share the resulting
   aggregate HTML using the existing office process.

The editable dashboard keeps its terminal running. A publication command pasted
after the launch command will wait until the server stops. Publish from a
second PowerShell window in the project folder, or stop the server with Ctrl+C.

## Everyday commands

Start editing or viewing existing records:

```powershell
.\Start_PG_Dashboard.bat
```

After roster additions or changes, with the source drive connected:

```powershell
.\Verify_PG_Records.bat
.\Start_PG_Dashboard.bat
```

After reviewing and saving corrections, update the coworker snapshot:

```powershell
.\Publish_Read_Only_Dashboard.bat
```

The publisher checks the saved records and prints the HTML destination. Review
its warnings before confirming publication. Pulling code alone does not verify
the office records or replace the published snapshot.

**Graduation after joining FSL** remains an optional degree-only analysis. Its
Unknown category means degree evidence is missing or conflicting, even when
a chapter status is known. **FSL membership** and **University enrollment** are
also optional separate analyses. The combined P&G view is the default in both
the editable dashboard and newly published viewers.

Recompilation now preserves the database if a previously imported roster is
missing, unreadable, or emptied. Restore the source files and retry. Only for a
deliberate removal, after checking the dated backup, use
`Verify_PG_Records.bat --allow-source-removals`. This cannot bypass unreadable
workbooks, an entirely empty import, or publication accuracy errors.
