# Install the P&G accuracy update on the office computer

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
4. Double-click **Verify_PG_Records.bat** inside the updated existing project.
   It takes a dated record backup, imports the rosters, and runs the accuracy
   checks. It does not replace the shared dashboard.
5. Start your usual editable dashboard and open **Record accuracy**. Review
   coverage, evidence conflicts, and representative student histories using
   the [maintenance guide](docs/pg_record_accuracy.md).
6. When the office verification is complete, run
   **Publish_Read_Only_Dashboard.bat**, review the report, and share the resulting
   aggregate HTML using the existing office process.

Pulling the code does not verify the office records or republish the shared
dashboard. Run the verification and review steps above before publishing.

The default measure is now **Graduation after joining FSL**. Membership,
university enrollment, and the legacy roster view are separately selectable.
University enrollment stays Unknown until explicit university records are
entered. Missing or contradictory evidence is not inferred into a graduation.
