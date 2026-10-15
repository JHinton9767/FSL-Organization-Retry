# P&G record accuracy and maintenance

The current SQL workflow is the P&G foundation. GPA, grade reports, and advisor tools remain later stages. Do not combine rates from the older canonical or focused-graduation pipelines with this dashboard.

## First office verification

These steps must run on the computer with the populated database and access to the source rosters. The development checkout cannot certify the office records.

1. Close the editable dashboard. Update the application code while preserving local configuration and manual CSVs.
2. Double-click **Verify_PG_Records.bat**. It backs up the database, review files, permanent correction histories, reporting settings, source-path settings, and existing viewer before recompiling. It records missing backup inputs explicitly. Keep the original roster archive on approved storage as well; this backup does not copy that archive.
3. Read the reported quality file under `output/sqlCompile/publication_checks/`. Errors block publication, including missing original join evidence, invalid cohort IDs, count mismatches, and contradictory coverage decisions. Warnings require owner review. This command never replaces the shared viewer.
4. Start the editable dashboard. Open **Record accuracy > Source coverage**. Check each chapter/semester against the actual archive. Mark Complete, Preliminary, Missing, Zero members, or Not expected with a supporting source and reviewer. Unreviewed is an explicit unknown, not proof of completeness. Confirm any legacy zero-member exceptions.
5. Open **Record accuracy > Student history** and compare representative histories with original records. Include a same-term join/exit, later I/S reactivation, Early Alumni without a degree, a chapter switch, and resignation followed by graduation. Also check earlier imported G decisions against their original labels; do not reimport the entire old manual archive.
6. Confirm the reporting cutoff. Spring 2026 remains the inherited default; later initial rosters stay stored but excluded until their term is confirmed complete. Enrollment coverage is independent of roster coverage.
7. Run **Verify_PG_Records.bat** again after corrections. Compare changes with the prior publication. Publish with **Publish_Read_Only_Dashboard.bat** only after reviewing its warnings and the student examples.

There is no automatic claim that every source record is true. Passing checks establishes consistency; staff source review establishes the verified baseline. Assign one owner and one backup reviewer to this routine.

## Defined measures (pg-2026-10-15.1)

- **Graduation after joining FSL** is the default view. Positive G/Graduated evidence or explicit university graduation establishes a degree; alumni, resignation, disappearance, GPA, or hours do not. The denominator contains every eligible student, including Unknown. No degree confirmed is used only with explicit negative evidence at the checkpoint; it is not a prediction of eventual graduation.
- **FSL membership** reports membership independently. It preserves an exit even when the student later graduates. A later explicit membership observation can establish a return or chapter change. Active requires an observation in the checkpoint semester. An earlier active roster is insufficient.
- A chapter gap is Unknown in the independent membership view until explicit status evidence or a researched CK correction confirms removal. The legacy view retains its earlier gap inference for comparison. A non-G manual correction overlapping roster G evidence also requires separate degree review; migration does not silently reinstate that old graduation claim. Degrees recorded before the FSL entry semester remain in the evidence history but do not count as graduation after joining.
- **University enrollment** requires explicit enrollment evidence in the checkpoint semester. A roster cannot establish university persistence. With no university evidence, this view remains Unknown.
- **Roster outcomes (legacy)** preserves the previous combined buckets and carry-forward rules for comparison. Its Manual Checker edits roster/status evidence. It is not an independent university enrollment measure.
- Cohorts use the earliest observed N and its chapter, including N followed by an exit in the same semester. Later chapter changes do not reattribute the entry cohort. Each student is counted once per checkpoint.
- Year N uses Spring in the join calendar year plus N for Fall/Spring joins; Summer joins use Summer plus N years. For example, Fall 2021 Year 1 is Spring 2022, and Spring 2021 Year 1 is also Spring 2022. These are named semester checkpoints, not exact anniversaries. All new independent measures require completed checkpoints. The legacy view retains its provisional first-year behavior solely for comparison.
- **Eligible at each year** includes every eligible cohort at each checkpoint, so denominators can differ. **Same students across selected years** includes only cohorts that have reached every selected year. Select fewer years to compare more recent cohorts. Future is excluded from measured denominators.
- Chart counts, student details, downloads, and the read-only viewer use the same checkpoint calculation. The older CLI cohort summary exports are legacy products with different resolved-only definitions; they are not exports of the current chart.
- The Manual Checker retains the selected measure and population. Membership/legacy views use the existing status editor. Degree/enrollment views provide a matching university evidence queue with batch selection and source-backed saves. Its latest selected checkpoint matches the chart; saving facts uses the actual effective semester supplied by the reviewer.

## Evidence and corrections

The compiler preserves current row-level observations with source path, sheet, row, semester, roster ordering, file fingerprint, and import time. The database also retains every parsed file version encountered by this compiler. It checks file contents on each import and reuses unchanged parsing results. Changed versions remain traceable; missing prior files produce issues. Existing sources are never edited. Files not previously seen by this version need an initial full parse.

Review CSVs remain compatible with existing saved decisions. Each save records the complete before/after content, timestamp, reviewer, reason, and supporting reference in a private `_history` database beside the CSV. History is permanent; rotating recovery backups are separate. Existing decisions acquire a baseline on their next save, without inventing historical authors or dates. An interrupted write is retained as pending/failed and should be reviewed. Copy histories with their CSVs when moving computers.

Use **Record accuracy > Correction history** to inspect or download an earlier version. To undo, review the earlier values and save the intended correction through the editor; this creates a new audit entry. Avoid replacing files directly outside the dashboard. File conflicts are rejected rather than overwriting another reviewer's save.

New or changed manual statuses require a valid student ID, effective semester, reviewer, and supporting explanation. Put the reason and source in the row's Notes, or complete both correction fields in the sidebar. Existing incomplete legacy notes are retained; unchanged rows do not require backfilling during an unrelated correction.

Use **Record accuracy > University records** to enter verified facts or preview a CSV using its downloadable template. Source and reviewer are required. Contradictory degree/enrollment evidence produces Unknown and a review warning. A researched **Use university record** decision with an explanatory note can resolve earlier degree evidence from its effective semester. Later conflicting evidence reopens the issue. It does not erase the original observations or correction history. Two conflicting university resolutions at the same effective semester remain unresolved.

## Every update and publication

1. Preserve the incoming source files; import new or changed files.
2. Review excluded records, changed/missing files, identity conflicts, and coverage decisions. Resolve identity/cohort errors first because they affect multiple outputs.
3. Save researched corrections with effective semesters and evidence. Refresh Dashboard Data when ready to recalculate.
4. Check student histories and the exact students behind changed chart counts. Unknowns may remain, with their limits visible.
5. Review the publication check. Errors cannot be bypassed by approving warnings. Do not dismiss warnings merely to obtain a publication.
6. Publish a dated snapshot. Owner-only `output/sqlCompile/publications/` retains candidate HTML, the prior HTML, quality report, and a successful-publication manifest with rules version and data fingerprint. Restore an earlier HTML only after checking its date/cutoff, and retain the current copy before rollback. Coworkers receive only the aggregate HTML, never the owner quality report or record histories.
7. At semester close, reconcile coverage, update university evidence when available, and deliberately advance the complete roster cutoff. Do not infer an outcome because the next semester has not arrived.

Keep `baselines/`, `publications/`, `_history/`, the database, local settings, and student exports in approved private storage and out of Git. Backups on the same disk provide rollback, not protection from loss of that disk; include them in the office's approved backup routine.
