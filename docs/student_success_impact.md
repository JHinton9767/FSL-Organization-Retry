# Student Success & Impact

Open **Student Success & Impact** in the owner dashboard. It has five workspaces:

- **Overview:** unique verified graduates and awards, confirmed enrollment, completed Year 1–6 checkpoints, outcome-verification coverage, student details, and joining-cohort comparisons.
- **Verified graduation records:** a review queue, an award registry with inclusion/exclusion reasons, and an audited award editor with CSV import preview.
- **Academic & community records:** term GPA, honors, earned credits, service hours, philanthropy, scholarships, and awards. Empty templates are available in the dashboard and `config/*.example.csv`.
- **External summary:** ready headline metrics, an explicit list of metrics awaiting information, shareable HTML, aggregate CSV, and summary text.
- **Definitions & data readiness:** formulas, scope, evidence requirements, and the remaining institutional data needs.

The owner dashboard retains the existing P&G sections and default combined roster view. No example student records or estimated results are installed. A code-only update requires a dashboard restart, not a roster recompile.

## Bring information in when available

### University award records

Use the graduation CSV template. One record represents one award, with:

`Record ID, Student ID, Award Date, Award Semester, Degree Level, Degree Title, Institution, Verification, Source, Reviewed By, Reviewed On, Notes`

Leave Record ID blank for new records; a stable ID is assigned at save. IDs on existing records should remain unchanged. The student ID must be A0 followed by seven digits. Dates use `YYYY-MM-DD`. Award Date means the actual award date, not a ceremony or review date. Award Semester is the university's effective semester, such as `Spring 2024`; its calendar year must agree with the date.

Verification is **Draft**, **Verified**, or **Excluded**. Drafts can be incomplete. Verified awards require every award detail, the supporting source, reviewer, and review date. The review cannot precede the award and dates cannot be in the future. Degree levels are Certificate, Associate, Bachelor's, Master's, Doctoral, Professional, and Other. Bachelor's is the default reporting selection.

The review queue uses roster G and term-level university graduation as leads. These do not automatically become fully verified award records. Download a draft award template for the queue, obtain the missing degree details, then preview and save the completed CSV. An incoming record with an existing ID but different contents is rejected; edit the existing row deliberately instead of silently replacing it through import.

The registry retains draft, excluded, unmatched, future, pre-join, different-degree-level, and conflicted rows with an explanation. A verified award only contributes when its ID matches the selected original joining cohort and it has no unresolved identity/degree issue. Awards before the join semester do not count. Same-semester awards are included, but the within-semester ordering cannot be established without exact joining dates. Multiple awards count separately in the private award count and once per student in the graduate count.

Verified awards also feed the existing independent **Graduation after joining FSL** view. They do not overwrite roster membership statuses or the university evidence CSV. To resolve a contradictory university record, use **Record accuracy → University records** with the existing documented resolution workflow. Excluding or correcting an award retains its change history.

### University enrollment and negative degree evidence

Continue using **Record accuracy → University records**. Source and reviewer are required. Use the actual effective semester. Enrollment must be observed at the checkpoint itself; an old enrollment row does not carry forward. A documented degree can carry forward. A `No degree` observation is only a confirmed negative at its own semester. Absence from a file is never an automatic negative.

Use consistent institutional scope for the supplied enrollment and degree records. The new report follows FSL joining cohorts; it does not establish enrollment or completion at other institutions that have not supplied evidence. External definitions distinguish these results from college-entry IPEDS rates or national transfer persistence.

### Academic records

Use one verified student/semester summary. Supply GPA Credits and Quality Points together on a 4.0 scale. The GPA is **sum of quality points / sum of GPA credits**, so chapter averages and student GPAs are not averaged without their weights. Zero-credit records have no GPA weight. Blank fields mean missing evidence; enter zero only when confirmed.

Earned Credits and Honors are optional independent fields. Honors choices are None, Dean's list, President's list, and Other honors. A documented honors count is unique students within a term, not a percentage or a claim that all honors have been captured.

The internal table shows GPA for reviewed records with the observed student count. External GPA is available only for the report-end semester, with complete enrollment evidence for the selected cohort population and verified academic summaries for every confirmed enrolled student. It uses that exact enrolled population; at least five must contribute GPA credits by default.

### Community impact

Record a reviewed semester total per chapter and metric, or an `All FSL` total. Supported measures are Service hours, Philanthropy raised (USD), Scholarships awarded (USD), and Awards received (whole awards). These count documented activity, not unique participants or volunteers. Overlapping totals and subtotals for the same metric/semester are rejected, including duplicate chapter totals.

Community totals follow the selected report period and chapters, independently of joining cohorts. An `All FSL` record cannot be apportioned to a selected chapter, so it is omitted from a chapter-only report. Use documented amounts raised or awarded; do not enter pledges, forecasts, or a combined total that overlaps existing chapter records. The external report labels these as documented totals that may not include every activity.

## Reporting controls and formulas

Joining semesters and chapters define the student population. The earliest observed original join is retained, even after resignation, inactivity, chapter changes, or graduation. Every eligible student remains in a checkpoint denominator. The award/activity period start limits period counts, not the students in historical checkpoint calculations. The report end determines which outcomes and checkpoints are observable and cannot exceed the owner-confirmed reporting cutoff.

- **Year N:** Spring in the joining calendar year plus N; Summer joins use Summer plus N. These are the existing named semester checkpoints, not exact elapsed anniversaries. All new success measures require a completed checkpoint; no provisional first-year rate is used here.
- **Documented graduation share:** verified graduates at the checkpoint / all eligible students. The internal table retains unknowns in the denominator and displays the coverage separately.
- **Degree coverage:** (verified graduates + explicit no-degree observations at that checkpoint) / eligible students.
- **University continuation:** (confirmed enrollment at the checkpoint OR a verified award of the selected degree level by that checkpoint) / all eligible students, with each student counted once. A known negative requires both not enrolled and no degree evidence; otherwise the outcome remains unknown.
- **Continuation coverage:** (confirmed positive continuation outcomes + confirmed negative outcomes) / eligible students.

Different checkpoints can have different eligible cohorts. The cohort comparison table provides the actual counts behind these measures. Original join-evidence completeness and unresolved identities also affect external readiness.

## Immediate external use

Documented graduate, honors, and activity counts can become available before every other student's outcome is known. Their wording explains that they are documented counts, with additional records potentially outstanding. Original cohort counts require original join evidence and resolved identities. Public graduation and continuation **rates** require every eligible student's relevant outcome to be verified. Missing records are not dropped to improve a rate.

Student headline metrics require a minimum group size of five by default; the owner can increase that threshold. This is a display precaution, not a claim of compliance with any institution-specific disclosure policy. A 0% rate can be published if the whole eligible population has verified negative outcomes; an empty dataset never becomes 0%.

Only ready metrics appear in the external HTML/text/CSV. When none are ready, the report says verified results are being prepared. Pending requirements remain visible in the owner dashboard. Public exports contain no student-level records, IDs, names, reviewer names, source paths, or notes. HTML is self-contained, escaped, printable, and needs no internet or server.

Download a report in **External summary**, or run:

```powershell
.\Publish_Student_Success.bat
```

This creates `output/sqlCompile/viewer/FSL_Student_Success_Impact.html`. To select a reporting period:

```powershell
uv run --with-requirements requirements.txt python -X utf8 publish_student_success.py --start "Fall 2025" --end "Spring 2026"
```

Optional flags include `--cohort`, `--chapter` (repeat either for multiple selections), `--degree-level`, `--minimum-group`, `--host-config`, and `--output`. The CLI checks that inputs did not change during calculation, writes atomically, and archives previous/candidate HTML plus a manifest under private `output/sqlCompile/success_publications/`. The dashboard download reflects the displayed snapshot; refresh after changes made elsewhere.

The existing coworker viewer includes an expandable verified summary for **all** published joining cohorts/chapters. Its P&G filters do not change that summary. Use the separate Student Success HTML for external distribution; it does not include the internal roster outcome chart.

## Storage and maintenance

The optional local files are `config/sqlCompile_graduation_records.csv`, `config/sqlCompile_academic_records.csv`, and `config/sqlCompile_impact_records.csv`. They are ignored by Git. They are created only on save; empty files are not required. The host configuration supports `graduation_records`, `academic_records`, and `impact_records` overrides for office storage. Existing host configurations remain valid.

All three use the existing atomic saves, backup rotation, permanent `_history` audit, and stale-edit protection. They participate in data refresh, baseline backups, publication fingerprints, and **Record accuracy → Correction history**. Retain the files and their history with the office database. There is no need to copy private data into Git.

The interface is ready for future records, but verification is still a human source-review decision. This development checkout has no populated office baseline, so production totals have not been certified. College-entry four/six-year graduation, matched campus comparisons, transfer persistence, and advisor interventions require additional data and definitions; they are identified as future work instead of producing guessed results.

## Implementation validation

Automated checks cover missing data, complete and incomplete denominators, verified zero rates, exact-term enrollment, award uniqueness, former chapter members, identity/degree contradictions, award cutoffs, weighted GPA, overlapping community totals, audited/stale saves, aggregate export privacy, and opening/saving through the owner interface. The owner screens and external HTML were also inspected with clearly labeled synthetic records.

A synthetic run with 11,705 students, 9,000 awards, 23,410 enrollment observations, and 11,705 academic summaries built 70,230 checkpoint rows in 4.8 seconds on the development computer after removing repeated per-student DataFrame conversion (29.8 seconds before optimization). This measures the success-report calculation, not importing office files or the complete publisher runtime. The interface caches completed reports between interactions.
