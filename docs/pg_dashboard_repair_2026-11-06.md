# P&G dashboard repair — November 6, 2026

The screenshot showed the editable dashboard using **Graduation after joining
FSL**. The October update made that optional degree-only analysis the default.
It labeled students without separate degree evidence Unknown even when their
membership history had a known outcome. The command sequence did not select
that view; the application and publisher defaults did.

The default is restored to **Persistence and graduation**, including Active,
Early Alumni, Inactive/Suspended, Dropped/Resigned, Revoked, Transfer, Chapter
Kicked, Unknown, and Graduated. Separate membership, degree, and enrollment
analyses remain available, with descriptions explaining their narrower scope.
The existing publication key is retained so prior snapshots remain comparable.

## Other fixes found during the audit

| Finding | Repair |
| --- | --- |
| Saved manual statuses could leave cached chart rates unchanged. | Returning from Manual Checker refreshes saved rates. Other drafts and their original conflict-check baseline are preserved. |
| Revising your own just-saved correction could be treated as another reviewer's conflicting edit. | The session advances the saved baseline only for its own successful changes. Other reviewers' conflicting changes remain protected. |
| University or coverage files could reload without invalidating the local chart cache. | Those source revisions now participate in chart-cache identity. |
| Chapter Kicked persisted through a later verified return to active membership. | Later active/new-member evidence can clear the chapter-removal status while retaining the original cohort chapter. |
| Outcome construction scanned and sorted the entire cohort separately for every student. | Sort once and look up each student's latest record and manual-evidence flag. |
| Publication rebuilt all four complete dashboard calculations a second time for validation. | Reconcile the already-calculated student records against the published aggregates. Corrupt counts still block publication. |
| Parsing cache loaded every historical roster revision into memory. | Query only the exact file fingerprints needed for the current import. Historical versions remain stored and reusable. |
| A partly missing, unreadable, or emptied roster archive could replace a previously populated database. | Preserve the existing database and report the affected files. Deliberate source removals require an explicit command option after reviewing the backup. |
| Cached source files could change between fingerprinting and reuse. | Recheck the fingerprint before accepting cached observations. |
| A brief Windows sharing lock could fail an otherwise valid atomic save. | Retry replacement for up to 0.75 seconds; persistent failures still preserve the prior file and report an error. |
| Chart labels could shrink or rotate, and the legend was below the visible chart. | Use horizontal two-line labels, a readable minimum font, and a legend above the bars. |
| The launch command keeps running, so the publication command after it cannot execute immediately in the same terminal. | Add `Start_PG_Dashboard.bat` and separate startup, source verification, and publication in the instructions. Batch wrappers preserve failure exit codes. |

## Verification and limits

The audit traced the active compiler, source cache, cohort and checkpoint
calculations, manual and name-correction workflows, Streamlit caches, chart
renderer, offline viewer, publication checks, storage, and launch commands.
All 94 tracked Python files parsed successfully, and the full regression suite
passed **402 tests**. The suite exercises
the active workflow and the repository's other calculation paths, including
Python/browser aggregate parity, saved corrections, concurrent saves, private
data exclusion, and publication safeguards.

A benchmark of the outcome-building step used **11,705 synthetic students**,
matching the population size in the screenshot. The old and new results were
identical. That step took **36.5 seconds before and 1.8 seconds after** on this
computer while other checks were running. This is a measurement of that step,
not a promised runtime for an entire office import.

The repaired chart was rendered and visually checked using synthetic records.
The populated office database and its complete source archive were unavailable
in this checkout, so this repair does not certify the office totals or claim
that every source record is correct. Historical GPA and alternate analytics
pipelines remain separate; this repair does not migrate or delete them.

See [update and run instructions](../PG_UPDATE_README.md). Pulling and restarting
is enough to apply the chart/default repair to existing records. Recompile when
source rosters change; republish to update the coworker HTML snapshot.
