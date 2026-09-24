# September 2026 Rate Corrections

This update fixes the eight calculation/import findings from the September 24 audit. It does not erase manual decisions, duplicate-name choices, recheck lists, or zero-member exceptions. The shared HTML is not replaced until you publish it.

## Required Office Update

Stop the editable dashboard with Ctrl+C in its running terminal. Keep your normal backup of the database and `config` folder. Then run these commands from the project directory:

```powershell
git pull
$env:UV_CACHE_DIR=".uv-cache"
uv run --with-requirements requirements.txt python sqlCompile.py --all-semesters
uv run --with-requirements requirements.txt python run_sql_compile_dashboard.py --server.port 8502
```

This update **requires one full Excel recompile**. Previous databases discarded some original N observations, so a dashboard restart alone cannot repair all cohort denominators. The compiler now preserves original join evidence in `sqlCompile_new_member_observations` alongside the unchanged four-column `sqlCompile` table. Existing databases remain readable, but display a warning until rebuilt. The publication check also flags missing join evidence.

The compiler uses an atomic database replacement and retains database backups. It does not rewrite your manual-status/name-choice files. Use another port, such as 8503, if a different program already owns 8502.

Verify the reporting cutoff is still **Spring 2026** and check representative semester/chapter selections before republishing. Initial Fall 2026 records remain outside that complete-data cutoff. In another terminal, or after stopping the editable dashboard:

```powershell
.\Publish_Read_Only_Dashboard.bat
```

Large changes may be legitimate results of these corrections, but review the publication comparison before approving replacement. Do not dismiss a missing-join-evidence warning instead of recompiling.

## Corrected Rules

1. **Inactive/Suspended is provisional.** I, S, and I/S still share a bucket. A later A/N observation can restore Active. With no later status, I/S remains the last-known bucket. A demonstrably later roster pass/month can also clear I/S within the same semester. G, D/RS, RV, T, AL, and CK retain their resolved-outcome carry-forward behavior; a later I/S does not undo them.
2. **Joining evidence is independent of semester outcome.** A student who starts N and ends D, G, S, or another status remains in the join cohort. Semester status precedence otherwise stays intact. The earliest observed N determines the student's cohort and join chapter.
3. **Unequal roster-pass counts are not chapter removal evidence.** An initial-only chapter is not kicked merely because another chapter submitted a final roster. Existing across-semester disappearance rules and zero-member exceptions remain. Explicit CK/manual evidence can still establish a midsemester removal.
4. **Duplicate cohorts share one history.** Earliest-cohort selection now happens before outcomes and reports are built. Corrections scoped to another observed N semester/chapter for that same ID are retained in the canonical history. Unrelated cohort scopes are not applied.
5. **Latest applicable manual decision wins.** Where a wildcard and a specific correction both apply to the same ID and semester, the last saved row takes precedence. Manual rows still override roster/inferred rows for that semester. Saved correction files are not silently rewritten or pruned.
6. **Name conflicts do not require multiple cohorts.** Multiple distinct observed names for an ID trigger name review even with just one join semester. Unresolved conflicts count as Unknown in milestone bars. Existing saved name choices remain honored. A name choice is not a tool for separating two real people's source records; actual ID collisions still require source correction.
7. **Chapter matching uses the roster name normalizer.** Legal suffixes, punctuation, and existing known name variants no longer prevent an otherwise applicable manual correction from matching a compiled short chapter name. Original saved names are preserved.
8. **Legacy graduation requires positive evidence.** Known codes and explicit positive labels such as Graduated Confirmed or Degree Awarded are accepted. Not Graduated, Non-Graduate Exit, No degree, uncertain labels, and unknown descriptions do not become G. New imported notes retain the original status text.

Roster ordering also uses the nearest filename/folder inside the configured source location. An unrelated ancestor such as `Final December backup` no longer makes every roster Final or changes its month. Sources in a second configured input location retain their relative folder labels.

## Check Previously Imported Decisions

An older import may already have converted an ambiguous label to G without saving the original status text. It is unsafe to automatically erase every imported graduation decision. The corrected importer exposes a review list from the original legacy files without modifying saved work:

```powershell
uv run --with-requirements requirements.txt python import_legacy_manual_to_sql_compile.py --legacy-path . --dry-run --review-output output\sqlCompile\legacy_status_review.csv
```

Use the actual legacy project/file location if it is elsewhere. In the editable dashboard, **Manual Checker > Reuse Legacy Manual Decisions > Preview Legacy Decisions** also shows rejected statuses and offers **Download Legacy Status Review**. Review any earlier saved G decision for those IDs against the original forms and correct its effective semester if necessary.

The review command does not alter the correction ledger. Do not append the entire legacy archive again merely to run this check: old source decisions could overwrite later work. A rejected status is not automatically a Drop, Transfer, or other outcome; resolve it from evidence.

## Verification and Scope

Regression tests cover all eight failures, positive and negative graduation labels, same-semester and later reactivation, terminal carry-forward, preservation of earliest cohorts and scoped corrections, old-database warnings, and the shared viewer's corrected counts. The original two-student test now reports 50% graduation, not 100%.

The office roster archive was not available in the development workspace. Actual affected-student counts and repaired production rates must be verified after the office recompile. No office database, saved corrections, or previously published HTML was replaced during development.

The audit's separate observations about legacy Excel formats, the older canonical dashboard, the focused graduation pipeline, and differing report-export definitions are not changes to this eight-finding sqlCompile repair. Current bar charts still measure semester checkpoints since joining FSL, not initial enrollment at TXST. A mixed-semester selection can have a different eligible denominator in each year; percentages across those bars need not rise monotonically.
