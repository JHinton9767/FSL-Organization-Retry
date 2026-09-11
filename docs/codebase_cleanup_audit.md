# Codebase Cleanup Audit

Date: 2026-09-11. Scope: tracked Python code, documented entry points, architecture/refactor documents, dependency declarations, and ignore rules. This pass preserves existing calculations and persisted formats. Validation used synthetic records, not a rebuild of the user's full roster archive.

## Remaining correctness risks

These findings require separate behavior changes. They were not silently changed during cleanup.

### High: name conflicts can bypass duplicate review

In `src/sqlCompile_dashboard.py`, `consolidate_duplicate_student_outcomes` only checks observed names for IDs repeated in the cohort-outcome table. If two roster names share one ID but generate only one new-member cohort row, the function returns without flagging the conflict.

Reproduced with one cohort outcome and two distinct name observations: the mismatch flag remained false. Future work should derive conflicting IDs from the name-observation table as well as repeated cohort rows. A saved display-name choice also does not separate two people's underlying semester records; that would require a distinct identity-correction workflow.

### High: recompilation is not an atomic database replacement

`src/sqlCompile.py:write_sqlite` drops/recreates the base table and then replaces side tables. `src/sqlCompile_cohort.py:write_report_tables` replaces report tables one at a time. Pandas SQL writes can commit between tables, so a late failure can leave a mixture of old and new tables. Explicit connection closure is fixed in this pass, but it does not make the entire compile atomic.

A separate change should build/validate the replacement in a temporary database, or introduce a verified transaction strategy for every table write. Preserve any unrelated tables and the user's existing database on failure.

### Medium: report exports and dashboard cohorts can disagree

`src/sqlCompile_cohort.py:build_new_member_cohort_tables` produces one outcome per new-member cohort, while `src/sqlCompile_dashboard.py:load_dashboard_tables` subsequently consolidates repeated IDs to their earliest cohort for the dashboard. The exported report tables and the dashboard can therefore count different populations.

Reproduced with one ID marked N in two join semesters: two report outcome rows, one dashboard outcome row. A future correction should centralize identity/cohort resolution before both consumers, and explicitly preserve or replace the historical report contract.

### Medium: legacy Excel formats are not scanned by sqlCompile

`src/sqlCompile.py:excel_files` uses the openpyxl-compatible `SUPPORTED_EXTENSIONS` from `src/build_master_roster.py`: `.xlsx`, `.xlsm`, `.xltx`, and `.xltm`. `.xls` and `.xlsb` files are excluded during discovery, so they do not appear as failed-file issues. This is a limitation of the existing "all Excel files" workflow, not a new restriction introduced here.

Adding these formats needs explicit readers and ingestion fixtures; simply adding extensions would not make openpyxl read them.

### Medium: a separate pipeline can silently ignore unreadable corrections

`src/graduation_pipeline/apply_corrections.py:load_manual_corrections` catches every exception when reading its CSV and substitutes an empty correction table. A malformed or inaccessible file can therefore produce reports without the saved corrections. This affects the separate focused graduation command, not the sqlCompile manual-status reader. Prefer an explicit failure or an auditable warning in a separate error-handling change.

### Maintenance risks

- The canonical pipeline remains over 7,000 lines and `app/main.py` remains over 3,600 lines. Splitting them now would be a broad refactor; add representative bundle parity fixtures first.
- There are three distinct reporting workflows with different evidence and denominator policies. The updated architecture documents their ownership. Do not combine their similarly named rates without choosing the authoritative policy.
- `requirements.txt` uses open-ended minimum versions and no dependency lock. Reproducible installation across computers remains a future task; this cleanup does not change package versions.
- sqlCompile chapter-kick inference still relies on roster coverage and configured zero-member periods. Incomplete source inventories can affect inferred outcomes. Keep the existing manual/exception workflow until recognition evidence is integrated explicitly.
- Manual status refresh is intentionally explicit in the dashboard for performance. Its caches do not automatically incorporate every external CSV edit; the existing Refresh Dashboard Data action remains necessary.
- Similar CSV normalization/read/write code remains in the two name-review ledgers. These paths are active and preserve saved corrections. A larger shared storage layer is not justified solely by their similar shape.

## Changes applied

- Removed 23 uncalled legacy loaders/helpers and their unused imports/constants. This includes three reference-file loaders superseded by `load_reference_inventory_table` and `build_reference_subset`, stale manual-save/review-queue wrappers, and unused roster/format/status helpers.
- Removed repeated chapter-disappearance calculation inside the cohort loop. The shared event result is reused while manual decisions and zero-member exceptions keep their existing effects.
- Replaced repeated per-milestone DataFrame copies, status conversion, and filtering with one prepared history per selected student and binary-search checkpoint lookup.
- Computed milestone dates once per selected cohort/year. Kept the available-data horizon based on the full timeline before filtering histories to selected students.
- Removed the unused unfiltered checkpoint fallback path and duplicate fall/spring return branch.
- Derived the small chart export from chart rows instead of building a second copy of each record.
- Closed all sqlCompile SQLite connections explicitly, including early returns and failures, while preserving transaction handling. Removed redundant explicit commits at context exit.
- Removed redundant ignore entries and replaced stale canonical-only architecture/refactor statements with the current workflow boundaries.

Preserved: raw inputs, correction ledgers, duplicate-name choices and recheck lists, zero-member periods, legacy decision import, older dashboard entry points, the separate graduation command, older SQLite side-table fallbacks, canonical CSV fallback reads, and locked-file/pending-action recovery. Six-year limits, status labels, source priorities, chart colors, and configured latest-roster markers are intentional domain/display settings, not dead code.

## Verification

The full test suite passed: **200 tests**. Added coverage for checkpoint boundary behavior, later resolved outcomes, same-semester manual priority, missing histories, input immutability, all-cohort versus individual-report parity, and SQLite closure after success/early return/failure.

Python compilation and the compiler/dashboard launcher help checks passed. Streamlit's help text required Python's `-X utf8` option in this terminal: its Unicode arrow fails with the terminal's default cp1252 encoding. No dashboard-launcher code was changed for that environment-specific help-output issue. A live browser session was not exercised in this cleanup.

The initial baseline was 193 tests. Two failed when a hidden temporary directory was used because the Banner ID scanner intentionally skips hidden paths; both passed under `pytest_tmp/`. All final tests used that visible, ignored folder.

Compared the pre-cleanup Git version with the new code on deterministic synthetic data. All chart/detail/table values, column order, dtypes, metadata, and selected semesters matched:

- Seven chart scenarios: full timeline, selected semesters, selected chapters, semester breakdown, chapter breakdown, empty selection, and no available milestone offsets.
- Four cohort scenarios: all semesters, selected semesters, missing inventory, and inventory with/without zero-member exceptions and manual decisions.

Final chart benchmark: 1,500 students, 9,738 timeline rows, 11 join years, and 20 chapters. Timings are local single-run measurements, not a guarantee for the full archive or total Streamlit page loading.

| Scenario | Before | After | Speedup |
|---|---:|---:|---:|
| All six milestones | 18.424 s | 3.942 s | 4.7x |
| Selected join semesters | 5.133 s | 1.065 s | 4.8x |
| Selected chapters | 3.107 s | 0.572 s | 5.4x |
| Semester breakdown | 4.101 s | 3.008 s | 1.4x |
| Chapter breakdown | 3.710 s | 2.908 s | 1.3x |

Static AST/reference checks found no remaining unreferenced top-level Python functions or unused top-level imports under the scan's rules. This is supporting evidence, not a proof that all dynamically reachable behavior or future integration risks have been exhaustively checked.
