# Refactor Plan

## Supported workflows

- **Current baseline:** `sqlCompile.py`, `sqlCompileCohort.py`, and `run_sql_compile_dashboard.py`. These own the SQLite roster/cohort workflow, 1-6 year outcome chart, and Manual Checker.
- **Older canonical app:** `run_canonical_pipeline.py` and `run_local_analytics_app.py`. Keep the canonical bundle, academic analytics, exports, and historical manual-correction workflow available.
- **Focused graduation reports:** `src/graduation_pipeline/run_pipeline.py`. This documented command has its own evidence rules and correction ledger.

Do not replace one workflow's outputs with another's without a separate migration and output comparison. The old "canonical-only" rule applies to the canonical app loader, not to sqlCompile.

## Maintenance rules

- Preserve explicit graduation evidence requirements. Disappearance alone is not graduation.
- Preserve sqlCompile status precedence: non-A/N overrides A/N; N overrides A; equivalent-priority statuses use roster pass/version order.
- Preserve unique-student cohort handling, manual name selections, selected-semester/chapter filters, and Future eligibility.
- Preserve current checkpoint behavior: the most recent resolved outcome through a checkpoint carries forward; Unknown can change when later evidence becomes available.
- Preserve all manual ledgers, pending-file imports, name recheck lists, and zero-member chapter exceptions.
- Keep raw student files unchanged and out of Git.
- Preserve existing command names, database schemas, output paths, and report formats.
- Use shared domain logic where semantics are identical. Similar-looking status/term parsers are not interchangeable without parity tests.
- Remove code only after checking imports, call sites, tests, documented commands, and indirect references.
- Keep changes scoped. Large module splits require output parity, not just passing unit tests.

## Cleanup completed

- Removed unused legacy reference loaders superseded by the unified reference inventory.
- Removed uncalled formatting, status, roster, and manual-save helpers plus unused imports/constants.
- Reused chapter-disappearance events across cohort builds.
- Prepared selected student checkpoint histories once instead of copying and reclassifying them for every milestone.
- Derived chart-detail exports from the chart rows instead of assembling a duplicate dictionary.
- Updated architecture documentation to recognize sqlCompile as the current baseline.

## Next work

See [the audit](docs/codebase_cleanup_audit.md) for remaining identity, reporting, and fallback risks. Address those as explicit correctness changes with fixtures before changing stored outcomes or rate definitions.

Extract focused modules from the large canonical pipeline and dashboard only when the extracted behavior can be compared against representative existing outputs. Keep legacy import support until the manual-review migration has a verified completion path.
