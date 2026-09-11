# Refactor Inventory

## Runtime ownership

| Surface | Entry point | Main modules | Persistence |
|---|---|---|---|
| Current sqlCompile workflow | `sqlCompile.py`, `sqlCompileCohort.py`, `run_sql_compile_dashboard.py` | `src/sqlCompile.py`, `src/sqlCompile_cohort.py`, `src/sqlCompile_dashboard.py`, `app/sql_compile_dashboard.py` | `output/sqlCompile/`, `config/sqlCompile_*.csv` |
| Canonical analytics | `run_canonical_pipeline.py`, `run_local_analytics_app.py`, Windows launchers | `src/build_canonical_pipeline.py`, `app/main.py`, `app/data_loader.py`, `app/analysis.py`, `app/metrics_engine.py` | Canonical Parquet/CSV bundles and legacy `config/` ledgers |
| Focused graduation reports | `src/graduation_pipeline/run_pipeline.py` | `src/graduation_pipeline/` | `output/graduation/`, `data/manual/manual_corrections.csv` |
| Legacy decision import | `import_legacy_manual_to_sql_compile.py` | `src/sqlCompile_legacy_manual.py` | Appends translated decisions to the sqlCompile manual-status ledger |
| Supporting tools | `scripts/build_banner_id_batches.py`, `scripts/build_grade_reports.py`, `scripts/build_chapter_semester_inventory.py`, `scripts/check_repo_hygiene.py` | Shared parsers, inventory, path config | Separate report/audit outputs |

## Shared modules

- `src/build_master_roster.py`: roster/header/name/status parsing still used by multiple workflows.
- `src/shared_utils.py`: text, chapter, numeric, and hour-bucket utilities.
- `src/path_config.py`: configured input/output paths.
- `src/persistence_outcomes.py`: common displayed outcome labels and checkpoint helpers.
- `src/chapter_semester_inventory.py`, `src/chapter_status_events.py`: chapter presence and status evidence.
- `app/config_loader.py`: canonical settings, manual ledgers, pending actions, and helper packages.
- `app/io_utils.py`, `app/status_framework.py`: canonical app reading/normalization and outcome semantics.
- `app/charts.py`, `app/exports.py`: chart construction and downloads.
- `app/models.py`, `app/presets.py`: app data containers and persisted filter presets.

## Persisted state to protect

- All raw source folders and configured shared-drive inputs.
- sqlCompile base table and roster/name side tables.
- Cohort report tables and CSV exports, including timestamped exports when files are locked.
- Manual status, duplicate-name resolution, name recheck, and zero-member-period CSVs.
- Older corrections, graduation evidence, exclusions, outcome overrides, manual adjustments, review queues/actions, and pending action files.
- Canonical source caches, prepared bundles, QA/audits, and optional archived runs.
- Transcript text, helper packages, and analysis presets.

CSV fallback loading for older bundles, absent side tables in older SQLite files, and pending manual-action imports remain in use. Their age does not make them dead code.

## Removed in the cleanup

The unused membership, GPA, and GPA-benchmark reference loaders and their numeric parsers were superseded by `load_reference_inventory_table` plus `build_reference_subset`. Uncalled manual-save wrappers, stale review-queue read/write wrappers, a redundant chapter-kick wrapper, and unused formatting/roster helpers were also removed.

Active correction readers/writers, the legacy importer, the older dashboard, and the focused graduation command remain supported. Historical `powerquery/` files are reference artifacts outside the Python runtime.

## Verification and remaining findings

[The cleanup audit](docs/codebase_cleanup_audit.md) records output comparisons, test results, measured performance, and unresolved correctness risks. [The refactor plan](REFACTOR_PLAN.md) records the current maintenance constraints.
