# Refactor Plan

## Simplified target architecture

The project should revolve around one canonical ETL pipeline plus thin downstream consumers.

### Core layers

1. `src/build_canonical_pipeline.py`
   - Keep as the orchestration entry point.
   - Reduce it to stage wiring and top-level workflow decisions.

2. Shared support modules
   - Add dedicated utility modules for:
     - shared text/numeric/rate helpers
     - shared Excel formatting helpers
     - reusable canonical pipeline runtime helpers such as schema/cache/performance helpers
   - Stop using builder files as utility libraries.

3. Downstream report/export builders
   - Keep each builder as a thin canonical-bundle consumer.
   - Move duplicated workbook/rate helpers into shared modules.

4. App layer
   - Keep canonical-first app behavior.
   - Preserve the legacy bridge only as a bounded compatibility adapter.

## Files that should remain

- `src/build_canonical_pipeline.py`
- `src/canonical_bundle.py`
- `app/main.py`
- `app/analysis.py`
- `app/metrics_engine.py`
- `app/status_framework.py`
- `app/config_loader.py`
- `app/io_utils.py`
- downstream builder modules that produce required outputs
- tests
- config files

## Files that should be isolated or treated as legacy

- `src/greek_life_pipeline.py`
  - Keep for now as legacy/manual fallback documentation and compatibility code.
  - Do not let new core behavior depend on it.
- `app/legacy_bridge.py`
  - Keep, but reduce new dependencies on it.
- `powerquery/*.pq`
  - Preserve, but treat as external/manual tooling rather than core pipeline logic.

## Files that should be added

- `src/shared_utils.py`
  - Canonical shared helpers for `clean_text`, numeric coercion, rate masks, rate calculations, GPA/hour summaries, and small text-format helpers.
- `src/excel_utils.py`
  - Canonical shared Excel formatting helpers such as `style_header`, `autosize_columns`, and safe sheet naming.

## Functions and behaviors to combine

- Combine duplicated text and numeric helpers now spread across builders.
- Combine repeated rate calculations:
  - simple rate
  - resolved-only graduation rate
  - yes/no masks
  - mean-or-blank helpers
- Combine repeated workbook formatting helpers.
- Remove sideways imports such as:
  - importing formatting helpers from `build_master_roster`
  - importing `bucket_30_hours` from a downstream snapshot builder
  - importing `clean_text`/`coerce_numeric` from unrelated builders

## Operations that should happen once

- Term normalization
- Status taxonomy resolution
- Graduation evidence gating
- Current-active tagging
- Chapter mapping and chapter provenance resolution
- Reference inventory parsing
- Transcript text parsing

These should remain canonical-pipeline responsibilities and not be recomputed in downstream builders.

## Data that should be normalized early

- Student IDs
- Names and emails
- Term codes and term sort values
- Chapter names
- Roster status buckets
- Academic standing buckets
- Graduation evidence flags and outcome taxonomy flags

## Outputs that must be preserved

- All canonical CSV outputs and QA outputs
- Existing downstream workbook/report outputs
- Streamlit app behavior against canonical bundle
- Existing file/folder naming conventions

## SQL vs pandas vs plain Python

- Keep pandas for workbook/CSV/PDF ingestion and cleaning.
- Keep pandas for the current canonical joins and aggregations.
- Avoid introducing SQL right now because the dominant complexity is messy file ingestion and spreadsheet-oriented export formatting, not database persistence.
- Use plain Python for small parsing helpers, manifest handling, and workbook orchestration only.

## Step-by-step rewrite plan

1. Add `REFACTOR_INVENTORY.md`.
2. Add `REFACTOR_PLAN.md`.
3. Create a shared utility module for text, numeric, rate, and bucketing helpers.
4. Create a shared Excel helper module for header styling, auto-sizing, and safe sheet naming.
5. Rewire downstream builders to consume shared helpers instead of importing from other builders.
6. Rewire app-side and pipeline-side modules that currently depend on downstream-builder utilities.
7. Keep the canonical pipeline behavior intact while removing obvious cross-module utility duplication.
8. Run compile/tests.
9. Re-read the changed files and remove remaining redundant imports, wrappers, and local helper copies that are now obsolete.

## Immediate safe deletions

No hard deletions will be performed in this pass because the repository contains multiple user-facing outputs and legacy/manual workflows that are not fully covered by tests.

Instead, this pass will:

- stop new code from depending on legacy builder-side helpers
- isolate shared behavior into explicit support modules
- preserve old files only where they remain part of current outputs or compatibility paths

## Backward-compatibility risks

- Any function moved out of a builder file could break imports if not rewired everywhere.
- Graduation and current-active logic must remain unchanged while utility extraction happens.
- Canonical pipeline caching and outputs must not change names or locations.

## Validation plan

- Compile changed Python modules.
- Run the existing targeted test suites where available.
- Preserve current CLI entry points.
- Keep output filenames and canonical schema names unchanged.
