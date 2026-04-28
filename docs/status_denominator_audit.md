# Status and Denominator Audit

## Problem that triggered the audit

The app was presenting two different "unknown" counts:

- a broader excluded population used by resolved-only metric views
- a narrower truly unresolved population shown in some status distributions

Those were being interpreted as if they were the same thing, which made chapter graduation rankings difficult to trust.

## Current standardized outcome taxonomy

All app and canonical summary logic now uses:

- `Graduated`
- `Resolved Non-Graduate Exit`
- `Still Active`
- `Truly Unknown / Unresolved`
- `Other / Unmapped`

Derived flags:

- `is_resolved_outcome`
- `is_active_outcome`
- `is_unknown_outcome`
- `is_graduated`
- `is_known_non_graduate_exit`

## Denominator rules

### Full Population

- numerator: metric-specific numerator
- denominator: metric-specific eligible denominator over the full filtered population
- includes still-active and unresolved students whenever they are otherwise eligible

### Resolved Outcomes Only

- numerator: same metric-specific numerator
- denominator: same metric-specific eligible denominator after excluding:
  - `Still Active`
  - `Truly Unknown / Unresolved`
  - `Other / Unmapped`

### Why counts differed before

- the larger count represented all students excluded from resolved-only views
- the smaller count represented only the truly unresolved subgroup
- the labels were too broad, so both appeared to mean "unknown"

## Chapter assignment provenance

The canonical roster layer now keeps:

- `chapter_assignment_source`
- `chapter_assignment_confidence`
- `chapter_assignment_notes`

Source values:

- `original`
- `matched_by_id_name`
- `matched_by_id`
- `inferred_from_file_name`
- `inferred_from_sheet_name`
- `unresolved`

## App presentation rules

- graduation-focused rankings default to `Resolved Outcomes Only`
- tables expose resolved counts, still-active counts, truly-unknown counts, and excluded totals
- audit views expose raw status counts, standardized status counts, and chapter-assignment-source counts

## Graduation evidence correction

Headcount logic is intentionally frozen and was not changed for this correction.

Graduation is no longer assigned from broad text matches alone. A student can be classified as `Graduated` only when there is confirmed manual evidence from `Copy of Rosters`, specifically an explicit graduated status on roster history sourced from `Copy of Rosters`.

Graduation-list matches without `Copy of Rosters` confirmation are kept as audit clues only and do not count as graduated outcomes.

Disappearance from later records is not graduation evidence. If there is no confirmed graduation signal and no resolved non-graduate exit, the student remains `Truly Unknown / Unresolved`.

The canonical run writes `graduation_status_audit.csv` with:

- total unique students used for graduation calculations
- students marked `Graduated`
- students with confirmed graduation evidence
- graduation claims corrected because evidence was missing
- active, unknown, and resolved non-graduate counts
- graduation evidence source counts
- duplicate-student checks for graduation calculations
- warnings for suspiciously high graduation rates
