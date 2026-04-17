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
