from __future__ import annotations

import json
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path

import pandas as pd

from src.sqlCompile import COMPILE_AUDIT_TABLE
from src.sqlCompile_cohort import (
    _prepared_roster_inventory, _prepared_zero_member_periods, _semester_sort,
    _zero_member_period_covers_gap, read_roster_inventory_table, read_sql_compile_table, read_zero_member_periods,
    read_student_name_observations_table,
)
from src.sqlCompile_dashboard import DUPLICATE_NAME_MISMATCH_OUTCOME, _observed_names_by_student_id, _duplicate_name_resolution_lookup, build_sql_compile_milestone_dashboard
from src.sqlCompile_reporting import through_reporting_cutoff
from src.sqlCompile_storage import read_database
from src.sqlCompile_quality import build_coverage, read_coverage, coverage_findings, reconcile_milestones
from src.sqlCompile_records import ROSTER_VIEW, METRIC_VERSION
from src.sqlCompile_sources import read_optional_table, SOURCE_MANIFEST
from src.sqlCompile_audit import audit_warning


COHORT_CHANGE_THRESHOLD = 0.10
OUTCOME_SHARE_CHANGE_THRESHOLD = 0.05


class _PublishedDataParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_payload = False
        self.payloads = []

    def handle_starttag(self, tag, attrs):
        if tag == "script" and dict(attrs).get("id") == "viewer-data":
            self.in_payload = True
            self.payloads.append("")

    def handle_endtag(self, tag):
        if tag == "script":
            self.in_payload = False

    def handle_data(self, data):
        if self.in_payload:
            self.payloads[-1] += data


def read_previous_publication(path: Path) -> dict | None:
    if not path.exists():
        return None
    parser = _PublishedDataParser()
    parser.feed(path.read_text(encoding="utf-8-sig"))
    if len(parser.payloads) != 1:
        raise ValueError("Previous file is not a recognized dashboard publication.")
    payload = json.loads(parser.payloads[0])
    if not isinstance(payload, dict) or payload.get("schema") != 1 or not isinstance(payload.get("units"), list):
        raise ValueError("Previous dashboard aggregate data is not recognized.")
    for row in payload["units"]:
        if (not isinstance(row, list) or len(row) != 5
                or not all(isinstance(row[i], str) for i in (0, 1, 3))
                or type(row[2]) is not int or not 1 <= row[2] <= 6
                or type(row[4]) is not int or row[4] < 0):
            raise ValueError("Previous dashboard aggregate rows are invalid.")
    return payload


def _publication_counts(payload: dict) -> tuple[Counter, Counter, Counter]:
    cohorts, outcomes, eligible = Counter(), Counter(), Counter()
    for semester, chapter, year, outcome, count in payload["units"]:
        if year == 1:
            cohorts[(semester, chapter)] += count
        if outcome != "Future":
            eligible[year] += count
            outcomes[(year, outcome)] += count
    return cohorts, outcomes, eligible


def publication_changes(current: dict, previous: dict | None) -> list[str]:
    if previous is None:
        return []
    warnings = []
    if previous.get("metricVersion") != current.get("metricVersion"):
        warnings.append("Calculation definitions changed. Review separate membership/university views before publishing this version.")
    if previous.get("sourceRevision") and previous.get("sourceRevision") != current.get("sourceRevision"):
        warnings.append("Roster source files or parsing rules changed since the previous publication. Recheck affected coverage decisions and student histories, including small changes below the rate-alert thresholds.")
    new_cohorts, new_outcomes, new_eligible = _publication_counts(current)
    old_cohorts, old_outcomes, old_eligible = _publication_counts(previous)
    if previous.get("dataThrough") != current["dataThrough"]:
        warnings.append(f"Reporting coverage changed from {previous.get('dataThrough', 'unknown')} to {current['dataThrough']}.")
    old_total, new_total = sum(old_cohorts.values()), sum(new_cohorts.values())
    if old_total and abs(new_total - old_total) / old_total >= COHORT_CHANGE_THRESHOLD:
        warnings.append(f"Total cohort size changed from {old_total:,} to {new_total:,} (at least 10%).")
    for cohort in sorted(old_cohorts.keys() | new_cohorts.keys()):
        old, new = old_cohorts[cohort], new_cohorts[cohort]
        if old and abs(new - old) / old >= COHORT_CHANGE_THRESHOLD:
            warnings.append(f"{cohort[0]} / {cohort[1]} cohort size changed from {old:,} to {new:,} (at least 10%).")
    for year, outcome in sorted(old_outcomes.keys() | new_outcomes.keys()):
        if not old_eligible[year] or not new_eligible[year]:
            continue
        old_rate = old_outcomes[(year, outcome)] / old_eligible[year]
        new_rate = new_outcomes[(year, outcome)] / new_eligible[year]
        if abs(new_rate - old_rate) >= OUTCOME_SHARE_CHANGE_THRESHOLD - 1e-12:
            warnings.append(f"Year {year} {outcome}: {old_rate:.1%} to {new_rate:.1%} (at least 5 percentage points; denominators may differ).")
    for view, units in current.get("views", {}).items():
        if view == ROSTER_VIEW:
            continue
        old_units = previous.get("views", {}).get(view)
        if old_units is None:
            warnings.append(f"{view}: no comparable previous measure; review its first publication.")
            continue
        warnings.extend(f"{view}: {message}" for message in publication_changes(
            {"dataThrough": current["dataThrough"], "metricVersion": current.get("metricVersion"), "units": units},
            {"dataThrough": previous.get("dataThrough"), "metricVersion": previous.get("metricVersion"), "units": old_units},
        ))
    return warnings


def build_publication_check(config, tables, payload: dict, previous: dict | None) -> dict:
    warnings = []
    errors = []
    if not tables.new_member_evidence_complete:
        errors.append("Original new-member evidence is unavailable. Recompile all Excel rosters with the updated compiler before relying on these rates; same-semester exits may be missing from cohorts.")
    cutoff = payload["dataThrough"]
    compiled = read_sql_compile_table(config.database)
    roster = read_roster_inventory_table(config.database)
    audit = None
    with read_database(config.database) as connection:
        if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (COMPILE_AUDIT_TABLE,)).fetchone():
            rows = pd.read_sql_query(f'SELECT * FROM "{COMPILE_AUDIT_TABLE}"', connection)
            if len(rows) == 1:
                audit = rows.iloc[0]
    skipped = int(audit["Issue Count"]) if audit is not None and pd.notna(audit.get("Issue Count")) else None
    if skipped is None:
        warnings.append("Skipped-roster audit is unavailable. Recompile with the updated compiler to record it; this does not mean zero skipped files.")
    elif skipped:
        warnings.append(f"The last compile recorded {skipped:,} skipped-source or excluded-record issues. Review the source issues in Record accuracy.")
    through = through_reporting_cutoff(compiled, cutoff)
    missing_chapters = int(through["Chapter"].fillna("").astype(str).str.strip().str.casefold().isin(["", "unknown"]).sum())
    if missing_chapters:
        warnings.append(f"{missing_chapters:,} compiled rows through the cutoff have no recognized chapter name.")
    invalid_terms = int(compiled["Semester"].map(_semester_sort).ge(999999).sum())
    if invalid_terms:
        warnings.append(f"{invalid_terms:,} compiled rows have unrecognized semesters and are excluded from cutoff reporting.")
    unresolved = set(tables.outcomes.loc[tables.outcomes["Last Known Outcome Bucket"].eq(DUPLICATE_NAME_MISMATCH_OUTCOME), "Student ID"])
    cohort_ids = set(tables.outcomes["Student ID"])
    observed_names = _observed_names_by_student_id(tables.outcomes, read_student_name_observations_table(config.database))
    resolutions = _duplicate_name_resolution_lookup(tables.duplicate_name_resolutions)
    unresolved.update(student for student, names in observed_names.items()
                      if len(names) > 1 and student in cohort_ids and student not in resolutions)
    mismatches = len(unresolved)
    if mismatches:
        warnings.append(f"{mismatches:,} student IDs still have unresolved name mismatches.")
    rechecks = tables.duplicate_name_rechecks["Student ID"].nunique()
    if rechecks:
        warnings.append(f"{rechecks:,} student IDs remain on the saved name recheck list.")

    gaps = []
    inventory = _prepared_roster_inventory(through_reporting_cutoff(roster, cutoff))
    if inventory.empty:
        warnings.append("Roster inventory is unavailable through the cutoff; chapter coverage cannot be verified. Recompile to record the inventory.")
    else:
        exceptions = _prepared_zero_member_periods(read_zero_member_periods(config.zero_member_periods))
        first_year = min(int(semester.split()[-1]) for semester in inventory["_semester_normalized"])
        terms = [f"{season} {year}" for year in range(first_year, int(cutoff.split()[-1]) + 1) for season in ("Spring", "Fall")]
        terms = [term for term in terms if _semester_sort(term) <= _semester_sort(cutoff)]
        for key, group in inventory.groupby("_chapter_key"):
            observed = set(group["_term_sort"])
            for term in terms:
                sort = _semester_sort(term)
                if sort < min(observed) or sort in observed or _zero_member_period_covers_gap(exceptions, key, sort, sort):
                    continue
                gaps.append({"Chapter": str(group["Chapter"].iloc[0]), "Semester": term})
        if gaps:
            warnings.append(f"{len(gaps):,} chapter/semester roster gaps need review. These can reflect real chapter departures, not necessarily missing files; zero-member exceptions are excluded.")
    changes = publication_changes(payload, previous)
    warnings.extend(changes)
    if tables.outcomes["Student ID"].duplicated().any():
        errors.append("Cohort records contain duplicate students.")
    if not tables.outcomes["Student ID"].str.fullmatch(r"A0\d{7}").all():
        errors.append("Cohort records contain invalid student identifiers.")
    if compiled.duplicated(["Student ID", "Semester"]).any():
        errors.append("Compiled records contain duplicate student/semester rows.")
    for view, units in payload.get("views", {ROSTER_VIEW: payload["units"]}).items():
        dashboard = build_sql_compile_milestone_dashboard(tables.timeline, tables.outcomes, tables.selected_semesters,
            chart_milestone_offsets=[1, 2, 3, 4, 5, 6], reporting_cutoff=cutoff,
            record_history=getattr(tables, "record_history", None), outcome_view=view)
        errors.extend(f"{view}: {error}" for error in reconcile_milestones(dashboard, units))
    detail = dashboard["detail_frame"]
    conflict_count = int(detail.loc[detail.get("Evidence Conflict", pd.Series("", index=detail.index)).fillna("").ne(""), "Student ID"].nunique())
    if conflict_count:
        warnings.append(f"{conflict_count:,} students have contradictory university/degree evidence. Affected independent outcomes remain Unknown until reviewed.")
    coverage = build_coverage(roster, cutoff, read_coverage(config.coverage_reviews), read_zero_member_periods(config.zero_member_periods))
    coverage_errors, coverage_warnings = coverage_findings(coverage)
    errors.extend(coverage_errors)
    warnings.extend(coverage_warnings)
    manifest = read_optional_table(config.database, SOURCE_MANIFEST)
    if manifest.empty:
        warnings.append("Detailed source observations are unavailable. Recompile to enable full record tracing and independent same-term evidence.")
    for path in (*config.data_paths[1:],):
        message = audit_warning(path)
        if message:
            warnings.append(message)
    return {
        "checked_at": payload["published"], "reporting_cutoff": cutoff,
        "cohort_students": sum(row[4] for row in payload["units"] if row[2] == 1),
        "new_member_evidence_complete": tables.new_member_evidence_complete,
        "source_files": int(audit["Source Files"]) if audit is not None and pd.notna(audit.get("Source Files")) else None,
        "skipped_sheet_file_issues": skipped, "unresolved_duplicate_ids": int(mismatches), "name_rechecks": int(rechecks),
        "missing_chapter_rows": missing_chapters, "invalid_semester_rows": invalid_terms,
        "deferred_roster_rows": int(compiled["Semester"].map(_semester_sort).between(_semester_sort(cutoff) + 1, 999998).sum()),
        "deferred_manual_rows": int(tables.manual_rows["Semester"].map(_semester_sort).between(_semester_sort(cutoff) + 1, 999998).sum()),
        "chapter_roster_gaps": gaps, "comparison_available": previous is not None,
        "large_changes": changes, "warnings": warnings, "errors": errors,
        "metric_version": METRIC_VERSION, "data_revision": payload.get("dataRevision"),
        "source_revision": payload.get("sourceRevision"), "source_manifest": manifest.to_dict("records"),
        "evidence_conflicts": conflict_count, "coverage": coverage.to_dict("records"),
        "coverage_verified": bool(not coverage.empty and not coverage_errors and not coverage_warnings),
    }
