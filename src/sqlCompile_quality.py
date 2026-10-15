"""Owner-reviewed source coverage and checks that must agree before publication."""
from __future__ import annotations

from collections import Counter
from pathlib import Path

import pandas as pd

from src.sqlCompile_cohort import (_semester_sort, _normalize_chapter_key, _prepared_zero_member_periods,
                                   _zero_member_period_covers_gap)
from src.sqlCompile_storage import read_review_csv, write_review_csv

COVERAGE_COLUMNS = ["Chapter", "Semester", "Coverage", "Source", "Reviewed By", "Notes"]
COVERAGE_STATES = ["Unreviewed", "Complete", "Preliminary", "Missing", "Zero members", "Not expected"]


def normalize_coverage(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.reindex(columns=COVERAGE_COLUMNS, fill_value="").fillna("").astype(str).apply(lambda c: c.str.strip())
    result = result.loc[result.ne("").any(axis=1)].copy()
    if result.empty:
        return result
    if result["Chapter"].eq("").any() or result["Semester"].map(_semester_sort).ge(999999).any():
        raise ValueError("Coverage rows require a chapter and recognized semester.")
    if not result["Coverage"].isin(COVERAGE_STATES).all():
        raise ValueError("Select a supported coverage status for every row.")
    reviewed = result["Coverage"].ne("Unreviewed")
    if (reviewed & (result["Source"].eq("") | result["Reviewed By"].eq(""))).any():
        raise ValueError("Reviewed coverage requires its source and reviewer, including zero-member or not-expected terms.")
    keys = pd.DataFrame({"chapter": result["Chapter"].map(_normalize_chapter_key), "term": result["Semester"].map(_semester_sort)})
    if keys.duplicated().any():
        raise ValueError("Keep one coverage decision per chapter and semester.")
    return result.reset_index(drop=True)


def read_coverage(path: Path) -> pd.DataFrame:
    return normalize_coverage(read_review_csv(path, COVERAGE_COLUMNS, create=False))


def save_coverage(frame: pd.DataFrame, path: Path, *, expected_rows=None) -> Path:
    return write_review_csv(frame, path, normalize_coverage, expected_rows=expected_rows)


def build_coverage(inventory: pd.DataFrame, cutoff: str, saved: pd.DataFrame, zero_periods: pd.DataFrame) -> pd.DataFrame:
    rows = {}
    cutoff_sort = _semester_sort(cutoff)
    work = inventory.copy()
    if not work.empty:
        work["_sort"] = work["Semester"].map(_semester_sort)
        work = work.loc[work["_sort"].le(cutoff_sort)].copy()
        work["_chapter"] = work["Chapter"].map(_normalize_chapter_key)
        zero = _prepared_zero_member_periods(zero_periods)
        for key, group in work.groupby("_chapter"):
            first = min(group["_sort"])
            terms = {f"{season} {year}" for year in range(first // 10, cutoff_sort // 10 + 1) for season in ("Spring", "Fall")}
            terms.update(group["Semester"])
            for term in sorted(terms, key=_semester_sort):
                sort = _semester_sort(term)
                if not first <= sort <= cutoff_sort:
                    continue
                observed = group.loc[group["_sort"].eq(sort)]
                exception = _zero_member_period_covers_gap(zero, key, sort, sort)
                rows[(key, sort)] = {
                    "Chapter": group["Chapter"].iloc[0], "Semester": term,
                    "Coverage": "Zero members" if exception else "Unreviewed",
                    "Source": "Existing zero-member exception" if exception else " | ".join(sorted(set(observed["Source File"]))),
                    "Reviewed By": "Legacy decision (author not recorded)" if exception else "", "Notes": "",
                    "Observed Rows": int(observed["Student Rows"].sum()),
                    "Roster Versions": " | ".join(sorted(set(observed["Roster Pass"]))),
                }
    for row in saved.to_dict("records"):
        sort = _semester_sort(row["Semester"])
        if sort > cutoff_sort:
            continue
        key = (_normalize_chapter_key(row["Chapter"]), sort)
        rows[key] = {**rows.get(key, {"Observed Rows": 0, "Roster Versions": ""}), **row}
    return pd.DataFrame(rows.values(), columns=[*COVERAGE_COLUMNS, "Observed Rows", "Roster Versions"])


def coverage_findings(coverage: pd.DataFrame) -> tuple[list[str], list[str]]:
    errors, warnings = [], []
    for row in coverage.to_dict("records"):
        name = f"{row['Chapter']} / {row['Semester']}"
        if row["Coverage"] == "Complete" and not row["Observed Rows"]:
            errors.append(f"{name}: marked complete but no compiled student observations exist. Use Zero members if verified.")
        elif row["Coverage"] == "Zero members" and row["Observed Rows"]:
            errors.append(f"{name}: zero-member decision conflicts with observed student rows.")
        elif row["Coverage"] in {"Missing", "Preliminary"}:
            errors.append(f"{name}: {row['Coverage'].lower()} coverage is inside the complete reporting cutoff.")
    unreviewed = int(coverage["Coverage"].eq("Unreviewed").sum())
    if unreviewed:
        warnings.append(f"{unreviewed:,} chapter/semester coverage decisions remain unreviewed. Unknown coverage is not confirmation of a chapter removal.")
    return errors, warnings


def reconcile_milestones(dashboard: dict, units: list | None = None) -> list[str]:
    errors = []
    detail = dashboard["detail_frame"]
    if detail.empty:
        return ["No student checkpoint records are available."]
    if detail.duplicated(["Student ID", "Milestone Sort"]).any():
        errors.append("A student appears more than once at the same checkpoint.")
    counts = detail.groupby("Milestone Sort")["Student ID"].nunique()
    if counts.nunique() > 1 or not counts.eq(dashboard["meta"]["students"]).all():
        errors.append("Checkpoint student totals do not reconcile to the selected cohorts.")
    table = dashboard["table_frame"]
    for row in table.to_dict("records"):
        count = sum(int(value) for key, value in row.items() if key.endswith(" Count"))
        if count != row["Measured Students"]:
            errors.append(f"{row['Milestone']}: outcome counts do not equal the eligible denominator.")
    if units is not None:
        columns = ["Cohort Semester", "Cohort Chapter", "Milestone Sort", "P&G Outcome Bucket"]
        expected = Counter(tuple(row) for row in detail[columns].itertuples(index=False, name=None))
        actual = Counter()
        for semester, chapter, year, outcome, count in units:
            actual[(semester, chapter, year, outcome)] += count
        if actual != expected:
            errors.append("Published counts do not match the student checkpoint records.")
    return errors
