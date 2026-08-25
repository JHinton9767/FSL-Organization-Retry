from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence, Tuple

import pandas as pd

from src.sqlCompile import DEFAULT_OUTPUT_PATH, TABLE_NAME
from src.sqlCompile_cohort import (
    DEFAULT_MANUAL_STATUS_PATH,
    MANUAL_STATUS_COLUMNS,
    build_new_member_cohort_tables,
    read_manual_status_rows,
    read_sql_compile_table,
)


RATE_COLUMNS = [
    "Cohort Semester",
    "Cohort Students",
    "Resolved Students",
    "Needs Manual Review",
    "Manual Review Share",
    "Persisted / Active",
    "Graduated",
    "Known Non-Graduate Exits",
    "Other / Unresolved",
    "Persistence Rate",
    "Graduation Rate",
    "Known Exit Rate",
]
OUTCOME_DISTRIBUTION_COLUMNS = [
    "Cohort Semester",
    "Final Outcome Bucket",
    "Student Count",
    "Cohort Students",
    "Share of Cohort",
]
ODD_RECORD_COLUMNS = [
    "Cohort Semester",
    "Cohort Chapter",
    "Student ID",
    "Last Known Semester",
    "Last Known Chapter",
    "Last Known Status",
    "Semester",
    "Chapter",
    "Status",
    "Notes",
]
KNOWN_NON_GRADUATE_BUCKETS = {
    "Dropped/Inactive",
    "Early Alumni",
    "Resigned",
    "Revoked",
    "Suspended",
    "Transfer",
}


@dataclass(frozen=True)
class SqlCompileDashboardTables:
    timeline: pd.DataFrame
    outcomes: pd.DataFrame
    review: pd.DataFrame
    summary: pd.DataFrame
    rate_table: pd.DataFrame
    outcome_distribution: pd.DataFrame
    manual_entry_template: pd.DataFrame
    manual_rows: pd.DataFrame
    selected_semesters: list[str]


def _count_bucket(frame: pd.DataFrame, bucket: str) -> int:
    return int(frame["Final Outcome Bucket"].eq(bucket).sum()) if "Final Outcome Bucket" in frame.columns else 0


def _count_buckets(frame: pd.DataFrame, buckets: set[str]) -> int:
    return int(frame["Final Outcome Bucket"].isin(buckets).sum()) if "Final Outcome Bucket" in frame.columns else 0


def build_dashboard_rate_table(outcomes: pd.DataFrame) -> pd.DataFrame:
    if outcomes.empty:
        return pd.DataFrame(columns=RATE_COLUMNS)

    rows: list[dict[str, object]] = []
    for cohort_semester, group in outcomes.groupby("Cohort Semester", dropna=False):
        cohort_students = int(len(group))
        manual_review = int(group["Needs Manual Form Review"].eq("Yes").sum())
        resolved = max(cohort_students - manual_review, 0)
        persisted = _count_bucket(group, "Active / Still On Roster")
        graduated = _count_bucket(group, "Graduated")
        known_exits = _count_buckets(group, KNOWN_NON_GRADUATE_BUCKETS)
        other = max(resolved - persisted - graduated - known_exits, 0)
        rows.append(
            {
                "Cohort Semester": cohort_semester,
                "Cohort Students": cohort_students,
                "Resolved Students": resolved,
                "Needs Manual Review": manual_review,
                "Manual Review Share": manual_review / cohort_students if cohort_students else 0,
                "Persisted / Active": persisted,
                "Graduated": graduated,
                "Known Non-Graduate Exits": known_exits,
                "Other / Unresolved": other,
                "Persistence Rate": persisted / resolved if resolved else pd.NA,
                "Graduation Rate": graduated / resolved if resolved else pd.NA,
                "Known Exit Rate": known_exits / resolved if resolved else pd.NA,
            }
        )

    result = pd.DataFrame(rows, columns=RATE_COLUMNS)
    if result.empty:
        return result
    result["_sort"] = result["Cohort Semester"].map(_cohort_sort)
    return result.sort_values(["_sort", "Cohort Semester"], na_position="last").drop(columns=["_sort"]).reset_index(drop=True)


def build_outcome_distribution(outcomes: pd.DataFrame) -> pd.DataFrame:
    if outcomes.empty:
        return pd.DataFrame(columns=OUTCOME_DISTRIBUTION_COLUMNS)

    rows: list[dict[str, object]] = []
    cohort_sizes = outcomes.groupby("Cohort Semester", dropna=False)["Student ID"].count().to_dict()
    counts = (
        outcomes.groupby(["Cohort Semester", "Final Outcome Bucket"], dropna=False)["Student ID"]
        .count()
        .reset_index(name="Student Count")
    )
    for row in counts.to_dict("records"):
        cohort_size = int(cohort_sizes.get(row["Cohort Semester"], 0))
        rows.append(
            {
                "Cohort Semester": row["Cohort Semester"],
                "Final Outcome Bucket": row["Final Outcome Bucket"],
                "Student Count": int(row["Student Count"]),
                "Cohort Students": cohort_size,
                "Share of Cohort": int(row["Student Count"]) / cohort_size if cohort_size else 0,
            }
        )
    result = pd.DataFrame(rows, columns=OUTCOME_DISTRIBUTION_COLUMNS)
    result["_sort"] = result["Cohort Semester"].map(_cohort_sort)
    return result.sort_values(["_sort", "Final Outcome Bucket"], na_position="last").drop(columns=["_sort"]).reset_index(drop=True)


def build_manual_entry_template(review: pd.DataFrame) -> pd.DataFrame:
    if review.empty:
        return pd.DataFrame(columns=ODD_RECORD_COLUMNS)
    result = pd.DataFrame(
        {
            "Cohort Semester": review.get("Cohort Semester", pd.Series("", index=review.index)),
            "Cohort Chapter": review.get("Cohort Chapter", pd.Series("", index=review.index)),
            "Student ID": review.get("Student ID", pd.Series("", index=review.index)),
            "Last Known Semester": review.get("Last Known Semester", pd.Series("", index=review.index)),
            "Last Known Chapter": review.get("Last Known Chapter", pd.Series("", index=review.index)),
            "Last Known Status": review.get("Last Known Status", pd.Series("", index=review.index)),
            "Semester": "",
            "Chapter": review.get("Last Known Chapter", pd.Series("", index=review.index)),
            "Status": "",
            "Notes": "",
        }
    )
    return result.loc[:, ODD_RECORD_COLUMNS]


def odd_record_editor_to_manual_rows(edited: pd.DataFrame) -> pd.DataFrame:
    if edited.empty:
        return pd.DataFrame(columns=MANUAL_STATUS_COLUMNS)
    prepared = edited.copy()
    rename_map = {
        "Cohort Semester": "Cohort Semester",
        "Cohort Chapter": "Cohort Chapter",
        "Semester": "Semester",
        "Chapter": "Chapter",
        "Student ID": "Student ID",
        "Status": "Status",
        "Notes": "Notes",
    }
    for column in rename_map:
        if column not in prepared.columns:
            prepared[column] = ""
    return prepared.loc[:, list(rename_map)].rename(columns=rename_map)


def load_dashboard_tables(
    database_path: str | Path = DEFAULT_OUTPUT_PATH,
    manual_status_file: str | Path = DEFAULT_MANUAL_STATUS_PATH,
    table_name: str = TABLE_NAME,
    cohort_semesters: Optional[Sequence[str]] = None,
    all_cohorts: bool = True,
) -> SqlCompileDashboardTables:
    compiled_rows = read_sql_compile_table(database_path, table_name=table_name)
    manual_rows = read_manual_status_rows(manual_status_file)
    timeline, outcomes, review, summary, selected_semesters = build_new_member_cohort_tables(
        compiled_rows,
        manual_rows,
        cohort_semesters=cohort_semesters,
        all_cohorts=all_cohorts,
    )
    return SqlCompileDashboardTables(
        timeline=timeline,
        outcomes=outcomes,
        review=review,
        summary=summary,
        rate_table=build_dashboard_rate_table(outcomes),
        outcome_distribution=build_outcome_distribution(outcomes),
        manual_entry_template=build_manual_entry_template(review),
        manual_rows=manual_rows,
        selected_semesters=selected_semesters,
    )


def _cohort_sort(value: object) -> int:
    from src.sqlCompile_cohort import _semester_sort

    return _semester_sort(value)
