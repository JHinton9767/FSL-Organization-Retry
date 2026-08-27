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
    normalize_status_code,
    read_manual_status_rows,
    read_roster_inventory_table,
    read_sql_compile_table,
)
from src.persistence_outcomes import PERSISTENCE_OUTCOME_ORDER, persistence_outcome_from_status


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
MILESTONE_CHART_COLUMNS = [
    "Milestone",
    "Milestone Sort",
    "Outcome",
    "Share",
    "Count",
    "Denominator",
    "Label",
]
MILESTONE_TABLE_COLUMNS = [
    "Milestone",
    "Term",
    "Measured Students",
    *[
        column
        for outcome in PERSISTENCE_OUTCOME_ORDER
        for column in (outcome, f"{outcome} Count")
    ],
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
MANUAL_CHECKER_SELECT_COLUMN = "Select"
MANUAL_CHECKER_COLUMNS = [MANUAL_CHECKER_SELECT_COLUMN, *ODD_RECORD_COLUMNS]
KNOWN_NON_GRADUATE_BUCKETS = {
    "Chapter Kicked",
    "Dropped/Inactive",
    "Early Alumni",
    "Resigned",
    "Revoked",
    "Suspended",
    "Transfer",
}
SQL_COMPILE_ALL_TIME_LABEL = "All Time"


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


def build_sql_compile_milestone_dashboard(
    timeline: pd.DataFrame,
    outcomes: pd.DataFrame,
    selected_semesters: Optional[Sequence[str]] = None,
    *,
    selection_label: str = SQL_COMPILE_ALL_TIME_LABEL,
    max_years: int = 6,
) -> dict[str, object]:
    filtered_outcomes = _filter_by_selected_semesters(outcomes, selected_semesters)
    empty = {
        "chart_frame": pd.DataFrame(columns=MILESTONE_CHART_COLUMNS),
        "table_frame": pd.DataFrame(columns=MILESTONE_TABLE_COLUMNS),
        "meta": {
            "students": 0,
            "cohort_term": selection_label,
            "distinction": "ALL",
            "max_milestone": "",
            "note": "No new-member cohort rows matched the current selection.",
        },
    }
    if filtered_outcomes.empty:
        return empty

    cohort_students = (
        filtered_outcomes.loc[:, ["Cohort Semester", "Cohort Chapter", "Student ID"]]
        .fillna("")
        .astype(str)
        .apply(lambda column: column.str.strip())
        .replace("", pd.NA)
        .dropna(subset=["Cohort Semester", "Student ID"])
        .drop_duplicates(subset=["Cohort Semester", "Student ID"], keep="first")
        .reset_index(drop=True)
    )
    if cohort_students.empty:
        return empty

    timeline_work = timeline.copy()
    if not timeline_work.empty:
        timeline_work = _ensure_missing_columns(
            timeline_work,
            [
                "Cohort Semester",
                "Student ID",
                "Semester",
                "Status",
                "Status Code",
                "Source",
                "Included In Outcome",
            ],
        )
        for column in ["Cohort Semester", "Student ID", "Semester", "Status", "Status Code", "Source", "Included In Outcome"]:
            timeline_work[column] = timeline_work[column].fillna("").astype(str).str.strip()
        timeline_work["_term_sort"] = timeline_work["Semester"].map(_cohort_sort)
        timeline_work["_status_code"] = timeline_work.apply(_timeline_status_code, axis=1)
        timeline_work["_manual_priority"] = timeline_work["Source"].eq("manual_status").astype(int)
        if "Included In Outcome" in timeline_work.columns:
            timeline_work = timeline_work.loc[timeline_work["Included In Outcome"].eq("Yes")].copy()

    latest_sort = _latest_timeline_sort(timeline_work)
    if latest_sort == 0:
        latest_sort = max([_cohort_sort(value) for value in cohort_students["Cohort Semester"].tolist()] or [0])

    chart_rows: list[dict[str, object]] = []
    table_rows: list[dict[str, object]] = []
    last_milestone = ""
    capped_max_years = max(0, min(int(max_years), 6))

    for offset in range(0, capped_max_years + 1):
        measured = cohort_students.copy()
        if measured.empty:
            continue

        counts = {outcome: 0 for outcome in PERSISTENCE_OUTCOME_ORDER}
        for _, student in measured.iterrows():
            target_sort = _milestone_target_sort(student["Cohort Semester"], offset)
            if latest_sort and target_sort < 999999 and target_sort > latest_sort:
                target_sort = latest_sort
            outcome = _checkpoint_outcome(
                timeline_work,
                student["Cohort Semester"],
                student["Student ID"],
                offset,
                target_sort=target_sort,
            )
            counts[outcome] = int(counts.get(outcome, 0)) + 1

        denominator = int(len(measured))
        milestone_name = _milestone_name(offset)
        last_milestone = milestone_name
        table_row: dict[str, object] = {
            "Milestone": milestone_name,
            "Term": selection_label,
            "Measured Students": denominator,
        }
        for outcome in PERSISTENCE_OUTCOME_ORDER:
            count = int(counts.get(outcome, 0))
            share = count / denominator if denominator else pd.NA
            table_row[outcome] = share
            table_row[f"{outcome} Count"] = count
            chart_rows.append(
                {
                    "Milestone": _milestone_label(offset, selection_label),
                    "Milestone Sort": offset,
                    "Outcome": outcome,
                    "Share": share,
                    "Count": count,
                    "Denominator": denominator,
                    "Label": f"{outcome}<br>{share:.1%}<br>(n={count:,})" if count and share >= 0.085 else "",
                }
            )
        table_rows.append(table_row)

    chart_frame = pd.DataFrame(chart_rows, columns=MILESTONE_CHART_COLUMNS)
    table_frame = pd.DataFrame(table_rows, columns=MILESTONE_TABLE_COLUMNS)
    return {
        "chart_frame": chart_frame.sort_values(["Milestone Sort", "Outcome"]).reset_index(drop=True),
        "table_frame": table_frame.reset_index(drop=True),
        "meta": {
            "students": int(len(cohort_students)),
            "cohort_term": selection_label,
            "distinction": "ALL",
            "max_milestone": last_milestone,
            "note": (
                "Milestone bars use a fixed selected-cohort denominator. Outcome buckets carry forward across later "
                "checkpoints; checkpoints beyond loaded roster coverage use the latest loaded roster term."
            ),
        },
    }


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


def build_manual_checker_queue(review: pd.DataFrame) -> pd.DataFrame:
    template = build_manual_entry_template(review)
    if template.empty:
        return pd.DataFrame(columns=MANUAL_CHECKER_COLUMNS)
    result = template.copy()
    result.insert(0, MANUAL_CHECKER_SELECT_COLUMN, False)
    return result.loc[:, MANUAL_CHECKER_COLUMNS]


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
    roster_inventory = read_roster_inventory_table(database_path)
    manual_rows = read_manual_status_rows(manual_status_file)
    timeline, outcomes, review, summary, selected_semesters = build_new_member_cohort_tables(
        compiled_rows,
        manual_rows,
        roster_inventory=roster_inventory,
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


def _filter_by_selected_semesters(frame: pd.DataFrame, selected_semesters: Optional[Sequence[str]]) -> pd.DataFrame:
    if selected_semesters is None or frame.empty or "Cohort Semester" not in frame.columns:
        return frame.copy()
    selected = {str(value).strip() for value in selected_semesters if str(value).strip()}
    if not selected:
        return frame.iloc[0:0].copy()
    return frame.loc[frame["Cohort Semester"].fillna("").astype(str).str.strip().isin(selected)].copy()


def _timeline_status_code(row: pd.Series) -> str:
    status_code = str(row.get("Status Code", "") or "").strip()
    if status_code:
        return normalize_status_code(status_code)
    return normalize_status_code(row.get("Status", ""))


def _latest_timeline_sort(timeline: pd.DataFrame) -> int:
    if timeline.empty or "_term_sort" not in timeline.columns:
        return 0
    sorts = pd.to_numeric(timeline["_term_sort"], errors="coerce")
    sorts = sorts.loc[sorts.notna() & sorts.lt(999999)]
    return int(sorts.max()) if not sorts.empty else 0


def _milestone_name(offset: int) -> str:
    return "Cohort Year" if offset == 0 else f"{offset} Year"


def _milestone_label(offset: int, selection_label: str) -> str:
    label = str(selection_label or SQL_COMPILE_ALL_TIME_LABEL).strip() or SQL_COMPILE_ALL_TIME_LABEL
    return f"{_milestone_name(offset)}<br>{label}"


def _milestone_target_sort(cohort_semester: object, offset: int) -> int:
    cohort_sort = _cohort_sort(cohort_semester)
    if cohort_sort >= 999999:
        return cohort_sort
    return cohort_sort + (int(offset) * 10)


def _milestone_is_measurable(cohort_semester: object, offset: int, latest_sort: int) -> bool:
    if offset == 0:
        return True
    target_sort = _milestone_target_sort(cohort_semester, offset)
    return target_sort < 999999 and latest_sort >= target_sort


def _checkpoint_outcome(
    timeline: pd.DataFrame,
    cohort_semester: str,
    student_id: str,
    offset: int,
    *,
    target_sort: int | None = None,
) -> str:
    if timeline.empty:
        return "Active" if offset == 0 else "Unknown"

    checkpoint_sort = target_sort if target_sort is not None else _milestone_target_sort(cohort_semester, offset)
    student_rows = timeline.loc[
        timeline["Cohort Semester"].eq(str(cohort_semester).strip())
        & timeline["Student ID"].eq(str(student_id).strip())
    ].copy()
    if student_rows.empty:
        return "Active" if offset == 0 else "Unknown"

    student_rows = student_rows.sort_values(["_term_sort", "_manual_priority", "Semester"], na_position="last")
    row_sorts = pd.to_numeric(student_rows["_term_sort"], errors="coerce")
    before_or_at = student_rows.loc[row_sorts.le(checkpoint_sort)].copy()
    if before_or_at.empty:
        return "Active" if offset == 0 else "Unknown"

    before_or_at["_outcome"] = before_or_at["_status_code"].map(persistence_outcome_from_status)
    terminal = before_or_at.loc[~before_or_at["_outcome"].isin(["Active", "Unknown"])].copy()
    if not terminal.empty:
        return str(terminal.iloc[-1]["_outcome"])

    latest_outcome = str(before_or_at.iloc[-1].get("_outcome", "") or "Unknown").strip() or "Unknown"
    if offset == 0 or latest_outcome != "Active":
        return latest_outcome

    at_or_after = student_rows.loc[row_sorts.ge(checkpoint_sort)]
    return "Active" if not at_or_after.empty else "Unknown"


def _ensure_missing_columns(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = ""
    return result.loc[:, list(columns)]
