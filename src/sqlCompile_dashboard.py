from __future__ import annotations

import re
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
    read_student_name_observations_table,
    read_manual_status_rows,
    read_roster_inventory_table,
    read_student_name_table,
    read_sql_compile_table,
)
from src.path_config import ROOT
from src.persistence_outcomes import PERSISTENCE_OUTCOME_ORDER, persistence_outcome_from_status


FUTURE_MILESTONE_BUCKET = "Future"
DUPLICATE_NAME_MISMATCH_OUTCOME = "Duplicate ID / Name Mismatch"
DUPLICATE_NAME_RESOLUTION_COLUMNS = ["Student ID", "Student Name", "Notes"]
DEFAULT_DUPLICATE_NAME_RESOLUTION_PATH = ROOT / "config" / "sqlCompile_duplicate_name_resolutions.csv"
PG_CHART_BREAKDOWN_OVERALL = "Overall"
PG_CHART_BREAKDOWN_MILESTONE = PG_CHART_BREAKDOWN_OVERALL
PG_CHART_BREAKDOWN_SEMESTER = "Semester joined"
PG_CHART_BREAKDOWN_CHAPTER = "Chapter joined"
PG_CHART_BREAKDOWN_OPTIONS = [
    PG_CHART_BREAKDOWN_OVERALL,
    PG_CHART_BREAKDOWN_SEMESTER,
    PG_CHART_BREAKDOWN_CHAPTER,
]
PG_GRADUATION_MILESTONE_OFFSETS = (1, 2, 3, 4, 5, 6)
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
    "Chart Group",
    "Milestone Name",
    "Milestone Status",
    "Outcome",
    "Share",
    "Count",
    "Denominator",
    "Eligible Students",
    "Future Students",
    "Cohort Students",
    "Label",
]
MILESTONE_TABLE_COLUMNS = [
    "Milestone",
    "Term",
    "Milestone Status",
    "Measured Students",
    "Future Students",
    *[
        column
        for outcome in PERSISTENCE_OUTCOME_ORDER
        for column in (outcome, f"{outcome} Count")
    ],
]
MILESTONE_CHART_TABLE_COLUMNS = [
    "Chart Group",
    "Milestone",
    "Milestone Status",
    "Outcome",
    "Share",
    "Count",
    "Eligible Students",
    "Future Students",
    "Cohort Students",
]
MILESTONE_DETAIL_COLUMNS = [
    "Cohort Semester",
    "Cohort Chapter",
    "Student ID",
    "Milestone",
    "Milestone Sort",
    "Milestone Status",
    "P&G Outcome Bucket",
]
ODD_RECORD_COLUMNS = [
    "Cohort Semester",
    "Cohort Chapter",
    "Student ID",
    "Student Name",
    "Last Known Semester",
    "Last Known Chapter",
    "Last Known Status",
    "Semester",
    "Chapter",
    "Status",
    "Notes",
]
LAST_KNOWN_STATUS_COLUMNS = [
    "Cohort Semester",
    "Cohort Chapter",
    "Student ID",
    "Student Name",
    "Last Known Semester",
    "Last Known Chapter",
    "Last Known Status",
    "Last Known Outcome Bucket",
    "Needs Manual Form Review",
    "Manual Status Applied",
    "Semester",
    "Chapter",
    "Status",
    "Notes",
]
MANUAL_CHECKER_SELECT_COLUMN = "Select"
MANUAL_CHECKER_COLUMNS = [MANUAL_CHECKER_SELECT_COLUMN, *LAST_KNOWN_STATUS_COLUMNS]
KNOWN_NON_GRADUATE_BUCKETS = {
    "Chapter Kicked",
    "Dropped/Inactive",
    "Dropped/Resigned",
    "Early Alumni",
    "Inactive/Suspended",
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
    manual_checker_template: pd.DataFrame
    manual_rows: pd.DataFrame
    duplicate_name_resolutions: pd.DataFrame
    selected_semesters: list[str]


def _count_bucket(frame: pd.DataFrame, bucket: str) -> int:
    return int(frame["Final Outcome Bucket"].eq(bucket).sum()) if "Final Outcome Bucket" in frame.columns else 0


def _count_buckets(frame: pd.DataFrame, buckets: set[str]) -> int:
    return int(frame["Final Outcome Bucket"].isin(buckets).sum()) if "Final Outcome Bucket" in frame.columns else 0


def _rate_columns(group_columns: Sequence[str]) -> list[str]:
    return [*group_columns, *RATE_COLUMNS[1:]]


def build_dashboard_rate_table(
    outcomes: pd.DataFrame,
    group_columns: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    grouping = [str(column) for column in (group_columns or ["Cohort Semester"]) if str(column)]
    if not grouping:
        grouping = ["Cohort Semester"]
    columns = _rate_columns(grouping)
    if outcomes.empty:
        return pd.DataFrame(columns=columns)

    prepared = outcomes.copy()
    for column in grouping:
        if column not in prepared.columns:
            prepared[column] = ""
        prepared[column] = prepared[column].fillna("").astype(str).str.strip()
    if "Needs Manual Form Review" not in prepared.columns:
        prepared["Needs Manual Form Review"] = "No"

    rows: list[dict[str, object]] = []
    groupby_key: str | list[str] = grouping[0] if len(grouping) == 1 else grouping
    for key, group in prepared.groupby(groupby_key, dropna=False):
        key_values = (key,) if len(grouping) == 1 else tuple(key)
        group_values = {column: key_values[index] for index, column in enumerate(grouping)}
        cohort_students = int(len(group))
        manual_review = int(group["Needs Manual Form Review"].eq("Yes").sum())
        resolved = max(cohort_students - manual_review, 0)
        persisted = _count_bucket(group, "Active / Still On Roster")
        graduated = _count_bucket(group, "Graduated")
        known_exits = _count_buckets(group, KNOWN_NON_GRADUATE_BUCKETS)
        other = max(resolved - persisted - graduated - known_exits, 0)
        rows.append(
            {
                **group_values,
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

    result = pd.DataFrame(rows, columns=columns)
    if result.empty:
        return result
    sort_columns: list[str] = []
    if "Cohort Semester" in result.columns:
        result["_cohort_sort"] = result["Cohort Semester"].map(_cohort_sort)
        sort_columns.append("_cohort_sort")
    sort_columns.extend(column for column in grouping if column in result.columns)
    result = result.sort_values(sort_columns, na_position="last") if sort_columns else result
    return result.drop(columns=["_cohort_sort"], errors="ignore").reset_index(drop=True)


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


def attach_student_names(frame: pd.DataFrame, student_names: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if result.empty:
        if "Student Name" not in result.columns:
            result["Student Name"] = pd.Series(dtype="object")
        return result
    if "Student Name" not in result.columns:
        result["Student Name"] = ""
    if student_names.empty or "Student ID" not in result.columns:
        return result

    names = student_names.copy()
    for column in ["Student ID", "Student Name"]:
        if column not in names.columns:
            names[column] = ""
        names[column] = names[column].fillna("").astype(str).str.strip()
    names = names.loc[names["Student ID"].ne("") & names["Student Name"].ne("")].drop_duplicates(
        subset=["Student ID"],
        keep="last",
    )
    if names.empty:
        return result

    name_lookup = names.set_index("Student ID")["Student Name"]
    student_id = result["Student ID"].fillna("").astype(str).str.strip()
    mapped = student_id.map(name_lookup).fillna("")
    current = result["Student Name"].fillna("").astype(str).str.strip()
    result["Student Name"] = current.where(current.ne(""), mapped)
    return result


def _ensure_duplicate_name_resolution_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = _ensure_missing_columns(frame, DUPLICATE_NAME_RESOLUTION_COLUMNS)
    for column in DUPLICATE_NAME_RESOLUTION_COLUMNS:
        result[column] = result[column].fillna("").astype(str).str.strip()
    return result.loc[:, DUPLICATE_NAME_RESOLUTION_COLUMNS]


def ensure_duplicate_name_resolution_file(
    path: str | Path = DEFAULT_DUPLICATE_NAME_RESOLUTION_PATH,
) -> Path:
    destination = Path(path)
    if not destination.is_absolute():
        destination = ROOT / destination
    if not destination.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=DUPLICATE_NAME_RESOLUTION_COLUMNS).to_csv(destination, index=False)
    return destination


def read_duplicate_name_resolution_rows(
    path: str | Path = DEFAULT_DUPLICATE_NAME_RESOLUTION_PATH,
    *,
    create_if_missing: bool = True,
) -> pd.DataFrame:
    resolution_path = ensure_duplicate_name_resolution_file(path) if create_if_missing else Path(path)
    if not resolution_path.is_absolute():
        resolution_path = ROOT / resolution_path
    if not resolution_path.exists():
        return pd.DataFrame(columns=DUPLICATE_NAME_RESOLUTION_COLUMNS)
    try:
        frame = pd.read_csv(resolution_path, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=DUPLICATE_NAME_RESOLUTION_COLUMNS)
    return _ensure_duplicate_name_resolution_columns(frame)


def write_duplicate_name_resolution_rows(
    frame: pd.DataFrame,
    path: str | Path = DEFAULT_DUPLICATE_NAME_RESOLUTION_PATH,
) -> Path:
    destination = ensure_duplicate_name_resolution_file(path)
    _ensure_duplicate_name_resolution_columns(frame).to_csv(destination, index=False)
    return destination


def append_duplicate_name_resolution_rows(
    frame: pd.DataFrame,
    path: str | Path = DEFAULT_DUPLICATE_NAME_RESOLUTION_PATH,
) -> tuple[Path, int]:
    incoming = _ensure_duplicate_name_resolution_columns(frame)
    incoming = incoming.loc[incoming["Student ID"].ne("") & incoming["Student Name"].ne("")].copy()
    destination = ensure_duplicate_name_resolution_file(path)
    if incoming.empty:
        return destination, 0

    existing = read_duplicate_name_resolution_rows(destination)
    combined = pd.concat([existing, incoming], ignore_index=True)
    combined = combined.drop_duplicates(subset=["Student ID"], keep="last")
    write_duplicate_name_resolution_rows(combined, destination)
    return destination, len(incoming)


def _duplicate_name_resolution_lookup(resolutions: Optional[pd.DataFrame]) -> dict[str, str]:
    if resolutions is None or resolutions.empty:
        return {}
    prepared = _ensure_duplicate_name_resolution_columns(resolutions)
    prepared = prepared.loc[prepared["Student ID"].ne("") & prepared["Student Name"].ne("")].copy()
    if prepared.empty:
        return {}
    prepared = prepared.drop_duplicates(subset=["Student ID"], keep="last")
    return prepared.set_index("Student ID")["Student Name"].to_dict()


def _normalized_student_name_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _observed_names_by_student_id(outcomes: pd.DataFrame, name_observations: pd.DataFrame) -> dict[str, list[str]]:
    source = name_observations.copy() if name_observations is not None and not name_observations.empty else pd.DataFrame()
    if source.empty:
        source = outcomes.loc[:, [column for column in ["Student ID", "Student Name"] if column in outcomes.columns]].copy()
    source = _ensure_missing_columns(source, ["Student ID", "Student Name", "Observation Count"])
    if source.empty:
        return {}

    for column in ["Student ID", "Student Name"]:
        source[column] = source[column].fillna("").astype(str).str.strip()
    source["Observation Count"] = pd.to_numeric(source["Observation Count"], errors="coerce").fillna(0)
    source["_name_key"] = source["Student Name"].map(_normalized_student_name_key)
    source = source.loc[source["Student ID"].ne("") & source["_name_key"].ne("")].copy()
    if source.empty:
        return {}

    source = source.sort_values(
        ["Student ID", "Observation Count", "Student Name"],
        ascending=[True, False, True],
        na_position="last",
    )
    names_by_id: dict[str, list[str]] = {}
    for student_id, group in source.groupby("Student ID", sort=False):
        names: list[str] = []
        seen_keys: set[str] = set()
        for _, row in group.iterrows():
            name = str(row.get("Student Name", "") or "").strip()
            key = _normalized_student_name_key(name)
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            names.append(name)
        if names:
            names_by_id[str(student_id)] = names
    return names_by_id


def _row_with_latest_last_known(group: pd.DataFrame) -> pd.Series:
    work = group.copy()
    work["_last_known_sort"] = work["Last Known Semester"].map(_cohort_sort)
    work["_cohort_sort"] = work["Cohort Semester"].map(_cohort_sort)
    return work.sort_values(
        ["_last_known_sort", "_cohort_sort", "Cohort Semester"],
        na_position="last",
    ).iloc[-1]


def _row_with_earliest_cohort(group: pd.DataFrame) -> pd.Series:
    work = group.copy()
    work["_cohort_sort"] = work["Cohort Semester"].map(_cohort_sort)
    return work.sort_values(["_cohort_sort", "Cohort Semester"], na_position="last").iloc[0]


def consolidate_duplicate_student_outcomes(
    outcomes: pd.DataFrame,
    name_observations: Optional[pd.DataFrame] = None,
    duplicate_name_resolutions: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    if outcomes.empty or "Student ID" not in outcomes.columns:
        return outcomes.copy()

    columns = list(outcomes.columns)
    extra_columns = [column for column in ["Last Known Outcome Bucket", "Notes"] if column not in columns]
    work = _ensure_missing_columns(outcomes, [*columns, *extra_columns])
    for column in [
        "Cohort Semester",
        "Cohort Chapter",
        "Student ID",
        "Student Name",
        "Last Known Semester",
        "Last Known Chapter",
        "Last Known Status",
        "Last Known Status Code",
        "Final Outcome Bucket",
        "Needs Manual Form Review",
        "Manual Status Applied",
        "Last Known Outcome Bucket",
        "Notes",
    ]:
        if column in work.columns:
            work[column] = work[column].fillna("").astype(str).str.strip()

    duplicate_ids = set(
        work.loc[work["Student ID"].ne(""), "Student ID"]
        .value_counts()
        .loc[lambda counts: counts.gt(1)]
        .index
        .tolist()
    )
    if not duplicate_ids:
        return work.loc[:, [*columns, *extra_columns]].reset_index(drop=True)

    observed_names = _observed_names_by_student_id(work, name_observations if name_observations is not None else pd.DataFrame())
    resolved_names = _duplicate_name_resolution_lookup(duplicate_name_resolutions)
    rows: list[pd.Series] = []
    non_duplicates = work.loc[~work["Student ID"].isin(duplicate_ids)].copy()
    rows.extend(row for _, row in non_duplicates.iterrows())

    for student_id, group in work.loc[work["Student ID"].isin(duplicate_ids)].groupby("Student ID", sort=False):
        resolved_name = str(resolved_names.get(str(student_id), "") or "").strip()
        names = [resolved_name] if resolved_name else observed_names.get(str(student_id), [])
        if not names:
            names = [
                name
                for name in group["Student Name"].fillna("").astype(str).str.strip().tolist()
                if _normalized_student_name_key(name)
            ]
        name_keys = {_normalized_student_name_key(name) for name in names if _normalized_student_name_key(name)}
        earliest = _row_with_earliest_cohort(group)
        latest = _row_with_latest_last_known(group)
        row = earliest.copy()
        for column in [
            "Last Known Semester",
            "Last Known Chapter",
            "Last Known Status",
            "Last Known Status Code",
            "Final Outcome Bucket",
            "Needs Manual Form Review",
            "Manual Status Applied",
            "Last Known Outcome Bucket",
        ]:
            if column in row.index:
                row[column] = latest.get(column, "")
        if names:
            row["Student Name"] = " | ".join(names) if len(name_keys) > 1 else names[0]
        if len(name_keys) > 1:
            row["Final Outcome Bucket"] = "Needs Manual Form Review"
            row["Needs Manual Form Review"] = "Yes"
            row["Last Known Outcome Bucket"] = DUPLICATE_NAME_MISMATCH_OUTCOME
            row["Notes"] = f"Duplicate Student ID has mismatched observed names: {' | '.join(names)}"
        rows.append(row)

    result = pd.DataFrame(rows)
    result["_cohort_sort"] = result["Cohort Semester"].map(_cohort_sort)
    result = result.sort_values(["_cohort_sort", "Cohort Semester", "Cohort Chapter", "Student ID"], na_position="last")
    return result.drop(columns=["_cohort_sort"], errors="ignore").loc[:, [*columns, *extra_columns]].reset_index(drop=True)


def build_sql_compile_milestone_dashboard(
    timeline: pd.DataFrame,
    outcomes: pd.DataFrame,
    selected_semesters: Optional[Sequence[str]] = None,
    *,
    selected_chapters: Optional[Sequence[str]] = None,
    selection_label: str = SQL_COMPILE_ALL_TIME_LABEL,
    max_years: int = 6,
    chart_breakdown: str = PG_CHART_BREAKDOWN_OVERALL,
    chart_milestone_offset: int = 6,
    chart_milestone_offsets: Optional[Sequence[int]] = None,
) -> dict[str, object]:
    chart_breakdown = _normalize_pg_chart_breakdown(chart_breakdown)
    capped_max_years = max(0, min(int(max_years), 6))
    selected_offsets = _normalize_pg_milestone_offsets(
        chart_milestone_offsets if chart_milestone_offsets is not None else [chart_milestone_offset],
        max_years=capped_max_years,
    )
    if chart_breakdown == PG_CHART_BREAKDOWN_OVERALL and chart_milestone_offsets is None:
        selected_offsets = _normalize_pg_milestone_offsets(
            PG_GRADUATION_MILESTONE_OFFSETS,
            max_years=capped_max_years,
        )
    if not selected_offsets:
        fallback_offsets = PG_GRADUATION_MILESTONE_OFFSETS if chart_breakdown == PG_CHART_BREAKDOWN_OVERALL else (6,)
        selected_offsets = _normalize_pg_milestone_offsets(
            fallback_offsets,
            max_years=capped_max_years,
        )
    if chart_breakdown != PG_CHART_BREAKDOWN_OVERALL:
        selected_offsets = selected_offsets[:1]
    chart_milestone_label = _milestone_selection_label(selected_offsets)
    filtered_outcomes = _filter_by_selected_semesters(outcomes, selected_semesters)
    filtered_outcomes = _filter_by_selected_chapters(filtered_outcomes, selected_chapters)
    empty = {
        "chart_frame": pd.DataFrame(columns=MILESTONE_CHART_COLUMNS),
        "table_frame": pd.DataFrame(columns=MILESTONE_TABLE_COLUMNS),
        "chart_table_frame": pd.DataFrame(columns=MILESTONE_CHART_TABLE_COLUMNS),
        "detail_frame": pd.DataFrame(columns=MILESTONE_DETAIL_COLUMNS),
        "meta": {
            "students": 0,
            "cohort_term": selection_label,
            "distinction": "ALL",
            "max_milestone": "",
            "note": "No new-member cohort rows matched the current selection.",
            "chart_breakdown": chart_breakdown,
            "chart_milestone": chart_milestone_label,
        },
    }
    if filtered_outcomes.empty:
        return empty

    cohort_students = (
        _ensure_missing_columns(
            filtered_outcomes,
            ["Cohort Semester", "Cohort Chapter", "Student ID", "Last Known Outcome Bucket"],
        )
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

    timeline_groups: dict[tuple[str, str], pd.DataFrame] = {}
    if not timeline_work.empty:
        sort_columns = ["_term_sort", "_manual_priority", "Semester"]
        for key, group in timeline_work.groupby(["Cohort Semester", "Student ID"], sort=False):
            timeline_groups[(str(key[0]).strip(), str(key[1]).strip())] = group.sort_values(
                sort_columns,
                na_position="last",
            )

    chart_rows: list[dict[str, object]] = []
    chart_table_rows: list[dict[str, object]] = []
    detail_rows: list[dict[str, object]] = []
    table_rows: list[dict[str, object]] = []
    last_milestone = ""

    for offset in selected_offsets:
        measurable_mask = cohort_students["Cohort Semester"].map(
            lambda value: _milestone_is_measurable(value, offset, latest_sort)
        )
        measured = cohort_students.loc[measurable_mask].copy()
        future_students = cohort_students.loc[~measurable_mask].copy()
        counts = {outcome: 0 for outcome in PERSISTENCE_OUTCOME_ORDER}
        chart_groups: dict[str, dict[str, object]] = {}
        milestone_name = _milestone_name(offset)
        for _, student in future_students.iterrows():
            chart_group = _chart_group_state(chart_groups, student, offset, chart_breakdown)
            chart_group["future"] = int(chart_group["future"]) + 1
            detail_rows.append(
                {
                    "Cohort Semester": student["Cohort Semester"],
                    "Cohort Chapter": student.get("Cohort Chapter", ""),
                    "Student ID": student["Student ID"],
                    "Milestone": milestone_name,
                    "Milestone Sort": offset,
                    "Milestone Status": FUTURE_MILESTONE_BUCKET,
                    "P&G Outcome Bucket": FUTURE_MILESTONE_BUCKET,
                }
            )
        for _, student in measured.iterrows():
            chart_group = _chart_group_state(chart_groups, student, offset, chart_breakdown)
            chart_group["eligible"] = int(chart_group["eligible"]) + 1
            target_sort = _milestone_target_sort(student["Cohort Semester"], offset)
            if offset == 1 and latest_sort and target_sort < 999999 and target_sort > latest_sort:
                target_sort = latest_sort
            if str(student.get("Last Known Outcome Bucket", "") or "").strip() == DUPLICATE_NAME_MISMATCH_OUTCOME:
                outcome = "Unknown"
            else:
                outcome = _checkpoint_outcome(
                    timeline_groups.get(
                        (str(student["Cohort Semester"]).strip(), str(student["Student ID"]).strip()),
                        pd.DataFrame(),
                    ),
                    student["Cohort Semester"],
                    student["Student ID"],
                    offset,
                    target_sort=target_sort,
                    prefiltered=True,
                )
            counts[outcome] = int(counts.get(outcome, 0)) + 1
            chart_counts = chart_group["counts"]
            if isinstance(chart_counts, dict):
                chart_counts[outcome] = int(chart_counts.get(outcome, 0)) + 1
            detail_rows.append(
                {
                    "Cohort Semester": student["Cohort Semester"],
                    "Cohort Chapter": student.get("Cohort Chapter", ""),
                    "Student ID": student["Student ID"],
                    "Milestone": milestone_name,
                    "Milestone Sort": offset,
                    "Milestone Status": "Measured",
                    "P&G Outcome Bucket": outcome,
                }
            )

        denominator = int(len(measured))
        future_count = int(len(future_students))
        if denominator:
            last_milestone = milestone_name
        milestone_status = _milestone_status(denominator, future_count)
        table_row: dict[str, object] = {
            "Milestone": milestone_name,
            "Term": selection_label,
            "Milestone Status": milestone_status,
            "Measured Students": denominator,
            "Future Students": future_count,
        }
        for outcome in PERSISTENCE_OUTCOME_ORDER:
            count = int(counts.get(outcome, 0))
            share = count / denominator if denominator else pd.NA
            table_row[outcome] = share
            table_row[f"{outcome} Count"] = count
        table_rows.append(table_row)

        ordered_groups = sorted(
            chart_groups.values(),
            key=lambda value: (
                value.get("sort_group", 0),
                value.get("sort_label", ""),
            ),
        )
        for group_index, chart_group in enumerate(ordered_groups):
            eligible_students = int(chart_group.get("eligible", 0) or 0)
            future_students_count = int(chart_group.get("future", 0) or 0)
            cohort_total = eligible_students + future_students_count
            if cohort_total <= 0:
                continue
            milestone_name = str(chart_group.get("milestone_name", "") or _milestone_name(offset))
            chart_group_label = str(chart_group.get("label", "") or milestone_name)
            axis_label = (
                _milestone_label(offset, selection_label, eligible_students)
                if chart_breakdown == PG_CHART_BREAKDOWN_OVERALL
                else chart_group_label
            )
            chart_sort = offset if chart_breakdown == PG_CHART_BREAKDOWN_OVERALL else group_index
            chart_counts = chart_group["counts"]
            if not isinstance(chart_counts, dict):
                chart_counts = {}
            if eligible_students:
                chart_outcomes = [
                    (outcome, int(chart_counts.get(outcome, 0)))
                    for outcome in PERSISTENCE_OUTCOME_ORDER
                    if int(chart_counts.get(outcome, 0)) > 0
                ]
            else:
                chart_outcomes = []
            if future_students_count and not eligible_students:
                chart_outcomes.append((FUTURE_MILESTONE_BUCKET, future_students_count))
            for outcome, count in chart_outcomes:
                share_denominator = eligible_students if outcome != FUTURE_MILESTONE_BUCKET else cohort_total
                share = count / share_denominator if share_denominator else pd.NA
                label = f"{outcome}<br>{share:.1%}<br>(n={count:,})" if count and share >= 0.085 else ""
                if outcome == FUTURE_MILESTONE_BUCKET and future_students_count == cohort_total:
                    label = FUTURE_MILESTONE_BUCKET
                row = {
                    "Milestone": axis_label,
                    "Milestone Sort": chart_sort,
                    "Chart Group": chart_group_label,
                    "Milestone Name": milestone_name,
                    "Milestone Status": _milestone_status(eligible_students, future_students_count),
                    "Outcome": outcome,
                    "Share": share,
                    "Count": count,
                    "Denominator": eligible_students,
                    "Eligible Students": eligible_students,
                    "Future Students": future_students_count,
                    "Cohort Students": cohort_total,
                    "Label": label,
                }
                chart_rows.append(row)
                chart_table_rows.append(
                    {
                        "Chart Group": chart_group_label,
                        "Milestone": milestone_name,
                        "Milestone Status": row["Milestone Status"],
                        "Outcome": outcome,
                        "Share": share,
                        "Count": count,
                        "Eligible Students": eligible_students,
                        "Future Students": future_students_count,
                        "Cohort Students": cohort_total,
                    }
                )

    chart_frame = pd.DataFrame(chart_rows, columns=MILESTONE_CHART_COLUMNS)
    table_frame = pd.DataFrame(table_rows, columns=MILESTONE_TABLE_COLUMNS)
    chart_table_frame = pd.DataFrame(chart_table_rows, columns=MILESTONE_CHART_TABLE_COLUMNS)
    detail_frame = pd.DataFrame(detail_rows, columns=MILESTONE_DETAIL_COLUMNS)
    return {
        "chart_frame": chart_frame.sort_values(["Milestone Sort", "Outcome"]).reset_index(drop=True),
        "table_frame": table_frame.reset_index(drop=True),
        "chart_table_frame": chart_table_frame.reset_index(drop=True),
        "detail_frame": detail_frame.reset_index(drop=True),
        "meta": {
            "students": int(len(cohort_students)),
            "cohort_term": selection_label,
            "distinction": "ALL",
            "max_milestone": last_milestone,
            "chart_breakdown": chart_breakdown,
            "chart_milestone": chart_milestone_label,
            "note": (
                "Rates use only students old enough to reach the selected checkpoint. Bars marked Future are "
                "selected cohorts with no eligible students yet. Partially future groups keep those newer students "
                "out of the percentage denominator but show the future count in the chart data. Resolved outcome "
                "buckets carry forward across later checkpoints."
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
            "Student Name": review.get("Student Name", pd.Series("", index=review.index)),
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


def _manual_checker_outcome_bucket(row: pd.Series) -> str:
    needs_review = str(row.get("Needs Manual Form Review", "") or "").strip().lower()
    if needs_review in {"yes", "true", "1", "y"}:
        return "Unknown"

    final_bucket = str(row.get("Final Outcome Bucket", "") or "").strip()
    if final_bucket in {
        "Needs Manual Form Review",
        "New Member / No Later Status",
        "Hold",
        "Other / Unmapped",
    }:
        return "Unknown"
    if final_bucket == "Active / Still On Roster":
        return "Active"

    mapped = persistence_outcome_from_status(final_bucket)
    if mapped != "Unknown":
        return mapped

    status_code = normalize_status_code(row.get("Last Known Status Code", "") or row.get("Last Known Status", ""))
    return persistence_outcome_from_status(status_code)


def build_last_known_status_template(outcomes: pd.DataFrame) -> pd.DataFrame:
    if outcomes.empty:
        return pd.DataFrame(columns=LAST_KNOWN_STATUS_COLUMNS)

    computed_outcome_bucket = outcomes.apply(_manual_checker_outcome_bucket, axis=1)
    if "Last Known Outcome Bucket" in outcomes.columns:
        provided_outcome_bucket = outcomes["Last Known Outcome Bucket"].fillna("").astype(str).str.strip()
        outcome_bucket_series = provided_outcome_bucket.where(provided_outcome_bucket.ne(""), computed_outcome_bucket)
    else:
        outcome_bucket_series = computed_outcome_bucket
    result = pd.DataFrame(
        {
            "Cohort Semester": outcomes.get("Cohort Semester", pd.Series("", index=outcomes.index)),
            "Cohort Chapter": outcomes.get("Cohort Chapter", pd.Series("", index=outcomes.index)),
            "Student ID": outcomes.get("Student ID", pd.Series("", index=outcomes.index)),
            "Student Name": outcomes.get("Student Name", pd.Series("", index=outcomes.index)),
            "Last Known Semester": outcomes.get("Last Known Semester", pd.Series("", index=outcomes.index)),
            "Last Known Chapter": outcomes.get("Last Known Chapter", pd.Series("", index=outcomes.index)),
            "Last Known Status": outcomes.get("Last Known Status", pd.Series("", index=outcomes.index)),
            "Last Known Outcome Bucket": outcome_bucket_series,
            "Needs Manual Form Review": outcomes.get("Needs Manual Form Review", pd.Series("", index=outcomes.index)),
            "Manual Status Applied": outcomes.get("Manual Status Applied", pd.Series("", index=outcomes.index)),
            "Semester": "",
            "Chapter": outcomes.get("Last Known Chapter", pd.Series("", index=outcomes.index)),
            "Status": "",
            "Notes": outcomes.get("Notes", pd.Series("", index=outcomes.index)),
        }
    )
    return result.loc[:, LAST_KNOWN_STATUS_COLUMNS]


def build_pg_aligned_manual_checker_template(
    outcomes: pd.DataFrame,
    milestone_detail: pd.DataFrame,
    *,
    milestone_offset: Optional[int] = None,
) -> pd.DataFrame:
    template = build_last_known_status_template(outcomes)
    if template.empty:
        return template

    detail = _ensure_missing_columns(milestone_detail, MILESTONE_DETAIL_COLUMNS)
    if detail.empty:
        return template.iloc[0:0].copy()

    detail["_milestone_sort"] = pd.to_numeric(detail["Milestone Sort"], errors="coerce")
    if milestone_offset is None:
        sorts = detail["_milestone_sort"].dropna()
        if sorts.empty:
            return template.iloc[0:0].copy()
        milestone_offset = int(sorts.max())

    detail = detail.loc[detail["_milestone_sort"].eq(int(milestone_offset))].copy()
    if detail.empty:
        return template.iloc[0:0].copy()

    for column in ["Cohort Semester", "Student ID", "P&G Outcome Bucket"]:
        detail[column] = detail[column].fillna("").astype(str).str.strip()
    detail = (
        detail.loc[detail["Cohort Semester"].ne("") & detail["Student ID"].ne("")]
        .drop_duplicates(subset=["Cohort Semester", "Student ID"], keep="last")
        .loc[:, ["Cohort Semester", "Student ID", "P&G Outcome Bucket"]]
    )
    if detail.empty:
        return template.iloc[0:0].copy()

    merged = template.merge(
        detail,
        on=["Cohort Semester", "Student ID"],
        how="inner",
        validate="one_to_one",
    )
    pg_bucket = merged["P&G Outcome Bucket"].fillna("").astype(str).str.strip()
    existing_bucket = merged["Last Known Outcome Bucket"].fillna("").astype(str).str.strip()
    preserve_existing = existing_bucket.eq(DUPLICATE_NAME_MISMATCH_OUTCOME)
    merged["Last Known Outcome Bucket"] = existing_bucket.where(preserve_existing | pg_bucket.eq(""), pg_bucket)
    return merged.loc[:, LAST_KNOWN_STATUS_COLUMNS].reset_index(drop=True)


def build_manual_checker_queue(source: pd.DataFrame) -> pd.DataFrame:
    template = build_last_known_status_template(source)
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
    duplicate_name_resolution_file: str | Path = DEFAULT_DUPLICATE_NAME_RESOLUTION_PATH,
    table_name: str = TABLE_NAME,
    cohort_semesters: Optional[Sequence[str]] = None,
    all_cohorts: bool = True,
) -> SqlCompileDashboardTables:
    compiled_rows = read_sql_compile_table(database_path, table_name=table_name)
    roster_inventory = read_roster_inventory_table(database_path)
    student_names = read_student_name_table(database_path)
    student_name_observations = read_student_name_observations_table(database_path)
    manual_rows = read_manual_status_rows(manual_status_file)
    duplicate_name_resolutions = read_duplicate_name_resolution_rows(duplicate_name_resolution_file)
    timeline, outcomes, review, summary, selected_semesters = build_new_member_cohort_tables(
        compiled_rows,
        manual_rows,
        roster_inventory=roster_inventory,
        cohort_semesters=cohort_semesters,
        all_cohorts=all_cohorts,
    )
    outcomes = attach_student_names(outcomes, student_names)
    outcomes = consolidate_duplicate_student_outcomes(
        outcomes,
        student_name_observations,
        duplicate_name_resolutions,
    )
    review = attach_student_names(review, student_names)
    return SqlCompileDashboardTables(
        timeline=timeline,
        outcomes=outcomes,
        review=review,
        summary=summary,
        rate_table=build_dashboard_rate_table(outcomes),
        outcome_distribution=build_outcome_distribution(outcomes),
        manual_entry_template=build_manual_entry_template(review),
        manual_checker_template=build_last_known_status_template(outcomes),
        manual_rows=manual_rows,
        duplicate_name_resolutions=duplicate_name_resolutions,
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


def _filter_by_selected_chapters(frame: pd.DataFrame, selected_chapters: Optional[Sequence[str]]) -> pd.DataFrame:
    if selected_chapters is None or frame.empty or "Cohort Chapter" not in frame.columns:
        return frame.copy()
    selected = {str(value).strip() for value in selected_chapters if str(value).strip()}
    if not selected:
        return frame.iloc[0:0].copy()
    return frame.loc[frame["Cohort Chapter"].fillna("").astype(str).str.strip().isin(selected)].copy()


def _clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_pg_chart_breakdown(value: object) -> str:
    text = str(value or "").strip()
    if text == "Milestone timeline":
        return PG_CHART_BREAKDOWN_OVERALL
    return text if text in PG_CHART_BREAKDOWN_OPTIONS else PG_CHART_BREAKDOWN_OVERALL


def _normalize_pg_milestone_offsets(values: Sequence[object], *, max_years: int) -> list[int]:
    allowed = {offset for offset in PG_GRADUATION_MILESTONE_OFFSETS if offset <= int(max_years)}
    result: list[int] = []
    for value in values:
        try:
            offset = int(value)
        except (TypeError, ValueError):
            continue
        if offset in allowed and offset not in result:
            result.append(offset)
    return result


def _milestone_selection_label(offsets: Sequence[int]) -> str:
    labels = [_milestone_name(offset) for offset in offsets]
    if not labels:
        return ""
    return ", ".join(labels)


def _chart_group_state(
    chart_groups: dict[str, dict[str, object]],
    student: pd.Series,
    offset: int,
    chart_breakdown: str,
) -> dict[str, object]:
    cohort_semester = _clean_text(student.get("Cohort Semester", "")) or "Unknown Semester"
    cohort_chapter = _clean_text(student.get("Cohort Chapter", "")) or "Unknown Chapter"
    milestone_name = _milestone_name(offset)

    if chart_breakdown == PG_CHART_BREAKDOWN_SEMESTER:
        key = f"semester|{cohort_semester}"
        label = cohort_semester
        sort_group = _cohort_sort(cohort_semester)
        sort_label = cohort_semester
    elif chart_breakdown == PG_CHART_BREAKDOWN_CHAPTER:
        key = f"chapter|{cohort_chapter}"
        label = cohort_chapter
        sort_group = 0
        sort_label = cohort_chapter.casefold()
    else:
        key = f"milestone|{offset}"
        label = milestone_name
        sort_group = offset
        sort_label = milestone_name

    if key not in chart_groups:
        chart_groups[key] = {
            "label": label,
            "sort_group": sort_group,
            "sort_label": sort_label,
            "milestone_name": milestone_name,
            "eligible": 0,
            "future": 0,
            "counts": {outcome: 0 for outcome in PERSISTENCE_OUTCOME_ORDER},
        }
    return chart_groups[key]


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


def _milestone_label(offset: int, selection_label: str, denominator: int | None = None) -> str:
    label = str(selection_label or SQL_COMPILE_ALL_TIME_LABEL).strip() or SQL_COMPILE_ALL_TIME_LABEL
    measured = f"<br>n={int(denominator):,}" if denominator is not None else ""
    return f"{_milestone_name(offset)}<br>{label}{measured}"


def _milestone_target_sort(cohort_semester: object, offset: int) -> int:
    from src.build_canonical_pipeline import parse_term_code, sort_term_code

    cohort_sort = _cohort_sort(cohort_semester)
    if int(offset) <= 0 or cohort_sort >= 999999:
        return cohort_sort

    code, _, year, season = parse_term_code(cohort_semester)
    if not code or pd.isna(year):
        return cohort_sort + (int(offset) * 10)

    season_text = str(season or "").strip().lower()
    year_value = int(year)
    if season_text == "fall":
        return sort_term_code(f"{year_value + int(offset)}SP")
    if season_text == "spring":
        return sort_term_code(f"{year_value + int(offset)}SP")
    return cohort_sort + (int(offset) * 10)


def _milestone_is_measurable(cohort_semester: object, offset: int, latest_sort: int) -> bool:
    if offset in {0, 1}:
        return True
    target_sort = _milestone_target_sort(cohort_semester, offset)
    return target_sort < 999999 and latest_sort >= target_sort


def _milestone_status(measured_students: int, future_students: int) -> str:
    if measured_students <= 0 and future_students > 0:
        return FUTURE_MILESTONE_BUCKET
    if measured_students > 0 and future_students > 0:
        return "Partially Future"
    return "Measured"


def _checkpoint_outcome(
    timeline: pd.DataFrame,
    cohort_semester: str,
    student_id: str,
    offset: int,
    *,
    target_sort: int | None = None,
    prefiltered: bool = False,
) -> str:
    if timeline.empty:
        return "Active" if offset == 0 else "Unknown"

    checkpoint_sort = target_sort if target_sort is not None else _milestone_target_sort(cohort_semester, offset)
    if prefiltered:
        student_rows = timeline.copy()
    else:
        student_rows = timeline.loc[
            timeline["Cohort Semester"].eq(str(cohort_semester).strip())
            & timeline["Student ID"].eq(str(student_id).strip())
        ].copy()
    if student_rows.empty:
        return "Active" if offset == 0 else "Unknown"

    if not prefiltered:
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
