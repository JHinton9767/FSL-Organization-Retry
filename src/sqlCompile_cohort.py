from __future__ import annotations

import argparse
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from src.build_canonical_pipeline import parse_term_code, sort_term_code
from src.path_config import ROOT
from src.shared_utils import clean_text
from src.sqlCompile import DEFAULT_OUTPUT_PATH, OUTPUT_COLUMNS, TABLE_NAME, _quote_identifier, _resolve_path


MANUAL_STATUS_COLUMNS = [
    "Cohort Semester",
    "Cohort Chapter",
    "Semester",
    "Chapter",
    "Student ID",
    "Status",
    "Notes",
]
DEFAULT_MANUAL_STATUS_PATH = ROOT / "config" / "sqlCompile_manual_status.csv"
DEFAULT_COHORT_OUTPUT_DIR = ROOT / "output" / "sqlCompile" / "cohorts"
REPORT_TABLES = {
    "timeline": "new_member_timeline",
    "outcomes": "new_member_outcomes",
    "review": "new_member_form_review",
    "summary": "new_member_rate_summary",
}
KNOWN_NON_GRADUATE_EXIT_BUCKETS = {
    "Dropped/Inactive",
    "Early Alumni",
    "Resigned",
    "Revoked",
    "Suspended",
    "Transfer",
}
OTHER_UNRESOLVED_BUCKETS = {"Hold", "New Member / No Later Status", "Other / Unmapped"}


@dataclass(frozen=True)
class NewMemberCohortReportResult:
    database_path: Path
    output_dir: Path
    manual_status_path: Path
    cohort_semesters: List[str]
    timeline_rows: int
    outcome_rows: int
    review_rows: int
    summary_rows: int


def _normalize_semester(value: object) -> str:
    text = clean_text(value)
    code, label, _, _ = parse_term_code(text)
    return label if code else text


def _semester_sort(value: object) -> int:
    code, _, _, _ = parse_term_code(value)
    return sort_term_code(code) if code else 999999


def _normalize_chapter_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean_text(value).lower())


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", clean_text(value).lower()).strip("_")
    return slug or "cohort"


def normalize_status_code(value: object) -> str:
    text = clean_text(value).upper()
    compact = re.sub(r"[^A-Z0-9]+", "", text)
    if not compact:
        return ""
    if compact in {"A", "ACTIVE", "ACTIVEMEMBER", "MEMBER"}:
        return "A"
    if compact in {"N", "NEW", "NEWMEMBER", "NEWMEMBERS", "ASSOCIATEMEMBER", "ASSOCIATEMEMBERS"}:
        return "N"
    if compact in {"RS", "RESIGN", "RESIGNED"}:
        return "RS"
    if compact in {"RV", "REVOKE", "REVOKED"}:
        return "RV"
    if compact in {"G", "GRAD", "GRADUATE", "GRADUATED"}:
        return "G"
    if compact in {"T", "TRANSFER", "TRANSFERRED"}:
        return "T"
    if compact in {"S", "SUSPEND", "SUSPENDED"}:
        return "S"
    if compact in {"D", "DROP", "DROPPED", "I", "INACTIVE", "REMOVE", "REMOVED"}:
        return "D"
    if compact in {"AL", "ALUMNI", "EARLYALUMNI", "EARLYALUM"}:
        return "AL"
    if compact == "H":
        return "H"
    return compact


def outcome_bucket(status_code: str, needs_manual_review: bool) -> str:
    if needs_manual_review:
        return "Needs Manual Form Review"
    if status_code == "G":
        return "Graduated"
    if status_code == "A":
        return "Active / Still On Roster"
    if status_code == "N":
        return "New Member / No Later Status"
    if status_code == "D":
        return "Dropped/Inactive"
    if status_code == "RS":
        return "Resigned"
    if status_code == "RV":
        return "Revoked"
    if status_code == "S":
        return "Suspended"
    if status_code == "T":
        return "Transfer"
    if status_code == "AL":
        return "Early Alumni"
    if status_code == "H":
        return "Hold"
    return "Other / Unmapped"


def ensure_manual_status_file(path: str | Path = DEFAULT_MANUAL_STATUS_PATH) -> Path:
    destination = _resolve_path(path)
    if not destination.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=MANUAL_STATUS_COLUMNS).to_csv(destination, index=False)
    return destination


def _ensure_columns(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = ""
    for column in columns:
        result[column] = result[column].fillna("").astype(str).map(clean_text)
    return result.loc[:, list(columns)]


def read_manual_status_rows(path: str | Path = DEFAULT_MANUAL_STATUS_PATH, create_if_missing: bool = True) -> pd.DataFrame:
    manual_path = ensure_manual_status_file(path) if create_if_missing else _resolve_path(path)
    if not manual_path.exists():
        return pd.DataFrame(columns=MANUAL_STATUS_COLUMNS)
    try:
        frame = pd.read_csv(manual_path, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=MANUAL_STATUS_COLUMNS)
    return _ensure_columns(frame, MANUAL_STATUS_COLUMNS)


def write_manual_status_rows(frame: pd.DataFrame, path: str | Path = DEFAULT_MANUAL_STATUS_PATH) -> Path:
    destination = _resolve_path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    _ensure_columns(frame, MANUAL_STATUS_COLUMNS).to_csv(destination, index=False)
    return destination


def completed_manual_status_rows(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = _ensure_columns(frame, MANUAL_STATUS_COLUMNS)
    return prepared.loc[
        prepared["Student ID"].ne("") & prepared["Semester"].ne("") & prepared["Status"].ne("")
    ].copy()


def append_manual_status_rows(frame: pd.DataFrame, path: str | Path = DEFAULT_MANUAL_STATUS_PATH) -> tuple[Path, int]:
    incoming = completed_manual_status_rows(frame)
    destination = ensure_manual_status_file(path)
    if incoming.empty:
        return destination, 0

    existing = read_manual_status_rows(destination)
    combined = pd.concat([existing, incoming], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["Cohort Semester", "Cohort Chapter", "Semester", "Chapter", "Student ID"],
        keep="last",
    )
    write_manual_status_rows(combined, destination)
    return destination, len(incoming)


def read_sql_compile_table(database_path: str | Path = DEFAULT_OUTPUT_PATH, table_name: str = TABLE_NAME) -> pd.DataFrame:
    database = _resolve_path(database_path)
    if not database.exists():
        raise FileNotFoundError(f"SQL compile database not found: {database}")

    columns = ", ".join(_quote_identifier(column) for column in OUTPUT_COLUMNS)
    with sqlite3.connect(database) as connection:
        frame = pd.read_sql_query(f"SELECT {columns} FROM {_quote_identifier(table_name)}", connection)
    return _ensure_columns(frame, OUTPUT_COLUMNS)


def _prepared_compile_rows(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = _ensure_columns(frame, OUTPUT_COLUMNS)
    prepared["_semester_normalized"] = prepared["Semester"].map(_normalize_semester)
    prepared["_term_sort"] = prepared["_semester_normalized"].map(_semester_sort)
    prepared["_status_code"] = prepared["Status"].map(normalize_status_code)
    prepared["_source"] = "sqlCompile"
    prepared["_manual_priority"] = 0
    return prepared


def _prepared_manual_rows(frame: pd.DataFrame) -> pd.DataFrame:
    prepared = _ensure_columns(frame, MANUAL_STATUS_COLUMNS)
    prepared = prepared.loc[
        prepared["Student ID"].ne("") & prepared["Semester"].ne("") & prepared["Status"].ne("")
    ].copy()
    prepared["_cohort_semester_normalized"] = prepared["Cohort Semester"].map(_normalize_semester)
    prepared["_cohort_chapter_key"] = prepared["Cohort Chapter"].map(_normalize_chapter_key)
    prepared["_semester_normalized"] = prepared["Semester"].map(_normalize_semester)
    prepared["_term_sort"] = prepared["_semester_normalized"].map(_semester_sort)
    prepared["_status_code"] = prepared["Status"].map(normalize_status_code)
    prepared["_source"] = "manual_status"
    prepared["_manual_priority"] = 1
    return prepared


def _selected_cohort_semesters(prepared: pd.DataFrame, cohort_semesters: Optional[Sequence[str]], all_cohorts: bool) -> List[str]:
    if cohort_semesters:
        return [_normalize_semester(value) for value in cohort_semesters]
    if all_cohorts:
        new_member_rows = prepared.loc[prepared["_status_code"].eq("N")].copy()
        ordered = new_member_rows.sort_values(["_term_sort", "_semester_normalized"], na_position="last")
        return ordered["_semester_normalized"].drop_duplicates().tolist()
    raise ValueError("Pass at least one --cohort-semester, or use --all-cohorts.")


def _manual_rows_for_cohort(manual: pd.DataFrame, cohort_label: str, cohort_students: pd.DataFrame) -> pd.DataFrame:
    if manual.empty:
        return manual.copy()

    student_chapter_key: Dict[str, str] = dict(zip(cohort_students["Student ID"], cohort_students["_cohort_chapter_key"]))
    filtered = manual.loc[manual["Student ID"].isin(student_chapter_key)].copy()
    if filtered.empty:
        return filtered

    semester_matches = filtered["_cohort_semester_normalized"].eq("") | filtered["_cohort_semester_normalized"].eq(cohort_label)
    chapter_matches = filtered.apply(
        lambda row: not row["_cohort_chapter_key"]
        or row["_cohort_chapter_key"] == student_chapter_key.get(row["Student ID"], ""),
        axis=1,
    )
    filtered = filtered.loc[semester_matches & chapter_matches].copy()
    return filtered


def _timeline_rows_for_cohort(
    compiled: pd.DataFrame,
    manual: pd.DataFrame,
    cohort_label: str,
    cohort_source_rows: pd.DataFrame,
) -> pd.DataFrame:
    cohort_sort = _semester_sort(cohort_label)
    cohort_students = (
        cohort_source_rows.loc[:, ["Student ID", "Chapter"]]
        .drop_duplicates(subset=["Student ID"], keep="first")
        .rename(columns={"Chapter": "Cohort Chapter"})
    )
    cohort_students["Cohort Semester"] = cohort_label
    cohort_students["_cohort_chapter_key"] = cohort_students["Cohort Chapter"].map(_normalize_chapter_key)

    base_timeline = compiled.loc[compiled["Student ID"].isin(cohort_students["Student ID"])].copy()
    base_timeline["Cohort Semester"] = cohort_label
    base_timeline = base_timeline.merge(
        cohort_students.loc[:, ["Student ID", "Cohort Chapter"]],
        on="Student ID",
        how="left",
    )
    base_timeline["Notes"] = ""

    manual_timeline = _manual_rows_for_cohort(manual, cohort_label, cohort_students)
    if not manual_timeline.empty:
        manual_timeline = manual_timeline.rename(columns={"_cohort_semester_normalized": "_manual_cohort_semester"})
        manual_timeline["Cohort Semester"] = cohort_label
        manual_timeline = manual_timeline.drop(columns=["Cohort Chapter"], errors="ignore").merge(
            cohort_students.loc[:, ["Student ID", "Cohort Chapter"]],
            on="Student ID",
            how="left",
        )
    else:
        manual_timeline = pd.DataFrame(columns=base_timeline.columns)

    common_columns = [
        "Cohort Semester",
        "Cohort Chapter",
        "Semester",
        "Chapter",
        "Student ID",
        "Status",
        "Notes",
        "_semester_normalized",
        "_term_sort",
        "_status_code",
        "_source",
        "_manual_priority",
    ]
    combined = pd.concat(
        [
            _ensure_missing_columns(base_timeline, common_columns),
            _ensure_missing_columns(manual_timeline, common_columns),
        ],
        ignore_index=True,
    )
    if combined.empty:
        return pd.DataFrame(columns=_timeline_output_columns())

    combined["_included_sort"] = combined["_term_sort"].ge(cohort_sort).astype(int)
    combined = combined.sort_values(
        ["Student ID", "_semester_normalized", "_manual_priority", "_included_sort", "_source"],
        ascending=[True, True, False, False, True],
        na_position="last",
    )
    resolved = combined.drop_duplicates(subset=["Student ID", "_semester_normalized"], keep="first").copy()
    resolved["Included In Outcome"] = resolved["_term_sort"].ge(cohort_sort).map(lambda value: "Yes" if value else "No")
    resolved["Source"] = resolved["_source"]
    resolved["Status Code"] = resolved["_status_code"]
    resolved = resolved.sort_values(["Cohort Semester", "Student ID", "_term_sort", "Semester"], na_position="last")
    return resolved.loc[:, _timeline_output_columns() + ["_term_sort", "_manual_priority", "_status_code", "_source"]]


def _ensure_missing_columns(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = ""
    return result.loc[:, list(columns)]


def _timeline_output_columns() -> List[str]:
    return [
        "Cohort Semester",
        "Cohort Chapter",
        "Student ID",
        "Semester",
        "Chapter",
        "Status",
        "Status Code",
        "Source",
        "Included In Outcome",
        "Notes",
    ]


def _build_outcomes_for_cohort(cohort_label: str, cohort_source_rows: pd.DataFrame, timeline: pd.DataFrame) -> pd.DataFrame:
    cohort_students = (
        cohort_source_rows.loc[:, ["Student ID", "Chapter"]]
        .drop_duplicates(subset=["Student ID"], keep="first")
        .rename(columns={"Chapter": "Cohort Chapter"})
        .sort_values(["Cohort Chapter", "Student ID"])
    )
    rows: List[dict] = []
    outcome_rows = timeline.loc[timeline["Included In Outcome"].eq("Yes")].copy()
    for _, student in cohort_students.iterrows():
        student_id = student["Student ID"]
        student_rows = outcome_rows.loc[outcome_rows["Student ID"].eq(student_id)].copy()
        if student_rows.empty:
            latest = {}
            latest_status_code = ""
            needs_review = True
            manual_status_applied = "No"
        else:
            student_rows = student_rows.sort_values(["_term_sort", "_manual_priority", "Semester"], na_position="last")
            latest = student_rows.iloc[-1].to_dict()
            latest_status_code = clean_text(latest.get("_status_code", ""))
            manual_status_applied = "Yes" if student_rows["_source"].eq("manual_status").any() else "No"
            needs_review = latest_status_code == "A" and clean_text(latest.get("_source", "")) != "manual_status"

        rows.append(
            {
                "Cohort Semester": cohort_label,
                "Cohort Chapter": student["Cohort Chapter"],
                "Student ID": student_id,
                "Last Known Semester": latest.get("Semester", ""),
                "Last Known Chapter": latest.get("Chapter", ""),
                "Last Known Status": latest.get("Status", ""),
                "Last Known Status Code": latest_status_code,
                "Final Outcome Bucket": outcome_bucket(latest_status_code, needs_review),
                "Needs Manual Form Review": "Yes" if needs_review else "No",
                "Manual Status Applied": manual_status_applied,
            }
        )
    return pd.DataFrame(rows)


def _build_review_rows(outcomes: pd.DataFrame) -> pd.DataFrame:
    review = outcomes.loc[outcomes["Needs Manual Form Review"].eq("Yes")].copy()
    if review.empty:
        return pd.DataFrame(
            columns=[
                *MANUAL_STATUS_COLUMNS,
                "Last Known Semester",
                "Last Known Chapter",
                "Last Known Status",
                "Suggested Action",
            ]
        )
    result = pd.DataFrame(
        {
            "Cohort Semester": review["Cohort Semester"],
            "Cohort Chapter": review["Cohort Chapter"],
            "Semester": "",
            "Chapter": review["Last Known Chapter"],
            "Student ID": review["Student ID"],
            "Status": "",
            "Notes": "",
            "Last Known Semester": review["Last Known Semester"],
            "Last Known Chapter": review["Last Known Chapter"],
            "Last Known Status": review["Last Known Status"],
            "Suggested Action": "Find the form/source, then add one completed row to config/sqlCompile_manual_status.csv.",
        }
    )
    return result


def _summary_row(
    cohort_label: str,
    metric: str,
    bucket: str,
    count: int,
    cohort_count: int,
    resolved_count: int,
    use_resolved_rate: bool = True,
) -> dict:
    return {
        "Cohort Semester": cohort_label,
        "Metric": metric,
        "Outcome Bucket": bucket,
        "Student Count": count,
        "Cohort Students": cohort_count,
        "Resolved Denominator": resolved_count,
        "Share of Cohort": count / cohort_count if cohort_count else 0,
        "Rate of Resolved": count / resolved_count if use_resolved_rate and resolved_count else "",
    }


def _build_summary_rows(outcomes: pd.DataFrame) -> pd.DataFrame:
    rows: List[dict] = []
    if outcomes.empty:
        return pd.DataFrame(
            columns=[
                "Cohort Semester",
                "Metric",
                "Outcome Bucket",
                "Student Count",
                "Cohort Students",
                "Resolved Denominator",
                "Share of Cohort",
                "Rate of Resolved",
            ]
        )

    for cohort_label, group in outcomes.groupby("Cohort Semester", dropna=False):
        cohort_count = len(group)
        review_count = int(group["Needs Manual Form Review"].eq("Yes").sum())
        resolved = group.loc[group["Needs Manual Form Review"].ne("Yes")].copy()
        resolved_count = len(resolved)
        bucket_counts = resolved["Final Outcome Bucket"].value_counts().to_dict()

        rows.append(_summary_row(cohort_label, "Cohort Size", "All New Members", cohort_count, cohort_count, resolved_count, use_resolved_rate=False))
        rows.append(_summary_row(cohort_label, "Manual Review", "Needs Manual Form Review", review_count, cohort_count, resolved_count, use_resolved_rate=False))
        rows.append(_summary_row(cohort_label, "Resolved Size", "Resolved Students", resolved_count, cohort_count, resolved_count))

        for bucket in sorted(bucket_counts):
            rows.append(
                _summary_row(
                    cohort_label,
                    "Outcome Bucket Rate",
                    bucket,
                    int(bucket_counts[bucket]),
                    cohort_count,
                    resolved_count,
                )
            )

        retained_count = int(bucket_counts.get("Active / Still On Roster", 0))
        graduated_count = int(bucket_counts.get("Graduated", 0))
        non_graduate_exit_count = sum(int(bucket_counts.get(bucket, 0)) for bucket in KNOWN_NON_GRADUATE_EXIT_BUCKETS)
        other_unresolved_count = sum(int(bucket_counts.get(bucket, 0)) for bucket in OTHER_UNRESOLVED_BUCKETS)

        rows.append(_summary_row(cohort_label, "Retention Rate", "Active / Still On Roster", retained_count, cohort_count, resolved_count))
        rows.append(_summary_row(cohort_label, "Graduation Rate", "Graduated", graduated_count, cohort_count, resolved_count))
        rows.append(_summary_row(cohort_label, "Known Non-Graduate Exit Rate", "Known Non-Graduate Exit", non_graduate_exit_count, cohort_count, resolved_count))
        rows.append(_summary_row(cohort_label, "Other / Unresolved Rate", "Other / Unresolved", other_unresolved_count, cohort_count, resolved_count))

    return pd.DataFrame(rows)


def build_new_member_cohort_tables(
    compiled_rows: pd.DataFrame,
    manual_rows: pd.DataFrame,
    cohort_semesters: Optional[Sequence[str]] = None,
    all_cohorts: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, List[str]]:
    compiled = _prepared_compile_rows(compiled_rows)
    manual = _prepared_manual_rows(manual_rows)
    selected_semesters = _selected_cohort_semesters(compiled, cohort_semesters, all_cohorts)

    timeline_frames: List[pd.DataFrame] = []
    outcome_frames: List[pd.DataFrame] = []
    review_frames: List[pd.DataFrame] = []

    for cohort_label in selected_semesters:
        cohort_rows = compiled.loc[
            compiled["_semester_normalized"].eq(cohort_label) & compiled["_status_code"].eq("N")
        ].copy()
        if cohort_rows.empty:
            continue
        timeline = _timeline_rows_for_cohort(compiled, manual, cohort_label, cohort_rows)
        outcomes = _build_outcomes_for_cohort(cohort_label, cohort_rows, timeline)
        review = _build_review_rows(outcomes)
        timeline_frames.append(timeline)
        outcome_frames.append(outcomes)
        review_frames.append(review)

    timeline_result = _concat_or_empty(timeline_frames, _timeline_output_columns())
    outcomes_result = _concat_or_empty(
        outcome_frames,
        [
            "Cohort Semester",
            "Cohort Chapter",
            "Student ID",
            "Last Known Semester",
            "Last Known Chapter",
            "Last Known Status",
            "Last Known Status Code",
            "Final Outcome Bucket",
            "Needs Manual Form Review",
            "Manual Status Applied",
        ],
    )
    review_result = _concat_or_empty(
        review_frames,
        [
            *MANUAL_STATUS_COLUMNS,
            "Last Known Semester",
            "Last Known Chapter",
            "Last Known Status",
            "Suggested Action",
        ],
    )
    summary_result = _build_summary_rows(outcomes_result)
    return timeline_result, outcomes_result, review_result, summary_result, selected_semesters


def _concat_or_empty(frames: Sequence[pd.DataFrame], columns: Sequence[str]) -> pd.DataFrame:
    usable = [frame.loc[:, [column for column in columns if column in frame.columns]].copy() for frame in frames if frame is not None and not frame.empty]
    if not usable:
        return pd.DataFrame(columns=list(columns))
    result = pd.concat(usable, ignore_index=True)
    return _ensure_missing_columns(result, columns)


def write_report_tables(
    database_path: str | Path,
    timeline: pd.DataFrame,
    outcomes: pd.DataFrame,
    review: pd.DataFrame,
    summary: pd.DataFrame,
) -> None:
    database = _resolve_path(database_path)
    with sqlite3.connect(database) as connection:
        for frame, table_name in [
            (timeline, REPORT_TABLES["timeline"]),
            (outcomes, REPORT_TABLES["outcomes"]),
            (review, REPORT_TABLES["review"]),
            (summary, REPORT_TABLES["summary"]),
        ]:
            frame.to_sql(table_name, connection, if_exists="replace", index=False)
        connection.commit()


def write_report_csvs(
    output_dir: str | Path,
    selected_semesters: Sequence[str],
    timeline: pd.DataFrame,
    outcomes: pd.DataFrame,
    review: pd.DataFrame,
    summary: pd.DataFrame,
) -> Path:
    root = _resolve_path(output_dir)
    destination = root / _slug(selected_semesters[0]) if len(selected_semesters) == 1 else root / "all_new_member_cohorts"
    destination.mkdir(parents=True, exist_ok=True)
    timeline.to_csv(destination / "new_member_timeline.csv", index=False)
    outcomes.to_csv(destination / "new_member_outcomes.csv", index=False)
    review.to_csv(destination / "new_member_form_review.csv", index=False)
    summary.to_csv(destination / "new_member_rate_summary.csv", index=False)
    return destination


def build_new_member_cohort_report(
    database_path: str | Path = DEFAULT_OUTPUT_PATH,
    cohort_semesters: Optional[Sequence[str]] = None,
    all_cohorts: bool = False,
    manual_status_file: str | Path = DEFAULT_MANUAL_STATUS_PATH,
    output_dir: str | Path = DEFAULT_COHORT_OUTPUT_DIR,
    table_name: str = TABLE_NAME,
) -> NewMemberCohortReportResult:
    database = _resolve_path(database_path)
    manual_path = ensure_manual_status_file(manual_status_file)
    compiled_rows = read_sql_compile_table(database, table_name=table_name)
    manual_rows = read_manual_status_rows(manual_path)
    timeline, outcomes, review, summary, selected_semesters = build_new_member_cohort_tables(
        compiled_rows,
        manual_rows,
        cohort_semesters=cohort_semesters,
        all_cohorts=all_cohorts,
    )
    write_report_tables(database, timeline, outcomes, review, summary)
    report_dir = write_report_csvs(output_dir, selected_semesters, timeline, outcomes, review, summary)
    return NewMemberCohortReportResult(
        database_path=database,
        output_dir=report_dir,
        manual_status_path=manual_path,
        cohort_semesters=selected_semesters,
        timeline_rows=len(timeline),
        outcome_rows=len(outcomes),
        review_rows=len(review),
        summary_rows=len(summary),
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build new-member cohort reports from sqlCompile.sqlite.")
    parser.add_argument("--database", default=str(DEFAULT_OUTPUT_PATH), help="Path to sqlCompile.sqlite.")
    parser.add_argument("--table", default=TABLE_NAME, help="Compiled roster table name.")
    parser.add_argument("--cohort-semester", action="append", dest="cohort_semesters", default=None, help='Semester with Status N, for example "Fall 2025". Repeat for multiple cohorts.')
    parser.add_argument("--all-cohorts", "--all-semesters", dest="all_cohorts", action="store_true", help="Build reports for every semester that has Status N rows.")
    parser.add_argument("--manual-status-file", default=str(DEFAULT_MANUAL_STATUS_PATH), help="CSV of manually researched status rows.")
    parser.add_argument("--output-dir", default=str(DEFAULT_COHORT_OUTPUT_DIR), help="Folder where report CSVs are written.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    result = build_new_member_cohort_report(
        database_path=args.database,
        cohort_semesters=args.cohort_semesters,
        all_cohorts=args.all_cohorts,
        manual_status_file=args.manual_status_file,
        output_dir=args.output_dir,
        table_name=args.table,
    )
    print(f"New-member cohort report written to: {result.output_dir}")
    print(f"Database updated: {result.database_path}")
    print(f"Manual status file: {result.manual_status_path}")
    print(f"Cohort semesters: {', '.join(result.cohort_semesters) if result.cohort_semesters else '(none)'}")
    print(f"Timeline rows: {result.timeline_rows}")
    print(f"Outcome rows: {result.outcome_rows}")
    print(f"Manual form review rows: {result.review_rows}")
    print(f"Summary rows: {result.summary_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
