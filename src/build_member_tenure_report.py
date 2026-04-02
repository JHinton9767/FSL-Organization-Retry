from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill

from src.build_master_roster import (
    DEFAULT_INPUT_ROOT,
    ExtractedRow,
    SUPPORTED_EXTENSIONS,
    autosize_columns,
    clean_text,
    extract_rows_from_workbook,
    normalize_banner_id,
    normalize_status,
    style_header,
    term_sort_key,
)


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MASTER_ROSTER_WORKBOOK = ROOT / "Master_FSL_Roster.xlsx"
DEFAULT_MASTER_GRADES_WORKBOOK = ROOT / "Master_Roster_Grades.xlsx"
DEFAULT_MASTER_WORKBOOK = (
    DEFAULT_MASTER_GRADES_WORKBOOK if DEFAULT_MASTER_GRADES_WORKBOOK.exists() else DEFAULT_MASTER_ROSTER_WORKBOOK
)
DEFAULT_OUTPUT_WORKBOOK = ROOT / "Member_Tenure_Report.xlsx"
MASTER_REQUIRED_COLUMNS = {
    "Academic Year",
    "Term",
    "Source File",
    "Chapter",
    "Last Name",
    "First Name",
    "Banner ID",
    "Email",
    "Status",
    "Semester Joined",
    "Position",
}
GRADE_AWARE_COLUMNS = {
    "Grade Student Status",
    "Semester Hours",
    "Cumulative Hours",
    "Current Academic Standing",
    "Term GPA",
    "TxState Cumulative GPA",
    "Overall Cumulative GPA",
    "Term Passed Hours",
}

TERMINAL_STATUSES = {
    "Graduated",
    "Alumni",
    "Suspended",
    "Revoked",
    "Resigned",
    "Transfer",
    "Inactive",
}

STATUS_PRIORITY = {
    "Graduated": 90,
    "Alumni": 85,
    "Suspended": 80,
    "Revoked": 75,
    "Resigned": 70,
    "Transfer": 65,
    "Inactive": 60,
    "Active": 50,
    "New Member": 55,
    "": 0,
}

OUTCOME_ORDER = [
    "Graduated",
    "Dropped",
    "Suspended",
    "Transfer",
    "Still Active / Unknown",
]
HOURS_PER_SEMESTER = 15.0
JOIN_HOURS_BUCKET_SIZE = 30


@dataclass(frozen=True)
class Observation:
    academic_year: str
    term: str
    source_file: str
    chapter: str
    last_name: str
    first_name: str
    banner_id: str
    email: str
    status: str
    semester_joined: str
    position: str
    grade_student_status: str = ""
    semester_hours: str = ""
    cumulative_hours: str = ""
    current_academic_standing: str = ""
    term_gpa: str = ""
    txstate_cumulative_gpa: str = ""
    overall_cumulative_gpa: str = ""
    term_passed_hours: str = ""


@dataclass(frozen=True)
class GpaPoint:
    banner_id: str
    email: str
    chapter: str
    term: str
    semester_at_school: int
    term_gpa: Optional[float]
    txstate_cumulative_gpa: Optional[float]
    overall_cumulative_gpa: Optional[float]
    cumulative_hours: Optional[float]


@dataclass(frozen=True)
class MemberJourney:
    chapter: str
    last_name: str
    first_name: str
    banner_id: str
    email: str
    start_term: str
    start_basis: str
    first_new_member_term: str
    last_observed_term: str
    left_term: str
    exit_reason: str
    final_status: str
    outcome_group: str
    returned_later: str
    semester_count: int
    semesters_from_new_member: int
    join_semester_at_school: int
    exit_semester_at_school: int
    join_cumulative_hours: Optional[float]
    join_cumulative_hours_bucket: str
    avg_term_gpa: Optional[float]
    latest_txstate_cumulative_gpa: Optional[float]
    latest_overall_cumulative_gpa: Optional[float]
    term_history: str
    status_history: str

    def as_list(self) -> List[object]:
        return [
            self.chapter,
            self.last_name,
            self.first_name,
            self.banner_id,
            self.email,
            self.start_term,
            self.start_basis,
            self.first_new_member_term,
            self.last_observed_term,
            self.left_term,
            self.exit_reason,
            self.final_status,
            self.outcome_group,
            self.returned_later,
            self.semester_count,
            self.semesters_from_new_member,
            self.join_semester_at_school,
            self.exit_semester_at_school,
            self.join_cumulative_hours,
            self.join_cumulative_hours_bucket,
            self.avg_term_gpa,
            self.latest_txstate_cumulative_gpa,
            self.latest_overall_cumulative_gpa,
            self.term_history,
            self.status_history,
        ]

    @property
    def confirmed_join_within_window(self) -> str:
        return "Yes" if self.first_new_member_term else "No"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a semester-count member tenure workbook from Master_Roster_Grades.xlsx "
            "(or Master_FSL_Roster.xlsx when the merged workbook is unavailable)."
        )
    )
    parser.add_argument(
        "--master",
        default=str(DEFAULT_MASTER_WORKBOOK),
        help=(
            "Path to Master_Roster_Grades.xlsx or Master_FSL_Roster.xlsx. "
            "Default: Master_Roster_Grades.xlsx when present, otherwise Master_FSL_Roster.xlsx."
        ),
    )
    parser.add_argument(
        "--raw-root",
        default=str(DEFAULT_INPUT_ROOT),
        help="Path to the Copy of Rosters folder. Used only as a fallback for roster-only runs.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=str(DEFAULT_OUTPUT_WORKBOOK),
        help="Output workbook path. Default: Member_Tenure_Report.xlsx",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each raw workbook as it is processed.",
    )
    return parser.parse_args()


def to_float(value: str) -> Optional[float]:
    text = clean_text(value).replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def row_identity(row: Observation) -> Optional[Tuple[str, ...]]:
    if row.banner_id:
        return ("banner", row.chapter.lower(), row.banner_id.lower())
    if row.email:
        return ("email", row.chapter.lower(), row.email.lower())
    if row.last_name or row.first_name:
        return ("name", row.chapter.lower(), row.last_name.lower(), row.first_name.lower())
    return None


def canonical_text(value: str) -> str:
    return " ".join(clean_text(value).lower().replace("/", " ").replace("-", " ").split())


def is_new_member_marker(status: str, position: str) -> bool:
    values = [canonical_text(status), canonical_text(position)]
    return any(
        value == "new member"
        or "new member" in value
        or value == "nm"
        for value in values
    )


def has_grade_data(row: Observation) -> bool:
    return any(
        [
            row.term_gpa,
            row.txstate_cumulative_gpa,
            row.overall_cumulative_gpa,
            row.cumulative_hours,
            row.semester_hours,
            row.current_academic_standing,
        ]
    )


def dedupe_term_rows(rows: Sequence[Observation]) -> List[Observation]:
    best_rows: Dict[Tuple[str, ...], Observation] = {}
    for row in rows:
        identity = row_identity(row)
        if identity is None:
            continue
        key = identity + (row.term.lower(),)
        existing = best_rows.get(key)
        if existing is None or row_score(row) > row_score(existing):
            best_rows[key] = row
    return list(best_rows.values())


def row_score(row: Observation) -> int:
    score = STATUS_PRIORITY.get(row.status, 10)
    if is_new_member_marker(row.status, row.position):
        score += 10
    if row.banner_id:
        score += 5
    if row.email:
        score += 3
    if row.semester_joined:
        score += 1
    if has_grade_data(row):
        score += 4
    return score


def choose_best_identity_row(rows: Sequence[Observation]) -> Observation:
    return max(rows, key=row_score)


def choose_status(rows: Sequence[Observation]) -> str:
    return max((row.status for row in rows), key=lambda status: STATUS_PRIORITY.get(status, 10))


def choose_best_term_observation(rows: Sequence[Observation]) -> Observation:
    return max(rows, key=row_score)


def term_label_sort(term_label: str) -> Tuple[int, int, str]:
    year_match = next((part for part in term_label.split() if part.isdigit() and len(part) == 4), "9999")
    return term_sort_key(year_match, term_label)


def extract_term_year(term_label: str) -> Optional[int]:
    for part in clean_text(term_label).split():
        if part.isdigit() and len(part) == 4:
            return int(part)
    return None


def classify_outcome(final_status: str) -> str:
    if final_status in {"Graduated", "Alumni"}:
        return "Graduated"
    if final_status == "Suspended":
        return "Suspended"
    if final_status == "Transfer":
        return "Transfer"
    if final_status in {"Inactive", "Resigned", "Revoked"}:
        return "Dropped"
    return "Still Active / Unknown"


def bucket_cumulative_hours(cumulative_hours: Optional[float]) -> str:
    if cumulative_hours is None or cumulative_hours < 0:
        return "Unknown"
    lower = int(cumulative_hours // JOIN_HOURS_BUCKET_SIZE) * JOIN_HOURS_BUCKET_SIZE
    upper = lower + JOIN_HOURS_BUCKET_SIZE - 1
    return f"{lower}-{upper}"


def join_hours_bucket_sort_key(bucket: str) -> Tuple[int, int, str]:
    if bucket == "Unknown":
        return (1, 999999, bucket)
    lower_text = bucket.split("-", 1)[0]
    if lower_text.isdigit():
        return (0, int(lower_text), bucket)
    return (1, 999999, bucket)


def estimate_semester_number_from_cumulative_hours(cumulative_hours: Optional[float]) -> Optional[int]:
    if cumulative_hours is None:
        return None
    return max(1, int(round(cumulative_hours / HOURS_PER_SEMESTER)) + 1)


def infer_semester_numbers(term_rows: Sequence[Sequence[Observation]]) -> List[int]:
    candidate_numbers: List[Optional[int]] = []
    for rows in term_rows:
        candidates = [
            estimate_semester_number_from_cumulative_hours(to_float(row.cumulative_hours))
            for row in rows
            if to_float(row.cumulative_hours) is not None
        ]
        candidate_numbers.append(max(candidates) if candidates else None)

    start_estimate = 1
    for idx, candidate in enumerate(candidate_numbers):
        if candidate is not None:
            start_estimate = max(start_estimate, candidate - idx)

    semester_numbers: List[int] = []
    for idx, candidate in enumerate(candidate_numbers):
        estimated = start_estimate + idx
        if candidate is not None:
            estimated = max(estimated, candidate)
        if semester_numbers:
            estimated = max(estimated, semester_numbers[-1] + 1)
        semester_numbers.append(estimated)
    return semester_numbers


def load_master_observations(master_path: Path) -> Tuple[List[Observation], bool]:
    rows: List[Observation] = []
    if not master_path.exists():
        return rows, False

    grade_aware = False
    wb = load_workbook(master_path, data_only=True, read_only=True)
    try:
        for ws in wb.worksheets:
            if ws.title.lower() == "summary":
                continue

            sheet_rows = list(ws.iter_rows(values_only=True))
            if not sheet_rows:
                continue

            header_values = [clean_text(value) for value in sheet_rows[0]]
            header_set = set(header_values)
            if not MASTER_REQUIRED_COLUMNS.issubset(header_set):
                continue

            grade_aware = grade_aware or bool(GRADE_AWARE_COLUMNS.intersection(header_set))
            header_map = {header: idx for idx, header in enumerate(header_values)}

            for row in sheet_rows[1:]:
                observation = Observation(
                    academic_year=get_value(row, header_map, "Academic Year"),
                    term=get_value(row, header_map, "Term"),
                    source_file=get_value(row, header_map, "Source File"),
                    chapter=get_value(row, header_map, "Chapter"),
                    last_name=get_value(row, header_map, "Last Name"),
                    first_name=get_value(row, header_map, "First Name"),
                    banner_id=normalize_banner_id(get_value(row, header_map, "Banner ID")),
                    email=get_value(row, header_map, "Email").lower(),
                    status=normalize_status(get_value(row, header_map, "Status")),
                    semester_joined=get_value(row, header_map, "Semester Joined"),
                    position=get_value(row, header_map, "Position"),
                    grade_student_status=get_value(row, header_map, "Grade Student Status"),
                    semester_hours=get_value(row, header_map, "Semester Hours"),
                    cumulative_hours=get_value(row, header_map, "Cumulative Hours"),
                    current_academic_standing=get_value(row, header_map, "Current Academic Standing"),
                    term_gpa=get_value(row, header_map, "Term GPA"),
                    txstate_cumulative_gpa=get_value(row, header_map, "TxState Cumulative GPA"),
                    overall_cumulative_gpa=get_value(row, header_map, "Overall Cumulative GPA"),
                    term_passed_hours=get_value(row, header_map, "Term Passed Hours"),
                )
                if not any([observation.chapter, observation.last_name, observation.first_name, observation.banner_id, observation.email, observation.status]):
                    continue
                rows.append(observation)
    finally:
        wb.close()
    return rows, grade_aware


def get_value(row: Tuple[object, ...], header_map: Dict[str, int], column: str) -> str:
    idx = header_map.get(column)
    if idx is None or idx >= len(row):
        return ""
    return clean_text(row[idx])


def observation_from_extracted_row(row: ExtractedRow) -> Observation:
    return Observation(
        academic_year=row.academic_year,
        term=row.term,
        source_file=row.source_file,
        chapter=row.chapter,
        last_name=row.last_name,
        first_name=row.first_name,
        banner_id=row.banner_id,
        email=row.email.lower(),
        status=normalize_status(row.status),
        semester_joined=row.semester_joined,
        position=row.position,
    )


def load_raw_rosters(raw_root: Path, verbose: bool = False) -> List[Observation]:
    rows: List[Observation] = []
    if not raw_root.exists():
        return rows

    files = sorted(path for path in raw_root.rglob("*") if path.suffix.lower() in SUPPORTED_EXTENSIONS)
    for path in files:
        extracted, _ = extract_rows_from_workbook(path, verbose=verbose)
        rows.extend(observation_from_extracted_row(row) for row in extracted)
    return rows


def build_member_journeys(rows: Sequence[Observation]) -> Tuple[List[MemberJourney], List[GpaPoint]]:
    deduped_rows = dedupe_term_rows(rows)
    rows_by_member: Dict[Tuple[str, ...], List[Observation]] = defaultdict(list)

    for row in deduped_rows:
        identity = row_identity(row)
        if identity is None:
            continue
        rows_by_member[identity].append(row)

    journeys: List[MemberJourney] = []
    gpa_points: List[GpaPoint] = []

    for member_rows in rows_by_member.values():
        best_identity = choose_best_identity_row(member_rows)
        rows_by_term: Dict[str, List[Observation]] = defaultdict(list)
        for row in member_rows:
            rows_by_term[row.term].append(row)

        ordered_terms = sorted(rows_by_term.keys(), key=term_label_sort)
        ordered_term_rows = [rows_by_term[term] for term in ordered_terms]
        best_term_rows = [choose_best_term_observation(term_rows) for term_rows in ordered_term_rows]
        semester_numbers = infer_semester_numbers(ordered_term_rows)
        semester_count = len(ordered_terms)

        new_member_indices = [
            idx
            for idx, term_rows in enumerate(ordered_term_rows)
            if any(is_new_member_marker(term_row.status, term_row.position) for term_row in term_rows)
        ]

        if new_member_indices:
            start_idx = new_member_indices[0]
            start_term = ordered_terms[start_idx]
            start_basis = "Observed New Member"
            join_cumulative_hours = to_float(best_term_rows[start_idx].cumulative_hours)
            join_cumulative_hours_bucket = bucket_cumulative_hours(join_cumulative_hours)
        else:
            start_idx = 0
            start_term = ordered_terms[0]
            start_basis = "First Observed"
            join_cumulative_hours = None
            join_cumulative_hours_bucket = "Unknown"

        first_new_member_term = ordered_terms[start_idx] if new_member_indices else ""
        semesters_from_new_member = len(ordered_terms[start_idx:])
        join_semester_at_school = semester_numbers[start_idx]
        exit_semester_at_school = semester_numbers[-1]
        last_observed_term = ordered_terms[-1]
        final_status = choose_status(ordered_term_rows[-1])
        outcome_group = classify_outcome(final_status)

        left_term = ""
        exit_reason = ""
        if final_status in TERMINAL_STATUSES:
            left_term = last_observed_term
            exit_reason = final_status

        returned_later = "No"
        for idx, term_rows in enumerate(ordered_term_rows[:-1]):
            if choose_status(term_rows) in TERMINAL_STATUSES:
                returned_later = "Yes"
                break

        term_history = " | ".join(ordered_terms)
        status_history = " | ".join(
            f"{term}: {choose_status(term_rows)}" for term, term_rows in zip(ordered_terms, ordered_term_rows)
        )

        term_gpas = [to_float(row.term_gpa) for row in best_term_rows[start_idx:] if to_float(row.term_gpa) is not None]
        avg_term_gpa = sum(term_gpas) / len(term_gpas) if term_gpas else None

        txstate_cumulative_values = [
            to_float(row.txstate_cumulative_gpa)
            for row in best_term_rows[start_idx:]
            if to_float(row.txstate_cumulative_gpa) is not None
        ]
        latest_txstate_cumulative_gpa = txstate_cumulative_values[-1] if txstate_cumulative_values else None

        overall_cumulative_values = [
            to_float(row.overall_cumulative_gpa)
            for row in best_term_rows[start_idx:]
            if to_float(row.overall_cumulative_gpa) is not None
        ]
        latest_overall_cumulative_gpa = overall_cumulative_values[-1] if overall_cumulative_values else None

        journeys.append(
            MemberJourney(
                chapter=best_identity.chapter,
                last_name=best_identity.last_name,
                first_name=best_identity.first_name,
                banner_id=best_identity.banner_id,
                email=best_identity.email,
                start_term=start_term,
                start_basis=start_basis,
                first_new_member_term=first_new_member_term,
                last_observed_term=last_observed_term,
                left_term=left_term,
                exit_reason=exit_reason,
                final_status=final_status,
                outcome_group=outcome_group,
                returned_later=returned_later,
                semester_count=semester_count,
                semesters_from_new_member=semesters_from_new_member,
                join_semester_at_school=join_semester_at_school,
                exit_semester_at_school=exit_semester_at_school,
                join_cumulative_hours=join_cumulative_hours,
                join_cumulative_hours_bucket=join_cumulative_hours_bucket,
                avg_term_gpa=avg_term_gpa,
                latest_txstate_cumulative_gpa=latest_txstate_cumulative_gpa,
                latest_overall_cumulative_gpa=latest_overall_cumulative_gpa,
                term_history=term_history,
                status_history=status_history,
            )
        )

        new_member_year = extract_term_year(first_new_member_term)
        if first_new_member_term and new_member_year is not None and new_member_year >= 2015:
            for idx in range(start_idx, len(ordered_terms)):
                term_row = best_term_rows[idx]
                gpa_points.append(
                    GpaPoint(
                        banner_id=best_identity.banner_id,
                        email=best_identity.email,
                        chapter=best_identity.chapter,
                        term=ordered_terms[idx],
                        semester_at_school=semester_numbers[idx],
                        term_gpa=to_float(term_row.term_gpa),
                        txstate_cumulative_gpa=to_float(term_row.txstate_cumulative_gpa),
                        overall_cumulative_gpa=to_float(term_row.overall_cumulative_gpa),
                        cumulative_hours=to_float(term_row.cumulative_hours),
                    )
                )

    return (
        sorted(
            journeys,
            key=lambda item: (
                item.exit_semester_at_school,
                item.chapter.lower(),
                item.last_name.lower(),
                item.first_name.lower(),
                item.start_term.lower(),
            ),
        ),
        gpa_points,
    )


def filter_2015_plus_new_members(journeys: Sequence[MemberJourney]) -> List[MemberJourney]:
    filtered: List[MemberJourney] = []
    for journey in journeys:
        if not journey.first_new_member_term:
            continue
        new_member_year = extract_term_year(journey.first_new_member_term)
        if new_member_year is None or new_member_year < 2015:
            continue
        filtered.append(journey)
    return filtered


def write_outcome_rates_sheet(wb: Workbook, journeys: Sequence[MemberJourney]) -> None:
    ws = wb.create_sheet(title="Outcome Rates")
    ws.append(
        [
            "Estimated Semesters At School",
            "Members",
            "Graduated Count",
            "Graduated Rate",
            "Dropped Count",
            "Dropped Rate",
            "Suspended Count",
            "Suspended Rate",
            "Transfer Count",
            "Transfer Rate",
            "Still Active / Unknown Count",
            "Still Active / Unknown Rate",
            "Average Term GPA",
            "Latest TxState Cumulative GPA",
            "Latest Overall Cumulative GPA",
        ]
    )
    style_header(ws)

    grouped: Dict[int, List[MemberJourney]] = defaultdict(list)
    for journey in journeys:
        grouped[journey.exit_semester_at_school].append(journey)

    for semester_at_school in sorted(grouped):
        semester_group = grouped[semester_at_school]
        members = len(semester_group)
        counts = {outcome: 0 for outcome in OUTCOME_ORDER}
        for journey in semester_group:
            counts[journey.outcome_group] = counts.get(journey.outcome_group, 0) + 1

        avg_term_values = [journey.avg_term_gpa for journey in semester_group if journey.avg_term_gpa is not None]
        latest_txstate_values = [
            journey.latest_txstate_cumulative_gpa
            for journey in semester_group
            if journey.latest_txstate_cumulative_gpa is not None
        ]
        latest_overall_values = [
            journey.latest_overall_cumulative_gpa
            for journey in semester_group
            if journey.latest_overall_cumulative_gpa is not None
        ]

        ws.append(
            [
                semester_at_school,
                members,
                counts["Graduated"],
                counts["Graduated"] / members if members else 0,
                counts["Dropped"],
                counts["Dropped"] / members if members else 0,
                counts["Suspended"],
                counts["Suspended"] / members if members else 0,
                counts["Transfer"],
                counts["Transfer"] / members if members else 0,
                counts["Still Active / Unknown"],
                counts["Still Active / Unknown"] / members if members else 0,
                sum(avg_term_values) / len(avg_term_values) if avg_term_values else "",
                sum(latest_txstate_values) / len(latest_txstate_values) if latest_txstate_values else "",
                sum(latest_overall_values) / len(latest_overall_values) if latest_overall_values else "",
            ]
        )

    ws.freeze_panes = "A2"
    autosize_columns(ws)


def write_join_hours_outcome_rates_sheet(wb: Workbook, journeys: Sequence[MemberJourney]) -> None:
    ws = wb.create_sheet(title="Join Hours Rates")
    ws.append(
        [
            "Join Cumulative Hours Bucket",
            "Members",
            "Graduated Count",
            "Graduated Rate",
            "Dropped Count",
            "Dropped Rate",
            "Suspended Count",
            "Suspended Rate",
            "Transfer Count",
            "Transfer Rate",
            "Still Active / Unknown Count",
            "Still Active / Unknown Rate",
            "Average Join Cumulative Hours",
            "Average Term GPA",
            "Latest TxState Cumulative GPA",
            "Latest Overall Cumulative GPA",
        ]
    )
    style_header(ws)

    grouped: Dict[str, List[MemberJourney]] = defaultdict(list)
    for journey in journeys:
        grouped[journey.join_cumulative_hours_bucket].append(journey)

    for bucket in sorted(grouped, key=join_hours_bucket_sort_key):
        bucket_group = grouped[bucket]
        members = len(bucket_group)
        counts = {outcome: 0 for outcome in OUTCOME_ORDER}
        for journey in bucket_group:
            counts[journey.outcome_group] = counts.get(journey.outcome_group, 0) + 1

        join_hours_values = [
            journey.join_cumulative_hours for journey in bucket_group if journey.join_cumulative_hours is not None
        ]
        avg_term_values = [journey.avg_term_gpa for journey in bucket_group if journey.avg_term_gpa is not None]
        latest_txstate_values = [
            journey.latest_txstate_cumulative_gpa
            for journey in bucket_group
            if journey.latest_txstate_cumulative_gpa is not None
        ]
        latest_overall_values = [
            journey.latest_overall_cumulative_gpa
            for journey in bucket_group
            if journey.latest_overall_cumulative_gpa is not None
        ]

        ws.append(
            [
                bucket,
                members,
                counts["Graduated"],
                counts["Graduated"] / members if members else 0,
                counts["Dropped"],
                counts["Dropped"] / members if members else 0,
                counts["Suspended"],
                counts["Suspended"] / members if members else 0,
                counts["Transfer"],
                counts["Transfer"] / members if members else 0,
                counts["Still Active / Unknown"],
                counts["Still Active / Unknown"] / members if members else 0,
                sum(join_hours_values) / len(join_hours_values) if join_hours_values else "",
                sum(avg_term_values) / len(avg_term_values) if avg_term_values else "",
                sum(latest_txstate_values) / len(latest_txstate_values) if latest_txstate_values else "",
                sum(latest_overall_values) / len(latest_overall_values) if latest_overall_values else "",
            ]
        )

    ws.freeze_panes = "A2"
    autosize_columns(ws)


def write_gpa_by_semester_sheet(wb: Workbook, points: Sequence[GpaPoint]) -> None:
    ws = wb.create_sheet(title="GPA by Semester")
    ws.append(
        [
            "Estimated Semesters At School",
            "Records",
            "Distinct Members",
            "Average Term GPA",
            "Average TxState Cumulative GPA",
            "Average Overall Cumulative GPA",
            "Average Cumulative Hours",
        ]
    )
    style_header(ws)

    grouped: Dict[int, List[GpaPoint]] = defaultdict(list)
    for point in points:
        grouped[point.semester_at_school].append(point)

    for semester_at_school in sorted(grouped):
        semester_points = grouped[semester_at_school]
        members = {
            (point.banner_id.lower() if point.banner_id else "", point.email.lower())
            for point in semester_points
        }
        term_gpas = [point.term_gpa for point in semester_points if point.term_gpa is not None]
        txstate_cumulative = [
            point.txstate_cumulative_gpa
            for point in semester_points
            if point.txstate_cumulative_gpa is not None
        ]
        overall_cumulative = [
            point.overall_cumulative_gpa
            for point in semester_points
            if point.overall_cumulative_gpa is not None
        ]
        cumulative_hours = [point.cumulative_hours for point in semester_points if point.cumulative_hours is not None]

        ws.append(
            [
                semester_at_school,
                len(semester_points),
                len(members),
                sum(term_gpas) / len(term_gpas) if term_gpas else "",
                sum(txstate_cumulative) / len(txstate_cumulative) if txstate_cumulative else "",
                sum(overall_cumulative) / len(overall_cumulative) if overall_cumulative else "",
                sum(cumulative_hours) / len(cumulative_hours) if cumulative_hours else "",
            ]
        )

    ws.freeze_panes = "A2"
    autosize_columns(ws)


def write_new_member_journeys_sheet(wb: Workbook, journeys: Sequence[MemberJourney]) -> None:
    ws = wb.create_sheet(title="2015+ New Members")
    headers = [
        "Chapter",
        "Last Name",
        "First Name",
        "Banner ID",
        "Email",
        "Start Term",
        "Start Basis",
        "First New Member Term",
        "Last Observed Term",
        "Left Term",
        "Exit Reason",
        "Final Status",
        "Outcome Group",
        "Returned Later",
        "Confirmed Join In Window",
        "Semester Count",
        "Semesters From New Member",
        "Join Semester At School",
        "Exit Semester At School",
        "Join Cumulative Hours",
        "Join Cumulative Hours Bucket",
        "Average Term GPA",
        "Latest TxState Cumulative GPA",
        "Latest Overall Cumulative GPA",
        "Term History",
        "Status History",
    ]
    ws.append(headers)
    style_header(ws)

    for journey in sorted(
        journeys,
        key=lambda item: (
            item.exit_semester_at_school,
            item.chapter.lower(),
            item.last_name.lower(),
            item.first_name.lower(),
            item.first_new_member_term.lower(),
        ),
    ):
        ws.append(journey.as_list()[:13] + [journey.returned_later, journey.confirmed_join_within_window] + journey.as_list()[14:])

    ws.freeze_panes = "A2"
    autosize_columns(ws)


def write_summary_sheet(
    wb: Workbook,
    journeys: Sequence[MemberJourney],
    gpa_points: Sequence[GpaPoint],
    master_path: Path,
    raw_root: Path,
    used_raw_fallback: bool,
) -> None:
    ws = wb.active
    ws.title = "Summary"
    ws.append(["Metric", "Value"])
    style_header(ws)

    counts_by_semester = defaultdict(int)
    counts_by_school_semester = defaultdict(int)
    counts_by_join_hours_bucket = defaultdict(int)
    confirmed_counts_by_semester = defaultdict(int)
    counts_by_exit_reason = defaultdict(int)
    returned_count = 0
    new_member_2015_plus = filter_2015_plus_new_members(journeys)
    outcome_counts = defaultdict(int)

    for journey in journeys:
        counts_by_semester[journey.semester_count] += 1
        counts_by_school_semester[journey.exit_semester_at_school] += 1
        if journey.first_new_member_term:
            counts_by_join_hours_bucket[journey.join_cumulative_hours_bucket] += 1
        if journey.first_new_member_term:
            confirmed_counts_by_semester[journey.semester_count] += 1
        if journey.exit_reason:
            counts_by_exit_reason[journey.exit_reason] += 1
        if journey.returned_later == "Yes":
            returned_count += 1

    for journey in new_member_2015_plus:
        outcome_counts[journey.outcome_group] += 1

    metrics = [
        ["Master workbook", str(master_path)],
        ["Raw roster folder", str(raw_root)],
        ["Used raw roster fallback", "Yes" if used_raw_fallback else "No"],
        ["Distinct member journeys", len(journeys)],
        ["Journeys with observed new-member term", sum(1 for item in journeys if item.first_new_member_term)],
        ["Journeys with inferred first-observed start", sum(1 for item in journeys if not item.first_new_member_term)],
        ["Confirmed in-window joins", sum(1 for item in journeys if item.first_new_member_term)],
        ["Unconfirmed pre-window carryovers", sum(1 for item in journeys if not item.first_new_member_term)],
        ["Members who returned after a terminal status", returned_count],
        ["2015+ new-member journeys", len(new_member_2015_plus)],
        ["2015+ GPA observations", len(gpa_points)],
    ]

    for metric in metrics:
        ws.append(metric)

    ws.append([])
    ws.append(["Semester Count", "Member Count"])
    for cell in ws[ws.max_row]:
        cell.fill = PatternFill("solid", fgColor="D9EAF7")
        cell.font = Font(bold=True)
    for semester_count in sorted(counts_by_semester):
        ws.append([semester_count, counts_by_semester[semester_count]])

    ws.append([])
    ws.append(["Estimated Semesters At School", "Member Count"])
    for cell in ws[ws.max_row]:
        cell.fill = PatternFill("solid", fgColor="D9EAF7")
        cell.font = Font(bold=True)
    for semester_count in sorted(counts_by_school_semester):
        ws.append([semester_count, counts_by_school_semester[semester_count]])

    ws.append([])
    ws.append(["Observed Join Cumulative Hours Bucket", "Member Count"])
    for cell in ws[ws.max_row]:
        cell.fill = PatternFill("solid", fgColor="D9EAF7")
        cell.font = Font(bold=True)
    for bucket in sorted(counts_by_join_hours_bucket, key=join_hours_bucket_sort_key):
        ws.append([bucket, counts_by_join_hours_bucket[bucket]])

    ws.append([])
    ws.append(["Semester Count", "Confirmed In-Window Join Count"])
    for cell in ws[ws.max_row]:
        cell.fill = PatternFill("solid", fgColor="D9EAF7")
        cell.font = Font(bold=True)
    for semester_count in sorted(confirmed_counts_by_semester):
        ws.append([semester_count, confirmed_counts_by_semester[semester_count]])

    ws.append([])
    ws.append(["Exit Reason", "Member Count"])
    for cell in ws[ws.max_row]:
        cell.fill = PatternFill("solid", fgColor="D9EAF7")
        cell.font = Font(bold=True)
    if counts_by_exit_reason:
        for exit_reason in sorted(counts_by_exit_reason):
            ws.append([exit_reason, counts_by_exit_reason[exit_reason]])
    else:
        ws.append(["None observed", 0])

    ws.append([])
    ws.append(["2015+ New Member Outcomes", "Member Count", "Rate"])
    for cell in ws[ws.max_row]:
        cell.fill = PatternFill("solid", fgColor="D9EAF7")
        cell.font = Font(bold=True)
    if new_member_2015_plus:
        total_new_members = len(new_member_2015_plus)
        for outcome in OUTCOME_ORDER:
            count = outcome_counts[outcome]
            ws.append([outcome, count, count / total_new_members])
    else:
        ws.append(["None observed", 0, 0])

    ws.freeze_panes = "A2"
    autosize_columns(ws)


def write_semester_sheets(wb: Workbook, journeys: Sequence[MemberJourney]) -> None:
    grouped: Dict[int, List[MemberJourney]] = defaultdict(list)
    for journey in journeys:
        grouped[journey.exit_semester_at_school].append(journey)

    headers = [
        "Chapter",
        "Last Name",
        "First Name",
        "Banner ID",
        "Email",
        "Start Term",
        "Start Basis",
        "First New Member Term",
        "Last Observed Term",
        "Left Term",
        "Exit Reason",
        "Final Status",
        "Outcome Group",
        "Returned Later",
        "Confirmed Join In Window",
        "Observed Semester Count",
        "Semesters From New Member",
        "Join Semester At School",
        "Exit Semester At School",
        "Join Cumulative Hours",
        "Join Cumulative Hours Bucket",
        "Average Term GPA",
        "Latest TxState Cumulative GPA",
        "Latest Overall Cumulative GPA",
        "Term History",
        "Status History",
    ]

    for semester_at_school in sorted(grouped):
        ws = wb.create_sheet(title=f"{semester_at_school}_Semester"[:31])
        ws.append(["All Observed Members"])
        ws[ws.max_row][0].font = Font(bold=True)
        ws.append(headers)
        for cell in ws[ws.max_row]:
            cell.fill = PatternFill("solid", fgColor="D9EAF7")
            cell.font = Font(bold=True)
        for journey in grouped[semester_at_school]:
            ws.append(journey.as_list()[:14] + [journey.confirmed_join_within_window] + journey.as_list()[14:])

        ws.append([])
        ws.append(["Confirmed In-Window Joins Only"])
        ws[ws.max_row][0].font = Font(bold=True)
        ws.append(headers)
        for cell in ws[ws.max_row]:
            cell.fill = PatternFill("solid", fgColor="D9EAF7")
            cell.font = Font(bold=True)
        for journey in grouped[semester_at_school]:
            if journey.first_new_member_term:
                ws.append(journey.as_list()[:14] + [journey.confirmed_join_within_window] + journey.as_list()[14:])
        ws.freeze_panes = "A2"
        autosize_columns(ws)


def build_member_tenure_report(master_path: Path, raw_root: Path, output_path: Path, verbose: bool = False) -> None:
    master_rows, grade_aware = load_master_observations(master_path)
    raw_rows: List[Observation] = []
    used_raw_fallback = False
    if not grade_aware:
        raw_rows = load_raw_rosters(raw_root, verbose=verbose)
        used_raw_fallback = bool(raw_rows)

    all_rows = master_rows + raw_rows
    if not all_rows:
        raise FileNotFoundError(
            "No usable roster rows were found in the supplied master workbook or the Copy of Rosters folder."
        )

    journeys, gpa_points = build_member_journeys(all_rows)
    new_member_journeys = filter_2015_plus_new_members(journeys)

    wb = Workbook()
    write_summary_sheet(wb, journeys, gpa_points, master_path, raw_root, used_raw_fallback)
    write_outcome_rates_sheet(wb, new_member_journeys)
    write_join_hours_outcome_rates_sheet(wb, new_member_journeys)
    write_gpa_by_semester_sheet(wb, gpa_points)
    write_new_member_journeys_sheet(wb, new_member_journeys)
    write_semester_sheets(wb, journeys)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)


def main() -> None:
    args = parse_args()
    master_path = Path(args.master).expanduser().resolve()
    raw_root = Path(args.raw_root).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    build_member_tenure_report(
        master_path=master_path,
        raw_root=raw_root,
        output_path=output_path,
        verbose=args.verbose,
    )
    print(f"Member tenure report created: {output_path}")


if __name__ == "__main__":
    main()
