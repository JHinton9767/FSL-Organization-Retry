from __future__ import annotations

import argparse
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from openpyxl import Workbook, load_workbook

from src.build_master_roster import (
    SUPPORTED_EXTENSIONS,
    autosize_columns,
    canonical_header,
    clean_text,
    normalize_banner_id,
    style_header,
    term_sort_key,
)
from src.build_member_tenure_report import DEFAULT_MASTER_WORKBOOK


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_GRADES_ROOT = ROOT / "data" / "inbox" / "academic"
DEFAULT_TENURE_WORKBOOK = ROOT / "Member_Tenure_Report.xlsx"
DEFAULT_OUTPUT_WORKBOOK = ROOT / "Master_Roster_Grades.xlsx"
GRADE_TERM_RE = re.compile(
    r"(Fall|Spring|Summer|Winter)\s+(20\d{2})(?:\s+\d+)?(?:\s*\(\d{1,2}\.\d{1,2}\.\d{2,4}\))?$",
    re.IGNORECASE,
)
GRADE_UPDATE_RE = re.compile(r"\((\d{1,2})\.(\d{1,2})\.(\d{2,4})\)")

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

TENURE_REQUIRED_COLUMNS = {
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
    "Term History",
    "Status History",
}

GRADE_COLUMN_ALIASES = {
    "Last Name": {"last name", "lastname"},
    "First Name": {"first name", "firstname"},
    "Banner ID": {"banner id", "student id", "banner", "student number", "banner number"},
    "Email": {"email", "e mail", "email address", "student email"},
    "Student Status": {"student status"},
    "Major": {"major"},
    "Semester Hours": {"semester hours"},
    "Current Academic Standing": {"current academic standing", "academic standing"},
    "Texas State GPA": {"texas state gpa", "txstate gpa"},
    "Overall GPA": {"overall gpa"},
    "Transfer GPA": {"transfer gpa"},
    "Term GPA": {"term gpa"},
    "Term Passed Hours": {"term passed hours"},
    "TxState Cumulative GPA": {"txstate cumulative gpa", "texas state cumulative gpa"},
    "Overall Cumulative GPA": {"overall cumulative gpa"},
}

COMBINED_COLUMNS = [
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
    "Grade Student Status",
    "Major",
    "Semester Hours",
    "Current Academic Standing",
    "Texas State GPA",
    "Overall GPA",
    "Transfer GPA",
    "Term GPA",
    "Term Passed Hours",
    "TxState Cumulative GPA",
    "Overall Cumulative GPA",
    "Tenure Start Term",
    "Tenure Start Basis",
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
    "Term History",
    "Status History",
]


@dataclass(frozen=True)
class MasterRosterRow:
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


@dataclass(frozen=True)
class GradeRow:
    term: str
    source_file: str
    last_name: str
    first_name: str
    banner_id: str
    email: str
    student_status: str
    major: str
    semester_hours: str
    current_academic_standing: str
    texas_state_gpa: str
    overall_gpa: str
    transfer_gpa: str
    term_gpa: str
    term_passed_hours: str
    txstate_cumulative_gpa: str
    overall_cumulative_gpa: str


@dataclass(frozen=True)
class TenureRow:
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
    confirmed_join_within_window: str
    semester_count: str
    semesters_from_new_member: str
    term_history: str
    status_history: str


@dataclass(frozen=True)
class CombinedRow:
    master: MasterRosterRow
    grade: Optional[GradeRow]
    tenure: Optional[TenureRow]

    def as_list(self) -> List[str]:
        grade = self.grade
        tenure = self.tenure
        return [
            self.master.academic_year,
            self.master.term,
            self.master.source_file,
            self.master.chapter,
            self.master.last_name,
            self.master.first_name,
            self.master.banner_id,
            self.master.email,
            self.master.status,
            self.master.semester_joined,
            self.master.position,
            grade.student_status if grade else "",
            grade.major if grade else "",
            grade.semester_hours if grade else "",
            grade.current_academic_standing if grade else "",
            grade.texas_state_gpa if grade else "",
            grade.overall_gpa if grade else "",
            grade.transfer_gpa if grade else "",
            grade.term_gpa if grade else "",
            grade.term_passed_hours if grade else "",
            grade.txstate_cumulative_gpa if grade else "",
            grade.overall_cumulative_gpa if grade else "",
            tenure.start_term if tenure else "",
            tenure.start_basis if tenure else "",
            tenure.first_new_member_term if tenure else "",
            tenure.last_observed_term if tenure else "",
            tenure.left_term if tenure else "",
            tenure.exit_reason if tenure else "",
            tenure.final_status if tenure else "",
            tenure.outcome_group if tenure else "",
            tenure.returned_later if tenure else "",
            tenure.confirmed_join_within_window if tenure else "",
            tenure.semester_count if tenure else "",
            tenure.semesters_from_new_member if tenure else "",
            tenure.term_history if tenure else "",
            tenure.status_history if tenure else "",
        ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Combine Master_FSL_Roster.xlsx, semester grade reports, and Member_Tenure_Report.xlsx "
            "into one semester-chunked roster/grades workbook."
        )
    )
    parser.add_argument(
        "--master",
        default=str(DEFAULT_MASTER_WORKBOOK),
        help="Path to Master_FSL_Roster.xlsx. Default: Master_FSL_Roster.xlsx next to the code.",
    )
    parser.add_argument(
        "--grades-root",
        default=str(DEFAULT_GRADES_ROOT),
        help="Folder containing semester grade report workbooks. Default: data\\inbox\\academic",
    )
    parser.add_argument(
        "--tenure",
        default=str(DEFAULT_TENURE_WORKBOOK),
        help="Path to Member_Tenure_Report.xlsx. Default: Member_Tenure_Report.xlsx next to the code.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=str(DEFAULT_OUTPUT_WORKBOOK),
        help="Output workbook path. Default: Master_Roster_Grades.xlsx",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Number of rows per semester sheet. Default: 1000",
    )
    return parser.parse_args()


def get_cell(row: Tuple[object, ...], index: Optional[int]) -> str:
    if index is None or index >= len(row):
        return ""
    return clean_text(row[index])


def canonical_person_name(last_name: str, first_name: str) -> Tuple[str, str]:
    return clean_text(last_name).lower(), clean_text(first_name).lower()


def parse_grade_term(path: Path, sheet_name: str) -> Optional[str]:
    candidates = [path.stem, sheet_name] + list(path.parts)
    for candidate in candidates:
        cleaned = clean_text(candidate)
        match = GRADE_TERM_RE.search(cleaned)
        if match:
            return f"{match.group(1).title()} {match.group(2)}"
    return None


def parse_grade_update_key(source_name: str) -> Tuple[int, int, int]:
    match = GRADE_UPDATE_RE.search(source_name)
    if not match:
        return (0, 0, 0)

    month = int(match.group(1))
    day = int(match.group(2))
    year = int(match.group(3))
    if year < 100:
        year += 2000
    return (year, month, day)


def grade_row_priority(row: GradeRow) -> Tuple[Tuple[int, int, int], int, int]:
    return (
        parse_grade_update_key(row.source_file),
        1 if row.banner_id else 0,
        1 if row.email else 0,
    )


def map_grade_headers(headers: Sequence[object]) -> Dict[str, int]:
    mapped: Dict[str, int] = {}
    canon_headers = [canonical_header(value) for value in headers]
    for idx, header in enumerate(canon_headers):
        for target, aliases in GRADE_COLUMN_ALIASES.items():
            if header in aliases and target not in mapped:
                mapped[target] = idx
    return mapped


def load_master_rows(master_path: Path) -> List[MasterRosterRow]:
    rows: List[MasterRosterRow] = []
    wb = load_workbook(master_path, data_only=True, read_only=True)
    try:
        for ws in wb.worksheets:
            if ws.title.lower() == "summary":
                continue

            sheet_rows = list(ws.iter_rows(values_only=True))
            if not sheet_rows:
                continue

            header_values = [clean_text(value) for value in sheet_rows[0]]
            if not MASTER_REQUIRED_COLUMNS.issubset(set(header_values)):
                continue

            header_map = {header: idx for idx, header in enumerate(header_values)}
            for row in sheet_rows[1:]:
                roster_row = MasterRosterRow(
                    academic_year=get_cell(row, header_map.get("Academic Year")),
                    term=get_cell(row, header_map.get("Term")),
                    source_file=get_cell(row, header_map.get("Source File")),
                    chapter=get_cell(row, header_map.get("Chapter")),
                    last_name=get_cell(row, header_map.get("Last Name")),
                    first_name=get_cell(row, header_map.get("First Name")),
                    banner_id=normalize_banner_id(get_cell(row, header_map.get("Banner ID"))),
                    email=get_cell(row, header_map.get("Email")).lower(),
                    status=get_cell(row, header_map.get("Status")),
                    semester_joined=get_cell(row, header_map.get("Semester Joined")),
                    position=get_cell(row, header_map.get("Position")),
                )
                if not any([roster_row.last_name, roster_row.first_name, roster_row.banner_id, roster_row.email]):
                    continue
                rows.append(roster_row)
    finally:
        wb.close()
    return rows


def load_grade_rows(grades_root: Path) -> List[GradeRow]:
    rows: List[GradeRow] = []
    files = sorted(path for path in grades_root.rglob("*") if path.suffix.lower() in SUPPORTED_EXTENSIONS)
    if not files:
        raise FileNotFoundError(
            f"No Excel files found under {grades_root}. Supported types: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
        )

    for path in files:
        wb = load_workbook(path, data_only=True, read_only=True)
        try:
            for ws in wb.worksheets:
                term = parse_grade_term(path, ws.title)
                if not term:
                    continue

                sheet_rows = list(ws.iter_rows(values_only=True))
                if not sheet_rows:
                    continue

                header_map = map_grade_headers(sheet_rows[0])
                required = {"Last Name", "First Name"}
                has_identifier = "Banner ID" in header_map or "Email" in header_map
                if not required.issubset(set(header_map.keys())) or not has_identifier:
                    continue

                for row in sheet_rows[1:]:
                    grade_row = GradeRow(
                        term=term,
                        source_file=path.name,
                        last_name=get_cell(row, header_map.get("Last Name")),
                        first_name=get_cell(row, header_map.get("First Name")),
                        banner_id=normalize_banner_id(get_cell(row, header_map.get("Banner ID"))),
                        email=get_cell(row, header_map.get("Email")).lower(),
                        student_status=get_cell(row, header_map.get("Student Status")),
                        major=get_cell(row, header_map.get("Major")),
                        semester_hours=get_cell(row, header_map.get("Semester Hours")),
                        current_academic_standing=get_cell(row, header_map.get("Current Academic Standing")),
                        texas_state_gpa=get_cell(row, header_map.get("Texas State GPA")),
                        overall_gpa=get_cell(row, header_map.get("Overall GPA")),
                        transfer_gpa=get_cell(row, header_map.get("Transfer GPA")),
                        term_gpa=get_cell(row, header_map.get("Term GPA")),
                        term_passed_hours=get_cell(row, header_map.get("Term Passed Hours")),
                        txstate_cumulative_gpa=get_cell(row, header_map.get("TxState Cumulative GPA")),
                        overall_cumulative_gpa=get_cell(row, header_map.get("Overall Cumulative GPA")),
                    )
                    if not any([grade_row.last_name, grade_row.first_name, grade_row.banner_id, grade_row.email]):
                        continue
                    rows.append(grade_row)
        finally:
            wb.close()

    return rows


def load_tenure_rows(tenure_path: Path) -> List[TenureRow]:
    rows: List[TenureRow] = []
    if not tenure_path.exists():
        return rows

    wb = load_workbook(tenure_path, data_only=True, read_only=True)
    try:
        for ws in wb.worksheets:
            sheet_rows = list(ws.iter_rows(values_only=True))
            if not sheet_rows:
                continue

            header_values = [clean_text(value) for value in sheet_rows[0]]
            if not TENURE_REQUIRED_COLUMNS.issubset(set(header_values)):
                continue

            header_map = {header: idx for idx, header in enumerate(header_values)}
            for row in sheet_rows[1:]:
                tenure_row = TenureRow(
                    chapter=get_cell(row, header_map.get("Chapter")),
                    last_name=get_cell(row, header_map.get("Last Name")),
                    first_name=get_cell(row, header_map.get("First Name")),
                    banner_id=normalize_banner_id(get_cell(row, header_map.get("Banner ID"))),
                    email=get_cell(row, header_map.get("Email")).lower(),
                    start_term=get_cell(row, header_map.get("Start Term")),
                    start_basis=get_cell(row, header_map.get("Start Basis")),
                    first_new_member_term=get_cell(row, header_map.get("First New Member Term")),
                    last_observed_term=get_cell(row, header_map.get("Last Observed Term")),
                    left_term=get_cell(row, header_map.get("Left Term")),
                    exit_reason=get_cell(row, header_map.get("Exit Reason")),
                    final_status=get_cell(row, header_map.get("Final Status")),
                    outcome_group=get_cell(row, header_map.get("Outcome Group")),
                    returned_later=get_cell(row, header_map.get("Returned Later")),
                    confirmed_join_within_window=get_cell(row, header_map.get("Confirmed Join In Window")),
                    semester_count=get_cell(row, header_map.get("Semester Count")),
                    semesters_from_new_member=get_cell(row, header_map.get("Semesters From New Member")),
                    term_history=get_cell(row, header_map.get("Term History")),
                    status_history=get_cell(row, header_map.get("Status History")),
                )
                if not any([tenure_row.last_name, tenure_row.first_name, tenure_row.banner_id, tenure_row.email]):
                    continue
                rows.append(tenure_row)
    finally:
        wb.close()
    return rows


def build_master_lookup(rows: Sequence[MasterRosterRow]) -> Dict[Tuple[str, ...], MasterRosterRow]:
    lookup: Dict[Tuple[str, ...], MasterRosterRow] = {}
    for row in rows:
        if row.banner_id:
            lookup[("banner", row.term.lower(), row.banner_id.lower())] = row
        if row.email:
            lookup[("email", row.term.lower(), row.email.lower())] = row
        if row.last_name or row.first_name:
            last_name, first_name = canonical_person_name(row.last_name, row.first_name)
            lookup[("name", row.term.lower(), last_name, first_name)] = row
    return lookup


def build_tenure_lookup(rows: Sequence[TenureRow]) -> Dict[Tuple[str, ...], TenureRow]:
    lookup: Dict[Tuple[str, ...], TenureRow] = {}

    def as_int(value: str) -> int:
        text = clean_text(value)
        return int(text) if text.isdigit() else 0

    def score(item: TenureRow) -> Tuple[int, int]:
        return (
            1 if item.first_new_member_term else 0,
            as_int(item.semesters_from_new_member) or as_int(item.semester_count),
        )

    for row in rows:
        keys: List[Tuple[str, ...]] = []
        if row.banner_id:
            keys.append(("banner", row.banner_id.lower()))
        if row.email:
            keys.append(("email", row.email.lower()))
        if row.last_name or row.first_name:
            last_name, first_name = canonical_person_name(row.last_name, row.first_name)
            keys.append(("name", last_name, first_name))

        for key in keys:
            existing = lookup.get(key)
            if existing is None or score(row) > score(existing):
                lookup[key] = row

    return lookup


def match_grade_row(grade_row: GradeRow, master_lookup: Dict[Tuple[str, ...], MasterRosterRow]) -> Optional[MasterRosterRow]:
    if grade_row.banner_id:
        match = master_lookup.get(("banner", grade_row.term.lower(), grade_row.banner_id.lower()))
        if match:
            return match
    if grade_row.email:
        match = master_lookup.get(("email", grade_row.term.lower(), grade_row.email.lower()))
        if match:
            return match

    last_name, first_name = canonical_person_name(grade_row.last_name, grade_row.first_name)
    return master_lookup.get(("name", grade_row.term.lower(), last_name, first_name))


def build_grade_lookup(
    master_rows: Sequence[MasterRosterRow],
    grade_rows: Sequence[GradeRow],
) -> Tuple[Dict[Tuple[str, ...], GradeRow], List[GradeRow]]:
    master_lookup = build_master_lookup(master_rows)
    grade_lookup: Dict[Tuple[str, ...], GradeRow] = {}
    unmatched: List[GradeRow] = []

    for grade_row in grade_rows:
        matched_master = match_grade_row(grade_row, master_lookup)
        if matched_master is None:
            unmatched.append(grade_row)
            continue

        if matched_master.banner_id:
            key = ("banner", matched_master.term.lower(), matched_master.banner_id.lower())
        elif matched_master.email:
            key = ("email", matched_master.term.lower(), matched_master.email.lower())
        else:
            last_name, first_name = canonical_person_name(matched_master.last_name, matched_master.first_name)
            key = ("name", matched_master.term.lower(), last_name, first_name)

        existing = grade_lookup.get(key)
        if existing is None or grade_row_priority(grade_row) >= grade_row_priority(existing):
            grade_lookup[key] = grade_row

    return grade_lookup, unmatched


def find_tenure_row(master_row: MasterRosterRow, tenure_lookup: Dict[Tuple[str, ...], TenureRow]) -> Optional[TenureRow]:
    if master_row.banner_id:
        match = tenure_lookup.get(("banner", master_row.banner_id.lower()))
        if match:
            return match
    if master_row.email:
        match = tenure_lookup.get(("email", master_row.email.lower()))
        if match:
            return match

    last_name, first_name = canonical_person_name(master_row.last_name, master_row.first_name)
    return tenure_lookup.get(("name", last_name, first_name))


def merge_rows(master_rows: Sequence[MasterRosterRow], grade_lookup: Dict[Tuple[str, ...], GradeRow], tenure_lookup: Dict[Tuple[str, ...], TenureRow]) -> List[CombinedRow]:
    combined: List[CombinedRow] = []
    for master_row in master_rows:
        if master_row.banner_id:
            key = ("banner", master_row.term.lower(), master_row.banner_id.lower())
        elif master_row.email:
            key = ("email", master_row.term.lower(), master_row.email.lower())
        else:
            last_name, first_name = canonical_person_name(master_row.last_name, master_row.first_name)
            key = ("name", master_row.term.lower(), last_name, first_name)

        combined.append(
            CombinedRow(
                master=master_row,
                grade=grade_lookup.get(key),
                tenure=find_tenure_row(master_row, tenure_lookup),
            )
        )
    return combined


def write_summary_sheet(wb: Workbook, combined_rows: Sequence[CombinedRow], unmatched_grade_rows: Sequence[GradeRow], master_path: Path, grades_root: Path, tenure_path: Path) -> None:
    ws = wb.active
    ws.title = "Summary"
    ws.append(["Metric", "Value"])
    style_header(ws)

    grade_matches = sum(1 for row in combined_rows if row.grade is not None)
    tenure_matches = sum(1 for row in combined_rows if row.tenure is not None)
    both_matches = sum(1 for row in combined_rows if row.grade is not None and row.tenure is not None)

    metrics = [
        ["Master workbook", str(master_path)],
        ["Grades root", str(grades_root)],
        ["Tenure workbook", str(tenure_path)],
        ["Combined roster rows", len(combined_rows)],
        ["Rows matched to grades", grade_matches],
        ["Rows matched to tenure", tenure_matches],
        ["Rows matched to both", both_matches],
        ["Unmatched grade rows", len(unmatched_grade_rows)],
    ]
    for metric in metrics:
        ws.append(metric)

    ws.freeze_panes = "A2"
    autosize_columns(ws)


def write_combined_sheets(wb: Workbook, combined_rows: Sequence[CombinedRow], chunk_size: int) -> None:
    grouped: Dict[Tuple[str, str], List[CombinedRow]] = defaultdict(list)
    for row in combined_rows:
        grouped[(row.master.academic_year, row.master.term)].append(row)

    for academic_year, term in sorted(grouped.keys(), key=lambda item: term_sort_key(item[0], item[1])):
        semester_rows = sorted(
            grouped[(academic_year, term)],
            key=lambda item: (
                item.master.chapter.lower(),
                item.master.last_name.lower(),
                item.master.first_name.lower(),
                item.master.banner_id.lower() if item.master.banner_id else "zzzzzzzz",
            ),
        )

        for start in range(0, len(semester_rows), chunk_size):
            end = min(start + chunk_size, len(semester_rows))
            chunk_number = (start // chunk_size) + 1
            sheet_name = f"{clean_text(term)} {chunk_number}"
            ws = wb.create_sheet(title=sheet_name[:31])
            ws.append(COMBINED_COLUMNS)
            style_header(ws)
            for row in semester_rows[start:end]:
                ws.append(row.as_list())
            ws.freeze_panes = "A2"
            autosize_columns(ws)


def write_unmatched_grades_sheet(wb: Workbook, rows: Sequence[GradeRow]) -> None:
    ws = wb.create_sheet(title="Unmatched Grades")
    ws.append(
        [
            "Term",
            "Source File",
            "Last Name",
            "First Name",
            "Banner ID",
            "Email",
            "Student Status",
            "Major",
            "Semester Hours",
            "Current Academic Standing",
            "Texas State GPA",
            "Overall GPA",
            "Transfer GPA",
            "Term GPA",
            "Term Passed Hours",
            "TxState Cumulative GPA",
            "Overall Cumulative GPA",
        ]
    )
    style_header(ws)
    for row in rows:
        ws.append(
            [
                row.term,
                row.source_file,
                row.last_name,
                row.first_name,
                row.banner_id,
                row.email,
                row.student_status,
                row.major,
                row.semester_hours,
                row.current_academic_standing,
                row.texas_state_gpa,
                row.overall_gpa,
                row.transfer_gpa,
                row.term_gpa,
                row.term_passed_hours,
                row.txstate_cumulative_gpa,
                row.overall_cumulative_gpa,
            ]
        )
    ws.freeze_panes = "A2"
    autosize_columns(ws)


def build_master_roster_grades(master_path: Path, grades_root: Path, tenure_path: Path, output_path: Path, chunk_size: int) -> None:
    if not master_path.exists():
        raise FileNotFoundError(f"Master roster workbook not found: {master_path}")

    master_rows = load_master_rows(master_path)
    if not master_rows:
        raise FileNotFoundError(f"No usable master roster rows were found in {master_path}")

    grade_rows = load_grade_rows(grades_root)
    tenure_rows = load_tenure_rows(tenure_path)
    grade_lookup, unmatched_grade_rows = build_grade_lookup(master_rows, grade_rows)
    tenure_lookup = build_tenure_lookup(tenure_rows)
    combined_rows = merge_rows(master_rows, grade_lookup, tenure_lookup)

    wb = Workbook()
    write_summary_sheet(wb, combined_rows, unmatched_grade_rows, master_path, grades_root, tenure_path)
    write_combined_sheets(wb, combined_rows, chunk_size=chunk_size)
    write_unmatched_grades_sheet(wb, unmatched_grade_rows)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)


def main() -> None:
    args = parse_args()
    master_path = Path(args.master).expanduser().resolve()
    grades_root = Path(args.grades_root).expanduser().resolve()
    tenure_path = Path(args.tenure).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    build_master_roster_grades(
        master_path=master_path,
        grades_root=grades_root,
        tenure_path=tenure_path,
        output_path=output_path,
        chunk_size=args.chunk_size,
    )
    print(f"Master roster/grades workbook created: {output_path}")


if __name__ == "__main__":
    main()
