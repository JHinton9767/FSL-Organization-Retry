from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill

from src.build_executive_report import (
    DEFAULT_ENHANCED_ROOT,
    adjusted_graduation_rate,
    clean_text,
    coerce_numeric,
    load_latest_bundle,
    selected_cumulative_gpa,
    yes_mask,
)
from src.build_master_roster import autosize_columns, is_excluded_chapter


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_ROOT = ROOT / "output" / "chapter_history"

HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
HEADER_FONT = Font(color="FFFFFF", bold=True)
TITLE_FONT = Font(bold=True, size=14)
SECTION_FONT = Font(bold=True, size=12)

YEAR_DETAIL_COLUMNS = [
    "Term",
    "Student ID",
    "Last Name",
    "First Name",
    "Email",
    "Roster Present",
    "Academic Present",
    "Organization Entry Term",
    "Organization Entry Cohort",
    "Relative Term Index From Org Entry",
    "Roster Status Raw",
    "Roster Status Bucket",
    "Roster Position",
    "New Member Marker",
    "Latest Known Outcome Bucket",
    "Academic Student Status Raw",
    "Major",
    "Semester Hours",
    "Term Passed Hours",
    "Cumulative Hours",
    "Current Academic Standing Raw",
    "Academic Standing Bucket",
    "Term GPA",
    "TxState Cumulative GPA",
    "Overall Cumulative GPA",
    "Roster Source File",
    "Academic Source File",
    "No Further Observation Flag",
]
PERCENT_HEADERS = {
    "Observed Graduation Rate (Resolved)",
    "Observed 4-Year Graduation Rate (Resolved)",
    "Observed 6-Year Graduation Rate (Resolved)",
    "Retained In Chapter To Next Term",
    "Retained In Chapter To Next Fall",
    "Stayed In School To Next Term",
    "Stayed In School To Next Fall",
    "Share Of Entry Students",
    "Good Standing Rate",
}
DECIMAL_HEADERS = {
    "Average First-Term GPA",
    "Average First-Year GPA",
    "Average Latest Cumulative GPA",
    "Average Term GPA",
    "Average Cumulative GPA",
    "Average Passed Hours",
    "Average TxState Cumulative GPA",
    "Average Overall Cumulative GPA",
}
SEASON_ORDER = {"Winter": 0, "Spring": 1, "Summer": 2, "Fall": 3}


@dataclass(frozen=True)
class ChapterBuildResult:
    output_folder: Path
    workbook_folder: Path
    index_workbook: Path
    index_csv: Path
    readme_path: Path
    chapters_written: int
    source_folder: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build one workbook per chapter from the enhanced analytics bundle. "
            "Each chapter workbook contains a summary sheet plus year-by-year detail sheets."
        )
    )
    parser.add_argument("--enhanced-root", default=str(DEFAULT_ENHANCED_ROOT))
    parser.add_argument("--enhanced-folder", default="")
    parser.add_argument("--enhanced-workbook", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    return parser.parse_args()


def safe_filename(value: str) -> str:
    text = clean_text(value) or "Unknown"
    for char in '<>:"/\\|?*':
        text = text.replace(char, "_")
    return text[:120].strip(" ._") or "Unknown"


def safe_sheet_name(value: str) -> str:
    text = clean_text(value) or "Unknown"
    for char in '[]:*?/\\':
        text = text.replace(char, "")
    return text[:31] or "Unknown"


def style_row_as_header(ws, row_idx: int) -> None:
    for cell in ws[row_idx]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT


def write_section_title(ws, row_idx: int, title: str, note: str = "") -> int:
    ws.cell(row=row_idx, column=1, value=title).font = SECTION_FONT
    if note:
        ws.cell(row=row_idx + 1, column=1, value=note)
        return row_idx + 2
    return row_idx + 1


def format_table_columns(ws, header_row: int, start_data_row: int, end_data_row: int) -> None:
    if end_data_row < start_data_row:
        return
    headers = [clean_text(cell.value) for cell in ws[header_row]]
    for col_idx, header in enumerate(headers, start=1):
        if header in PERCENT_HEADERS:
            for row_idx in range(start_data_row, end_data_row + 1):
                ws.cell(row=row_idx, column=col_idx).number_format = "0.0%"
        elif header in DECIMAL_HEADERS:
            for row_idx in range(start_data_row, end_data_row + 1):
                ws.cell(row=row_idx, column=col_idx).number_format = "0.00"


def append_table(
    ws,
    start_row: int,
    title: str,
    note: str,
    headers: Sequence[str],
    rows: Iterable[Sequence[object]],
) -> int:
    row_idx = write_section_title(ws, start_row, title, note)
    header_row = row_idx
    ws.append(list(headers))
    style_row_as_header(ws, header_row)
    data_start = header_row + 1
    data_end = header_row
    for row in rows:
        ws.append(list(row))
        data_end = ws.max_row
    format_table_columns(ws, header_row, data_start, data_end)
    return ws.max_row + 2


def choose_display_names(values: Iterable[str]) -> Dict[str, str]:
    counts: Dict[str, Counter[str]] = defaultdict(Counter)
    for value in values:
        text = clean_text(value)
        if not text:
            continue
        counts[text.lower()][text] += 1
    return {
        normalized: counter.most_common(1)[0][0]
        for normalized, counter in counts.items()
    }


def chapter_sort_key(value: str) -> Tuple[int, str]:
    text = clean_text(value)
    return (1 if not text else 0, text.lower())


def filter_to_chapter_entry(summary: pd.DataFrame, chapter: str) -> pd.DataFrame:
    target = clean_text(chapter).lower()
    return summary.loc[summary["Initial Chapter"].fillna("").astype(str).str.strip().str.lower().eq(target)].copy()


def filter_to_chapter_rows(longitudinal: pd.DataFrame, chapter: str) -> pd.DataFrame:
    target = clean_text(chapter).lower()
    return longitudinal.loc[
        longitudinal["Chapter"].fillna("").astype(str).str.strip().str.lower().eq(target)
    ].copy()


def unique_non_blank_count(series: pd.Series) -> int:
    cleaned = series.fillna("").astype(str).str.strip()
    return int(cleaned.replace("", pd.NA).dropna().nunique())


def mean_or_blank(series: pd.Series) -> object:
    numeric = coerce_numeric(series)
    if numeric.dropna().empty:
        return ""
    return float(numeric.dropna().mean())


def percent_display(value: object) -> str:
    if value == "" or pd.isna(value):
        return ""
    return f"{float(value):.1%}"


def decimal_display(value: object) -> str:
    if value == "" or pd.isna(value):
        return ""
    return f"{float(value):.2f}"


def yes_rate(frame: pd.DataFrame, value_field: str, measurable_field: Optional[str] = None) -> Tuple[object, int]:
    eligible = frame.copy()
    if measurable_field and measurable_field in eligible.columns:
        eligible = eligible.loc[yes_mask(eligible[measurable_field])]
    if eligible.empty:
        return "", 0
    return float(yes_mask(eligible[value_field]).sum()) / float(len(eligible)), int(len(eligible))


def selected_summary_cumulative_gpa(frame: pd.DataFrame) -> pd.Series:
    return selected_cumulative_gpa(frame)


def selected_longitudinal_cumulative_gpa(frame: pd.DataFrame) -> pd.Series:
    overall = coerce_numeric(frame["Overall Cumulative GPA"]) if "Overall Cumulative GPA" in frame.columns else pd.Series(index=frame.index, dtype=float)
    txstate = coerce_numeric(frame["TxState Cumulative GPA"]) if "TxState Cumulative GPA" in frame.columns else pd.Series(index=frame.index, dtype=float)
    return overall.where(overall.notna(), txstate)


def prepare_summary(summary: pd.DataFrame) -> pd.DataFrame:
    frame = summary.copy()
    for column in [
        "Initial Chapter",
        "Latest Chapter",
        "Organization Entry Cohort",
        "Latest Known Outcome Bucket",
        "Latest Academic Standing",
        "Preferred Last Name",
        "Preferred First Name",
        "Preferred Email",
    ]:
        if column in frame.columns:
            frame[column] = frame[column].fillna("").astype(str).str.strip()
    return frame


def prepare_longitudinal(longitudinal: pd.DataFrame) -> pd.DataFrame:
    frame = longitudinal.copy()
    for column in [
        "Chapter",
        "Term",
        "Term Season",
        "Last Name",
        "First Name",
        "Email",
        "Student ID",
    ]:
        if column in frame.columns:
            frame[column] = frame[column].fillna("").astype(str).str.strip()
    frame["_term_year_numeric"] = coerce_numeric(frame["Term Year"]) if "Term Year" in frame.columns else pd.Series(index=frame.index, dtype=float)
    frame["_relative_term_numeric"] = coerce_numeric(frame["Relative Term Index From Org Entry"]) if "Relative Term Index From Org Entry" in frame.columns else pd.Series(index=frame.index, dtype=float)
    frame["_selected_cumulative_gpa"] = selected_longitudinal_cumulative_gpa(frame)
    return frame


def build_overview_rows(chapter: str, entry_students: pd.DataFrame, chapter_rows: pd.DataFrame) -> List[List[object]]:
    cohorts = sorted(
        {
            clean_text(value)
            for value in entry_students["Organization Entry Cohort"].tolist()
            if clean_text(value)
        }
    )
    grad_rate, grad_n = adjusted_graduation_rate(entry_students, "Eventual Observed Graduation From Org Entry")
    grad4_rate, grad4_n = adjusted_graduation_rate(
        entry_students,
        "Observed Graduation Within 4 Years Of Org Entry",
        measurable_field="Observed Graduation Within 4 Years Of Org Entry Measurable",
    )
    grad6_rate, grad6_n = adjusted_graduation_rate(
        entry_students,
        "Observed Graduation Within 6 Years Of Org Entry",
        measurable_field="Observed Graduation Within 6 Years Of Org Entry Measurable",
    )
    org_next_term_rate, org_next_term_n = yes_rate(
        entry_students,
        "Retained In Organization To Next Observed Term",
        measurable_field="Organization Next Observed Term Measurable",
    )
    org_next_fall_rate, org_next_fall_n = yes_rate(
        entry_students,
        "Retained In Organization To Next Fall",
        measurable_field="Organization Next Fall Measurable",
    )
    acad_next_term_rate, acad_next_term_n = yes_rate(
        entry_students,
        "Continued Academically To Next Observed Term",
        measurable_field="Academic Next Observed Term Measurable",
    )
    acad_next_fall_rate, acad_next_fall_n = yes_rate(
        entry_students,
        "Continued Academically To Next Fall",
        measurable_field="Academic Next Fall Measurable",
    )
    latest_cumulative = selected_summary_cumulative_gpa(entry_students)
    return [
        [
            "Distinct students ever observed in this chapter",
            unique_non_blank_count(chapter_rows["Student ID"]) if not chapter_rows.empty else 0,
            "Counts anyone observed in this chapter in any term.",
        ],
        [
            "Students with observed entry into this chapter",
            len(entry_students),
            "These students start their observed organization history in this chapter.",
        ],
        [
            "Organization-entry cohorts covered",
            len(cohorts),
            "Counts distinct entry cohorts tied to this chapter.",
        ],
        [
            "Observed eventual graduation rate from chapter entry (excluding unresolved outcomes)",
            percent_display(grad_rate),
            f"Uses {grad_n} resolved students from this chapter's observed entry group.",
        ],
        [
            "Observed 4-year graduation rate from chapter entry (excluding unresolved outcomes)",
            percent_display(grad4_rate),
            f"Uses {grad4_n} measurable and resolved students.",
        ],
        [
            "Observed 6-year graduation rate from chapter entry (excluding unresolved outcomes)",
            percent_display(grad6_rate),
            f"Uses {grad6_n} measurable and resolved students.",
        ],
        [
            "Retained in chapter to next observed term",
            percent_display(org_next_term_rate),
            f"Uses {org_next_term_n} students whose next-term follow-up is observable.",
        ],
        [
            "Retained in chapter to next fall",
            percent_display(org_next_fall_rate),
            f"Uses {org_next_fall_n} students whose next-fall follow-up is observable.",
        ],
        [
            "Stayed in school to next observed term",
            percent_display(acad_next_term_rate),
            f"Uses {acad_next_term_n} students whose next-term academic follow-up is observable.",
        ],
        [
            "Stayed in school to next fall",
            percent_display(acad_next_fall_rate),
            f"Uses {acad_next_fall_n} students whose next-fall academic follow-up is observable.",
        ],
        [
            "Average first-term GPA after chapter entry",
            decimal_display(mean_or_blank(entry_students["First Post-Entry Term GPA"])) if not entry_students.empty else "",
            "Averages the first academic term GPA observed after chapter entry.",
        ],
        [
            "Average first-year GPA after chapter entry",
            decimal_display(mean_or_blank(entry_students["First-Year Average Term GPA After Org Entry"])) if not entry_students.empty else "",
            "Averages the first-year GPA across observed post-entry terms.",
        ],
        [
            "Average latest cumulative GPA",
            decimal_display(mean_or_blank(latest_cumulative)) if not entry_students.empty else "",
            "Uses the latest overall cumulative GPA when available, otherwise the latest TxState cumulative GPA.",
        ],
    ]


def build_cohort_rows(entry_students: pd.DataFrame) -> List[List[object]]:
    rows: List[List[object]] = []
    if entry_students.empty:
        return rows
    cohorts = sorted(
        {
            clean_text(value)
            for value in entry_students["Organization Entry Cohort"].tolist()
            if clean_text(value)
        }
    )
    for cohort in cohorts:
        frame = entry_students.loc[
            entry_students["Organization Entry Cohort"].fillna("").astype(str).str.strip().eq(cohort)
        ].copy()
        grad_rate, grad_n = adjusted_graduation_rate(frame, "Eventual Observed Graduation From Org Entry")
        grad4_rate, _ = adjusted_graduation_rate(
            frame,
            "Observed Graduation Within 4 Years Of Org Entry",
            measurable_field="Observed Graduation Within 4 Years Of Org Entry Measurable",
        )
        grad6_rate, _ = adjusted_graduation_rate(
            frame,
            "Observed Graduation Within 6 Years Of Org Entry",
            measurable_field="Observed Graduation Within 6 Years Of Org Entry Measurable",
        )
        org_next_term_rate, _ = yes_rate(
            frame,
            "Retained In Organization To Next Observed Term",
            measurable_field="Organization Next Observed Term Measurable",
        )
        org_next_fall_rate, _ = yes_rate(
            frame,
            "Retained In Organization To Next Fall",
            measurable_field="Organization Next Fall Measurable",
        )
        acad_next_term_rate, _ = yes_rate(
            frame,
            "Continued Academically To Next Observed Term",
            measurable_field="Academic Next Observed Term Measurable",
        )
        acad_next_fall_rate, _ = yes_rate(
            frame,
            "Continued Academically To Next Fall",
            measurable_field="Academic Next Fall Measurable",
        )
        rows.append(
            [
                cohort,
                len(frame),
                grad_n,
                grad_rate,
                grad4_rate,
                grad6_rate,
                org_next_term_rate,
                org_next_fall_rate,
                acad_next_term_rate,
                acad_next_fall_rate,
                mean_or_blank(frame["First Post-Entry Term GPA"]),
                mean_or_blank(selected_summary_cumulative_gpa(frame)),
            ]
        )
    return rows


def build_outcome_rows(entry_students: pd.DataFrame) -> List[List[object]]:
    if entry_students.empty:
        return []
    counts = Counter(clean_text(value) or "Unknown" for value in entry_students["Latest Known Outcome Bucket"].tolist())
    total = sum(counts.values())
    rows: List[List[object]] = []
    for bucket in [
        "Graduated",
        "Suspended",
        "Transfer",
        "Dropped/Resigned/Revoked/Inactive",
        "No Further Observation",
        "Active/Unknown",
        "Unknown",
    ]:
        count = counts.get(bucket, 0)
        if not count:
            continue
        rows.append([bucket, count, (float(count) / float(total)) if total else ""])
    return rows


def build_yearly_trend_rows(chapter_rows: pd.DataFrame) -> List[List[object]]:
    if chapter_rows.empty:
        return []
    rows: List[List[object]] = []
    grouped = chapter_rows.loc[chapter_rows["_term_year_numeric"].notna()].groupby("_term_year_numeric")
    for year_value, frame in sorted(grouped, key=lambda item: int(item[0])):
        academic_only = frame.loc[yes_mask(frame["Academic Present"])]
        standing_known = academic_only.loc[
            academic_only["Academic Standing Bucket"].fillna("").astype(str).str.strip().ne("")
        ]
        good_standing_rate = ""
        if not standing_known.empty:
            good_standing_rate = float(
                standing_known["Academic Standing Bucket"].fillna("").astype(str).str.strip().eq("Good Standing").sum()
            ) / float(len(standing_known))
        rows.append(
            [
                int(year_value),
                unique_non_blank_count(frame["Student ID"]),
                int(yes_mask(frame["Roster Present"]).sum()),
                int(yes_mask(frame["Academic Present"]).sum()),
                mean_or_blank(frame["Term GPA"]),
                mean_or_blank(frame["_selected_cumulative_gpa"]),
                good_standing_rate,
                mean_or_blank(frame["Term Passed Hours"]),
            ]
        )
    return rows


def build_relative_term_gpa_rows(chapter_rows: pd.DataFrame) -> List[List[object]]:
    if chapter_rows.empty:
        return []
    eligible = chapter_rows.loc[
        yes_mask(chapter_rows["Academic Present"])
        & chapter_rows["_relative_term_numeric"].notna()
        & chapter_rows["_relative_term_numeric"].ge(0)
    ].copy()
    if eligible.empty:
        return []
    rows: List[List[object]] = []
    grouped = eligible.groupby("_relative_term_numeric")
    for relative_term, frame in sorted(grouped, key=lambda item: int(item[0])):
        rows.append(
            [
                int(relative_term),
                len(frame),
                unique_non_blank_count(frame["Student ID"]),
                mean_or_blank(frame["Term GPA"]),
                mean_or_blank(frame["TxState Cumulative GPA"]),
                mean_or_blank(frame["Overall Cumulative GPA"]),
            ]
        )
    return rows


def build_year_sheet_rows(frame: pd.DataFrame) -> List[List[object]]:
    if frame.empty:
        return []
    sorted_frame = frame.copy()
    sorted_frame["_season_sort"] = sorted_frame["Term Season"].map(lambda value: SEASON_ORDER.get(clean_text(value), 9))
    sorted_frame = sorted_frame.sort_values(
        by=["_term_year_numeric", "_season_sort", "Last Name", "First Name", "Student ID"],
        ascending=[True, True, True, True, True],
        na_position="last",
    )
    rows: List[List[object]] = []
    for _, row in sorted_frame.iterrows():
        rows.append([row.get(column, "") for column in YEAR_DETAIL_COLUMNS])
    return rows


def create_index_row(chapter: str, entry_students: pd.DataFrame, chapter_rows: pd.DataFrame, workbook_path: Path) -> List[object]:
    grad_rate, _ = adjusted_graduation_rate(entry_students, "Eventual Observed Graduation From Org Entry")
    org_next_fall_rate, _ = yes_rate(
        entry_students,
        "Retained In Organization To Next Fall",
        measurable_field="Organization Next Fall Measurable",
    )
    acad_next_fall_rate, _ = yes_rate(
        entry_students,
        "Continued Academically To Next Fall",
        measurable_field="Academic Next Fall Measurable",
    )
    return [
        chapter,
        workbook_path.name,
        unique_non_blank_count(chapter_rows["Student ID"]) if not chapter_rows.empty else 0,
        len(entry_students),
        unique_non_blank_count(entry_students["Organization Entry Cohort"]) if not entry_students.empty else 0,
        grad_rate,
        org_next_fall_rate,
        acad_next_fall_rate,
        mean_or_blank(entry_students["First Post-Entry Term GPA"]) if not entry_students.empty else "",
        mean_or_blank(selected_summary_cumulative_gpa(entry_students)) if not entry_students.empty else "",
    ]


def write_summary_sheet(ws, chapter: str, entry_students: pd.DataFrame, chapter_rows: pd.DataFrame, source_folder: Path) -> None:
    ws.title = "Summary"
    ws["A1"] = f"{chapter} Chapter History"
    ws["A1"].font = TITLE_FONT
    ws["A2"] = (
        "This workbook flips the yearly roster builder into one workbook per chapter. "
        "Summary rates are based on students whose observed organization entry begins in this chapter."
    )
    ws["A3"] = f"Enhanced analytics source folder: {source_folder}"

    row_idx = 5
    row_idx = append_table(
        ws,
        row_idx,
        "Overview",
        "What this tells us: these are the headline counts and chapter-level rates built from observed organization entry and follow-up data.",
        ["Metric", "Value", "How To Read This"],
        build_overview_rows(chapter, entry_students, chapter_rows),
    )
    row_idx = append_table(
        ws,
        row_idx,
        "Entry Cohort Rates",
        "How to read this: each row is one observed chapter-entry cohort. Retention uses measurable follow-up windows only, and graduation excludes unresolved outcomes.",
        [
            "Organization Entry Cohort",
            "Students",
            "Resolved Outcome Students",
            "Observed Graduation Rate (Resolved)",
            "Observed 4-Year Graduation Rate (Resolved)",
            "Observed 6-Year Graduation Rate (Resolved)",
            "Retained In Chapter To Next Term",
            "Retained In Chapter To Next Fall",
            "Stayed In School To Next Term",
            "Stayed In School To Next Fall",
            "Average First-Term GPA",
            "Average Latest Cumulative GPA",
        ],
        build_cohort_rows(entry_students),
    )
    row_idx = append_table(
        ws,
        row_idx,
        "Latest Outcome Breakdown",
        "Why this matters: this shows the latest observed outcomes for students whose observed chapter entry starts here.",
        ["Latest Known Outcome", "Students", "Share Of Entry Students"],
        build_outcome_rows(entry_students),
    )
    row_idx = append_table(
        ws,
        row_idx,
        "GPA Trend After Chapter Entry",
        "What this tells us: this averages academic performance by relative term after observed chapter entry for students seen in this chapter.",
        [
            "Relative Term After Entry",
            "Academic Records",
            "Distinct Students",
            "Average Term GPA",
            "Average TxState Cumulative GPA",
            "Average Overall Cumulative GPA",
        ],
        build_relative_term_gpa_rows(chapter_rows),
    )
    append_table(
        ws,
        row_idx,
        "Yearly Trend",
        "How to read this: this summarizes the chapter's observed student-term records by year, including GPA and standing patterns.",
        [
            "Year",
            "Distinct Students",
            "Roster Rows",
            "Academic Rows",
            "Average Term GPA",
            "Average Cumulative GPA",
            "Good Standing Rate",
            "Average Passed Hours",
        ],
        build_yearly_trend_rows(chapter_rows),
    )
    ws.freeze_panes = "A5"
    autosize_columns(ws)


def write_year_sheet(wb: Workbook, chapter: str, year_label: str, frame: pd.DataFrame) -> None:
    ws = wb.create_sheet(title=safe_sheet_name(year_label))
    ws["A1"] = f"{chapter} - {year_label}"
    ws["A1"].font = TITLE_FONT
    ws["A2"] = (
        "One row per observed student-term for this chapter and year. "
        "Rows include both roster and academic fields when available."
    )
    ws.append(list(YEAR_DETAIL_COLUMNS))
    style_row_as_header(ws, 3)
    for row in build_year_sheet_rows(frame):
        ws.append(row)
    for header in [
        "Semester Hours",
        "Term Passed Hours",
        "Cumulative Hours",
        "Term GPA",
        "TxState Cumulative GPA",
        "Overall Cumulative GPA",
    ]:
        col_idx = YEAR_DETAIL_COLUMNS.index(header) + 1
        for row_idx in range(4, ws.max_row + 1):
            ws.cell(row=row_idx, column=col_idx).number_format = "0.00"
    ws.freeze_panes = "A4"
    autosize_columns(ws)


def write_chapter_workbook(
    chapter: str,
    entry_students: pd.DataFrame,
    chapter_rows: pd.DataFrame,
    output_path: Path,
    source_folder: Path,
) -> None:
    wb = Workbook()
    summary_ws = wb.active
    write_summary_sheet(summary_ws, chapter, entry_students, chapter_rows, source_folder)

    if not chapter_rows.empty:
        grouped = chapter_rows.loc[chapter_rows["_term_year_numeric"].notna()].groupby("_term_year_numeric")
        for year_value, frame in sorted(grouped, key=lambda item: int(item[0])):
            write_year_sheet(wb, chapter, str(int(year_value)), frame.copy())

    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)


def write_index_workbook(index_rows: List[List[object]], output_path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Chapters"
    ws.append(
        [
            "Chapter",
            "Workbook File",
            "Distinct Students Ever Observed",
            "Students With Observed Chapter Entry",
            "Entry Cohorts Covered",
            "Observed Graduation Rate (Resolved)",
            "Retained In Chapter To Next Fall",
            "Stayed In School To Next Fall",
            "Average First-Term GPA",
            "Average Latest Cumulative GPA",
        ]
    )
    style_row_as_header(ws, 1)
    for row in index_rows:
        ws.append(row)
    format_table_columns(ws, 1, 2, ws.max_row)
    ws.freeze_panes = "A2"
    autosize_columns(ws)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)


def write_readme(result: ChapterBuildResult) -> None:
    lines = [
        "# Chapter History Workbooks",
        "",
        "This folder contains one workbook per chapter, built additively from the enhanced analytics bundle.",
        "",
        "## Files",
        "",
        "- `Chapter_History_Index.xlsx`: one-row summary per chapter",
        "- `chapter_history_index.csv`: CSV copy of the chapter index",
        "- `workbooks/*.xlsx`: one workbook per chapter",
        "",
        "## Workbook layout",
        "",
        "- `Summary`: chapter-level graduation, retention, school continuation, GPA, outcome, and yearly trend tables",
        "- `YYYY`: one row per observed student-term for that chapter in that year",
        "",
        "## Source bundle",
        "",
        f"- `{result.source_folder}`",
        "",
        "## Important note",
        "",
        "Graduation rates in these workbooks exclude unresolved outcomes such as `Active/Unknown` and `No Further Observation` to avoid understating results for recent or incomplete cases.",
    ]
    result.readme_path.write_text("\n".join(lines), encoding="utf-8")


def build_chapter_history_workbooks(
    enhanced_root: Path,
    explicit_folder: Optional[Path],
    explicit_workbook: Optional[Path],
    output_root: Path,
) -> ChapterBuildResult:
    bundle = load_latest_bundle(
        enhanced_root=enhanced_root,
        explicit_folder=explicit_folder,
        explicit_workbook=explicit_workbook,
    )
    if "master_longitudinal" not in bundle.tables:
        raise FileNotFoundError(
            "The enhanced analytics bundle does not include Master_Longitudinal. "
            "Run py run_enhanced_org_analytics.py first so the chapter workbooks can build year sheets."
        )

    summary = prepare_summary(bundle.tables["student_summary"])
    longitudinal = prepare_longitudinal(bundle.tables["master_longitudinal"])

    chapter_names = choose_display_names(
        list(summary["Initial Chapter"].tolist())
        + list(summary["Latest Chapter"].tolist())
        + list(longitudinal["Chapter"].tolist())
    )
    chapters = [
        display
        for _, display in sorted(chapter_names.items(), key=lambda item: chapter_sort_key(item[1]))
        if display and display.lower() != "unknown" and not is_excluded_chapter(display)
    ]
    if not chapters:
        raise FileNotFoundError("No usable chapters were found in the enhanced analytics bundle.")

    timestamp = datetime.now().strftime("run_%Y%m%d_%H%M%S")
    output_folder = output_root / timestamp
    workbook_folder = output_folder / "workbooks"
    workbook_folder.mkdir(parents=True, exist_ok=True)

    index_rows: List[List[object]] = []
    chapters_written = 0
    for chapter in chapters:
        entry_students = filter_to_chapter_entry(summary, chapter)
        chapter_rows = filter_to_chapter_rows(longitudinal, chapter)
        if entry_students.empty and chapter_rows.empty:
            continue
        workbook_path = workbook_folder / f"{safe_filename(chapter)}.xlsx"
        write_chapter_workbook(
            chapter=chapter,
            entry_students=entry_students,
            chapter_rows=chapter_rows,
            output_path=workbook_path,
            source_folder=bundle.enhanced_folder,
        )
        index_rows.append(create_index_row(chapter, entry_students, chapter_rows, workbook_path))
        chapters_written += 1

    if not chapters_written:
        raise FileNotFoundError("No chapter workbooks were written because no chapter rows were available.")

    index_rows.sort(key=lambda row: chapter_sort_key(str(row[0])))
    index_workbook = output_folder / "Chapter_History_Index.xlsx"
    index_csv = output_folder / "chapter_history_index.csv"
    readme_path = output_folder / "README.md"

    write_index_workbook(index_rows, index_workbook)
    pd.DataFrame(
        index_rows,
        columns=[
            "Chapter",
            "Workbook File",
            "Distinct Students Ever Observed",
            "Students With Observed Chapter Entry",
            "Entry Cohorts Covered",
            "Observed Graduation Rate (Resolved)",
            "Retained In Chapter To Next Fall",
            "Stayed In School To Next Fall",
            "Average First-Term GPA",
            "Average Latest Cumulative GPA",
        ],
    ).to_csv(index_csv, index=False)

    result = ChapterBuildResult(
        output_folder=output_folder,
        workbook_folder=workbook_folder,
        index_workbook=index_workbook,
        index_csv=index_csv,
        readme_path=readme_path,
        chapters_written=chapters_written,
        source_folder=bundle.enhanced_folder,
    )
    write_readme(result)
    return result


def main() -> None:
    args = parse_args()
    result = build_chapter_history_workbooks(
        enhanced_root=Path(args.enhanced_root).expanduser().resolve(),
        explicit_folder=Path(args.enhanced_folder).expanduser().resolve() if args.enhanced_folder else None,
        explicit_workbook=Path(args.enhanced_workbook).expanduser().resolve() if args.enhanced_workbook else None,
        output_root=Path(args.output_root).expanduser().resolve(),
    )
    print(f"Chapter history workbooks created in: {result.output_folder}")
    print(f"Chapter workbooks: {result.workbook_folder}")
    print(f"Chapter index workbook: {result.index_workbook}")


if __name__ == "__main__":
    main()
