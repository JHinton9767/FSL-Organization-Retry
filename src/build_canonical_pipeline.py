from __future__ import annotations

import argparse
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
from openpyxl import load_workbook

from app.config_loader import load_chapter_mapping, load_settings
from app.status_framework import build_outcome_resolution_fields
from src.build_master_roster import (
    DEFAULT_INPUT_ROOT,
    SUPPORTED_EXTENSIONS,
    canonical_header,
    clean_text,
    detect_inline_chapter_label,
    find_header_row,
    find_status_column,
    get_cell,
    normalize_banner_id,
    normalize_chapter_name,
    normalize_status,
)


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ROSTER_ROOT = DEFAULT_INPUT_ROOT
DEFAULT_ROSTER_INBOX = ROOT / "data" / "inbox" / "rosters"
DEFAULT_ACADEMIC_ROOT = ROOT / "data" / "inbox" / "academic"
DEFAULT_GRADUATION_ROOT = ROOT / "data" / "inbox" / "graduation"
DEFAULT_REFERENCE_DATA_ROOT = ROOT / "data" / "inbox" / "reference_data"
DEFAULT_MEMBERSHIP_REFERENCE_ROOT = ROOT / "data" / "inbox" / "membership_reference"
DEFAULT_GPA_REFERENCE_ROOT = ROOT / "data" / "inbox" / "gpa_reference"
DEFAULT_GPA_BENCHMARK_ROOT = ROOT / "data" / "inbox" / "gpa_benchmark_reference"
DEFAULT_OUTPUT_ROOT = ROOT / "output" / "canonical"
SCHEMA_PATH = ROOT / "config" / "canonical_schema.json"

TERM_RE = re.compile(r"(Winter|Spring|Summer|Fall)\s+(19\d{2}|20\d{2})", re.IGNORECASE)
TERM_CODE_RE = re.compile(r"^(19\d{2}|20\d{2})(WI|SP|SU|FA)$", re.IGNORECASE)
UPDATE_RE = re.compile(r"\((\d{1,2})\.(\d{1,2})\.(\d{2,4})\)")
SEASON_ORDER = {"WI": 0, "SP": 1, "SU": 2, "FA": 3}
SEASON_NAME = {"WI": "Winter", "SP": "Spring", "SU": "Summer", "FA": "Fall"}
UNRESOLVED_OUTCOMES = {"Active/Unknown", "No Further Observation", "Unknown", ""}

SNAPSHOT_ALIAS_GROUPS = {
    "Student ID": {"student id", "banner id", "banner", "student number", "PLID", "plid"},
    "First Name": {"first name", "firstname"},
    "Last Name": {"last name", "lastname"},
    "NetID": {"netid", "net id"},
    "High School GPA": {"high school gpa", "hs gpa"},
    "Overall GPA": {"overall gpa"},
    "Institutional GPA": {"institutional gpa", "txst gpa", "texas state gpa"},
    "Transfer GPA": {"transfer gpa"},
    "Total Credit Hours": {"total credit hours", "total hours", "overall credit hours"},
    "TXST Credit Hours": {"txst credit hours", "texas state credit hours", "institutional credit hours"},
    "Previous Semester GPA": {"previous semester gpa", "previous term gpa"},
    "Student Status": {"student status"},
    "Student Status (FT/PT)": {"student status ftpt", "student status ft pt", "student status (ft/pt)", "ft/pt"},
}

SNAPSHOT_COLUMNS = [
    "Student ID",
    "First Name",
    "Last Name",
    "NetID",
    "High School GPA",
    "Overall GPA",
    "Institutional GPA",
    "Transfer GPA",
    "Total Credit Hours",
    "TXST Credit Hours",
    "Previous Semester GPA",
    "Student Status",
    "Student Status (FT/PT)",
    "Snapshot Source File",
]

GRADUATION_ALIAS_GROUPS = {
    "Student ID": {"student id", "banner id", "banner", "student number"},
    "First Name": {"first name", "firstname"},
    "Last Name": {"last name", "lastname"},
    "Graduation Term": {
        "graduation term",
        "graduation semester",
        "graduation date",
        "grad term",
        "grad semester",
        "graduated term",
    },
    "Outcome": {"status", "outcome", "graduation status", "degree status"},
}

GRADUATION_COLUMNS = [
    "Student ID",
    "First Name",
    "Last Name",
    "Graduation Term",
    "Outcome",
    "Graduation Source File",
]

QA_COLUMNS = ["Check Group", "Check", "Status", "Value", "Notes"]

GRADE_COLUMN_ALIASES = {
    "Banner ID": {"banner id", "student id", "banner", "student number"},
    "Last Name": {"last name", "lastname"},
    "First Name": {"first name", "firstname"},
    "Email": {"email", "email address"},
    "Student Status": {"student status", "status"},
    "Major": {"major", "primary major"},
    "Semester Hours": {"semester hours", "credits attempted", "attempted hours", "hours attempted"},
    "Cumulative Hours": {"cumulative hours", "total credit hours", "total hours"},
    "Current Academic Standing": {"current academic standing", "academic standing", "standing"},
    "Texas State GPA": {"texas state gpa", "institutional gpa", "txst gpa"},
    "Overall GPA": {"overall gpa"},
    "Transfer GPA": {"transfer gpa"},
    "Term GPA": {"term gpa", "gpa"},
    "Term Passed Hours": {"term passed hours", "credits earned", "earned hours", "passed hours"},
    "TxState Cumulative GPA": {"txstate cumulative gpa", "texas state cumulative gpa", "institutional cumulative gpa"},
    "Overall Cumulative GPA": {"overall cumulative gpa"},
    "Graduation Term": {"graduation term", "graduation semester", "graduated term", "grad term"},
}


@dataclass(frozen=True)
class CanonicalBuildResult:
    output_folder: Path
    files: Dict[str, Path]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build the canonical FSL analytics tables and QA outputs. "
            "Authoritative outputs: roster_term, academic_term, master_longitudinal, student_summary, cohort_metrics, qa_checks."
        )
    )
    parser.add_argument("--roster-root", default=str(DEFAULT_ROSTER_ROOT))
    parser.add_argument("--roster-inbox", default=str(DEFAULT_ROSTER_INBOX))
    parser.add_argument("--academic-root", default=str(DEFAULT_ACADEMIC_ROOT))
    parser.add_argument("--graduation-root", default=str(DEFAULT_GRADUATION_ROOT))
    parser.add_argument("--reference-data-root", default=str(DEFAULT_REFERENCE_DATA_ROOT))
    parser.add_argument("--membership-reference-root", default=str(DEFAULT_MEMBERSHIP_REFERENCE_ROOT))
    parser.add_argument("--gpa-reference-root", default=str(DEFAULT_GPA_REFERENCE_ROOT))
    parser.add_argument("--gpa-benchmark-root", default=str(DEFAULT_GPA_BENCHMARK_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    return parser.parse_args()


def load_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def ensure_columns(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    result = frame.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = pd.NA
    return result.loc[:, list(columns)]


def combine_reference_frames(
    frames: Sequence[pd.DataFrame],
    columns: Sequence[str],
    dedupe_subset: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    usable = [frame for frame in frames if frame is not None and not frame.empty]
    if not usable:
        return pd.DataFrame(columns=list(columns))
    combined = pd.concat([ensure_columns(frame, columns) for frame in usable], ignore_index=True)
    if dedupe_subset:
        combined = combined.drop_duplicates(subset=list(dedupe_subset), keep="first")
    return ensure_columns(combined, columns)


def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def bucket_30_hours(value: object) -> str:
    number = coerce_numeric(pd.Series([value])).iloc[0]
    if pd.isna(number):
        return "Unknown"
    lower = int(math.floor(float(number) / 30.0) * 30)
    upper = lower + 29
    return f"{lower}-{upper}"


def sort_term_code(term_code: str) -> int:
    match = TERM_CODE_RE.fullmatch(clean_text(term_code))
    if not match:
        return 999999
    return int(match.group(1)) * 10 + SEASON_ORDER.get(match.group(2).upper(), 9)


def parse_term_code(value: object) -> Tuple[str, str, object, str]:
    text = clean_text(value)
    match = TERM_CODE_RE.fullmatch(text)
    if match:
        year = int(match.group(1))
        season_code = match.group(2).upper()
        return (
            f"{year}{season_code}",
            f"{SEASON_NAME[season_code]} {year}",
            year,
            SEASON_NAME[season_code],
        )

    match = TERM_RE.search(text)
    if match:
        season_name = match.group(1).title()
        year = int(match.group(2))
        season_code = next(code for code, label in SEASON_NAME.items() if label == season_name)
        return (
            f"{year}{season_code}",
            f"{season_name} {year}",
            year,
            season_name,
        )

    year_match = re.search(r"(19\d{2}|20\d{2})", text)
    if year_match:
        year = int(year_match.group(1))
        return (f"{year}UN", str(year), year, "Unknown")

    return ("", clean_text(value), pd.NA, "Unknown")


def term_label_from_code(term_code: object) -> str:
    code, label, _, _ = parse_term_code(term_code)
    return label if code else clean_text(term_code)


def update_key_from_name(value: str) -> Tuple[int, int, int]:
    match = UPDATE_RE.search(clean_text(value))
    if not match:
        return (0, 0, 0)
    month = int(match.group(1))
    day = int(match.group(2))
    year = int(match.group(3))
    if year < 100:
        year += 2000
    return (year, month, day)


def path_term_candidates(path: Path) -> List[str]:
    candidates: List[str] = [clean_text(path.stem), clean_text(path.name)]
    for part in reversed(path.parts):
        text = clean_text(part)
        if text and text not in candidates:
            candidates.append(text)
    return candidates


def parse_grade_term(path: Path, sheet_name: object) -> str:
    for candidate in [clean_text(sheet_name), *path_term_candidates(path)]:
        term_code, term_label, _, _ = parse_term_code(candidate)
        if term_code:
            return term_label
    return ""


def canonical_snapshot_header(value: object) -> str:
    text = canonical_header(value)
    match = next(
        (
            standard
            for standard, aliases in SNAPSHOT_ALIAS_GROUPS.items()
            if text in aliases
        ),
        clean_text(value),
    )
    return match


def build_person_identity_key(frame: pd.DataFrame) -> pd.Series:
    student_id = frame.get("student_id", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip()
    email = frame.get("email", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip().str.lower()
    first_name = frame.get("first_name", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip().str.lower()
    last_name = frame.get("last_name", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip().str.lower()
    name_key = (last_name + "|" + first_name).where(last_name.ne("") | first_name.ne(""), "")
    return student_id.where(student_id.ne(""), email.where(email.ne(""), name_key))


def detect_membership_reference_header_row(frame: pd.DataFrame) -> Tuple[Optional[int], Dict[int, Tuple[str, str]], int]:
    best_row: Optional[int] = None
    best_terms: Dict[int, Tuple[str, str]] = {}
    best_chapter_col = 0
    search_rows = min(len(frame.index), 25)
    search_cols = min(len(frame.columns), 40)
    for row_idx in range(search_rows):
        term_columns: Dict[int, Tuple[str, str]] = {}
        for col_idx in range(search_cols):
            term_code, term_label, _, _ = parse_term_code(frame.iat[row_idx, col_idx])
            if term_code:
                term_columns[col_idx] = (term_code, term_label)
        if len(term_columns) < 2:
            continue
        first_term_col = min(term_columns)
        chapter_col = 0
        for candidate in range(first_term_col):
            candidate_values = frame.iloc[row_idx + 1 :, candidate].fillna("").astype(str).map(clean_text)
            has_chapter_text = candidate_values.map(
                lambda value: bool(value)
                and not parse_term_code(value)[0]
                and not re.search(r"(average|total|council)", value, re.IGNORECASE)
            ).any()
            if has_chapter_text:
                chapter_col = candidate
                break
        if len(term_columns) > len(best_terms):
            best_row = row_idx
            best_terms = term_columns
            best_chapter_col = chapter_col
    return best_row, best_terms, best_chapter_col


def parse_membership_count_value(value: object) -> Optional[int]:
    text = clean_text(value)
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    return None


def parse_reference_gpa_value(value: object) -> Optional[float]:
    text = clean_text(value)
    if not text:
        return None
    text = text.replace("%", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


def parse_reference_numeric_entry(value: object) -> Optional[Tuple[float, bool]]:
    text = clean_text(value)
    if not text or text in {"-", "--"}:
        return None
    had_percent = "%" in text
    cleaned = text.replace(",", "").replace("%", "").strip()
    try:
        return float(cleaned), had_percent
    except ValueError:
        return None


def is_integer_like(value: float) -> bool:
    return abs(value - round(value)) < 1e-9


def classify_reference_row(
    label: str,
    source_file: Path,
    source_sheet: str,
    numeric_entries: Sequence[dict],
) -> Tuple[str, str, str, str]:
    normalized_chapter = normalize_chapter_name(label)
    entity_type = "chapter" if normalized_chapter else "benchmark"
    entity_label_normalized = normalized_chapter if normalized_chapter else clean_text(label)
    context = " ".join(
        [
            clean_text(label),
            clean_text(source_sheet),
            clean_text(source_file.stem),
            clean_text(source_file.name),
        ]
    ).lower()
    values = [float(item["reference_value"]) for item in numeric_entries]
    has_percent = any(bool(item.get("had_percent")) for item in numeric_entries)
    all_small = bool(values) and all(0 <= value <= 4.5 for value in values)
    all_integer = bool(values) and all(is_integer_like(value) for value in values)

    if any(token in context for token in ["new member", "new members", "associate member", "nm count", "new mem"]):
        return "new_member_count", entity_type, entity_label_normalized, "keyword:new_member"
    if any(token in context for token in ["gpa", "grade point"]):
        return "average_gpa", entity_type, entity_label_normalized, "keyword:gpa"
    if any(token in context for token in ["retention", "retained", "return rate", "returned", "continuation", "continued", "persistence", "persist"]):
        return "retention_rate", entity_type, entity_label_normalized, "keyword:retention"
    if all_small:
        return "average_gpa", entity_type, entity_label_normalized, "value-range:gpa"
    if has_percent:
        return "retention_rate", entity_type, entity_label_normalized, "value-format:percent"
    if any(token in context for token in ["membership", "member count", "members", "chapter size", "roster", "headcount", "count"]):
        return "membership_count", entity_type, entity_label_normalized, "keyword:membership"
    if all_integer and values and max(values) > 4:
        return "membership_count", entity_type, entity_label_normalized, "value-shape:count"
    return "unknown", entity_type, entity_label_normalized, "unclassified"


def list_reference_files(roots: Sequence[Path]) -> List[Path]:
    seen: set[str] = set()
    files: List[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.suffix.lower() not in {".xlsx", ".xlsm", ".xls"}:
                continue
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            files.append(path)
    return files


def load_reference_inventory_table(roots: Sequence[Path]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    inventory_rows: List[dict] = []
    issue_rows: List[dict] = []
    inventory_columns = [
        "reference_type",
        "entity_type",
        "entity_label_raw",
        "entity_label_normalized",
        "term_code",
        "term_label",
        "reference_value",
        "classification_basis",
        "source_file",
        "source_sheet",
    ]
    issue_columns = ["exception_type", "source_file", "student_id", "term_code", "details"]

    for path in list_reference_files(roots):
        try:
            workbook = pd.read_excel(path, sheet_name=None, header=None)
        except Exception as exc:
            issue_rows.append(
                {
                    "exception_type": "reference_inventory_unreadable",
                    "source_file": str(path),
                    "student_id": "",
                    "term_code": "",
                    "details": clean_text(exc),
                }
            )
            continue

        for sheet_name, raw_sheet in workbook.items():
            frame = raw_sheet.fillna("")
            header_row, term_columns, label_col = detect_membership_reference_header_row(frame)
            if header_row is None or not term_columns:
                continue
            for row_idx in range(header_row + 1, len(frame.index)):
                label = clean_text(frame.iat[row_idx, label_col]) if label_col < len(frame.columns) else ""
                if not label:
                    continue
                numeric_entries: List[dict] = []
                for col_idx, (term_code, term_label) in term_columns.items():
                    parsed = parse_reference_numeric_entry(frame.iat[row_idx, col_idx])
                    if parsed is None:
                        continue
                    numeric_value, had_percent = parsed
                    numeric_entries.append(
                        {
                            "term_code": term_code,
                            "term_label": term_label,
                            "reference_value": numeric_value,
                            "had_percent": had_percent,
                        }
                    )
                if not numeric_entries:
                    continue
                reference_type, entity_type, normalized_label, basis = classify_reference_row(
                    label,
                    path,
                    clean_text(sheet_name),
                    numeric_entries,
                )
                for entry in numeric_entries:
                    inventory_rows.append(
                        {
                            "reference_type": reference_type,
                            "entity_type": entity_type,
                            "entity_label_raw": label,
                            "entity_label_normalized": normalized_label,
                            "term_code": entry["term_code"],
                            "term_label": entry["term_label"],
                            "reference_value": entry["reference_value"],
                            "classification_basis": basis,
                            "source_file": str(path),
                            "source_sheet": clean_text(sheet_name),
                        }
                    )
                if reference_type == "unknown":
                    issue_rows.append(
                        {
                            "exception_type": "reference_inventory_unclassified_row",
                            "source_file": str(path),
                            "student_id": "",
                            "term_code": "",
                            "details": f"{sheet_name}: {label}",
                        }
                    )

    inventory = pd.DataFrame(inventory_rows, columns=inventory_columns)
    if not inventory.empty:
        inventory = inventory.drop_duplicates(
            subset=["reference_type", "entity_type", "entity_label_raw", "term_code", "source_file", "source_sheet"],
            keep="first",
        ).reset_index(drop=True)
    issues = pd.DataFrame(issue_rows, columns=issue_columns)
    return inventory, issues


def build_reference_subset(
    inventory: pd.DataFrame,
    reference_type: str,
    entity_type: str,
    value_column: str,
    columns: Sequence[str],
    dedupe_subset: Sequence[str],
) -> pd.DataFrame:
    if inventory.empty:
        return pd.DataFrame(columns=list(columns))
    frame = inventory.loc[
        inventory["reference_type"].eq(reference_type)
        & inventory["entity_type"].eq(entity_type)
    ].copy()
    if frame.empty:
        return pd.DataFrame(columns=list(columns))
    if entity_type == "chapter":
        frame = frame.rename(
            columns={
                "entity_label_raw": "chapter_raw",
                "entity_label_normalized": "chapter",
                "reference_value": value_column,
            }
        )
    else:
        frame["benchmark_label"] = frame["entity_label_normalized"]
        frame = frame.rename(columns={"reference_value": value_column})
    subset = frame.loc[:, list(columns)]
    return subset.drop_duplicates(subset=list(dedupe_subset), keep="first").reset_index(drop=True)


def load_membership_reference_table(root: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    reference_rows: List[dict] = []
    issue_rows: List[dict] = []
    columns = [
        "chapter",
        "chapter_raw",
        "term_code",
        "term_label",
        "membership_count_reference",
        "source_file",
        "source_sheet",
    ]
    empty_reference = pd.DataFrame(columns=columns)
    empty_issues = pd.DataFrame(columns=["exception_type", "source_file", "student_id", "term_code", "details"])
    if not root.exists():
        return empty_reference, empty_issues

    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".xlsx", ".xlsm", ".xls"}:
            continue
        try:
            workbook = pd.read_excel(path, sheet_name=None, header=None)
        except Exception as exc:
            issue_rows.append(
                {
                    "exception_type": "membership_reference_unreadable",
                    "source_file": str(path),
                    "student_id": "",
                    "term_code": "",
                    "details": clean_text(exc),
                }
            )
            continue

        for sheet_name, raw_sheet in workbook.items():
            frame = raw_sheet.fillna("")
            header_row, term_columns, chapter_col = detect_membership_reference_header_row(frame)
            if header_row is None or not term_columns:
                continue
            for row_idx in range(header_row + 1, len(frame.index)):
                chapter_raw = clean_text(frame.iat[row_idx, chapter_col]) if chapter_col < len(frame.columns) else ""
                if not chapter_raw:
                    continue
                if re.search(r"(average|total|council)", chapter_raw, re.IGNORECASE):
                    continue
                chapter = normalize_chapter_name(chapter_raw)
                if not chapter:
                    issue_rows.append(
                        {
                            "exception_type": "membership_reference_unmapped_chapter",
                            "source_file": str(path),
                            "student_id": "",
                            "term_code": "",
                            "details": f"{sheet_name}: {chapter_raw}",
                        }
                    )
                    continue
                found_numeric_count = False
                for col_idx, (term_code, term_label) in term_columns.items():
                    count_value = parse_membership_count_value(frame.iat[row_idx, col_idx])
                    if count_value is None:
                        continue
                    found_numeric_count = True
                    reference_rows.append(
                        {
                            "chapter": chapter,
                            "chapter_raw": chapter_raw,
                            "term_code": term_code,
                            "term_label": term_label,
                            "membership_count_reference": count_value,
                            "source_file": str(path),
                            "source_sheet": clean_text(sheet_name),
                        }
                    )
                if not found_numeric_count:
                    issue_rows.append(
                        {
                            "exception_type": "membership_reference_row_without_counts",
                            "source_file": str(path),
                            "student_id": "",
                            "term_code": "",
                            "details": f"{sheet_name}: {chapter_raw}",
                        }
                    )

    reference = pd.DataFrame(reference_rows, columns=columns)
    if not reference.empty:
        reference = (
            reference.sort_values(["chapter", "term_code", "source_file", "source_sheet"])
            .drop_duplicates(subset=["chapter", "term_code"], keep="first")
            .reset_index(drop=True)
        )
    issues = pd.DataFrame(issue_rows, columns=["exception_type", "source_file", "student_id", "term_code", "details"])
    return reference, issues


def load_gpa_reference_table(root: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    reference_rows: List[dict] = []
    issue_rows: List[dict] = []
    columns = [
        "chapter",
        "chapter_raw",
        "term_code",
        "term_label",
        "chapter_average_gpa_reference",
        "source_file",
        "source_sheet",
    ]
    empty_reference = pd.DataFrame(columns=columns)
    empty_issues = pd.DataFrame(columns=["exception_type", "source_file", "student_id", "term_code", "details"])
    if not root.exists():
        return empty_reference, empty_issues

    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".xlsx", ".xlsm", ".xls"}:
            continue
        try:
            workbook = pd.read_excel(path, sheet_name=None, header=None)
        except Exception as exc:
            issue_rows.append(
                {
                    "exception_type": "gpa_reference_unreadable",
                    "source_file": str(path),
                    "student_id": "",
                    "term_code": "",
                    "details": clean_text(exc),
                }
            )
            continue

        for sheet_name, raw_sheet in workbook.items():
            frame = raw_sheet.fillna("")
            header_row, term_columns, chapter_col = detect_membership_reference_header_row(frame)
            if header_row is None or not term_columns:
                continue
            for row_idx in range(header_row + 1, len(frame.index)):
                chapter_raw = clean_text(frame.iat[row_idx, chapter_col]) if chapter_col < len(frame.columns) else ""
                if not chapter_raw:
                    continue
                if re.search(r"(average|total|council)", chapter_raw, re.IGNORECASE):
                    continue
                chapter = normalize_chapter_name(chapter_raw)
                if not chapter:
                    issue_rows.append(
                        {
                            "exception_type": "gpa_reference_unmapped_chapter",
                            "source_file": str(path),
                            "student_id": "",
                            "term_code": "",
                            "details": f"{sheet_name}: {chapter_raw}",
                        }
                    )
                    continue
                found_numeric_gpa = False
                for col_idx, (term_code, term_label) in term_columns.items():
                    gpa_value = parse_reference_gpa_value(frame.iat[row_idx, col_idx])
                    if gpa_value is None:
                        continue
                    found_numeric_gpa = True
                    reference_rows.append(
                        {
                            "chapter": chapter,
                            "chapter_raw": chapter_raw,
                            "term_code": term_code,
                            "term_label": term_label,
                            "chapter_average_gpa_reference": gpa_value,
                            "source_file": str(path),
                            "source_sheet": clean_text(sheet_name),
                        }
                    )
                if not found_numeric_gpa:
                    issue_rows.append(
                        {
                            "exception_type": "gpa_reference_row_without_values",
                            "source_file": str(path),
                            "student_id": "",
                            "term_code": "",
                            "details": f"{sheet_name}: {chapter_raw}",
                        }
                    )

    reference = pd.DataFrame(reference_rows, columns=columns)
    if not reference.empty:
        reference = (
            reference.sort_values(["chapter", "term_code", "source_file", "source_sheet"])
            .drop_duplicates(subset=["chapter", "term_code"], keep="first")
            .reset_index(drop=True)
        )
    issues = pd.DataFrame(issue_rows, columns=["exception_type", "source_file", "student_id", "term_code", "details"])
    return reference, issues


def load_gpa_benchmark_reference_table(root: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    reference_rows: List[dict] = []
    issue_rows: List[dict] = []
    columns = [
        "benchmark_label",
        "term_code",
        "term_label",
        "benchmark_average_gpa_reference",
        "source_file",
        "source_sheet",
    ]
    empty_reference = pd.DataFrame(columns=columns)
    empty_issues = pd.DataFrame(columns=["exception_type", "source_file", "student_id", "term_code", "details"])
    if not root.exists():
        return empty_reference, empty_issues

    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".xlsx", ".xlsm", ".xls"}:
            continue
        try:
            workbook = pd.read_excel(path, sheet_name=None, header=None)
        except Exception as exc:
            issue_rows.append(
                {
                    "exception_type": "gpa_benchmark_unreadable",
                    "source_file": str(path),
                    "student_id": "",
                    "term_code": "",
                    "details": clean_text(exc),
                }
            )
            continue

        for sheet_name, raw_sheet in workbook.items():
            frame = raw_sheet.fillna("")
            header_row, term_columns, label_col = detect_membership_reference_header_row(frame)
            if header_row is None or not term_columns:
                continue
            for row_idx in range(header_row + 1, len(frame.index)):
                benchmark_label = clean_text(frame.iat[row_idx, label_col]) if label_col < len(frame.columns) else ""
                if not benchmark_label:
                    continue
                found_numeric_gpa = False
                for col_idx, (term_code, term_label) in term_columns.items():
                    gpa_value = parse_reference_gpa_value(frame.iat[row_idx, col_idx])
                    if gpa_value is None:
                        continue
                    found_numeric_gpa = True
                    reference_rows.append(
                        {
                            "benchmark_label": benchmark_label,
                            "term_code": term_code,
                            "term_label": term_label,
                            "benchmark_average_gpa_reference": gpa_value,
                            "source_file": str(path),
                            "source_sheet": clean_text(sheet_name),
                        }
                    )
                if not found_numeric_gpa:
                    issue_rows.append(
                        {
                            "exception_type": "gpa_benchmark_row_without_values",
                            "source_file": str(path),
                            "student_id": "",
                            "term_code": "",
                            "details": f"{sheet_name}: {benchmark_label}",
                        }
                    )

    reference = pd.DataFrame(reference_rows, columns=columns)
    if not reference.empty:
        reference = (
            reference.sort_values(["benchmark_label", "term_code", "source_file", "source_sheet"])
            .drop_duplicates(subset=["benchmark_label", "term_code"], keep="first")
            .reset_index(drop=True)
        )
    issues = pd.DataFrame(issue_rows, columns=["exception_type", "source_file", "student_id", "term_code", "details"])
    return reference, issues


def build_membership_reference_validation(roster: pd.DataFrame, reference: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "chapter",
        "term_code",
        "term_label",
        "membership_count_reference",
        "membership_count_pipeline",
        "difference",
        "comparison_status",
        "source_file",
        "source_sheet",
    ]
    if reference.empty:
        return pd.DataFrame(columns=columns)

    roster_counts = roster.copy()
    roster_counts["_person_key"] = build_person_identity_key(roster_counts)
    roster_counts = roster_counts.loc[roster_counts["_person_key"].ne("")]
    pipeline_counts = (
        roster_counts.groupby(["chapter", "term_code"], dropna=False)["_person_key"]
        .nunique()
        .reset_index(name="membership_count_pipeline")
    )
    validation = reference.merge(pipeline_counts, on=["chapter", "term_code"], how="left")
    validation["term_label"] = validation["term_label"].fillna(validation["term_code"].map(term_label_from_code))
    validation["difference"] = validation["membership_count_pipeline"] - validation["membership_count_reference"]
    validation.loc[validation["membership_count_pipeline"].isna(), "difference"] = pd.NA
    validation["comparison_status"] = "Match"
    validation.loc[validation["membership_count_pipeline"].isna(), "comparison_status"] = "Reference Only"
    validation.loc[
        validation["membership_count_pipeline"].notna()
        & validation["difference"].fillna(0).ne(0),
        "comparison_status",
    ] = "Mismatch"
    return validation.loc[:, columns].sort_values(["chapter", "term_code"]).reset_index(drop=True)


def build_new_member_reference_validation(roster: pd.DataFrame, reference: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "chapter",
        "term_code",
        "term_label",
        "new_member_count_reference",
        "new_member_count_pipeline",
        "difference",
        "comparison_status",
        "source_file",
        "source_sheet",
    ]
    if reference.empty:
        return pd.DataFrame(columns=columns)

    roster_counts = roster.copy()
    roster_counts["_person_key"] = build_person_identity_key(roster_counts)
    roster_counts = roster_counts.loc[
        roster_counts["_person_key"].ne("")
        & roster_counts["new_member_flag"].fillna("").astype(str).eq("Yes")
    ]
    pipeline_counts = (
        roster_counts.groupby(["chapter", "term_code"], dropna=False)["_person_key"]
        .nunique()
        .reset_index(name="new_member_count_pipeline")
    )
    validation = reference.merge(pipeline_counts, on=["chapter", "term_code"], how="left")
    validation["term_label"] = validation["term_label"].fillna(validation["term_code"].map(term_label_from_code))
    validation["difference"] = validation["new_member_count_pipeline"] - validation["new_member_count_reference"]
    validation.loc[validation["new_member_count_pipeline"].isna(), "difference"] = pd.NA
    validation["comparison_status"] = "Match"
    validation.loc[validation["new_member_count_pipeline"].isna(), "comparison_status"] = "Reference Only"
    validation.loc[
        validation["new_member_count_pipeline"].notna()
        & validation["difference"].fillna(0).ne(0),
        "comparison_status",
    ] = "Mismatch"
    return validation.loc[:, columns].sort_values(["chapter", "term_code"]).reset_index(drop=True)


def build_gpa_reference_validation(master: pd.DataFrame, reference: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "chapter",
        "term_code",
        "term_label",
        "chapter_average_gpa_reference",
        "chapter_average_gpa_pipeline",
        "difference",
        "comparison_status",
        "source_file",
        "source_sheet",
    ]
    if reference.empty:
        return pd.DataFrame(columns=columns)

    pipeline_frame = master.copy()
    pipeline_frame["term_gpa_numeric"] = coerce_numeric(pipeline_frame["term_gpa"])
    pipeline_frame = pipeline_frame.loc[
        pipeline_frame["chapter"].fillna("").astype(str).str.strip().ne("")
        & pipeline_frame["academic_present"].fillna("").astype(str).eq("Yes")
        & pipeline_frame["term_gpa_numeric"].notna()
    ]
    pipeline_gpa = (
        pipeline_frame.groupby(["chapter", "term_code"], dropna=False)["term_gpa_numeric"]
        .mean()
        .reset_index(name="chapter_average_gpa_pipeline")
    )
    validation = reference.merge(pipeline_gpa, on=["chapter", "term_code"], how="left")
    validation["term_label"] = validation["term_label"].fillna(validation["term_code"].map(term_label_from_code))
    validation["difference"] = validation["chapter_average_gpa_pipeline"] - validation["chapter_average_gpa_reference"]
    validation.loc[validation["chapter_average_gpa_pipeline"].isna(), "difference"] = pd.NA
    validation["comparison_status"] = "Match"
    validation.loc[validation["chapter_average_gpa_pipeline"].isna(), "comparison_status"] = "Reference Only"
    validation.loc[
        validation["chapter_average_gpa_pipeline"].notna()
        & validation["difference"].abs().fillna(0).gt(0.01),
        "comparison_status",
    ] = "Mismatch"
    return validation.loc[:, columns].sort_values(["chapter", "term_code"]).reset_index(drop=True)


def compute_pipeline_gpa_benchmarks(master: pd.DataFrame, chapter_mapping: pd.DataFrame) -> pd.DataFrame:
    columns = ["benchmark_label", "term_code", "benchmark_average_gpa_pipeline"]
    if master.empty:
        return pd.DataFrame(columns=columns)

    base = master.copy()
    base["term_gpa_numeric"] = coerce_numeric(base["term_gpa"])
    base = base.loc[
        base["academic_present"].fillna("").astype(str).eq("Yes")
        & base["term_gpa_numeric"].notna()
        & base["chapter"].fillna("").astype(str).str.strip().ne("")
    ]
    if base.empty:
        return pd.DataFrame(columns=columns)

    mapping = chapter_mapping.copy()
    if not mapping.empty and "chapter" in mapping.columns:
        mapping["chapter"] = mapping["chapter"].fillna("").astype(str).str.strip()
        mapping["org_type"] = mapping.get("org_type", "").astype(str) if "org_type" in mapping.columns else ""
        base = base.merge(mapping[["chapter", "org_type"]].drop_duplicates(subset=["chapter"]), on="chapter", how="left")
    else:
        base["org_type"] = ""

    benchmark_frames: List[pd.DataFrame] = []

    all_greek = (
        base.groupby("term_code", dropna=False)["term_gpa_numeric"]
        .mean()
        .reset_index(name="benchmark_average_gpa_pipeline")
    )
    all_greek["benchmark_label"] = "All Greek Average"
    benchmark_frames.append(all_greek)

    org_type_series = base["org_type"].fillna("").astype(str).str.lower()
    sorority = base.loc[org_type_series.str.contains("soror")]
    if not sorority.empty:
        frame = sorority.groupby("term_code", dropna=False)["term_gpa_numeric"].mean().reset_index(name="benchmark_average_gpa_pipeline")
        frame["benchmark_label"] = "All Sorority Average"
        benchmark_frames.append(frame)

    fraternity = base.loc[org_type_series.str.contains("fratern")]
    if not fraternity.empty:
        frame = fraternity.groupby("term_code", dropna=False)["term_gpa_numeric"].mean().reset_index(name="benchmark_average_gpa_pipeline")
        frame["benchmark_label"] = "All Fraternity Average"
        benchmark_frames.append(frame)

    if not benchmark_frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(benchmark_frames, ignore_index=True).loc[:, columns]


def build_gpa_benchmark_validation(master: pd.DataFrame, reference: pd.DataFrame, chapter_mapping: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "benchmark_label",
        "term_code",
        "term_label",
        "benchmark_average_gpa_reference",
        "benchmark_average_gpa_pipeline",
        "difference",
        "comparison_status",
        "source_file",
        "source_sheet",
    ]
    if reference.empty:
        return pd.DataFrame(columns=columns)

    pipeline_benchmarks = compute_pipeline_gpa_benchmarks(master, chapter_mapping)
    validation = reference.merge(pipeline_benchmarks, on=["benchmark_label", "term_code"], how="left")
    validation["term_label"] = validation["term_label"].fillna(validation["term_code"].map(term_label_from_code))
    validation["difference"] = validation["benchmark_average_gpa_pipeline"] - validation["benchmark_average_gpa_reference"]
    validation.loc[validation["benchmark_average_gpa_pipeline"].isna(), "difference"] = pd.NA
    validation["comparison_status"] = "Match"
    validation.loc[validation["benchmark_average_gpa_pipeline"].isna(), "comparison_status"] = "Reference Only"
    validation.loc[
        validation["benchmark_average_gpa_pipeline"].notna()
        & validation["difference"].abs().fillna(0).gt(0.01),
        "comparison_status",
    ] = "Mismatch"
    return validation.loc[:, columns].sort_values(["benchmark_label", "term_code"]).reset_index(drop=True)


def canonical_graduation_header(value: object) -> str:
    text = canonical_header(value)
    return next(
        (
            standard
            for standard, aliases in GRADUATION_ALIAS_GROUPS.items()
            if text in aliases
        ),
        clean_text(value),
    )


def is_snapshot_filename(path: Path) -> bool:
    return clean_text(path.stem).lower().startswith("new member")


def list_source_files(folder: Path) -> List[Path]:
    if not folder.exists():
        return []
    return sorted(path for path in folder.rglob("*") if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS.union({".csv"}))


def roster_files(roots: Sequence[Path]) -> List[Path]:
    files: List[Path] = []
    seen: set[Path] = set()
    for root in roots:
        for path in list_source_files(root):
            resolved = path.resolve()
            if resolved not in seen:
                seen.add(resolved)
                files.append(path)
    return sorted(files)


def academic_files(root: Path) -> List[Path]:
    return [path for path in list_source_files(root) if not is_snapshot_filename(path)]


def snapshot_files(root: Path) -> List[Path]:
    return [path for path in list_source_files(root) if is_snapshot_filename(path)]


def graduation_files(root: Path) -> List[Path]:
    if not root.exists():
        return []
    return sorted(path for path in list_source_files(root) if clean_text(path.stem).lower().find("graduat") >= 0)


def normalize_email(value: object) -> str:
    return clean_text(value).lower()


def person_name_key(first_name: object, last_name: object) -> Tuple[str, str]:
    return clean_text(first_name).lower(), clean_text(last_name).lower()


def map_grade_headers(headers: Sequence[object]) -> Dict[str, int]:
    mapped: Dict[str, int] = {}
    canon_headers = [canonical_header(value) for value in headers]
    for idx, header in enumerate(canon_headers):
        for target, aliases in GRADE_COLUMN_ALIASES.items():
            if header in aliases and target not in mapped:
                mapped[target] = idx
    return mapped


def roster_status_bucket(raw_status: object, raw_position: object) -> str:
    status = normalize_status(clean_text(raw_status))
    combined = f"{status} {clean_text(raw_position)}".upper()
    if "GRAD" in combined or "ALUM" in combined:
        return "Graduated"
    if "SUSPEND" in combined:
        return "Suspended"
    if "TRANSFER" in combined:
        return "Transfer"
    if "REVOK" in combined:
        return "Revoked"
    if "RESIGN" in combined:
        return "Resigned"
    if "INACTIVE" in combined or "DROP" in combined or "REMOVE" in combined:
        return "Inactive"
    if "NEW MEMBER" in combined:
        return "New Member"
    if "ACTIVE" in combined or "MEMBER" in combined or "COUNCIL" in combined:
        return "Active"
    return status or "Unknown"


def outcome_bucket_from_signals(status_bucket: str, academic_status_raw: str, snapshot_status_raw: str) -> Tuple[str, str]:
    signals = " ".join([status_bucket, clean_text(academic_status_raw), clean_text(snapshot_status_raw)]).upper()
    if any(token in signals for token in ["GRADUAT", "ALUMNI", "DEGREE"]):
        return "Graduated", "Explicit graduation signal"
    if "SUSPEND" in signals:
        return "Suspended", "Explicit suspension signal"
    if "TRANSFER" in signals:
        return "Transfer", "Explicit transfer signal"
    if any(token in signals for token in ["INACTIVE", "DROP", "RESIGN", "REVOK", "REMOVE", "WITHDRAW", "TERMINAT", "DISMISS", "EXPEL"]):
        return "Dropped/Resigned/Revoked/Inactive", "Explicit non-graduate exit signal"
    if any(token in signals for token in ["ACTIVE", "CURRENT", "MEMBER", "NEW MEMBER", "COUNCIL", "ENROLLED"]):
        return "Active/Unknown", "Current or active signal only"
    return "No Further Observation", "No explicit outcome evidence"


def standing_bucket(value: object) -> str:
    text = clean_text(value).upper()
    if not text:
        return "Unknown"
    if any(token in text for token in ["GOOD", "CLEAR", "SATISFACTORY"]):
        return "Good Standing"
    if any(token in text for token in ["PROBATION", "WARNING"]):
        return "Probation/Warning"
    if "SUSPEND" in text:
        return "Suspended"
    if any(token in text for token in ["DISMISS", "SEPARAT", "EXPEL", "TERMINAT"]):
        return "Dismissed/Separated"
    return "Other/Unmapped"


def metric_row(metric_group: str, metric_label: str, cohort: str, eligible: int, numerator: Optional[int] = None, rate: Optional[float] = None, average: Optional[float] = None, dimension: str = "", value: str = "", notes: str = "") -> dict:
    return {
        "Metric Group": metric_group,
        "Metric Label": metric_label,
        "Cohort": cohort,
        "Dimension": dimension,
        "Value": value,
        "Eligible Students": eligible,
        "Student Count": numerator if numerator is not None else "",
        "Rate": rate if rate is not None else "",
        "Average Value": average if average is not None else "",
        "Notes": notes,
    }


def choose_best_snapshot_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    ranked = frame.copy()
    ranked["_filled_fields"] = ranked.notna().sum(axis=1)
    ranked["_status_present"] = ranked["Student Status"].fillna("").astype(str).str.strip().ne("").astype(int)
    ranked = ranked.sort_values(
        by=["Student ID", "_filled_fields", "_status_present", "Last Name", "First Name"],
        ascending=[True, False, False, True, True],
    )
    ranked = ranked.drop_duplicates(subset=["Student ID"], keep="first")
    return ranked.drop(columns=["_filled_fields", "_status_present"])


def load_snapshot_table(root: Path) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for path in snapshot_files(root):
        if path.suffix.lower() == ".csv":
            frame = pd.read_csv(path)
            frame.columns = [canonical_snapshot_header(column) for column in frame.columns]
        else:
            selected: Optional[pd.DataFrame] = None
            for _, sheet in pd.read_excel(path, sheet_name=None).items():
                candidate = sheet.copy()
                candidate.columns = [canonical_snapshot_header(column) for column in candidate.columns]
                if {"Student ID", "First Name", "Last Name"}.issubset(set(candidate.columns)):
                    selected = candidate
                    break
            if selected is None:
                continue
            frame = selected

        for column in SNAPSHOT_COLUMNS:
            if column not in frame.columns:
                frame[column] = ""
        frame = frame[SNAPSHOT_COLUMNS[:-1]].copy()
        frame["Student ID"] = frame["Student ID"].map(normalize_banner_id)
        frame["First Name"] = frame["First Name"].map(clean_text)
        frame["Last Name"] = frame["Last Name"].map(clean_text)
        frame["NetID"] = frame["NetID"].map(clean_text)
        frame["Student Status"] = frame["Student Status"].map(clean_text)
        frame["Student Status (FT/PT)"] = frame["Student Status (FT/PT)"].map(clean_text)
        frame["Snapshot Source File"] = path.name
        frame = frame.loc[frame["Student ID"].ne("")]
        frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    combined = choose_best_snapshot_rows(combined)
    return combined.reset_index(drop=True)


def load_graduation_table(root: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows: List[dict] = []
    exceptions: List[dict] = []

    for path in graduation_files(root):
        try:
            if path.suffix.lower() == ".csv":
                frame = pd.read_csv(path)
                frame.columns = [canonical_graduation_header(column) for column in frame.columns]
                frames = [frame]
            else:
                workbook_frames = []
                for _, sheet in pd.read_excel(path, sheet_name=None).items():
                    candidate = sheet.copy()
                    candidate.columns = [canonical_graduation_header(column) for column in candidate.columns]
                    workbook_frames.append(candidate)
                frames = workbook_frames
        except Exception as exc:
            exceptions.append(
                {
                    "exception_type": "graduation_open_error",
                    "source_file": path.name,
                    "student_id": "",
                    "term_code": "",
                    "details": str(exc),
                }
            )
            continue

        for frame in frames:
            if "Student ID" not in frame.columns and not {"First Name", "Last Name"}.issubset(set(frame.columns)):
                continue
            for column in GRADUATION_COLUMNS:
                if column not in frame.columns:
                    frame[column] = ""
            subset = frame[GRADUATION_COLUMNS[:-1]].copy()
            subset["Student ID"] = subset["Student ID"].map(normalize_banner_id)
            subset["First Name"] = subset["First Name"].map(clean_text)
            subset["Last Name"] = subset["Last Name"].map(clean_text)
            subset["Graduation Term"] = subset["Graduation Term"].map(clean_text)
            subset["Outcome"] = subset["Outcome"].map(clean_text)
            subset["Graduation Source File"] = path.name
            rows.extend(subset.to_dict("records"))

    if not rows:
        return pd.DataFrame(columns=GRADUATION_COLUMNS), pd.DataFrame(exceptions)

    graduation = pd.DataFrame(rows)
    graduation = graduation.loc[
        graduation["Student ID"].fillna("").astype(str).str.strip().ne("")
        | (
            graduation["First Name"].fillna("").astype(str).str.strip().ne("")
            | graduation["Last Name"].fillna("").astype(str).str.strip().ne("")
        )
    ].copy()
    return graduation.reset_index(drop=True), pd.DataFrame(exceptions)


def load_roster_term_table(roots: Sequence[Path]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows: List[dict] = []
    exceptions: List[dict] = []
    schema_columns = load_schema()["tables"]["roster_term"]

    for path in roster_files(roots):
        try:
            workbook = load_workbook(path, data_only=True, read_only=True)
        except Exception as exc:
            exceptions.append(
                {
                    "exception_type": "roster_open_error",
                    "source_file": path.name,
                    "student_id": "",
                    "term_code": "",
                    "details": str(exc),
                }
            )
            continue

        try:
            for ws in workbook.worksheets:
                header_row_idx, header_map = find_header_row(ws)
                if header_row_idx is None:
                    exceptions.append(
                        {
                            "exception_type": "roster_header_missing",
                            "source_file": path.name,
                            "student_id": "",
                            "term_code": "",
                            "details": f"Sheet {ws.title} skipped because no usable header row was found.",
                        }
                    )
                    continue

                status_row_idx, status_col_idx = find_status_column(ws)
                if "status" not in header_map and status_col_idx is not None:
                    header_map["status"] = status_col_idx
                data_start_row = max(header_row_idx, status_row_idx or header_row_idx) + 1
                default_chapter = normalize_chapter_name(ws.title or path.stem) or "Unknown"
                current_chapter_raw = ws.title
                current_chapter = default_chapter

                term_label = ""
                for candidate in path_term_candidates(path):
                    code, label, _, _ = parse_term_code(candidate)
                    if code:
                        term_label = label
                        break
                term_code, term_label, term_year, term_season = parse_term_code(term_label)
                if not term_code:
                    exceptions.append(
                        {
                            "exception_type": "roster_term_unparsed",
                            "source_file": path.name,
                            "student_id": "",
                            "term_code": "",
                            "details": f"Could not infer term for {path.name}::{ws.title}",
                        }
                    )

                for row in ws.iter_rows(min_row=data_start_row, values_only=True):
                    inline_chapter_raw = detect_inline_chapter_label(row, header_map)
                    if inline_chapter_raw:
                        current_chapter_raw = inline_chapter_raw
                        current_chapter = normalize_chapter_name(inline_chapter_raw) or default_chapter
                        continue

                    last_name = clean_text(get_cell(row, header_map.get("last_name")))
                    first_name = clean_text(get_cell(row, header_map.get("first_name")))
                    if not last_name and not first_name:
                        continue

                    banner_raw = clean_text(get_cell(row, header_map.get("banner_id")))
                    email = normalize_email(get_cell(row, header_map.get("email")))
                    chapter_raw = clean_text(get_cell(row, header_map.get("chapter")))
                    status_raw = clean_text(get_cell(row, header_map.get("status")))
                    position_raw = clean_text(get_cell(row, header_map.get("position")))
                    semester_joined_raw = clean_text(get_cell(row, header_map.get("semester_joined")))
                    chapter = normalize_chapter_name(chapter_raw or current_chapter_raw or ws.title or path.stem) or current_chapter or "Unknown"
                    status_bucket = roster_status_bucket(status_raw, position_raw)

                    rows.append(
                        {
                            "student_id": normalize_banner_id(banner_raw),
                            "student_id_raw": banner_raw,
                            "identity_resolution_basis": "source_banner_id" if banner_raw else "",
                            "identity_resolution_notes": "",
                            "first_name": first_name,
                            "last_name": last_name,
                            "email": email,
                            "source_file": path.name,
                            "source_sheet": ws.title,
                            "term_code": term_code,
                            "term_label": term_label,
                            "term_year": term_year,
                            "term_season": term_season,
                            "term_source_basis": "folder_or_filename",
                            "chapter": chapter,
                            "chapter_raw": chapter_raw or current_chapter_raw or ws.title,
                            "org_status_raw": status_raw,
                            "org_status_bucket": status_bucket,
                            "org_position_raw": position_raw,
                            "semester_joined_raw": semester_joined_raw,
                            "new_member_flag": "Yes" if status_bucket == "New Member" else "No",
                            "org_entry_term_code": "",
                            "org_entry_term_basis": "",
                        }
                    )
        finally:
            workbook.close()

    roster = pd.DataFrame(rows)
    if roster.empty:
        roster = pd.DataFrame(columns=schema_columns)
    return ensure_columns(roster, schema_columns), pd.DataFrame(exceptions)


def load_academic_term_table(root: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows: List[dict] = []
    exceptions: List[dict] = []
    schema_columns = load_schema()["tables"]["academic_term"]

    for path in academic_files(root):
        if path.suffix.lower() == ".csv":
            raw = pd.read_csv(path)
            raw.columns = [canonical_header(column) for column in raw.columns]
            inferred_term = ""
            for candidate in path_term_candidates(path):
                code, label, _, _ = parse_term_code(candidate)
                if code:
                    inferred_term = label
                    break
            term_code, term_label, term_year, term_season = parse_term_code(inferred_term or path.stem)
            if not term_code:
                exceptions.append(
                    {
                        "exception_type": "academic_term_unparsed",
                        "source_file": path.name,
                        "student_id": "",
                        "term_code": "",
                        "details": f"Could not infer term for {path.name}",
                    }
                )
            rename_map = {
                "student id": "Banner ID",
                "banner id": "Banner ID",
                "last name": "Last Name",
                "first name": "First Name",
                "email": "Email",
                "student status": "Student Status",
                "major": "Major",
                "semester hours": "Semester Hours",
                "credits attempted": "Semester Hours",
                "cumulative hours": "Cumulative Hours",
                "current academic standing": "Current Academic Standing",
                "academic standing": "Current Academic Standing",
                "texas state gpa": "Texas State GPA",
                "overall gpa": "Overall GPA",
                "transfer gpa": "Transfer GPA",
                "term gpa": "Term GPA",
                "gpa": "Term GPA",
                "term passed hours": "Term Passed Hours",
                "credits earned": "Term Passed Hours",
                "txstate cumulative gpa": "TxState Cumulative GPA",
                "overall cumulative gpa": "Overall Cumulative GPA",
                "graduation term": "Graduation Term",
            }
            frame = raw.rename(columns={column: rename_map.get(column, column) for column in raw.columns})
            for record in frame.to_dict("records"):
                rows.append(
                    {
                        "student_id": normalize_banner_id(record.get("Banner ID", "")),
                        "student_id_raw": clean_text(record.get("Banner ID", "")),
                        "identity_resolution_basis": "source_student_id" if clean_text(record.get("Banner ID", "")) else "",
                        "identity_resolution_notes": "",
                        "first_name": clean_text(record.get("First Name", "")),
                        "last_name": clean_text(record.get("Last Name", "")),
                        "email": normalize_email(record.get("Email", "")),
                        "source_file": path.name,
                        "source_sheet": "csv",
                        "term_code": term_code,
                        "term_label": term_label,
                        "term_year": term_year,
                        "term_season": term_season,
                        "term_source_basis": "filename",
                        "academic_status_raw": clean_text(record.get("Student Status", "")),
                        "major": clean_text(record.get("Major", "")),
                        "term_gpa": record.get("Term GPA", ""),
                        "institutional_cumulative_gpa": record.get("TxState Cumulative GPA", record.get("Texas State GPA", "")),
                        "overall_cumulative_gpa": record.get("Overall Cumulative GPA", record.get("Overall GPA", "")),
                        "transfer_gpa": record.get("Transfer GPA", ""),
                        "attempted_hours_term": record.get("Semester Hours", ""),
                        "earned_hours_term": record.get("Term Passed Hours", ""),
                        "institutional_cumulative_hours": record.get("Cumulative Hours", ""),
                        "total_cumulative_hours": record.get("Cumulative Hours", ""),
                        "academic_standing_raw": clean_text(record.get("Current Academic Standing", "")),
                        "academic_standing_bucket": standing_bucket(record.get("Current Academic Standing", "")),
                        "graduation_term_code": parse_term_code(record.get("Graduation Term", ""))[0],
                        "graduation_term_label": parse_term_code(record.get("Graduation Term", ""))[1],
                    }
                )
            continue

        workbook = load_workbook(path, data_only=True, read_only=True)
        try:
            for ws in workbook.worksheets:
                term_label = parse_grade_term(path, ws.title)
                if not term_label:
                    continue
                term_code, term_label, term_year, term_season = parse_term_code(term_label)
                sheet_rows = list(ws.iter_rows(values_only=True))
                if not sheet_rows:
                    continue
                header_row_idx = None
                header_map: Dict[str, int] = {}
                best_score = 0
                for idx, candidate_row in enumerate(sheet_rows[:25]):
                    candidate_map = map_grade_headers(candidate_row)
                    score = len(candidate_map)
                    if {"Last Name", "First Name"}.issubset(set(candidate_map)):
                        score += 2
                    if "Banner ID" in candidate_map or "Email" in candidate_map:
                        score += 1
                    if score > best_score:
                        best_score = score
                        header_row_idx = idx
                        header_map = candidate_map
                required = {"Last Name", "First Name"}
                if header_row_idx is None or not required.issubset(set(header_map)):
                    continue
                for row in sheet_rows[header_row_idx + 1:]:
                    first_name = get_cell(row, header_map.get("First Name"))
                    last_name = get_cell(row, header_map.get("Last Name"))
                    if not first_name and not last_name:
                        continue
                    banner_raw = get_cell(row, header_map.get("Banner ID"))
                    rows.append(
                        {
                            "student_id": normalize_banner_id(banner_raw),
                            "student_id_raw": clean_text(banner_raw),
                            "identity_resolution_basis": "source_student_id" if clean_text(banner_raw) else "",
                            "identity_resolution_notes": "",
                            "first_name": clean_text(first_name),
                            "last_name": clean_text(last_name),
                            "email": normalize_email(get_cell(row, header_map.get("Email"))),
                            "source_file": path.name,
                            "source_sheet": ws.title,
                            "term_code": term_code,
                            "term_label": term_label,
                            "term_year": term_year,
                            "term_season": term_season,
                            "term_source_basis": "filename_or_sheet",
                            "academic_status_raw": clean_text(get_cell(row, header_map.get("Student Status"))),
                            "major": clean_text(get_cell(row, header_map.get("Major"))),
                            "term_gpa": get_cell(row, header_map.get("Term GPA")),
                            "institutional_cumulative_gpa": get_cell(row, header_map.get("TxState Cumulative GPA")) or get_cell(row, header_map.get("Texas State GPA")),
                            "overall_cumulative_gpa": get_cell(row, header_map.get("Overall Cumulative GPA")) or get_cell(row, header_map.get("Overall GPA")),
                            "transfer_gpa": get_cell(row, header_map.get("Transfer GPA")),
                            "attempted_hours_term": get_cell(row, header_map.get("Semester Hours")),
                            "earned_hours_term": get_cell(row, header_map.get("Term Passed Hours")),
                            "institutional_cumulative_hours": get_cell(row, header_map.get("Cumulative Hours")),
                            "total_cumulative_hours": get_cell(row, header_map.get("Cumulative Hours")),
                            "academic_standing_raw": clean_text(get_cell(row, header_map.get("Current Academic Standing"))),
                            "academic_standing_bucket": standing_bucket(get_cell(row, header_map.get("Current Academic Standing"))),
                            "graduation_term_code": "",
                            "graduation_term_label": "",
                        }
                    )
        finally:
            workbook.close()

    academic = pd.DataFrame(rows)
    if academic.empty:
        academic = pd.DataFrame(columns=schema_columns)
    else:
        for column in [
            "term_gpa",
            "institutional_cumulative_gpa",
            "overall_cumulative_gpa",
            "transfer_gpa",
            "attempted_hours_term",
            "earned_hours_term",
            "institutional_cumulative_hours",
            "total_cumulative_hours",
        ]:
            academic[column] = coerce_numeric(academic[column])
    return ensure_columns(academic, schema_columns), pd.DataFrame(exceptions)


def build_identity_maps(
    roster: pd.DataFrame,
    academic: pd.DataFrame,
    snapshot: pd.DataFrame,
    graduation: pd.DataFrame,
) -> Tuple[Dict[str, str], Dict[Tuple[str, str], str], pd.DataFrame]:
    email_candidates: Dict[str, set[str]] = defaultdict(set)
    name_candidates: Dict[Tuple[str, str], set[str]] = defaultdict(set)
    exceptions: List[dict] = []

    source_frames = [
        roster[["student_id", "email", "first_name", "last_name"]].copy(),
        academic[["student_id", "email", "first_name", "last_name"]].copy(),
    ]
    if not snapshot.empty:
        snap = pd.DataFrame(
            {
                "student_id": snapshot["Student ID"].map(normalize_banner_id),
                "email": pd.Series("", index=snapshot.index, dtype="object"),
                "first_name": snapshot["First Name"].map(clean_text),
                "last_name": snapshot["Last Name"].map(clean_text),
            }
        )
        source_frames.append(snap)
    if not graduation.empty:
        grad = pd.DataFrame(
            {
                "student_id": graduation["Student ID"].map(normalize_banner_id),
                "email": pd.Series("", index=graduation.index, dtype="object"),
                "first_name": graduation["First Name"].map(clean_text),
                "last_name": graduation["Last Name"].map(clean_text),
            }
        )
        source_frames.append(grad)

    combined = pd.concat(source_frames, ignore_index=True)
    combined = combined.loc[combined["student_id"].fillna("").astype(str).str.strip().ne("")]
    for row in combined.itertuples(index=False):
        if clean_text(row.email):
            email_candidates[clean_text(row.email).lower()].add(clean_text(row.student_id))
        if clean_text(row.first_name) or clean_text(row.last_name):
            name_candidates[person_name_key(row.first_name, row.last_name)].add(clean_text(row.student_id))

    email_map: Dict[str, str] = {}
    name_map: Dict[Tuple[str, str], str] = {}
    for email, ids in email_candidates.items():
        if len(ids) == 1:
            email_map[email] = next(iter(ids))
        else:
            exceptions.append({"exception_type": "ambiguous_email_match", "source_file": "", "student_id": "", "term_code": "", "details": f"Email {email} matched multiple student IDs: {', '.join(sorted(ids))}"})
    for key, ids in name_candidates.items():
        if len(ids) == 1:
            name_map[key] = next(iter(ids))
        else:
            exceptions.append({"exception_type": "ambiguous_name_match", "source_file": "", "student_id": "", "term_code": "", "details": f"Name {key[0]} {key[1]} matched multiple student IDs: {', '.join(sorted(ids))}"})

    return email_map, name_map, pd.DataFrame(exceptions)


def resolve_missing_ids(frame: pd.DataFrame, email_map: Dict[str, str], name_map: Dict[Tuple[str, str], str], source_label: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    result = frame.copy()
    exceptions: List[dict] = []

    for idx, row in result.iterrows():
        current_id = clean_text(row.get("student_id", ""))
        if current_id:
            continue
        email = normalize_email(row.get("email", ""))
        first_name = clean_text(row.get("first_name", ""))
        last_name = clean_text(row.get("last_name", ""))
        matched_id = ""
        basis = ""
        notes = ""
        if email and email in email_map:
            matched_id = email_map[email]
            basis = "unique_email_match"
            notes = "Resolved from unique email match."
        elif (first_name or last_name) and person_name_key(first_name, last_name) in name_map:
            matched_id = name_map[person_name_key(first_name, last_name)]
            basis = "unique_name_match"
            notes = "Resolved from unique exact name match."
        else:
            exceptions.append(
                {
                    "exception_type": f"{source_label}_missing_student_id",
                    "source_file": clean_text(row.get("source_file", "")),
                    "student_id": "",
                    "term_code": clean_text(row.get("term_code", "")),
                    "details": f"{first_name} {last_name}".strip() or email or "Unidentified row",
                }
            )
            continue

        result.at[idx, "student_id"] = matched_id
        result.at[idx, "identity_resolution_basis"] = basis
        result.at[idx, "identity_resolution_notes"] = notes

    return result, pd.DataFrame(exceptions)


def dedupe_table(frame: pd.DataFrame, unique_keys: Sequence[str], source_label: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty:
        return frame, pd.DataFrame(columns=["exception_type", "source_file", "student_id", "term_code", "details"])
    ranked = frame.copy()
    if "student_id" in unique_keys:
        ranked["_identity_key"] = ranked.apply(
            lambda row: (
                f"id:{clean_text(row.get('student_id', '')).lower()}"
                if clean_text(row.get("student_id", ""))
                else f"email:{clean_text(row.get('email', '')).lower()}"
                if clean_text(row.get("email", ""))
                else f"name:{clean_text(row.get('last_name', '')).lower()}|{clean_text(row.get('first_name', '')).lower()}"
            ),
            axis=1,
        )
        effective_keys = ["_identity_key" if key == "student_id" else key for key in unique_keys]
    else:
        effective_keys = list(unique_keys)
    ranked["_completeness"] = ranked.notna().sum(axis=1)
    ranked["_update_key"] = ranked["source_file"].map(update_key_from_name) if "source_file" in ranked.columns else [(0, 0, 0)] * len(ranked)
    ranked = ranked.sort_values(by=effective_keys + ["_completeness", "_update_key"], ascending=[True] * len(effective_keys) + [False, False])
    duplicate_mask = ranked.duplicated(subset=effective_keys, keep="first")
    exceptions = ranked.loc[duplicate_mask].copy()
    deduped = ranked.drop_duplicates(subset=effective_keys, keep="first").drop(columns=["_completeness", "_update_key"] + (["_identity_key"] if "_identity_key" in ranked.columns else []))
    if exceptions.empty:
        return deduped.reset_index(drop=True), pd.DataFrame(columns=["exception_type", "source_file", "student_id", "term_code", "details"])
    exception_rows = []
    for row in exceptions.itertuples(index=False):
        exception_rows.append(
            {
                "exception_type": f"{source_label}_duplicate_removed",
                "source_file": clean_text(getattr(row, "source_file", "")),
                "student_id": clean_text(getattr(row, "student_id", "")),
                "term_code": clean_text(getattr(row, "term_code", "")),
                "details": f"Duplicate row removed for keys {', '.join(str(getattr(row, key, '')) for key in unique_keys)}",
            }
        )
    return deduped.reset_index(drop=True), pd.DataFrame(exception_rows)


def resolve_roster_conflicts(roster: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if roster.empty:
        return roster, pd.DataFrame(columns=["exception_type", "source_file", "student_id", "term_code", "details"])
    exceptions: List[dict] = []
    resolved_rows: List[pd.Series] = []
    roster = roster.copy()
    roster["_identity_key"] = roster.apply(
        lambda row: (
            f"id:{clean_text(row.get('student_id', '')).lower()}"
            if clean_text(row.get("student_id", ""))
            else f"email:{clean_text(row.get('email', '')).lower()}"
            if clean_text(row.get("email", ""))
            else f"name:{clean_text(row.get('last_name', '')).lower()}|{clean_text(row.get('first_name', '')).lower()}"
        ),
        axis=1,
    )
    for (_, _), group in roster.groupby(["_identity_key", "term_code"], dropna=False):
        chapter_values = sorted({clean_text(value) for value in group["chapter"] if clean_text(value)})
        status_values = sorted({clean_text(value) for value in group["org_status_bucket"] if clean_text(value)})
        if len(chapter_values) > 1:
            exceptions.append(
                {
                    "exception_type": "chapter_conflict_same_term",
                    "source_file": " | ".join(sorted({clean_text(value) for value in group["source_file"] if clean_text(value)})),
                    "student_id": clean_text(group["student_id"].iloc[0]),
                    "term_code": clean_text(group["term_code"].iloc[0]),
                    "details": "Multiple chapters in same term: " + ", ".join(chapter_values),
                }
            )
        if len(status_values) > 1:
            exceptions.append(
                {
                    "exception_type": "roster_status_conflict_same_term",
                    "source_file": " | ".join(sorted({clean_text(value) for value in group["source_file"] if clean_text(value)})),
                    "student_id": clean_text(group["student_id"].iloc[0]),
                    "term_code": clean_text(group["term_code"].iloc[0]),
                    "details": "Multiple roster statuses in same term: " + ", ".join(status_values),
                }
            )
        ranked = group.copy()
        ranked["_new_member"] = ranked["new_member_flag"].eq("Yes").astype(int)
        ranked["_known_id"] = ranked["student_id"].fillna("").astype(str).str.strip().ne("").astype(int)
        ranked["_status_priority"] = ranked["org_status_bucket"].map(
            {
                "Graduated": 90,
                "Suspended": 80,
                "Transfer": 70,
                "Revoked": 65,
                "Resigned": 60,
                "Inactive": 55,
                "New Member": 54,
                "Active": 50,
            }
        ).fillna(0)
        ranked = ranked.sort_values(by=["_status_priority", "_new_member", "_known_id"], ascending=[False, False, False])
        resolved_rows.append(ranked.iloc[0].drop(labels=["_new_member", "_known_id", "_status_priority", "_identity_key"]))
    resolved = pd.DataFrame(resolved_rows).reset_index(drop=True)
    return resolved, pd.DataFrame(exceptions)


def attach_org_entry_terms(roster: pd.DataFrame) -> pd.DataFrame:
    if roster.empty:
        return roster
    result = roster.copy()
    result["_term_sort"] = result["term_code"].map(sort_term_code)
    for student_id, group in result.groupby("student_id", dropna=False):
        if not clean_text(student_id):
            continue
        explicit = group.loc[group["new_member_flag"].eq("Yes")].sort_values("_term_sort")
        if not explicit.empty:
            entry_code = clean_text(explicit.iloc[0]["term_code"])
            basis = "Explicit New Member"
        else:
            ordered = group.sort_values("_term_sort")
            entry_code = clean_text(ordered.iloc[0]["term_code"])
            basis = "First Observed Roster"
        result.loc[group.index, "org_entry_term_code"] = entry_code
        result.loc[group.index, "org_entry_term_basis"] = basis
    return result.drop(columns=["_term_sort"])


def build_master_longitudinal(roster: pd.DataFrame, academic: pd.DataFrame) -> pd.DataFrame:
    keys = sorted(
        set(
            list(roster[["student_id", "term_code"]].itertuples(index=False, name=None))
            + list(academic[["student_id", "term_code"]].itertuples(index=False, name=None))
        )
    )
    rows: List[dict] = []
    roster_lookup = {(clean_text(row.student_id), clean_text(row.term_code)): row for row in roster.itertuples(index=False)}
    academic_lookup = {(clean_text(row.student_id), clean_text(row.term_code)): row for row in academic.itertuples(index=False)}

    for student_id, term_code in keys:
        if not student_id or not term_code:
            continue
        roster_row = roster_lookup.get((student_id, term_code))
        academic_row = academic_lookup.get((student_id, term_code))
        source_first = roster_row or academic_row
        join_term_code = clean_text(getattr(roster_row, "org_entry_term_code", "")) if roster_row else ""
        join_term = term_label_from_code(join_term_code)
        rows.append(
            {
                "student_id": student_id,
                "first_name": clean_text(getattr(source_first, "first_name", "")),
                "last_name": clean_text(getattr(source_first, "last_name", "")),
                "email": normalize_email(getattr(source_first, "email", "")),
                "term_code": term_code,
                "term_label": term_label_from_code(term_code),
                "observed_year": parse_term_code(term_code)[2],
                "observed_term_sort": sort_term_code(term_code),
                "join_term_code": join_term_code,
                "join_term": join_term,
                "join_year": parse_term_code(join_term_code)[2] if join_term_code else pd.NA,
                "relative_term_index": pd.NA,
                "roster_present": "Yes" if roster_row is not None else "No",
                "academic_present": "Yes" if academic_row is not None else "No",
                "chapter": clean_text(getattr(roster_row, "chapter", "")),
                "chapter_raw": clean_text(getattr(roster_row, "chapter_raw", "")),
                "org_status_raw": clean_text(getattr(roster_row, "org_status_raw", "")),
                "org_status_bucket": clean_text(getattr(roster_row, "org_status_bucket", "")),
                "org_position_raw": clean_text(getattr(roster_row, "org_position_raw", "")),
                "new_member_flag": clean_text(getattr(roster_row, "new_member_flag", "")),
                "major": clean_text(getattr(academic_row, "major", "")),
                "term_gpa": getattr(academic_row, "term_gpa", pd.NA),
                "institutional_cumulative_gpa": getattr(academic_row, "institutional_cumulative_gpa", pd.NA),
                "overall_cumulative_gpa": getattr(academic_row, "overall_cumulative_gpa", pd.NA),
                "cumulative_gpa": getattr(academic_row, "overall_cumulative_gpa", pd.NA),
                "attempted_hours_term": getattr(academic_row, "attempted_hours_term", pd.NA),
                "earned_hours_term": getattr(academic_row, "earned_hours_term", pd.NA),
                "institutional_cumulative_hours": getattr(academic_row, "institutional_cumulative_hours", pd.NA),
                "total_cumulative_hours": getattr(academic_row, "total_cumulative_hours", pd.NA),
                "academic_status_raw": clean_text(getattr(academic_row, "academic_status_raw", "")),
                "academic_standing_raw": clean_text(getattr(academic_row, "academic_standing_raw", "")),
                "academic_standing_bucket": clean_text(getattr(academic_row, "academic_standing_bucket", "")),
                "final_outcome_bucket": "",
                "exit_reason_code": "",
                "graduation_term_code": clean_text(getattr(academic_row, "graduation_term_code", "")),
                "resolved_outcome_flag": "",
                "outcome_evidence_source": "",
                "school_entry_term_code": "",
                "school_entry_term_basis": "",
                "org_entry_term_basis": clean_text(getattr(roster_row, "org_entry_term_basis", "")),
            }
        )

    master = pd.DataFrame(rows)
    if master.empty:
        return pd.DataFrame(columns=load_schema()["tables"]["master_longitudinal"])

    master["cumulative_gpa"] = coerce_numeric(master["cumulative_gpa"]).where(
        coerce_numeric(master["cumulative_gpa"]).notna(),
        coerce_numeric(master["institutional_cumulative_gpa"]),
    )

    for student_id, group in master.groupby("student_id", dropna=False):
        ordered = group.sort_values("observed_term_sort")
        join_sort = sort_term_code(clean_text(ordered["join_term_code"].iloc[0])) if clean_text(ordered["join_term_code"].iloc[0]) else None
        school_entry_code = ""
        school_entry_basis = ""
        for index in ordered.index:
            term_sort = int(ordered.loc[index, "observed_term_sort"])
            relative = pd.NA
            if join_sort is not None and join_sort < 999999:
                relative = len([value for value in ordered["observed_term_sort"] if value <= term_sort and value >= join_sort]) - 1
            master.at[index, "relative_term_index"] = relative
            master.at[index, "school_entry_term_code"] = school_entry_code
            master.at[index, "school_entry_term_basis"] = school_entry_basis

    return ensure_columns(master, load_schema()["tables"]["master_longitudinal"])


def attach_snapshot_fields(summary: pd.DataFrame, snapshot: pd.DataFrame, longitudinal: pd.DataFrame) -> pd.DataFrame:
    result = summary.copy()
    if snapshot.empty:
        result["snapshot_matched"] = "No"
        result["current_total_hours"] = pd.NA
        result["estimated_pre_org_hours_txst"] = pd.NA
        result["estimated_pre_org_stage_txst"] = "Unknown"
        return result

    snap = snapshot.copy()
    snap["Student ID"] = snap["Student ID"].map(normalize_banner_id)
    snap = snap.drop_duplicates(subset=["Student ID"], keep="first")
    snap = snap.rename(
        columns={
            "Student ID": "student_id",
            "Total Credit Hours": "snapshot_total_hours",
            "TXST Credit Hours": "snapshot_txst_hours",
            "Overall GPA": "snapshot_overall_gpa",
            "Institutional GPA": "snapshot_institutional_gpa",
            "Student Status": "snapshot_student_status",
            "Student Status (FT/PT)": "snapshot_student_status_ftpt",
        }
    )
    result = result.merge(
        snap[
            [
                "student_id",
                "snapshot_total_hours",
                "snapshot_txst_hours",
                "snapshot_overall_gpa",
                "snapshot_institutional_gpa",
                "snapshot_student_status",
                "snapshot_student_status_ftpt",
            ]
        ],
        on="student_id",
        how="left",
    )
    observed_hours = (
        longitudinal.groupby("student_id", dropna=False)["earned_hours_term"]
        .sum(min_count=1)
        .rename("observed_earned_hours_since_org")
        .reset_index()
    )
    result = result.merge(observed_hours, on="student_id", how="left")
    result["snapshot_matched"] = result["snapshot_total_hours"].notna().map(lambda flag: "Yes" if flag else "No")
    result["current_total_hours"] = result["snapshot_total_hours"].where(result["snapshot_total_hours"].notna(), result["total_cumulative_hours"])
    result["estimated_pre_org_hours_txst"] = (coerce_numeric(result["snapshot_txst_hours"]) - coerce_numeric(result["observed_earned_hours_since_org"])).clip(lower=0)
    result["estimated_pre_org_stage_txst"] = result["estimated_pre_org_hours_txst"].map(bucket_30_hours)
    result["average_cumulative_gpa"] = result["average_cumulative_gpa"].where(
        result["average_cumulative_gpa"].notna(),
        coerce_numeric(result["snapshot_overall_gpa"]).where(
            coerce_numeric(result["snapshot_overall_gpa"]).notna(),
            coerce_numeric(result["snapshot_institutional_gpa"]),
        ),
    )
    result["latest_snapshot_student_status"] = result["snapshot_student_status"].fillna("").astype(str)
    return result


def build_graduation_maps(graduation: pd.DataFrame) -> Tuple[Dict[str, Tuple[str, str]], Dict[Tuple[str, str], Tuple[str, str]]]:
    id_map: Dict[str, Tuple[str, str]] = {}
    name_map: Dict[Tuple[str, str], Tuple[str, str]] = {}
    if graduation.empty:
        return id_map, name_map

    ranked = graduation.copy()
    ranked["_term_sort"] = ranked["Graduation Term"].map(lambda value: sort_term_code(parse_term_code(value)[0]))
    ranked = ranked.sort_values(by=["_term_sort", "Graduation Source File"], ascending=[True, True])

    for row in ranked.itertuples(index=False):
        student_id = normalize_banner_id(getattr(row, "Student ID", ""))
        first_name = clean_text(getattr(row, "First Name", ""))
        last_name = clean_text(getattr(row, "Last Name", ""))
        grad_term_code = parse_term_code(getattr(row, "Graduation Term", ""))[0]
        source = clean_text(getattr(row, "Graduation Source File", "")) or "Graduation List"
        if student_id and student_id not in id_map:
            id_map[student_id] = (grad_term_code, source)
        name_key = person_name_key(first_name, last_name)
        if (first_name or last_name) and name_key not in name_map:
            name_map[name_key] = (grad_term_code, source)

    return id_map, name_map


def explicit_exit_reason(latest_outcome_bucket: str, latest_status_bucket: str) -> str:
    outcome = clean_text(latest_outcome_bucket)
    status = clean_text(latest_status_bucket)
    if outcome == "Dropped/Resigned/Revoked/Inactive":
        for candidate in ["Revoked", "Resigned", "Inactive"]:
            if candidate == status:
                return candidate
        return "Dropped/Resigned/Revoked/Inactive"
    if outcome in {"Graduated", "Suspended", "Transfer"}:
        return outcome
    return ""


def build_student_summary(
    master: pd.DataFrame,
    snapshot: pd.DataFrame,
    graduation: pd.DataFrame,
    settings: dict[str, object],
    chapter_mapping: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if master.empty:
        empty = pd.DataFrame(columns=load_schema()["tables"]["student_summary"])
        return empty, pd.DataFrame(columns=QA_COLUMNS), pd.DataFrame(columns=["exception_type", "source_file", "student_id", "term_code", "details"])

    summary_rows: List[dict] = []
    qa_rows: List[dict] = []
    outcome_exceptions: List[dict] = []
    max_term_sort = int(master["observed_term_sort"].dropna().max()) if master["observed_term_sort"].dropna().shape[0] else 0
    graduation_by_id, graduation_by_name = build_graduation_maps(graduation)

    for student_id, group in master.groupby("student_id", dropna=False):
        ordered = group.sort_values("observed_term_sort")
        roster_rows = ordered.loc[ordered["roster_present"].eq("Yes")]
        academic_rows = ordered.loc[ordered["academic_present"].eq("Yes")]
        first_row = ordered.iloc[0]
        join_term_code = clean_text(first_row["join_term_code"])
        join_term = term_label_from_code(join_term_code)
        join_year = parse_term_code(join_term_code)[2] if join_term_code else pd.NA

        explicit_grad_term = ""
        evidence_source = ""
        if student_id in graduation_by_id:
            explicit_grad_term, evidence_source = graduation_by_id[student_id]
        else:
            name_key = person_name_key(first_row["first_name"], first_row["last_name"])
            if name_key in graduation_by_name:
                explicit_grad_term, evidence_source = graduation_by_name[name_key]
        if not explicit_grad_term and (academic_rows["graduation_term_code"].fillna("").astype(str).str.strip().ne("")).any():
            explicit_grad_term = clean_text(
                academic_rows.loc[
                    academic_rows["graduation_term_code"].fillna("").astype(str).str.strip().ne(""),
                    "graduation_term_code",
                ].iloc[0]
            )
            evidence_source = evidence_source or "Academic graduation term"
        if not explicit_grad_term and (academic_rows["academic_status_raw"].fillna("").str.contains("graduat", case=False, na=False)).any():
            explicit_grad_term = clean_text(academic_rows.loc[academic_rows["academic_status_raw"].fillna("").str.contains("graduat", case=False, na=False), "term_code"].iloc[0])
            evidence_source = evidence_source or "Academic status"
        elif not explicit_grad_term and (roster_rows["org_status_bucket"].fillna("").eq("Graduated")).any():
            explicit_grad_term = clean_text(roster_rows.loc[roster_rows["org_status_bucket"].fillna("").eq("Graduated"), "term_code"].iloc[-1])
            evidence_source = evidence_source or "Roster status"

        latest_status_bucket = clean_text(roster_rows["org_status_bucket"].iloc[-1]) if not roster_rows.empty else "Unknown"
        latest_outcome_bucket, derived_evidence_source = outcome_bucket_from_signals(
            " ".join(roster_rows["org_status_bucket"].fillna("").astype(str).tolist()),
            " ".join(academic_rows["academic_status_raw"].fillna("").astype(str).tolist()),
            " ".join(snapshot.loc[snapshot["Student ID"].map(normalize_banner_id).eq(student_id), "Student Status"].fillna("").astype(str).tolist()) if not snapshot.empty else "",
        )
        evidence_source = evidence_source or derived_evidence_source
        if explicit_grad_term:
            latest_outcome_bucket = "Graduated"
            evidence_source = evidence_source or "Graduation List"
        if latest_outcome_bucket == "No Further Observation":
            outcome_exceptions.append(
                {
                    "exception_type": "unresolved_outcome",
                    "source_file": "",
                    "student_id": student_id,
                    "term_code": clean_text(ordered["term_code"].iloc[-1]),
                    "details": "No explicit outcome evidence; student remains unresolved.",
                }
            )

        entry_row = None
        if join_term_code and not academic_rows.empty:
            entry_candidates = academic_rows.loc[academic_rows["term_code"].eq(join_term_code)]
            if not entry_candidates.empty:
                entry_row = entry_candidates.iloc[0]

        first_ac_term = clean_text(academic_rows["term_code"].iloc[0]) if not academic_rows.empty else ""
        last_ac_term = clean_text(academic_rows["term_code"].iloc[-1]) if not academic_rows.empty else ""
        first_org_term = clean_text(roster_rows["term_code"].iloc[0]) if not roster_rows.empty else ""
        last_org_term = clean_text(roster_rows["term_code"].iloc[-1]) if not roster_rows.empty else ""
        gpa_values = coerce_numeric(academic_rows["term_gpa"]).dropna()
        first_term_gpa = gpa_values.iloc[0] if gpa_values.shape[0] else pd.NA
        second_term_gpa = gpa_values.iloc[1] if gpa_values.shape[0] > 1 else pd.NA
        first_year_window = academic_rows.loc[academic_rows["relative_term_index"].fillna(-1).astype(float).between(0, 2, inclusive="both")]
        first_year_avg_gpa = coerce_numeric(first_year_window["term_gpa"]).mean() if not first_year_window.empty else pd.NA
        latest_overall_cum = coerce_numeric(academic_rows["overall_cumulative_gpa"]).dropna().iloc[-1] if coerce_numeric(academic_rows["overall_cumulative_gpa"]).dropna().shape[0] else pd.NA
        latest_txstate_cum = coerce_numeric(academic_rows["institutional_cumulative_gpa"]).dropna().iloc[-1] if coerce_numeric(academic_rows["institutional_cumulative_gpa"]).dropna().shape[0] else pd.NA
        latest_cumulative_hours = coerce_numeric(academic_rows["total_cumulative_hours"]).dropna().iloc[-1] if coerce_numeric(academic_rows["total_cumulative_hours"]).dropna().shape[0] else pd.NA
        entry_hours = coerce_numeric(pd.Series([entry_row["institutional_cumulative_hours"] if entry_row is not None else pd.NA])).iloc[0]
        first_passed = coerce_numeric(first_year_window["earned_hours_term"]).dropna().iloc[0] if coerce_numeric(first_year_window["earned_hours_term"]).dropna().shape[0] else pd.NA
        first_year_passed = coerce_numeric(first_year_window["earned_hours_term"]).sum(min_count=1) if not first_year_window.empty else pd.NA

        join_sort = sort_term_code(join_term_code) if join_term_code else None
        next_term_sort = None
        next_fall_sort = None
        one_year_sort = None
        if join_sort is not None and join_sort < 999999:
            join_year_value = int(join_term_code[:4])
            join_season_code = join_term_code[-2:]
            if join_season_code == "FA":
                next_term_sort = (join_year_value + 1) * 10 + SEASON_ORDER["SP"]
            elif join_season_code == "SP":
                next_term_sort = join_year_value * 10 + SEASON_ORDER["SU"]
            elif join_season_code == "SU":
                next_term_sort = join_year_value * 10 + SEASON_ORDER["FA"]
            elif join_season_code == "WI":
                next_term_sort = join_year_value * 10 + SEASON_ORDER["SP"]
            next_fall_sort = (join_year_value + (1 if join_season_code == "FA" else 0)) * 10 + SEASON_ORDER["FA"]
            one_year_sort = (join_year_value + 1) * 10 + SEASON_ORDER.get(join_season_code, 9)

        def has_term(frame: pd.DataFrame, target_sort: Optional[int]) -> str:
            if target_sort is None or target_sort > max_term_sort:
                return ""
            return "Yes" if frame["term_code"].map(sort_term_code).eq(target_sort).any() else "No"

        def measurable_for_years(years: int) -> str:
            if not join_term_code:
                return ""
            target_sort = sort_term_code(f"{int(join_term_code[:4]) + years}{join_term_code[-2:]}")
            return "Yes" if max_term_sort >= target_sort else "No"

        graduated_eventual = "Yes" if latest_outcome_bucket == "Graduated" else "No"
        graduated_eventual_measurable = "Yes" if join_term_code else "No"
        graduated_4yr_measurable = measurable_for_years(4)
        graduated_6yr_measurable = measurable_for_years(6)
        grad_4_target = sort_term_code(f"{int(join_term_code[:4]) + 4}{join_term_code[-2:]}") if join_term_code else 999999
        grad_6_target = sort_term_code(f"{int(join_term_code[:4]) + 6}{join_term_code[-2:]}") if join_term_code else 999999
        graduated_4yr = "Yes" if latest_outcome_bucket == "Graduated" and graduated_4yr_measurable == "Yes" and explicit_grad_term and sort_term_code(explicit_grad_term) <= grad_4_target else "No" if graduated_4yr_measurable == "Yes" else ""
        graduated_6yr = "Yes" if latest_outcome_bucket == "Graduated" and graduated_6yr_measurable == "Yes" and explicit_grad_term and sort_term_code(explicit_grad_term) <= grad_6_target else "No" if graduated_6yr_measurable == "Yes" else ""

        first_standing = clean_text(academic_rows["academic_standing_bucket"].iloc[0]) if not academic_rows.empty else "Unknown"
        latest_standing = clean_text(academic_rows["academic_standing_bucket"].iloc[-1]) if not academic_rows.empty else "Unknown"

        summary_rows.append(
            {
                "student_id": student_id,
                "student_name": f"{clean_text(first_row['first_name'])} {clean_text(first_row['last_name'])}".strip(),
                "chapter": clean_text(roster_rows["chapter"].iloc[0]) if not roster_rows.empty else "",
                "initial_chapter": clean_text(roster_rows["chapter"].iloc[0]) if not roster_rows.empty else "",
                "latest_chapter": clean_text(roster_rows["chapter"].iloc[-1]) if not roster_rows.empty else "",
                "join_term_code": join_term_code,
                "join_term": join_term,
                "join_year": join_year,
                "org_entry_cohort": join_term,
                "org_entry_term_basis": clean_text(roster_rows["org_entry_term_basis"].iloc[0]) if not roster_rows.empty else "",
                "school_entry_term_code": clean_text(first_row["school_entry_term_code"]),
                "school_entry_term": term_label_from_code(first_row["school_entry_term_code"]),
                "school_entry_term_basis": clean_text(first_row["school_entry_term_basis"]),
                "first_observed_org_term_code": first_org_term,
                "first_observed_org_term": term_label_from_code(first_org_term),
                "last_observed_org_term_code": last_org_term,
                "last_observed_org_term": term_label_from_code(last_org_term),
                "first_observed_academic_term_code": first_ac_term,
                "first_observed_academic_term": term_label_from_code(first_ac_term),
                "last_observed_academic_term_code": last_ac_term,
                "last_observed_academic_term": term_label_from_code(last_ac_term),
                "latest_outcome_bucket": latest_outcome_bucket,
                "exit_reason_code": explicit_exit_reason(latest_outcome_bucket, latest_status_bucket),
                "graduation_term_code": explicit_grad_term,
                "resolved_outcome_flag": "Yes" if latest_outcome_bucket not in UNRESOLVED_OUTCOMES else "No",
                "resolved_outcome_excluded_flag": "Yes" if latest_outcome_bucket in UNRESOLVED_OUTCOMES else "No",
                "resolved_outcome_exclusion_reason": latest_outcome_bucket if latest_outcome_bucket in UNRESOLVED_OUTCOMES else "",
                "outcome_evidence_source": evidence_source,
                "latest_roster_status_bucket": latest_status_bucket or "Unknown",
                "initial_roster_status_bucket": clean_text(roster_rows["org_status_bucket"].iloc[0]) if not roster_rows.empty else "Unknown",
                "active_flag": "Yes" if latest_status_bucket in {"Active", "New Member"} else "No",
                "major": clean_text(academic_rows["major"].iloc[-1]) if not academic_rows.empty else "",
                "pell_flag": "",
                "transfer_flag": "Yes" if latest_outcome_bucket == "Transfer" else "No" if latest_outcome_bucket else "",
                "graduation_term": term_label_from_code(explicit_grad_term),
                "graduation_year": parse_term_code(explicit_grad_term)[2] if explicit_grad_term else pd.NA,
                "entry_cumulative_hours": entry_hours,
                "entry_hours_bucket": bucket_30_hours(entry_hours),
                "estimated_pre_org_hours_txst": pd.NA,
                "estimated_pre_org_stage_txst": "Unknown",
                "current_total_hours": latest_cumulative_hours,
                "total_cumulative_hours": latest_cumulative_hours,
                "first_term_gpa": first_term_gpa,
                "second_term_gpa": second_term_gpa,
                "first_year_avg_term_gpa": first_year_avg_gpa,
                "average_term_gpa": first_year_avg_gpa if not pd.isna(first_year_avg_gpa) else coerce_numeric(academic_rows["term_gpa"]).mean(),
                "gpa_change": (second_term_gpa - first_term_gpa) if not pd.isna(first_term_gpa) and not pd.isna(second_term_gpa) else pd.NA,
                "latest_overall_cumulative_gpa": latest_overall_cum,
                "latest_txstate_cumulative_gpa": latest_txstate_cum,
                "average_cumulative_gpa": latest_overall_cum if not pd.isna(latest_overall_cum) else latest_txstate_cum,
                "first_term_passed_hours": first_passed,
                "first_year_passed_hours": first_year_passed,
                "graduated_eventual": graduated_eventual,
                "graduated_eventual_measurable": graduated_eventual_measurable,
                "graduated_4yr": graduated_4yr,
                "graduated_4yr_measurable": graduated_4yr_measurable,
                "graduated_6yr": graduated_6yr,
                "graduated_6yr_measurable": graduated_6yr_measurable,
                "retained_next_term": has_term(roster_rows, next_term_sort),
                "retained_next_term_measurable": "Yes" if next_term_sort and next_term_sort <= max_term_sort else "",
                "retained_next_fall": has_term(roster_rows, next_fall_sort),
                "retained_next_fall_measurable": "Yes" if next_fall_sort and next_fall_sort <= max_term_sort else "",
                "retained_one_year": has_term(roster_rows, one_year_sort),
                "retained_one_year_measurable": "Yes" if one_year_sort and one_year_sort <= max_term_sort else "",
                "continued_next_term": has_term(academic_rows, next_term_sort),
                "continued_next_term_measurable": "Yes" if next_term_sort and next_term_sort <= max_term_sort else "",
                "continued_next_fall": has_term(academic_rows, next_fall_sort),
                "continued_next_fall_measurable": "Yes" if next_fall_sort and next_fall_sort <= max_term_sort else "",
                "continued_one_year": has_term(academic_rows, one_year_sort),
                "continued_one_year_measurable": "Yes" if one_year_sort and one_year_sort <= max_term_sort else "",
                "low_gpa_2_0_flag": "Yes" if not pd.isna(first_term_gpa) and float(first_term_gpa) < 2.0 else "No" if not pd.isna(first_term_gpa) else "",
                "low_gpa_2_5_flag": "Yes" if not pd.isna(first_term_gpa) and float(first_term_gpa) < 2.5 else "No" if not pd.isna(first_term_gpa) else "",
                "first_year_low_gpa_2_0_flag": "Yes" if not pd.isna(first_year_avg_gpa) and float(first_year_avg_gpa) < 2.0 else "No" if not pd.isna(first_year_avg_gpa) else "",
                "first_year_low_gpa_2_5_flag": "Yes" if not pd.isna(first_year_avg_gpa) and float(first_year_avg_gpa) < 2.5 else "No" if not pd.isna(first_year_avg_gpa) else "",
                "good_standing_first_term": "Yes" if first_standing == "Good Standing" else "No" if first_standing != "Unknown" else "",
                "probation_warning_first_year": "Yes" if (first_year_window["academic_standing_bucket"].fillna("").eq("Probation/Warning")).any() else "No" if not first_year_window.empty else "",
                "academic_standing_suspended_ever": "Yes" if (academic_rows["academic_standing_bucket"].fillna("").eq("Suspended")).any() else "No",
                "first_academic_standing_bucket": first_standing,
                "latest_academic_standing_bucket": latest_standing,
                "snapshot_matched": "No",
                "source_logic": "canonical_pipeline",
            }
        )

    summary = pd.DataFrame(summary_rows)
    summary = attach_snapshot_fields(summary, snapshot, master)
    summary["chapter"] = summary["initial_chapter"].where(summary["initial_chapter"].fillna("").astype(str).str.strip().ne(""), summary["latest_chapter"])
    summary["is_fsl_member"] = summary["chapter"].fillna("").astype(str).str.strip().ne("")
    summary["chapter_size"] = summary.groupby("chapter", dropna=False)["student_id"].transform("nunique")

    def chapter_band(value: object) -> str:
        if pd.isna(value):
            return "Unknown"
        number = float(value)
        for band in settings.get("chapter_size_bands", []):
            lower = float(band.get("min", 0))
            upper = band.get("max")
            if number >= lower and (upper is None or number <= float(upper)):
                return str(band["label"])
        return "Unknown"

    summary["chapter_size_band"] = summary["chapter_size"].map(chapter_band)
    summary["high_hours_flag"] = coerce_numeric(summary["total_cumulative_hours"]).ge(settings.get("high_hours_threshold", 60))
    summary["high_hours_group"] = summary["high_hours_flag"].map(lambda value: "High Hours" if value else "Lower Hours" if value is not pd.NA and not pd.isna(value) else "Unknown")
    summary["active_membership_group"] = summary["active_flag"].map(lambda value: "Active" if value == "Yes" else "Inactive/Other" if value == "No" else "Unknown")
    summary["pell_group"] = summary["pell_flag"].map(lambda value: "Pell" if value == "Yes" else "Non-Pell" if value == "No" else "Unknown")
    summary["transfer_group"] = summary["transfer_flag"].map(lambda value: "Transfer" if value == "Yes" else "Non-Transfer" if value == "No" else "Unknown")
    summary["snapshot_group"] = summary["snapshot_matched"].map(lambda value: "Snapshot Matched" if value == "Yes" else "No Snapshot Match" if value == "No" else "Unknown")
    summary["status_group"] = summary["latest_outcome_bucket"].replace("", "Unknown")
    summary["major_group"] = summary["major"].replace("", "Unknown")

    if not chapter_mapping.empty:
        mapping = chapter_mapping.copy()
        mapping["_chapter_key"] = mapping["chapter"].fillna("").astype(str).str.strip().str.lower()
        summary["_chapter_key"] = summary["chapter"].fillna("").astype(str).str.strip().str.lower()
        summary = summary.merge(mapping[["_chapter_key", "chapter_group", "council", "org_type", "family", "custom_group"]], on="_chapter_key", how="left")
        summary = summary.drop(columns=["_chapter_key"])
    for column, default in {
        "chapter_group": "Unassigned",
        "council": "Unknown",
        "org_type": "Unknown",
        "family": "Unknown",
        "custom_group": "Unassigned",
    }.items():
        if column not in summary.columns:
            summary[column] = default
        summary[column] = summary[column].fillna("").astype(str).replace("", default)

    resolution_fields = build_outcome_resolution_fields(summary, settings.get("outcome_resolution", {}))
    for column in resolution_fields.columns:
        summary[column] = resolution_fields[column]

    completeness_fields = [field for field in settings.get("completeness_fields", []) if field in summary.columns]
    if completeness_fields:
        present = summary[completeness_fields].notna() & summary[completeness_fields].astype(str).ne("")
        summary["data_completeness_rate"] = present.sum(axis=1) / len(completeness_fields)
    else:
        summary["data_completeness_rate"] = pd.NA

    qa_rows.extend(
        [
            {"Check Group": "Coverage", "Check": "Unique students", "Status": "Pass", "Value": int(summary["student_id"].nunique()), "Notes": ""},
            {"Check Group": "Coverage", "Check": "Resolved outcomes", "Status": "Pass", "Value": int(summary["resolved_outcomes_only_flag"].fillna(False).astype(bool).sum()), "Notes": ""},
            {"Check Group": "Coverage", "Check": "Unresolved outcomes", "Status": "Pass", "Value": int(summary["resolved_outcome_excluded_flag"].fillna(False).astype(bool).sum()), "Notes": ""},
        ]
    )

    summary_columns = load_schema()["tables"]["student_summary"] + [
        "is_fsl_member",
        "chapter_size",
        "chapter_size_band",
        "high_hours_flag",
        "high_hours_group",
        "active_membership_group",
        "pell_group",
        "transfer_group",
        "snapshot_group",
        "status_group",
        "major_group",
        "chapter_group",
        "council",
        "org_type",
        "family",
        "custom_group",
        "outcome_resolution_group",
        "resolved_outcomes_only_flag",
        "data_completeness_rate",
        "latest_snapshot_student_status",
    ]
    return ensure_columns(summary, summary_columns), pd.DataFrame(qa_rows), pd.DataFrame(outcome_exceptions)


def build_cohort_metrics(summary: pd.DataFrame) -> pd.DataFrame:
    rows: List[dict] = []
    if summary.empty:
        return pd.DataFrame(columns=load_schema()["tables"]["cohort_metrics"])

    cohorts = ["Overall"] + sorted(value for value in summary["org_entry_cohort"].fillna("").astype(str).unique().tolist() if value)
    for cohort in cohorts:
        frame = summary if cohort == "Overall" else summary.loc[summary["org_entry_cohort"].eq(cohort)].copy()
        if frame.empty:
            continue

        def rate_row(label: str, numerator_col: str, denominator_col: str, group: str, notes: str = "") -> None:
            eligible = int(frame[denominator_col].fillna("").astype(str).eq("Yes").sum())
            numerator = int((frame[numerator_col].fillna("").astype(str).eq("Yes") & frame[denominator_col].fillna("").astype(str).eq("Yes")).sum())
            rows.append(metric_row(group, label, cohort, eligible, numerator=numerator, rate=(numerator / eligible) if eligible else None, notes=notes))

        def mean_row(label: str, value_col: str, group: str) -> None:
            values = coerce_numeric(frame[value_col]).dropna()
            rows.append(metric_row(group, label, cohort, int(values.shape[0]), average=float(values.mean()) if not values.empty else None))

        rows.append(metric_row("Coverage", "Students", cohort, int(frame["student_id"].nunique()), numerator=int(frame["student_id"].nunique()), rate=1.0))
        rate_row("Observed Eventual Graduation Rate", "graduated_eventual", "graduated_eventual_measurable", "Graduation")
        rate_row("Observed 4-Year Graduation Rate", "graduated_4yr", "graduated_4yr_measurable", "Graduation")
        rate_row("Observed 6-Year Graduation Rate", "graduated_6yr", "graduated_6yr_measurable", "Graduation")
        rate_row("Organization Retention To Next Term", "retained_next_term", "retained_next_term_measurable", "Retention")
        rate_row("Organization Retention To Next Fall", "retained_next_fall", "retained_next_fall_measurable", "Retention")
        rate_row("Academic Continuation To Next Fall", "continued_next_fall", "continued_next_fall_measurable", "Retention")
        mean_row("Average First-Year Term GPA", "first_year_avg_term_gpa", "GPA")
        mean_row("Average Cumulative GPA", "average_cumulative_gpa", "GPA")

        first_term_hours = coerce_numeric(frame["first_term_passed_hours"]).dropna()
        rows.append(metric_row("Credit Momentum", "First-Term 15+ Passed Hours Rate", cohort, int(first_term_hours.shape[0]), numerator=int((first_term_hours >= 15).sum()), rate=((first_term_hours >= 15).sum() / first_term_hours.shape[0]) if first_term_hours.shape[0] else None))
        first_year_hours = coerce_numeric(frame["first_year_passed_hours"]).dropna()
        rows.append(metric_row("Credit Momentum", "First-Year 30+ Passed Hours Rate", cohort, int(first_year_hours.shape[0]), numerator=int((first_year_hours >= 30).sum()), rate=((first_year_hours >= 30).sum() / first_year_hours.shape[0]) if first_year_hours.shape[0] else None))

    return ensure_columns(pd.DataFrame(rows), load_schema()["tables"]["cohort_metrics"])


def build_status_exceptions(roster: pd.DataFrame, academic: pd.DataFrame) -> pd.DataFrame:
    rows: List[dict] = []
    valid_roster_buckets = {
        "Graduated",
        "Suspended",
        "Transfer",
        "Revoked",
        "Resigned",
        "Inactive",
        "New Member",
        "Active",
        "Alumni",
        "Unknown",
    }
    if not roster.empty:
        unmapped_roster = roster.loc[
            roster["org_status_raw"].fillna("").astype(str).str.strip().ne("")
            & ~roster["org_status_bucket"].fillna("").astype(str).isin(valid_roster_buckets)
        ]
        for row in unmapped_roster.itertuples(index=False):
            rows.append(
                {
                    "exception_type": "unmapped_roster_status",
                    "source_file": clean_text(getattr(row, "source_file", "")),
                    "student_id": clean_text(getattr(row, "student_id", "")),
                    "term_code": clean_text(getattr(row, "term_code", "")),
                    "details": clean_text(getattr(row, "org_status_raw", "")),
                }
            )
    if not academic.empty:
        unmapped_standing = academic.loc[
            academic["academic_standing_raw"].fillna("").astype(str).str.strip().ne("")
            & academic["academic_standing_bucket"].fillna("").astype(str).eq("Other/Unmapped")
        ]
        for row in unmapped_standing.itertuples(index=False):
            rows.append(
                {
                    "exception_type": "unmapped_academic_standing",
                    "source_file": clean_text(getattr(row, "source_file", "")),
                    "student_id": clean_text(getattr(row, "student_id", "")),
                    "term_code": clean_text(getattr(row, "term_code", "")),
                    "details": clean_text(getattr(row, "academic_standing_raw", "")),
                }
            )
    return pd.DataFrame(rows, columns=["exception_type", "source_file", "student_id", "term_code", "details"])


def build_spring_coverage_checks(frame: pd.DataFrame, label: str) -> List[dict]:
    rows: List[dict] = []
    if frame.empty or "term_year" not in frame.columns or "term_season" not in frame.columns:
        return rows
    years = sorted({int(value) for value in coerce_numeric(frame["term_year"]).dropna().tolist()})
    for year in years:
        year_frame = frame.loc[coerce_numeric(frame["term_year"]).eq(year)]
        spring_count = int(year_frame["term_season"].fillna("").astype(str).str.strip().eq("Spring").sum())
        rows.append(
            {
                "Check Group": "Coverage",
                "Check": f"{label} spring coverage {year}",
                "Status": "Pass" if spring_count > 0 else "Review",
                "Value": spring_count,
                "Notes": "" if spring_count > 0 else f"No spring {label.lower()} rows found for {year}.",
            }
        )
    return rows


def build_measurable_window_checks(summary: pd.DataFrame) -> List[dict]:
    rows: List[dict] = []
    if summary.empty:
        return rows
    invalid_4yr = summary.loc[
        summary["graduated_4yr_measurable"].fillna("").astype(str).ne("Yes")
        & summary["graduated_4yr"].fillna("").astype(str).str.strip().ne("")
    ]
    invalid_6yr = summary.loc[
        summary["graduated_6yr_measurable"].fillna("").astype(str).ne("Yes")
        & summary["graduated_6yr"].fillna("").astype(str).str.strip().ne("")
    ]
    rows.append(
        {
            "Check Group": "Windows",
            "Check": "4-year graduation window enforcement",
            "Status": "Pass" if invalid_4yr.empty else "Fail",
            "Value": int(len(invalid_4yr)),
            "Notes": "" if invalid_4yr.empty else "Non-measurable students have populated 4-year graduation values.",
        }
    )
    rows.append(
        {
            "Check Group": "Windows",
            "Check": "6-year graduation window enforcement",
            "Status": "Pass" if invalid_6yr.empty else "Fail",
            "Value": int(len(invalid_6yr)),
            "Notes": "" if invalid_6yr.empty else "Non-measurable students have populated 6-year graduation values.",
        }
    )
    return rows


def build_qa_checks(
    roster: pd.DataFrame,
    academic: pd.DataFrame,
    master: pd.DataFrame,
    summary: pd.DataFrame,
    issue_frames: Dict[str, pd.DataFrame],
    membership_reference_validation: pd.DataFrame,
    new_member_reference_validation: pd.DataFrame,
    gpa_reference_validation: pd.DataFrame,
    gpa_benchmark_validation: pd.DataFrame,
    reference_inventory: pd.DataFrame,
    reference_unclassified_rows: pd.DataFrame,
    retention_reference: pd.DataFrame,
) -> pd.DataFrame:
    rows: List[dict] = [
        {"Check Group": "Schema", "Check": "Authoritative tables built", "Status": "Pass", "Value": 6, "Notes": "roster_term, academic_term, master_longitudinal, student_summary, cohort_metrics, qa_checks"},
        {"Check Group": "Duplicates", "Check": "Roster duplicate student-term rows", "Status": "Pass" if roster.duplicated(subset=["student_id", "term_code"]).sum() == 0 else "Fail", "Value": int(roster.duplicated(subset=["student_id", "term_code"]).sum()), "Notes": ""},
        {"Check Group": "Duplicates", "Check": "Academic duplicate student-term rows", "Status": "Pass" if academic.duplicated(subset=["student_id", "term_code"]).sum() == 0 else "Fail", "Value": int(academic.duplicated(subset=["student_id", "term_code"]).sum()), "Notes": ""},
        {"Check Group": "Duplicates", "Check": "Master duplicate student-term rows", "Status": "Pass" if master.duplicated(subset=["student_id", "term_code"]).sum() == 0 else "Fail", "Value": int(master.duplicated(subset=["student_id", "term_code"]).sum()), "Notes": ""},
        {"Check Group": "Coverage", "Check": "Students with roster but no academics", "Status": "Pass", "Value": int(summary.loc[summary["first_observed_org_term_code"].ne("") & summary["first_observed_academic_term_code"].eq("")].shape[0]), "Notes": ""},
        {"Check Group": "Coverage", "Check": "Students with academics but no roster", "Status": "Pass", "Value": int(summary.loc[summary["first_observed_org_term_code"].eq("") & summary["first_observed_academic_term_code"].ne("")].shape[0]), "Notes": ""},
        {"Check Group": "Coverage", "Check": "Resolved outcomes", "Status": "Pass", "Value": int(summary["resolved_outcomes_only_flag"].fillna(False).astype(bool).sum()), "Notes": ""},
        {"Check Group": "Coverage", "Check": "Unresolved outcomes", "Status": "Pass", "Value": int(summary["resolved_outcome_excluded_flag"].fillna(False).astype(bool).sum()), "Notes": ""},
    ]
    rows.extend(build_spring_coverage_checks(roster, "Roster"))
    rows.extend(build_spring_coverage_checks(academic, "Academic"))
    rows.extend(build_measurable_window_checks(summary))
    if reference_inventory.empty:
        rows.append(
            {
                "Check Group": "Reference Validation",
                "Check": "Reference inventory rows loaded",
                "Status": "Review",
                "Value": 0,
                "Notes": "No numeric reference rows were cataloged from the reference workbooks.",
            }
        )
    else:
        rows.extend(
            [
                {
                    "Check Group": "Reference Validation",
                    "Check": "Reference inventory rows loaded",
                    "Status": "Pass",
                    "Value": int(len(reference_inventory)),
                    "Notes": "",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Reference inventory unclassified rows",
                    "Status": "Pass" if reference_unclassified_rows.empty else "Review",
                    "Value": int(len(reference_unclassified_rows)),
                    "Notes": "" if reference_unclassified_rows.empty else "See reference_unclassified_rows.csv for rows that need manual interpretation.",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Retention reference rows cataloged",
                    "Status": "Pass" if not retention_reference.empty else "Review",
                    "Value": int(len(retention_reference)),
                    "Notes": "" if not retention_reference.empty else "No retention reference rows were cataloged.",
                },
            ]
        )
    if membership_reference_validation.empty:
        rows.append(
            {
                "Check Group": "Reference Validation",
                "Check": "Supplemental membership reference rows loaded",
                "Status": "Review",
                "Value": 0,
                "Notes": "No supplemental membership reference workbook rows were loaded.",
            }
        )
    else:
        match_count = int(membership_reference_validation["comparison_status"].eq("Match").sum())
        mismatch_count = int(membership_reference_validation["comparison_status"].eq("Mismatch").sum())
        reference_only_count = int(membership_reference_validation["comparison_status"].eq("Reference Only").sum())
        rows.extend(
            [
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental membership reference rows loaded",
                    "Status": "Pass",
                    "Value": int(len(membership_reference_validation)),
                    "Notes": "",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental membership count matches",
                    "Status": "Pass" if mismatch_count == 0 and reference_only_count == 0 else "Review",
                    "Value": match_count,
                    "Notes": "" if mismatch_count == 0 and reference_only_count == 0 else "See membership_reference_validation.csv for non-matching rows.",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental membership count mismatches",
                    "Status": "Pass" if mismatch_count == 0 else "Review",
                    "Value": mismatch_count,
                    "Notes": "" if mismatch_count == 0 else "Reference and pipeline counts differ for these chapter-term rows.",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental reference-only chapter-term rows",
                    "Status": "Pass" if reference_only_count == 0 else "Review",
                    "Value": reference_only_count,
                    "Notes": "" if reference_only_count == 0 else "Reference workbook contains chapter-term counts not present in the rebuilt roster data.",
                },
            ]
        )
    if new_member_reference_validation.empty:
        rows.append(
            {
                "Check Group": "Reference Validation",
                "Check": "Supplemental new-member reference rows loaded",
                "Status": "Review",
                "Value": 0,
                "Notes": "No supplemental new-member reference rows were loaded.",
            }
        )
    else:
        new_member_match_count = int(new_member_reference_validation["comparison_status"].eq("Match").sum())
        new_member_mismatch_count = int(new_member_reference_validation["comparison_status"].eq("Mismatch").sum())
        new_member_reference_only_count = int(new_member_reference_validation["comparison_status"].eq("Reference Only").sum())
        rows.extend(
            [
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental new-member reference rows loaded",
                    "Status": "Pass",
                    "Value": int(len(new_member_reference_validation)),
                    "Notes": "",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental new-member count matches",
                    "Status": "Pass" if new_member_mismatch_count == 0 and new_member_reference_only_count == 0 else "Review",
                    "Value": new_member_match_count,
                    "Notes": "" if new_member_mismatch_count == 0 and new_member_reference_only_count == 0 else "See new_member_reference_validation.csv for non-matching rows.",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental new-member count mismatches",
                    "Status": "Pass" if new_member_mismatch_count == 0 else "Review",
                    "Value": new_member_mismatch_count,
                    "Notes": "" if new_member_mismatch_count == 0 else "Reference and pipeline new-member counts differ for these chapter-term rows.",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental new-member reference-only rows",
                    "Status": "Pass" if new_member_reference_only_count == 0 else "Review",
                    "Value": new_member_reference_only_count,
                    "Notes": "" if new_member_reference_only_count == 0 else "Reference workbook contains chapter-term new-member rows not present in the rebuilt roster data.",
                },
            ]
        )
    if gpa_reference_validation.empty:
        rows.append(
            {
                "Check Group": "Reference Validation",
                "Check": "Supplemental GPA reference rows loaded",
                "Status": "Review",
                "Value": 0,
                "Notes": "No supplemental GPA reference workbook rows were loaded.",
            }
        )
    else:
        gpa_match_count = int(gpa_reference_validation["comparison_status"].eq("Match").sum())
        gpa_mismatch_count = int(gpa_reference_validation["comparison_status"].eq("Mismatch").sum())
        gpa_reference_only_count = int(gpa_reference_validation["comparison_status"].eq("Reference Only").sum())
        rows.extend(
            [
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental GPA reference rows loaded",
                    "Status": "Pass",
                    "Value": int(len(gpa_reference_validation)),
                    "Notes": "",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental chapter GPA matches",
                    "Status": "Pass" if gpa_mismatch_count == 0 and gpa_reference_only_count == 0 else "Review",
                    "Value": gpa_match_count,
                    "Notes": "" if gpa_mismatch_count == 0 and gpa_reference_only_count == 0 else "See gpa_reference_validation.csv for non-matching rows.",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental chapter GPA mismatches",
                    "Status": "Pass" if gpa_mismatch_count == 0 else "Review",
                    "Value": gpa_mismatch_count,
                    "Notes": "" if gpa_mismatch_count == 0 else "Reference and pipeline chapter GPAs differ for these chapter-term rows.",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental GPA reference-only chapter-term rows",
                    "Status": "Pass" if gpa_reference_only_count == 0 else "Review",
                    "Value": gpa_reference_only_count,
                    "Notes": "" if gpa_reference_only_count == 0 else "Reference workbook contains chapter-term GPA rows not present in the rebuilt pipeline data.",
                },
            ]
        )
    if gpa_benchmark_validation.empty:
        rows.append(
            {
                "Check Group": "Reference Validation",
                "Check": "Supplemental GPA benchmark rows loaded",
                "Status": "Review",
                "Value": 0,
                "Notes": "No supplemental GPA benchmark workbook rows were loaded.",
            }
        )
    else:
        benchmark_match_count = int(gpa_benchmark_validation["comparison_status"].eq("Match").sum())
        benchmark_mismatch_count = int(gpa_benchmark_validation["comparison_status"].eq("Mismatch").sum())
        benchmark_reference_only_count = int(gpa_benchmark_validation["comparison_status"].eq("Reference Only").sum())
        rows.extend(
            [
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental GPA benchmark rows loaded",
                    "Status": "Pass",
                    "Value": int(len(gpa_benchmark_validation)),
                    "Notes": "",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental GPA benchmark matches",
                    "Status": "Pass" if benchmark_mismatch_count == 0 and benchmark_reference_only_count == 0 else "Review",
                    "Value": benchmark_match_count,
                    "Notes": "" if benchmark_mismatch_count == 0 and benchmark_reference_only_count == 0 else "See gpa_benchmark_validation.csv for non-matching rows.",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental GPA benchmark mismatches",
                    "Status": "Pass" if benchmark_mismatch_count == 0 else "Review",
                    "Value": benchmark_mismatch_count,
                    "Notes": "" if benchmark_mismatch_count == 0 else "Reference and pipeline benchmark GPAs differ for these term rows.",
                },
                {
                    "Check Group": "Reference Validation",
                    "Check": "Supplemental GPA benchmark reference-only rows",
                    "Status": "Pass" if benchmark_reference_only_count == 0 else "Review",
                    "Value": benchmark_reference_only_count,
                    "Notes": "" if benchmark_reference_only_count == 0 else "Benchmark workbook contains rows the rebuilt pipeline cannot compute directly, such as TXST-wide averages.",
                },
            ]
        )
    for name, frame in issue_frames.items():
        rows.append(
            {
                "Check Group": "Exceptions",
                "Check": name,
                "Status": "Pass" if frame.empty else "Review",
                "Value": int(len(frame)),
                "Notes": "Manual review required." if not frame.empty else "",
            }
        )
    return ensure_columns(pd.DataFrame(rows), load_schema()["tables"]["qa_checks"])


def write_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def build_canonical_pipeline(
    roster_root: Path,
    roster_inbox: Path,
    academic_root: Path,
    graduation_root: Path,
    reference_data_root: Path,
    membership_reference_root: Path,
    gpa_reference_root: Path,
    gpa_benchmark_root: Path,
    output_root: Path,
) -> CanonicalBuildResult:
    schema = load_schema()
    settings = load_settings()
    chapter_mapping = load_chapter_mapping()

    roster_term, roster_load_issues = load_roster_term_table([roster_root, roster_inbox])
    academic_term, academic_load_issues = load_academic_term_table(academic_root)
    snapshot = load_snapshot_table(academic_root)
    graduation, graduation_load_issues = load_graduation_table(graduation_root)
    reference_inventory, reference_inventory_issues = load_reference_inventory_table(
        [reference_data_root, membership_reference_root, gpa_reference_root, gpa_benchmark_root]
    )
    membership_reference = build_reference_subset(
        reference_inventory,
        "membership_count",
        "chapter",
        "membership_count_reference",
        ["chapter", "chapter_raw", "term_code", "term_label", "membership_count_reference", "source_file", "source_sheet"],
        dedupe_subset=["chapter", "term_code"],
    )
    gpa_reference = build_reference_subset(
        reference_inventory,
        "average_gpa",
        "chapter",
        "chapter_average_gpa_reference",
        ["chapter", "chapter_raw", "term_code", "term_label", "chapter_average_gpa_reference", "source_file", "source_sheet"],
        dedupe_subset=["chapter", "term_code"],
    )
    gpa_benchmark_reference = build_reference_subset(
        reference_inventory,
        "average_gpa",
        "benchmark",
        "benchmark_average_gpa_reference",
        ["benchmark_label", "term_code", "term_label", "benchmark_average_gpa_reference", "source_file", "source_sheet"],
        dedupe_subset=["benchmark_label", "term_code"],
    )
    new_member_reference = build_reference_subset(
        reference_inventory,
        "new_member_count",
        "chapter",
        "new_member_count_reference",
        ["chapter", "chapter_raw", "term_code", "term_label", "new_member_count_reference", "source_file", "source_sheet"],
        dedupe_subset=["chapter", "term_code"],
    )
    retention_reference = ensure_columns(
        reference_inventory.loc[reference_inventory["reference_type"].eq("retention_rate")].rename(
            columns={
                "entity_label_raw": "entity_label_raw",
                "entity_label_normalized": "entity_label_normalized",
                "reference_value": "retention_rate_reference",
            }
        ),
        [
            "entity_type",
            "entity_label_raw",
            "entity_label_normalized",
            "term_code",
            "term_label",
            "retention_rate_reference",
            "source_file",
            "source_sheet",
        ],
    ).drop_duplicates(
        subset=["entity_type", "entity_label_normalized", "term_code", "source_file", "source_sheet"],
        keep="first",
    ).reset_index(drop=True)
    reference_unclassified_rows = ensure_columns(
        reference_inventory.loc[reference_inventory["reference_type"].eq("unknown")].copy(),
        [
            "reference_type",
            "entity_type",
            "entity_label_raw",
            "entity_label_normalized",
            "term_code",
            "term_label",
            "reference_value",
            "classification_basis",
            "source_file",
            "source_sheet",
        ],
    )

    email_map, name_map, identity_map_issues = build_identity_maps(roster_term, academic_term, snapshot, graduation)
    roster_term, roster_id_issues = resolve_missing_ids(roster_term, email_map, name_map, "roster")
    academic_term, academic_id_issues = resolve_missing_ids(academic_term, email_map, name_map, "academic")

    roster_term, roster_dup_issues = dedupe_table(roster_term, ["student_id", "term_code", "chapter"], "roster")
    academic_term, academic_dup_issues = dedupe_table(academic_term, ["student_id", "term_code"], "academic")
    roster_term, roster_conflicts = resolve_roster_conflicts(roster_term)
    roster_term = attach_org_entry_terms(roster_term)

    master_longitudinal = build_master_longitudinal(roster_term, academic_term)
    student_summary, summary_qa, outcome_issues = build_student_summary(master_longitudinal, snapshot, graduation, settings, chapter_mapping)

    if not student_summary.empty:
        summary_lookup = student_summary.set_index("student_id")[
            [
                "latest_outcome_bucket",
                "exit_reason_code",
                "graduation_term_code",
                "resolved_outcome_flag",
                "outcome_evidence_source",
                "school_entry_term_code",
                "school_entry_term_basis",
            ]
        ]
        master_longitudinal["final_outcome_bucket"] = master_longitudinal["student_id"].map(summary_lookup["latest_outcome_bucket"].to_dict())
        master_longitudinal["exit_reason_code"] = master_longitudinal["student_id"].map(summary_lookup["exit_reason_code"].to_dict())
        master_longitudinal["graduation_term_code"] = master_longitudinal["student_id"].map(summary_lookup["graduation_term_code"].to_dict())
        master_longitudinal["resolved_outcome_flag"] = master_longitudinal["student_id"].map(summary_lookup["resolved_outcome_flag"].to_dict())
        master_longitudinal["outcome_evidence_source"] = master_longitudinal["student_id"].map(summary_lookup["outcome_evidence_source"].to_dict())
        master_longitudinal["school_entry_term_code"] = master_longitudinal["student_id"].map(summary_lookup["school_entry_term_code"].to_dict())
        master_longitudinal["school_entry_term_basis"] = master_longitudinal["student_id"].map(summary_lookup["school_entry_term_basis"].to_dict())

    cohort_metrics = build_cohort_metrics(student_summary)
    membership_reference_validation = build_membership_reference_validation(roster_term, membership_reference)
    new_member_reference_validation = build_new_member_reference_validation(roster_term, new_member_reference)
    gpa_reference_validation = build_gpa_reference_validation(master_longitudinal, gpa_reference)
    gpa_benchmark_validation = build_gpa_benchmark_validation(master_longitudinal, gpa_benchmark_reference, chapter_mapping)
    empty_exception_frame = pd.DataFrame(columns=["exception_type", "source_file", "student_id", "term_code", "details"])
    identity_exceptions = pd.concat(
        [frame for frame in [identity_map_issues, roster_id_issues, academic_id_issues] if not frame.empty],
        ignore_index=True,
    ) if any(not frame.empty for frame in [identity_map_issues, roster_id_issues, academic_id_issues]) else empty_exception_frame
    term_exceptions = pd.concat(
        [frame for frame in [roster_load_issues, academic_load_issues, graduation_load_issues, reference_inventory_issues, roster_dup_issues, academic_dup_issues] if not frame.empty],
        ignore_index=True,
    ) if any(not frame.empty for frame in [roster_load_issues, academic_load_issues, graduation_load_issues, reference_inventory_issues, roster_dup_issues, academic_dup_issues]) else empty_exception_frame
    status_exceptions = build_status_exceptions(roster_term, academic_term)
    missing_evidence_cases = student_summary.loc[
        student_summary["latest_outcome_bucket"].isin(list(UNRESOLVED_OUTCOMES)),
        ["student_id", "student_name", "join_term", "latest_outcome_bucket", "outcome_evidence_source"],
    ].rename(
        columns={
            "student_name": "details",
            "join_term": "term_code",
            "latest_outcome_bucket": "exception_type",
            "outcome_evidence_source": "source_file",
        }
    ) if not student_summary.empty else pd.DataFrame(columns=["exception_type", "source_file", "student_id", "term_code", "details"])

    issue_frames = {
        "identity_exceptions": identity_exceptions,
        "term_exceptions": term_exceptions,
        "status_exceptions": status_exceptions,
        "chapter_conflicts": roster_conflicts,
        "outcome_exceptions": outcome_issues,
        "missing_evidence_cases": missing_evidence_cases,
    }
    qa_checks = build_qa_checks(
        roster_term,
        academic_term,
        master_longitudinal,
        student_summary,
        issue_frames,
        membership_reference_validation,
        new_member_reference_validation,
        gpa_reference_validation,
        gpa_benchmark_validation,
        reference_inventory,
        reference_unclassified_rows,
        retention_reference,
    )
    if not summary_qa.empty:
        qa_checks = pd.concat([qa_checks, ensure_columns(summary_qa, QA_COLUMNS)], ignore_index=True)

    timestamp = datetime.now().strftime("run_%Y%m%d_%H%M%S")
    output_folder = output_root / timestamp
    output_folder.mkdir(parents=True, exist_ok=True)

    files = {
        "roster_term": output_folder / "roster_term.csv",
        "academic_term": output_folder / "academic_term.csv",
        "master_longitudinal": output_folder / "master_longitudinal.csv",
        "student_summary": output_folder / "student_summary.csv",
        "cohort_metrics": output_folder / "cohort_metrics.csv",
        "qa_checks": output_folder / "qa_checks.csv",
        "reference_inventory": output_folder / "reference_inventory.csv",
        "reference_unclassified_rows": output_folder / "reference_unclassified_rows.csv",
        "membership_reference_counts": output_folder / "membership_reference_counts.csv",
        "membership_reference_validation": output_folder / "membership_reference_validation.csv",
        "new_member_reference_values": output_folder / "new_member_reference_values.csv",
        "new_member_reference_validation": output_folder / "new_member_reference_validation.csv",
        "gpa_reference_values": output_folder / "gpa_reference_values.csv",
        "gpa_reference_validation": output_folder / "gpa_reference_validation.csv",
        "gpa_benchmark_reference_values": output_folder / "gpa_benchmark_reference_values.csv",
        "gpa_benchmark_validation": output_folder / "gpa_benchmark_validation.csv",
        "retention_reference_values": output_folder / "retention_reference_values.csv",
        "schema": output_folder / "canonical_schema.json",
        "identity_exceptions": output_folder / "identity_exceptions.csv",
        "term_exceptions": output_folder / "term_exceptions.csv",
        "status_exceptions": output_folder / "status_exceptions.csv",
        "chapter_conflicts": output_folder / "chapter_conflicts.csv",
        "outcome_exceptions": output_folder / "outcome_exceptions.csv",
        "missing_evidence_cases": output_folder / "missing_evidence_cases.csv",
    }

    write_frame(files["roster_term"], roster_term)
    write_frame(files["academic_term"], academic_term)
    write_frame(files["master_longitudinal"], ensure_columns(master_longitudinal, schema["tables"]["master_longitudinal"]))
    write_frame(files["student_summary"], student_summary)
    write_frame(files["cohort_metrics"], cohort_metrics)
    write_frame(files["qa_checks"], qa_checks)
    write_frame(files["reference_inventory"], reference_inventory)
    write_frame(files["reference_unclassified_rows"], reference_unclassified_rows)
    write_frame(files["membership_reference_counts"], membership_reference)
    write_frame(files["membership_reference_validation"], membership_reference_validation)
    write_frame(files["new_member_reference_values"], new_member_reference)
    write_frame(files["new_member_reference_validation"], new_member_reference_validation)
    write_frame(files["gpa_reference_values"], gpa_reference)
    write_frame(files["gpa_reference_validation"], gpa_reference_validation)
    write_frame(files["gpa_benchmark_reference_values"], gpa_benchmark_reference)
    write_frame(files["gpa_benchmark_validation"], gpa_benchmark_validation)
    write_frame(files["retention_reference_values"], retention_reference)
    files["schema"].write_text(json.dumps(schema, indent=2), encoding="utf-8")
    for key in ["identity_exceptions", "term_exceptions", "status_exceptions", "chapter_conflicts", "outcome_exceptions", "missing_evidence_cases"]:
        write_frame(files[key], issue_frames.get(key, pd.DataFrame()))

    latest_folder = output_root / "latest"
    latest_folder.mkdir(parents=True, exist_ok=True)
    for key, path in files.items():
        target = latest_folder / path.name
        if path.suffix == ".csv":
            write_frame(target, pd.read_csv(path))
        elif path.suffix == ".json":
            target.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")

    return CanonicalBuildResult(output_folder=output_folder, files=files)


def main() -> None:
    args = parse_args()
    result = build_canonical_pipeline(
        roster_root=Path(args.roster_root).expanduser().resolve(),
        roster_inbox=Path(args.roster_inbox).expanduser().resolve(),
        academic_root=Path(args.academic_root).expanduser().resolve(),
        graduation_root=Path(args.graduation_root).expanduser().resolve(),
        reference_data_root=Path(args.reference_data_root).expanduser().resolve(),
        membership_reference_root=Path(args.membership_reference_root).expanduser().resolve(),
        gpa_reference_root=Path(args.gpa_reference_root).expanduser().resolve(),
        gpa_benchmark_root=Path(args.gpa_benchmark_root).expanduser().resolve(),
        output_root=Path(args.output_root).expanduser().resolve(),
    )
    print(f"Canonical outputs written to: {result.output_folder}")
    for key, path in result.files.items():
        print(f"{key}: {path}")


if __name__ == "__main__":
    main()
