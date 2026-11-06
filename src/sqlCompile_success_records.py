"""Source-backed records for Student Success & Impact; never infer a degree from a roster."""
from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path

import pandas as pd

from src.path_config import ROOT
from src.sqlCompile_cohort import _semester_sort
from src.sqlCompile_records import UNIVERSITY_COLUMNS
from src.sqlCompile_reporting import normalize_reporting_cutoff
from src.sqlCompile_storage import read_review_csv, write_review_csv


VERIFICATION_STATES = ["Draft", "Verified", "Excluded"]
DEGREE_LEVELS = ["Certificate", "Associate", "Bachelor's", "Master's", "Doctoral", "Professional", "Other"]
COMMON_COLUMNS = ["Verification", "Source", "Reviewed By", "Reviewed On", "Notes"]
GRADUATION_COLUMNS = ["Record ID", "Student ID", "Award Date", "Award Semester", "Degree Level", "Degree Title", "Institution", *COMMON_COLUMNS]
ACADEMIC_COLUMNS = ["Record ID", "Student ID", "Semester", "GPA Credits", "Quality Points", "Earned Credits", "Honors", *COMMON_COLUMNS]
IMPACT_COLUMNS = ["Record ID", "Semester", "Chapter", "Metric", "Value", *COMMON_COLUMNS]
IMPACT_UNITS = {"Service hours": "hours", "Philanthropy raised": "USD", "Scholarships awarded": "USD", "Awards received": "awards"}
HONORS = ["", "None", "Dean's list", "President's list", "Other honors"]
ALL_CHAPTERS = "All FSL"
DEFAULT_GRADUATION_PATH = ROOT / "config" / "sqlCompile_graduation_records.csv"
DEFAULT_ACADEMIC_PATH = ROOT / "config" / "sqlCompile_academic_records.csv"
DEFAULT_IMPACT_PATH = ROOT / "config" / "sqlCompile_impact_records.csv"


def _prepare(frame: pd.DataFrame, columns: list[str], prefix: str) -> pd.DataFrame:
    unknown = set(frame.columns) - set(columns)
    if unknown:
        raise ValueError(f"Unexpected columns: {', '.join(sorted(unknown))}. Use the downloadable template; no columns were discarded.")
    result = frame.reindex(columns=columns, fill_value="").fillna("").astype(str)
    result = result.apply(lambda column: column.str.strip())
    result = result.loc[result.ne("").any(axis=1)].drop_duplicates().reset_index(drop=True)
    result["Verification"] = result["Verification"].replace("", "Draft")
    if not result["Verification"].isin(VERIFICATION_STATES).all():
        raise ValueError("Verification must be Draft, Verified, or Excluded.")
    missing_ids = result["Record ID"].eq("")
    contents = result.loc[missing_ids, [column for column in columns if column != "Record ID"]].itertuples(index=False, name=None)
    result.loc[missing_ids, "Record ID"] = [prefix + hashlib.sha256("\0".join(row).encode()).hexdigest()[:20] for row in contents]
    if result["Record ID"].duplicated().any():
        raise ValueError("Record IDs must be unique. Edit the existing record instead of adding a second version.")
    if "Student ID" in result:
        result["Student ID"] = result["Student ID"].str.upper()
        if not result["Student ID"].str.fullmatch(r"A0\d{7}").all():
            raise ValueError("Every student record needs A0 followed by seven digits.")
    verified = result["Verification"].eq("Verified")
    if result.loc[verified, ["Source", "Reviewed By", "Reviewed On"]].eq("").any().any():
        raise ValueError("Verified records require a source, reviewer, and review date.")
    _dates(result, "Reviewed On")
    return result


def _dates(frame: pd.DataFrame, column: str) -> None:
    for text in frame.loc[frame[column].ne(""), column]:
        try:
            parsed = date.fromisoformat(text)
            if parsed.isoformat() != text or parsed > date.today():
                raise ValueError
        except ValueError:
            raise ValueError(f"{column} must be YYYY-MM-DD and cannot be in the future.") from None


def _terms(frame: pd.DataFrame, column: str) -> None:
    for value in frame.loc[frame[column].ne(""), column]:
        normalize_reporting_cutoff(value)


def _required(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    verified = frame.loc[frame["Verification"].eq("Verified")]
    if verified[columns].eq("").any().any():
        raise ValueError("Verified records require: " + ", ".join(columns) + ".")
    return verified


def normalize_graduation_records(frame: pd.DataFrame) -> pd.DataFrame:
    result = _prepare(frame, GRADUATION_COLUMNS, "degree-")
    _dates(result, "Award Date")
    _terms(result, "Award Semester")
    if not result["Degree Level"].isin(["", *DEGREE_LEVELS]).all():
        raise ValueError("Choose a degree level from the template choices.")
    verified = _required(result, ["Award Date", "Award Semester", "Degree Level", "Degree Title", "Institution"])
    for row in result.to_dict("records"):
        if row["Award Date"] and row["Award Semester"] and row["Award Date"][:4] != row["Award Semester"][-4:]:
            raise ValueError("Award date and award semester must have the same calendar year.")
        if row["Award Date"] and row["Reviewed On"] and row["Reviewed On"] < row["Award Date"]:
            raise ValueError("A degree cannot be verified before it was awarded.")
    natural_key = ["Student ID", "Award Date", "Institution", "Degree Level", "Degree Title"]
    if verified[natural_key].apply(lambda column: column.str.casefold()).duplicated().any():
        raise ValueError("The same award is verified more than once. Keep one verified record per award.")
    return result


def normalize_academic_records(frame: pd.DataFrame) -> pd.DataFrame:
    result = _prepare(frame, ACADEMIC_COLUMNS, "academic-")
    _terms(result, "Semester")
    for column in ["GPA Credits", "Quality Points", "Earned Credits"]:
        values = pd.to_numeric(result[column].replace("", float("nan")), errors="coerce")
        if (result[column].ne("") & (values.isna() | values.lt(0) | values.eq(float("inf")))).any():
            raise ValueError(f"{column} must be a finite, nonnegative number.")
    if (result["GPA Credits"].eq("") != result["Quality Points"].eq("")).any():
        raise ValueError("Enter both GPA Credits and Quality Points, or leave both blank.")
    credits = pd.to_numeric(result["GPA Credits"].replace("", "0"))
    points = pd.to_numeric(result["Quality Points"].replace("", "0"))
    if (points > credits * 4 + 1e-8).any():
        raise ValueError("Academic metrics use a 4.0 scale. Quality points cannot exceed four times GPA credits.")
    if not result["Honors"].isin(HONORS).all():
        raise ValueError("Choose a supported honors value.")
    verified = _required(result, ["Semester"])
    if verified[["GPA Credits", "Earned Credits", "Honors"]].eq("").all(axis=1).any():
        raise ValueError("Verified academic records need GPA, earned credits, or honors evidence.")
    if verified.duplicated(["Student ID", "Semester"]).any():
        raise ValueError("Keep one verified academic summary per student and semester.")
    return result


def normalize_impact_records(frame: pd.DataFrame) -> pd.DataFrame:
    result = _prepare(frame, IMPACT_COLUMNS, "impact-")
    _terms(result, "Semester")
    if not result["Metric"].isin(["", *IMPACT_UNITS]).all():
        raise ValueError("Choose a supported community impact metric.")
    values = pd.to_numeric(result["Value"].replace("", float("nan")), errors="coerce")
    if (result["Value"].ne("") & (values.isna() | values.lt(0) | values.eq(float("inf")))).any():
        raise ValueError("Impact values must be finite, nonnegative numbers.")
    if (result["Metric"].eq("Awards received") & values.notna() & values.mod(1).ne(0)).any():
        raise ValueError("Awards received must be a whole number.")
    verified = _required(result, ["Semester", "Chapter", "Metric", "Value"])
    for _, group in verified.groupby(["Semester", "Metric"]):
        chapters = group["Chapter"].str.casefold()
        if chapters.duplicated().any() or (ALL_CHAPTERS.casefold() in set(chapters) and len(group) > 1):
            raise ValueError("Impact totals overlap. Use one total per chapter, semester, and metric, or one All FSL total; do not mix totals and subtotals.")
    return result


RECORD_TYPES = {
    "graduation": (GRADUATION_COLUMNS, normalize_graduation_records),
    "academic": (ACADEMIC_COLUMNS, normalize_academic_records),
    "impact": (IMPACT_COLUMNS, normalize_impact_records),
}


def read_success_records(path: Path, kind: str) -> pd.DataFrame:
    columns, normalize = RECORD_TYPES[kind]
    return normalize(read_review_csv(Path(path), columns, create=False))


def save_success_records(frame: pd.DataFrame, path: Path, kind: str, *, expected_rows=None) -> Path:
    return write_review_csv(frame, Path(path), RECORD_TYPES[kind][1], expected_rows=expected_rows)


def graduation_university_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Verified awards feed the existing degree evidence engine, including its conflict checks."""
    rows = []
    for row in normalize_graduation_records(frame).to_dict("records"):
        if row["Verification"] == "Verified":
            rows.append({"Student ID": row["Student ID"], "Semester": row["Award Semester"],
                         "Enrollment Status": "", "Degree Status": "Graduated", "Source": row["Source"],
                         "Reviewed By": row["Reviewed By"], "Resolution": "", "Notes": row["Notes"]})
    return pd.DataFrame(rows, columns=UNIVERSITY_COLUMNS)
