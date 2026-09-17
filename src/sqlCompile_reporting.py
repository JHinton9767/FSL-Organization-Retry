from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from src.path_config import ROOT
from src.sqlCompile_cohort import _semester_sort
from src.sqlCompile_storage import atomic_write_text, data_lock


DEFAULT_REPORTING_SETTINGS = ROOT / "config" / "sqlCompile_reporting.json"
REPORTING_EXAMPLE = ROOT / "config" / "sqlCompile_reporting.example.json"


def normalize_reporting_cutoff(value: object) -> str:
    if (not isinstance(value, str) or not re.fullmatch(r"(Spring|Summer|Fall) [0-9]{4}", value.strip())
            or _semester_sort(value.strip()) >= 999999):
        raise ValueError("Reporting cutoff must be a semester such as Spring 2026.")
    return value.strip()


def read_reporting_cutoff(path: Path = DEFAULT_REPORTING_SETTINGS) -> str:
    source = path
    if not source.exists() and source.resolve() == DEFAULT_REPORTING_SETTINGS.resolve():
        source = REPORTING_EXAMPLE
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict) or set(payload) != {"reporting_cutoff"}:
        raise ValueError("Reporting settings must contain only reporting_cutoff.")
    return normalize_reporting_cutoff(payload["reporting_cutoff"])


def save_reporting_cutoff(value: str, path: Path = DEFAULT_REPORTING_SETTINGS) -> None:
    cutoff = normalize_reporting_cutoff(value)
    with data_lock(path):
        atomic_write_text(path, json.dumps({"reporting_cutoff": cutoff}, indent=2) + "\n")


def through_reporting_cutoff(frame: pd.DataFrame, cutoff: str, column: str = "Semester") -> pd.DataFrame:
    cutoff_sort = _semester_sort(normalize_reporting_cutoff(cutoff))
    return frame.loc[frame[column].map(_semester_sort).le(cutoff_sort)].copy()


def validate_reporting_cutoff(cutoff: str, compiled_rows: pd.DataFrame) -> None:
    cutoff_sort = _semester_sort(normalize_reporting_cutoff(cutoff))
    if compiled_rows.empty or cutoff_sort not in set(compiled_rows["Semester"].map(_semester_sort)):
        raise ValueError(f"No compiled roster records exist for the reporting cutoff {cutoff}. Compile or restore that semester before reporting it as complete.")
