from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from src.shared_utils import coerce_numeric
from src.greek_life_pipeline import canonicalize_column, parse_term


ROOT = Path(__file__).resolve().parent.parent
SUPPORTED_TABULAR_SUFFIXES = {".csv", ".xlsx", ".xls", ".xlsm", ".parquet"}
SEASON_CODES = {
    "winter": "WI",
    "spring": "SP",
    "summer": "SU",
    "fall": "FA",
    "unknown": "UN",
}


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def normalize_key(value: object) -> str:
    return canonicalize_column(value).replace(" ", "_")


def safe_slug(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "_", normalize_text(value).lower()).strip("_")
    return text or "dataset"


def read_tabular_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return pd.read_excel(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported file type: {path.suffix}")


def write_dataframe_cache(frame: pd.DataFrame, csv_path: Path, parquet_path: Optional[Path] = None) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False)
    if parquet_path is not None:
        frame.to_parquet(parquet_path, index=False)

def bool_from_flag(value: object) -> Optional[bool]:
    text = normalize_text(value).lower()
    if not text:
        return None
    if text in {"yes", "y", "true", "1", "active", "matched"}:
        return True
    if text in {"no", "n", "false", "0", "inactive", "unmatched"}:
        return False
    return None


def category_from_bool(value: Optional[bool], yes_label: str, no_label: str, unknown_label: str = "Unknown") -> str:
    if value is True:
        return yes_label
    if value is False:
        return no_label
    return unknown_label


def parse_term_label(value: object) -> dict[str, object]:
    parts = parse_term(value)
    year = parts.year
    season = parts.season
    label = parts.label or normalize_text(value)
    code = f"{year}{SEASON_CODES.get(season, 'UN')}" if year is not None else ""
    sort_value = parts.sort_key if parts.sort_key is not None else 999999
    return {
        "label": label,
        "year": year,
        "season": season.title() if season else "Unknown",
        "code": code,
        "sort_value": sort_value,
    }


def first_non_empty(*values: object) -> str:
    for value in values:
        text = normalize_text(value)
        if text:
            return text
    return ""


def first_non_null_numeric(*values: object) -> float | None:
    for value in values:
        numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.notna(numeric):
            return float(numeric)
    return None


def unique_values(series: pd.Series) -> list[str]:
    cleaned = series.fillna("").astype(str).str.strip()
    return sorted(value for value in cleaned.unique().tolist() if value)


def canonical_headers(columns: Iterable[object]) -> list[str]:
    return [normalize_key(column) for column in columns]
