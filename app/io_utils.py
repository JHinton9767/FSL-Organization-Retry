from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
SEASON_CODES = {
    "winter": "WI",
    "spring": "SP",
    "summer": "SU",
    "fall": "FA",
    "unknown": "UN",
}
TERM_ORDER = {
    "winter": 0,
    "spring": 1,
    "summer": 2,
    "fall": 3,
    "unknown": 9,
}


@dataclass(frozen=True)
class TermParts:
    year: int | None
    season: str

    @property
    def sort_key(self) -> int | None:
        if self.year is None:
            return None
        return self.year * 10 + TERM_ORDER.get(self.season, TERM_ORDER["unknown"])

    @property
    def label(self) -> str | None:
        if self.year is None:
            return None
        return f"{self.season.title()} {self.year}"


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def normalize_key(value: object) -> str:
    return canonicalize_column(value).replace(" ", "_")


def canonicalize_column(name: object) -> str:
    text = "" if name is None else str(name)
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


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


def parse_term(value: object) -> TermParts:
    if pd.isna(value):
        return TermParts(year=None, season="unknown")
    text = str(value).strip().lower()
    if not text:
        return TermParts(year=None, season="unknown")

    year_match = re.search(r"(20\d{2}|19\d{2})", text)
    year = int(year_match.group(1)) if year_match else None

    if re.search(r"(?:\b|_)(fa|fall)(?:\b|_)", text) or "fall" in text or text.endswith("fa") or text.startswith("fa"):
        season = "fall"
    elif re.search(r"(?:\b|_)(sp|spr|spring)(?:\b|_)", text) or "spring" in text or text.endswith("sp") or text.startswith("sp"):
        season = "spring"
    elif re.search(r"(?:\b|_)(sum|su|summer)(?:\b|_)", text) or "summer" in text or text.endswith("su") or text.startswith("su"):
        season = "summer"
    elif re.search(r"(?:\b|_)(win|winter)(?:\b|_)", text) or "winter" in text or text.endswith("wi") or text.startswith("wi"):
        season = "winter"
    elif re.fullmatch(r"\d{6}", text):
        code = text[-2:]
        season = {"10": "spring", "20": "summer", "30": "fall"}.get(code, "unknown")
        if year is None:
            year = int(text[:4])
    else:
        season = "unknown"

    return TermParts(year=year, season=season)


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


def canonical_headers(columns: Iterable[object]) -> list[str]:
    return [normalize_key(column) for column in columns]
