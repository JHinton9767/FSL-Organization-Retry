from __future__ import annotations

import math
import re
from typing import Optional, Tuple

import pandas as pd


CHAPTER_MAPPING_FIELDS = ("chapter_group", "council", "org_type", "family", "custom_group")
ROSTER_DISAPPEARED_UNKNOWN = "Roster Dissapeared/Unknown"


def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return re.sub(r"\s+", " ", text)


def normalize_chapter_key(value: object) -> str:
    text = clean_text(value).lower()
    if not text:
        return ""
    text = text.replace("&", " and ")
    text = re.sub(r"[,./'’()_-]+", " ", text)
    text = re.sub(r"\b(fraternity|sorority|incorporated|inc)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def chapter_key_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).map(normalize_chapter_key)


def apply_chapter_mapping_overrides(
    frame: pd.DataFrame,
    chapter_mapping: pd.DataFrame,
    chapter_column: str = "chapter",
    output_prefix: str = "",
) -> pd.DataFrame:
    result = frame.copy()
    if result.empty or chapter_mapping.empty or "chapter" not in chapter_mapping.columns:
        return result

    mapping = chapter_mapping.copy()
    mapping["_chapter_key"] = chapter_key_series(mapping["chapter"])
    mapping = mapping.loc[mapping["_chapter_key"].ne("")].copy()
    if mapping.empty:
        return result

    keep_columns = ["_chapter_key"] + [column for column in CHAPTER_MAPPING_FIELDS if column in mapping.columns]
    mapping = mapping.loc[:, keep_columns].drop_duplicates(subset=["_chapter_key"], keep="first")
    mapping = mapping.rename(columns={column: f"_mapped_{column}" for column in keep_columns if column != "_chapter_key"})

    chapter_series = result.get(chapter_column, pd.Series("", index=result.index, dtype="object"))
    result["_chapter_key"] = chapter_key_series(chapter_series)
    merged = result.merge(mapping, on="_chapter_key", how="left")

    for field in CHAPTER_MAPPING_FIELDS:
        output_column = f"{output_prefix}{field}" if output_prefix else field
        mapped_column = f"_mapped_{field}"
        if output_column not in merged.columns:
            merged[output_column] = ""
        if mapped_column not in merged.columns:
            continue
        mapped_values = merged[mapped_column].fillna("").astype(str).str.strip()
        existing_values = merged[output_column].fillna("").astype(str)
        merged[output_column] = existing_values.where(mapped_values.eq(""), mapped_values)
        merged = merged.drop(columns=[mapped_column])

    return merged.drop(columns=["_chapter_key"])


def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def yes_mask(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.lower().eq("yes")


def mean_or_blank(series: pd.Series) -> object:
    numeric = coerce_numeric(series)
    usable = numeric.dropna()
    if usable.empty:
        return ""
    return float(usable.mean())


def unique_non_blank_count(series: pd.Series) -> int:
    cleaned = series.fillna("").astype(str).str.strip()
    return int(cleaned.replace("", pd.NA).dropna().nunique())


def extract_year_from_text(value: object) -> Optional[int]:
    match = re.search(r"(19\d{2}|20\d{2})", clean_text(value))
    return int(match.group(1)) if match else None


def bucket_30_hours(value: object) -> str:
    number = coerce_numeric(pd.Series([value])).iloc[0]
    if pd.isna(number) or float(number) < 0:
        return "Unknown"
    lower = int(math.floor(float(number) / 30.0) * 30)
    upper = lower + 29
    return f"{lower}-{upper}"


def simple_rate(frame: pd.DataFrame, numerator_field: str, measurable_field: str | None = None) -> Tuple[object, int]:
    eligible = frame.copy()
    if measurable_field and measurable_field in eligible.columns:
        eligible = eligible.loc[yes_mask(eligible[measurable_field])]
    if eligible.empty:
        return "", 0
    numerator = int(yes_mask(eligible[numerator_field]).sum())
    return float(numerator) / float(len(eligible)), int(len(eligible))


def adjusted_grad_rate(
    frame: pd.DataFrame,
    numerator_field: str,
    measurable_field: str | None = None,
    resolved_flag_field: str = "resolved_outcome_flag",
    student_id_field: str = "student_id",
) -> Tuple[object, int]:
    eligible = frame.copy()
    if measurable_field and measurable_field in eligible.columns:
        eligible = eligible.loc[yes_mask(eligible[measurable_field])]
    if resolved_flag_field in eligible.columns:
        eligible = eligible.loc[yes_mask(eligible[resolved_flag_field])]
    if student_id_field in eligible.columns:
        eligible = eligible.drop_duplicates(subset=[student_id_field], keep="first")
    if eligible.empty:
        return "", 0
    numerator = int(yes_mask(eligible[numerator_field]).sum())
    return float(numerator) / float(len(eligible)), int(len(eligible))


def percent_text(value: object) -> str:
    if value in ("", None) or pd.isna(value):
        return "Not available"
    return f"{float(value):.1%}"


def decimal_text(value: object) -> str:
    if value in ("", None) or pd.isna(value):
        return "Not available"
    return f"{float(value):.2f}"


def count_text(value: object) -> str:
    if value in ("", None) or pd.isna(value):
        return "0"
    return f"{int(round(float(value))):,}"
