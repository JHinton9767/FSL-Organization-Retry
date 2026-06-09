from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.build_canonical_pipeline import (
    has_confirmed_graduation_text,
    parse_term_code,
    roster_status_bucket,
    sort_term_code,
    term_label_from_code,
)
from src.build_master_roster import normalize_banner_id, normalize_chapter_name
from src.shared_utils import clean_text


REQUIRED_COLUMNS = [
    "source_category",
    "source_file",
    "source_sheet",
    "row_number",
    "student_id_raw",
    "student_id",
    "first_name",
    "last_name",
    "term_raw",
    "term_code",
    "term_label",
    "term_sort",
    "chapter_raw",
    "chapter",
    "council",
    "status_raw",
    "status_bucket",
    "graduation_text_raw",
    "explicit_graduation_evidence",
    "evidence_detail",
]


def empty_required_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=REQUIRED_COLUMNS)


def normalize_term(value: object) -> tuple[str, str, int | None]:
    code, label, _, _ = parse_term_code(value)
    if not code:
        return "", clean_text(value), None
    return code, label, sort_term_code(code)


def infer_term_from_path(path: str | Path) -> tuple[str, str, int | None]:
    text = " ".join(Path(path).parts)
    code, label, sort_value = normalize_term(text)
    if code:
        return code, label, sort_value
    return "", "", None


def safe_required_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return empty_required_frame()
    result = frame.copy()
    for column in REQUIRED_COLUMNS:
        if column not in result.columns:
            result[column] = ""
    return result[REQUIRED_COLUMNS]


def normalize_required_fields(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize raw narrow records and split invalid Banner IDs out."""
    raw = safe_required_columns(raw)
    if raw.empty:
        return raw.copy(), pd.DataFrame(columns=["student_id_raw", "source_category", "source_file", "row_number", "invalid_reason"])

    normalized = raw.copy()
    normalized["student_id_raw"] = normalized["student_id_raw"].map(clean_text)
    normalized["student_id"] = normalized["student_id_raw"].map(normalize_banner_id)
    invalid_mask = normalized["student_id"].eq("")
    invalid = normalized.loc[invalid_mask, ["student_id_raw", "source_category", "source_file", "row_number"]].copy()
    invalid["invalid_reason"] = "missing_or_invalid_banner_id"

    normalized = normalized.loc[~invalid_mask].copy()
    if normalized.empty:
        return normalized[REQUIRED_COLUMNS], invalid

    normalized["first_name"] = normalized["first_name"].map(clean_text)
    normalized["last_name"] = normalized["last_name"].map(clean_text)
    normalized["term_raw"] = normalized["term_raw"].map(clean_text)
    normalized["term_code"] = normalized.apply(
        lambda row: clean_text(row["term_code"]) or normalize_term(row["term_raw"])[0] or infer_term_from_path(row["source_file"])[0],
        axis=1,
    )
    normalized["term_label"] = normalized.apply(
        lambda row: clean_text(row["term_label"]) or term_label_from_code(row["term_code"]) or normalize_term(row["term_raw"])[1],
        axis=1,
    )
    normalized["term_sort"] = normalized["term_code"].map(lambda value: sort_term_code(value) if clean_text(value) else None)
    normalized["chapter_raw"] = normalized["chapter_raw"].map(clean_text)
    normalized["chapter"] = normalized["chapter"].map(clean_text)
    normalized.loc[normalized["chapter"].eq(""), "chapter"] = normalized.loc[normalized["chapter"].eq(""), "chapter_raw"].map(
        normalize_chapter_name
    )
    normalized["council"] = normalized["council"].map(clean_text)
    normalized["status_raw"] = normalized["status_raw"].map(clean_text)
    normalized["status_bucket"] = normalized.apply(
        lambda row: clean_text(row["status_bucket"]) or roster_status_bucket(row["status_raw"], ""),
        axis=1,
    )
    normalized["graduation_text_raw"] = normalized["graduation_text_raw"].map(clean_text)
    normalized["explicit_graduation_evidence"] = normalized.apply(_explicit_evidence_flag, axis=1)
    normalized["evidence_detail"] = normalized["evidence_detail"].map(clean_text)
    return normalized[REQUIRED_COLUMNS], invalid


def _explicit_evidence_flag(row: pd.Series) -> bool:
    category = clean_text(row.get("source_category")).lower()
    if category == "roster" and clean_text(row.get("status_bucket")).lower() == "graduated":
        return True
    text = " ".join(
        [
            clean_text(row.get("status_raw")),
            clean_text(row.get("graduation_text_raw")),
            clean_text(row.get("evidence_detail")),
        ]
    )
    if category in {"transcript", "graduation", "academic"}:
        return has_confirmed_graduation_text(text)
    return bool(row.get("explicit_graduation_evidence")) and has_confirmed_graduation_text(text)


def unique_joined(values: Iterable[object]) -> str:
    seen: list[str] = []
    for value in values:
        text = clean_text(value)
        if text and text not in seen:
            seen.append(text)
    return "; ".join(seen)


def contains_graduation_text(value: object) -> bool:
    return has_confirmed_graduation_text(value)


BANNER_ID_IN_TEXT_RE = re.compile(r"\bA0\d{7}\b", re.IGNORECASE)


def banner_id_from_text(value: object) -> str:
    match = BANNER_ID_IN_TEXT_RE.search(clean_text(value).upper())
    return normalize_banner_id(match.group(0)) if match else ""

