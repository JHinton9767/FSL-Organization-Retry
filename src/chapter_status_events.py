from __future__ import annotations

import re
from typing import Mapping

import pandas as pd

from src.build_master_roster import canonical_header, normalize_chapter_name
from src.shared_utils import clean_text, normalize_chapter_key


CHAPTER_KICKED_OUTCOME = "Chapter Kicked"
CHAPTER_STATUS_EVENT_COLUMNS = [
    "chapter",
    "event_type",
    "effective_term",
    "return_term",
    "evidence_source",
    "evidence_file_or_url",
    "confidence",
    "active",
    "notes",
]

CHAPTER_STATUS_EVENT_INTERNAL_COLUMNS = [
    *CHAPTER_STATUS_EVENT_COLUMNS,
    "chapter_normalized",
    "chapter_key",
    "event_type_normalized",
    "effective_term_code",
    "effective_term_label",
    "effective_term_sort",
    "return_term_code",
    "return_term_label",
    "return_term_sort",
    "confirmed_for_outcomes",
]

TERM_CODE_RE = re.compile(r"^(19\d{2}|20\d{2})(WI|SP|SU|FA)$", re.IGNORECASE)
TERM_RE = re.compile(r"\b(Winter|Spring|Summer|Fall)\s+(19\d{2}|20\d{2})\b", re.IGNORECASE)
SEASON_ORDER = {"WI": 0, "SP": 1, "SU": 2, "FA": 3}
SEASON_NAME = {"WI": "Winter", "SP": "Spring", "SU": "Summer", "FA": "Fall"}
SEASON_CODE_BY_NAME = {name.lower(): code for code, name in SEASON_NAME.items()}


def empty_chapter_status_events() -> pd.DataFrame:
    return pd.DataFrame(columns=CHAPTER_STATUS_EVENT_INTERNAL_COLUMNS)


def parse_status_event_term(value: object) -> tuple[str, str, int]:
    text = clean_text(value)
    if not text:
        return "", "", 999999

    code_match = TERM_CODE_RE.fullmatch(text.upper())
    if code_match:
        year = int(code_match.group(1))
        season_code = code_match.group(2).upper()
        return f"{year}{season_code}", f"{SEASON_NAME[season_code]} {year}", year * 10 + SEASON_ORDER[season_code]

    label_match = TERM_RE.search(text)
    if label_match:
        season_name = label_match.group(1).title()
        season_code = SEASON_CODE_BY_NAME.get(season_name.lower(), "")
        year = int(label_match.group(2))
        if season_code:
            return f"{year}{season_code}", f"{season_name} {year}", year * 10 + SEASON_ORDER[season_code]

    return "", text, 999999


def normalize_chapter_status_event_type(value: object) -> str:
    text = clean_text(value)
    upper = text.upper()
    if not upper:
        return ""
    if upper in {"CHAPTER KICKED", "KICKED", "REMOVED", "SUSPENDED", "CHAPTER REMOVED"}:
        return CHAPTER_KICKED_OUTCOME
    if "KICK" in upper or "REMOVED" in upper or "OFF CAMPUS" in upper or "SUSPEND" in upper:
        return CHAPTER_KICKED_OUTCOME
    if upper in {"RETURNED", "RETURN", "RECOGNIZED", "REINSTATED"} or "RETURN" in upper or "REINSTAT" in upper:
        return "Returned"
    return text


def _truthy(value: object, *, default: bool = False) -> bool:
    text = clean_text(value).lower()
    if not text:
        return default
    return text in {"yes", "y", "true", "1", "active", "apply", "enabled"}


def _confirmed(value: object) -> bool:
    text = clean_text(value).lower()
    return text in {"confirmed", "verified", "official", "yes", "y", "true", "1", "high", "high confidence"}


def _resolve_columns(frame: pd.DataFrame) -> dict[str, str]:
    renamed = {column: canonical_header(column) for column in frame.columns}
    canonical_columns = {canonical: original for original, canonical in renamed.items()}
    aliases = {
        "chapter": ["chapter", "organization", "organization name", "org", "org name"],
        "event_type": ["event type", "event", "status", "chapter status", "final status"],
        "effective_term": ["effective term", "kicked off term", "removal term", "start term", "term"],
        "return_term": ["return term", "returned term", "reinstated term", "end term"],
        "evidence_source": ["evidence source", "source", "source type"],
        "evidence_file_or_url": ["evidence file or url", "evidence file", "url", "link", "file"],
        "confidence": ["confidence", "confirmation", "confirmed", "review status"],
        "active": ["active", "apply", "use", "enabled"],
        "notes": ["notes", "note", "comments", "comment"],
    }
    resolved: dict[str, str] = {}
    for target, target_aliases in aliases.items():
        source = next((canonical_columns[canonical_header(alias)] for alias in target_aliases if canonical_header(alias) in canonical_columns), None)
        if source:
            resolved[target] = source
    return resolved


def normalize_chapter_status_events(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return empty_chapter_status_events()

    source = frame.copy()
    resolved = _resolve_columns(source)
    standardized = pd.DataFrame(index=source.index)
    for column in CHAPTER_STATUS_EVENT_COLUMNS:
        source_column = resolved.get(column)
        if source_column:
            standardized[column] = source[source_column]
        elif column == "active":
            standardized[column] = "Yes"
        else:
            standardized[column] = ""

    standardized = standardized.fillna("").astype(str)
    for column in CHAPTER_STATUS_EVENT_COLUMNS:
        standardized[column] = standardized[column].str.strip()

    standardized["chapter_normalized"] = standardized["chapter"].map(normalize_chapter_name)
    standardized["chapter_key"] = standardized["chapter_normalized"].map(normalize_chapter_key)
    standardized["event_type_normalized"] = standardized["event_type"].map(normalize_chapter_status_event_type)

    effective_terms = standardized["effective_term"].map(parse_status_event_term)
    standardized["effective_term_code"] = effective_terms.map(lambda item: item[0])
    standardized["effective_term_label"] = effective_terms.map(lambda item: item[1])
    standardized["effective_term_sort"] = effective_terms.map(lambda item: item[2])

    return_terms = standardized["return_term"].map(parse_status_event_term)
    standardized["return_term_code"] = return_terms.map(lambda item: item[0])
    standardized["return_term_label"] = return_terms.map(lambda item: item[1])
    standardized["return_term_sort"] = return_terms.map(lambda item: item[2])

    standardized["confirmed_for_outcomes"] = [
        "Yes"
        if (
            _truthy(active, default=True)
            and event_type == CHAPTER_KICKED_OUTCOME
            and _confirmed(confidence)
            and effective_sort < 999999
            and bool(chapter_key)
        )
        else "No"
        for active, event_type, confidence, effective_sort, chapter_key in zip(
            standardized["active"],
            standardized["event_type_normalized"],
            standardized["confidence"],
            standardized["effective_term_sort"],
            standardized["chapter_key"],
        )
    ]

    standardized = standardized.loc[
        standardized["chapter_key"].ne("")
        & standardized["event_type_normalized"].ne("")
        & standardized["effective_term_sort"].lt(999999)
    ].copy()
    if standardized.empty:
        return empty_chapter_status_events()

    return standardized[CHAPTER_STATUS_EVENT_INTERNAL_COLUMNS].reset_index(drop=True)


def chapter_status_event_lookup(events: pd.DataFrame | None) -> dict[str, list[Mapping[str, object]]]:
    normalized = normalize_chapter_status_events(events)
    if normalized.empty:
        return {}
    confirmed = normalized.loc[normalized["confirmed_for_outcomes"].eq("Yes")].copy()
    if confirmed.empty:
        return {}
    return {
        key: group.sort_values(["effective_term_sort", "return_term_sort"]).to_dict("records")
        for key, group in confirmed.groupby("chapter_key", dropna=False)
        if key
    }


def chapter_kicked_by_status_event(
    event_lookup: dict[str, list[Mapping[str, object]]] | pd.DataFrame | None,
    chapter: object,
    student_last_roster_sort: object,
    checkpoint_sort: int,
    latest_roster_sort: int,
) -> bool:
    if isinstance(event_lookup, pd.DataFrame) or event_lookup is None:
        lookup = chapter_status_event_lookup(event_lookup)
    else:
        lookup = event_lookup

    chapter_key = normalize_chapter_key(normalize_chapter_name(chapter) or chapter)
    if not chapter_key or chapter_key not in lookup:
        return False

    student_sort = pd.to_numeric(pd.Series([student_last_roster_sort]), errors="coerce").iloc[0]
    if pd.isna(student_sort):
        return False
    student_sort = int(student_sort)
    if student_sort >= 999999:
        return False

    checkpoint = int(checkpoint_sort)
    latest = int(latest_roster_sort)
    for event in lookup[chapter_key]:
        effective_sort = int(event.get("effective_term_sort", 999999) or 999999)
        if effective_sort >= 999999:
            continue
        if latest < effective_sort or checkpoint < effective_sort:
            continue
        if student_sort <= effective_sort:
            return True
    return False
