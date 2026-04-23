from __future__ import annotations

import re
from typing import Any, Dict

import pandas as pd

from app.io_utils import normalize_text


ALL_STUDENTS_LABEL = "All Students"
RESOLVED_OUTCOMES_ONLY_LABEL = "Resolved Outcomes Only"
FULL_POPULATION_LABEL = "Full Population"
CUSTOM_FILTERED_LABEL = "Custom filtered population"

GRADUATED_GROUP = "Graduated"
RESOLVED_NON_GRADUATE_GROUP = "Resolved Non-Graduate Exit"
STILL_ACTIVE_GROUP = "Still Active"
TRULY_UNKNOWN_GROUP = "Truly Unknown / Unresolved"
OTHER_UNMAPPED_GROUP = "Other / Unmapped"

GROUP_ALIASES = {
    "known non-graduate exit": RESOLVED_NON_GRADUATE_GROUP,
    "resolved non-graduate exit": RESOLVED_NON_GRADUATE_GROUP,
    "unknown": TRULY_UNKNOWN_GROUP,
    "truly unknown / unresolved": TRULY_UNKNOWN_GROUP,
    "other / unmapped": OTHER_UNMAPPED_GROUP,
    "graduated": GRADUATED_GROUP,
    "still active": STILL_ACTIVE_GROUP,
}

DEFAULT_OUTCOME_RESOLUTION_CONFIG: Dict[str, Any] = {
    "priority_order": [
        GRADUATED_GROUP,
        RESOLVED_NON_GRADUATE_GROUP,
        STILL_ACTIVE_GROUP,
        TRULY_UNKNOWN_GROUP,
        OTHER_UNMAPPED_GROUP,
    ],
    "group_patterns": {
        GRADUATED_GROUP: [
            r"\bGRADUATED\b",
            r"\bGRAD\b",
            r"DEGREE AWARDED",
            r"AWARDED DEGREE",
            r"DEGREE CONFER",
            r"CONFERRED DEGREE",
        ],
        RESOLVED_NON_GRADUATE_GROUP: [
            r"\bINACTIVE\b",
            r"\bLEFT\b",
            r"\bRESIGN",
            r"\bREVOK",
            r"\bSUSPEND",
            r"\bTRANSFER\b",
            r"\bDROP",
            r"\bREMOVE",
            r"\bWITHDRAW",
            r"\bTERMINAT",
            r"\bDISMISS",
            r"\bEXPEL",
        ],
        STILL_ACTIVE_GROUP: [
            r"\bSTILL ACTIVE\b",
            r"\bACTIVE\b",
            r"\bCURRENT\b",
            r"\bMEMBER\b",
            r"\bNEW MEMBER\b",
            r"\bCOUNCIL\b",
            r"\bENROLLED\b",
        ],
        TRULY_UNKNOWN_GROUP: [
            r"\bUNKNOWN\b",
            r"\bUNRESOLVED\b",
            r"\bPENDING\b",
            r"\bNOT KNOWN\b",
            r"\bMISSING\b",
            r"\bUNMAPPED\b",
            r"\bNO OUTCOME\b",
            r"\bNO FURTHER OBSERVATION\b",
            r"\bACTIVE\/UNKNOWN\b",
        ],
        OTHER_UNMAPPED_GROUP: [],
    },
    "resolved_only_excluded_groups": [
        STILL_ACTIVE_GROUP,
        TRULY_UNKNOWN_GROUP,
        OTHER_UNMAPPED_GROUP,
    ],
}

CONFIRMED_GRADUATION_EVIDENCE_PATTERNS = [
    r"graduation list",
    r"academic graduation term",
    r"academic status",
    r"roster status",
    r"snapshot student status",
    r"explicit graduation flag",
    r"transcript explicit graduation",
]

EXPLICIT_GRADUATION_FLAG_COLUMNS = [
    "explicit_graduation_flag",
    "graduation_flag",
    "confirmed_graduation_flag",
    "degree_awarded_flag",
]

EXPLICIT_GRADUATION_TEXT_COLUMNS = [
    "academic_status_raw",
    "org_status_raw",
    "latest_roster_status_bucket",
    "latest_snapshot_student_status",
    "snapshot_student_status",
]


def _canonical_group_name(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    return GROUP_ALIASES.get(text.lower(), text)


def _merged_outcome_resolution_config(config: Dict[str, Any] | None) -> Dict[str, Any]:
    merged = {
        "priority_order": list(DEFAULT_OUTCOME_RESOLUTION_CONFIG["priority_order"]),
        "group_patterns": {
            key: list(value)
            for key, value in DEFAULT_OUTCOME_RESOLUTION_CONFIG["group_patterns"].items()
        },
        "resolved_only_excluded_groups": list(DEFAULT_OUTCOME_RESOLUTION_CONFIG["resolved_only_excluded_groups"]),
    }
    if not config:
        return merged

    if config.get("priority_order"):
        merged["priority_order"] = [_canonical_group_name(value) for value in config["priority_order"]]
    for group, patterns in config.get("group_patterns", {}).items():
        merged["group_patterns"][_canonical_group_name(group)] = list(patterns)
    if config.get("resolved_only_excluded_groups"):
        merged["resolved_only_excluded_groups"] = [_canonical_group_name(value) for value in config["resolved_only_excluded_groups"]]
    return merged


def _matches_group(text: str, patterns: list[str]) -> bool:
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def _is_true(value: object) -> bool:
    if isinstance(value, bool):
        return value
    try:
        return bool(pd.notna(value) and str(value).strip().lower() in {"true", "1", "yes"})
    except Exception:
        return False


def _bool_like_series(series: pd.Series) -> pd.Series:
    return pd.Series([_is_true(value) for value in series], index=series.index, dtype="boolean")


def _non_blank_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index, dtype="bool")
    cleaned = frame[column].fillna("").astype(str).str.strip()
    return cleaned.ne("") & ~cleaned.str.lower().isin({"nan", "none", "nat", "<na>"})


def _explicit_graduation_text_mask(series: pd.Series) -> pd.Series:
    cleaned = series.fillna("").astype(str).str.upper()
    if cleaned.empty:
        return pd.Series(False, index=series.index, dtype="bool")
    disallowed = (
        cleaned.str.contains(r"DEGREE SEEK", regex=True, na=False)
        | cleaned.str.contains(r"SEEKING DEGREE", regex=True, na=False)
        | cleaned.str.contains(r"NON-DEGREE", regex=True, na=False)
        | cleaned.str.contains(r"NON DEGREE", regex=True, na=False)
    )
    explicit = (
        cleaned.str.contains(r"\bGRADUATED\b", regex=True, na=False)
        | cleaned.str.contains(r"\bGRAD\b", regex=True, na=False)
        | cleaned.str.contains(r"DEGREE AWARDED", regex=True, na=False)
        | cleaned.str.contains(r"AWARDED DEGREE", regex=True, na=False)
        | cleaned.str.contains(r"DEGREE CONFER", regex=True, na=False)
        | cleaned.str.contains(r"CONFERRED DEGREE", regex=True, na=False)
    )
    return (explicit & ~disallowed).fillna(False).astype(bool)


def confirmed_graduation_evidence_mask(frame: pd.DataFrame) -> pd.Series:
    """Return rows with direct evidence that graduation actually occurred."""
    evidence = pd.Series(False, index=frame.index, dtype="bool")

    if "graduation_evidence_confirmed" in frame.columns:
        evidence = evidence | _bool_like_series(frame["graduation_evidence_confirmed"]).fillna(False).astype(bool)

    for column in EXPLICIT_GRADUATION_FLAG_COLUMNS:
        if column in frame.columns:
            evidence = evidence | _bool_like_series(frame[column]).fillna(False).astype(bool)

    for column in ["graduation_term_code", "graduation_term", "graduation_year"]:
        evidence = evidence | _non_blank_series(frame, column)

    if "outcome_evidence_source" in frame.columns:
        source_text = frame["outcome_evidence_source"].fillna("").astype(str)
        for pattern in CONFIRMED_GRADUATION_EVIDENCE_PATTERNS:
            evidence = evidence | source_text.str.contains(pattern, case=False, regex=True, na=False)

    for column in EXPLICIT_GRADUATION_TEXT_COLUMNS:
        if column in frame.columns:
            evidence = evidence | _explicit_graduation_text_mask(frame[column])

    return evidence.fillna(False).astype(bool)


def graduation_claim_mask(frame: pd.DataFrame, config: Dict[str, Any] | None = None) -> pd.Series:
    """Return rows that appear to claim graduation before evidence gating."""
    merged = _merged_outcome_resolution_config(config)
    patterns = merged["group_patterns"].get(GRADUATED_GROUP, [])
    claim = pd.Series(False, index=frame.index, dtype="bool")
    for column in ["latest_outcome_bucket", "latest_roster_status_bucket"]:
        if column in frame.columns:
            text = frame[column].fillna("").astype(str)
            claim = claim | text.str.strip().str.lower().eq("graduated")
            for pattern in patterns:
                claim = claim | text.str.contains(pattern, case=False, regex=True, na=False)
    for column in ["graduated_eventual", "graduated_4yr", "graduated_6yr"]:
        if column in frame.columns:
            claim = claim | _bool_like_series(frame[column]).fillna(False).astype(bool)
    return claim.fillna(False).astype(bool)


def classify_outcome_resolution(
    outcome_value: object,
    roster_value: object,
    active_flag: object,
    graduation_evidence: object = False,
    config: Dict[str, Any] | None = None,
) -> str:
    merged = _merged_outcome_resolution_config(config)
    outcome_text = normalize_text(outcome_value)
    roster_text = normalize_text(roster_value)
    candidates = [text for text in [outcome_text, roster_text] if text]
    active_is_true = _is_true(active_flag)

    if not candidates and active_is_true:
        return STILL_ACTIVE_GROUP
    if not candidates:
        return TRULY_UNKNOWN_GROUP

    has_graduation_evidence = _is_true(graduation_evidence)
    for group in [item for item in merged["priority_order"] if item not in {STILL_ACTIVE_GROUP, TRULY_UNKNOWN_GROUP, OTHER_UNMAPPED_GROUP}]:
        patterns = merged["group_patterns"].get(group, [])
        if group == GRADUATED_GROUP and not has_graduation_evidence:
            continue
        if any(_matches_group(candidate, patterns) for candidate in candidates):
            return group

    if active_is_true or _matches_group(roster_text, merged["group_patterns"].get(STILL_ACTIVE_GROUP, [])):
        return STILL_ACTIVE_GROUP

    if any(_matches_group(candidate, merged["group_patterns"].get(TRULY_UNKNOWN_GROUP, [])) for candidate in candidates):
        return TRULY_UNKNOWN_GROUP

    if any(_matches_group(candidate, merged["group_patterns"].get(STILL_ACTIVE_GROUP, [])) for candidate in candidates):
        return TRULY_UNKNOWN_GROUP

    return OTHER_UNMAPPED_GROUP


def build_outcome_resolution_fields(frame: pd.DataFrame, config: Dict[str, Any] | None = None) -> pd.DataFrame:
    merged = _merged_outcome_resolution_config(config)
    outcome_series = frame.get("latest_outcome_bucket", pd.Series("", index=frame.index, dtype="object"))
    roster_series = frame.get("latest_roster_status_bucket", pd.Series("", index=frame.index, dtype="object"))
    active_series = frame.get("active_flag", pd.Series(pd.NA, index=frame.index, dtype="object"))
    active_hints = _bool_like_series(active_series)
    graduation_evidence = confirmed_graduation_evidence_mask(frame)
    graduation_claim = graduation_claim_mask(frame, merged)
    if "outcome_evidence_source" in frame.columns:
        active_hints = active_hints | frame["outcome_evidence_source"].fillna("").astype(str).str.contains("current or active signal only", case=False, na=False)
    if "latest_snapshot_student_status" in frame.columns:
        active_hints = active_hints | frame["latest_snapshot_student_status"].fillna("").astype(str).str.contains(r"active|current|enrolled", case=False, na=False)

    groups = pd.Series(
        [
            classify_outcome_resolution(outcome_value, roster_value, active_value, grad_evidence, merged)
            for outcome_value, roster_value, active_value, grad_evidence in zip(outcome_series, roster_series, active_hints, graduation_evidence)
        ],
        index=frame.index,
        dtype="object",
    )
    graduation_columns = [
        column
        for column in ["graduated_eventual", "graduated_4yr", "graduated_6yr"]
        if column in frame.columns
    ]
    if graduation_columns:
        graduated_mask = pd.concat(
            [_bool_like_series(frame[column]) for column in graduation_columns],
            axis=1,
        ).fillna(False).any(axis=1) & graduation_evidence
        groups = groups.where(~graduated_mask, GRADUATED_GROUP)
    groups = groups.where(~(groups.eq(GRADUATED_GROUP) & ~graduation_evidence), TRULY_UNKNOWN_GROUP)

    excluded_groups = set(merged["resolved_only_excluded_groups"])
    included = ~groups.isin(excluded_groups)
    exclusion_reason = groups.where(~included, "")
    graduated = groups.eq(GRADUATED_GROUP)
    known_non_graduate = groups.eq(RESOLVED_NON_GRADUATE_GROUP)
    active = groups.eq(STILL_ACTIVE_GROUP)
    unknown = groups.eq(TRULY_UNKNOWN_GROUP)

    return pd.DataFrame(
        {
            "outcome_resolution_group": groups,
            "is_resolved_outcome": included,
            "is_active_outcome": active,
            "is_unknown_outcome": unknown,
            "is_graduated": graduated,
            "is_known_non_graduate_exit": known_non_graduate,
            "resolved_outcomes_only_flag": included,
            "resolved_outcome_excluded_flag": ~included,
            "resolved_outcome_exclusion_reason": exclusion_reason,
            "graduation_evidence_confirmed": graduation_evidence,
            "graduation_status_without_evidence": graduation_claim & ~graduation_evidence,
        },
        index=frame.index,
    )


def student_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    if "student_id" not in frame.columns:
        return int(len(frame))
    return int(
        frame["student_id"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .nunique()
    )


def resolved_outcomes_only_mask(frame: pd.DataFrame) -> pd.Series:
    if "is_resolved_outcome" in frame.columns:
        return _bool_like_series(frame["is_resolved_outcome"]).fillna(False).astype(bool)
    if "resolved_outcomes_only_flag" not in frame.columns:
        return pd.Series(True, index=frame.index, dtype="bool")
    return _bool_like_series(frame["resolved_outcomes_only_flag"]).fillna(False).astype(bool)


def resolved_outcomes_only_frame(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.loc[resolved_outcomes_only_mask(frame)].copy()


def outcome_population_summary(frame: pd.DataFrame) -> Dict[str, float]:
    total_students = student_count(frame)
    resolved_students = student_count(frame.loc[_bool_like_series(frame.get("is_resolved_outcome", frame.get("resolved_outcomes_only_flag", pd.Series(False, index=frame.index))))])
    graduated_students = student_count(frame.loc[_bool_like_series(frame.get("is_graduated", pd.Series(False, index=frame.index)))])
    known_non_graduate_students = student_count(frame.loc[_bool_like_series(frame.get("is_known_non_graduate_exit", pd.Series(False, index=frame.index)))])
    still_active_students = student_count(frame.loc[_bool_like_series(frame.get("is_active_outcome", pd.Series(False, index=frame.index)))])
    unknown_students = student_count(frame.loc[_bool_like_series(frame.get("is_unknown_outcome", pd.Series(False, index=frame.index)))])
    other_students = max(total_students - resolved_students - still_active_students - unknown_students, 0)
    excluded_students = max(total_students - resolved_students, 0)
    excluded_share = (excluded_students / total_students) if total_students else 0.0
    return {
        "all_students": total_students,
        "resolved_students": resolved_students,
        "graduated_students": graduated_students,
        "known_non_graduate_exit_students": known_non_graduate_students,
        "still_active_students": still_active_students,
        "unknown_students": unknown_students,
        "other_unmapped_students": other_students,
        "excluded_students": excluded_students,
        "excluded_share": excluded_share,
    }
