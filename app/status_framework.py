from __future__ import annotations

import re
from typing import Any, Dict

import pandas as pd

from app.io_utils import normalize_text


ALL_STUDENTS_LABEL = "All Students"
RESOLVED_OUTCOMES_ONLY_LABEL = "Resolved Outcomes Only"

DEFAULT_OUTCOME_RESOLUTION_CONFIG: Dict[str, Any] = {
    "priority_order": [
        "Graduated",
        "Known Non-Graduate Exit",
        "Still Active",
        "Unknown",
        "Other / Unmapped",
    ],
    "group_patterns": {
        "Graduated": [
            r"\bGRADUAT",
            r"\bALUM",
            r"\bDEGREE\b",
        ],
        "Known Non-Graduate Exit": [
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
        "Still Active": [
            r"\bSTILL ACTIVE\b",
            r"\bACTIVE\b",
            r"\bCURRENT\b",
            r"\bMEMBER\b",
            r"\bNEW MEMBER\b",
            r"\bCOUNCIL\b",
        ],
        "Unknown": [
            r"\bUNKNOWN\b",
            r"\bUNRESOLVED\b",
            r"\bPENDING\b",
            r"\bNOT KNOWN\b",
            r"\bMISSING\b",
            r"\bUNMAPPED\b",
            r"\bNO OUTCOME\b",
        ],
        "Other / Unmapped": [],
    },
    "resolved_only_excluded_groups": [
        "Still Active",
        "Unknown",
        "Other / Unmapped",
    ],
}


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
        merged["priority_order"] = list(config["priority_order"])
    for group, patterns in config.get("group_patterns", {}).items():
        merged["group_patterns"][group] = list(patterns)
    if config.get("resolved_only_excluded_groups"):
        merged["resolved_only_excluded_groups"] = list(config["resolved_only_excluded_groups"])
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


def classify_outcome_resolution(
    outcome_value: object,
    roster_value: object,
    active_flag: object,
    config: Dict[str, Any] | None = None,
) -> str:
    merged = _merged_outcome_resolution_config(config)
    outcome_text = normalize_text(outcome_value)
    roster_text = normalize_text(roster_value)
    candidates = [text for text in [outcome_text, roster_text] if text]
    active_is_true = _is_true(active_flag)

    if not candidates and active_is_true:
        return "Still Active"
    if not candidates:
        return "Unknown"

    for group in [item for item in merged["priority_order"] if item not in {"Still Active", "Unknown"}]:
        patterns = merged["group_patterns"].get(group, [])
        if any(_matches_group(candidate, patterns) for candidate in candidates):
            return group

    if active_is_true:
        return "Still Active"

    for group in [item for item in merged["priority_order"] if item in {"Still Active", "Unknown"}]:
        patterns = merged["group_patterns"].get(group, [])
        if any(_matches_group(candidate, patterns) for candidate in candidates):
            return group
    return "Other / Unmapped"


def build_outcome_resolution_fields(frame: pd.DataFrame, config: Dict[str, Any] | None = None) -> pd.DataFrame:
    merged = _merged_outcome_resolution_config(config)
    outcome_series = frame.get("latest_outcome_bucket", pd.Series("", index=frame.index, dtype="object"))
    roster_series = frame.get("latest_roster_status_bucket", pd.Series("", index=frame.index, dtype="object"))
    active_series = frame.get("active_flag", pd.Series(pd.NA, index=frame.index, dtype="object"))

    groups = pd.Series(
        [
            classify_outcome_resolution(outcome_value, roster_value, active_value, merged)
            for outcome_value, roster_value, active_value in zip(outcome_series, roster_series, active_series)
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
            [frame[column].fillna(False).astype("boolean") for column in graduation_columns],
            axis=1,
        ).fillna(False).any(axis=1)
        groups = groups.where(~graduated_mask, "Graduated")

    excluded_groups = set(merged["resolved_only_excluded_groups"])
    included = ~groups.isin(excluded_groups)
    exclusion_reason = groups.where(~included, "")

    return pd.DataFrame(
        {
            "outcome_resolution_group": groups,
            "resolved_outcomes_only_flag": included,
            "resolved_outcome_excluded_flag": ~included,
            "resolved_outcome_exclusion_reason": exclusion_reason,
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
    if "resolved_outcomes_only_flag" not in frame.columns:
        return pd.Series(True, index=frame.index, dtype="bool")
    return frame["resolved_outcomes_only_flag"].fillna(False).astype(bool)


def resolved_outcomes_only_frame(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.loc[resolved_outcomes_only_mask(frame)].copy()


def outcome_population_summary(frame: pd.DataFrame) -> Dict[str, float]:
    total_students = student_count(frame)
    resolved_frame = resolved_outcomes_only_frame(frame)
    resolved_students = student_count(resolved_frame)
    excluded_students = max(total_students - resolved_students, 0)
    excluded_share = (excluded_students / total_students) if total_students else 0.0
    return {
        "all_students": total_students,
        "resolved_students": resolved_students,
        "excluded_students": excluded_students,
        "excluded_share": excluded_share,
    }
