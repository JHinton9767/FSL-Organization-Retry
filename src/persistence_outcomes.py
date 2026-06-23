from __future__ import annotations

from typing import Dict

import pandas as pd


PERSISTENCE_OUTCOME_ORDER = [
    "Active",
    "Early Alumni",
    "Inactive/Suspended",
    "Resigned",
    "Revoked",
    "Dropped",
    "Transfer",
    "Unknown",
    "Graduated",
]

_ACTIVE_STATUSES = {"A", "ACTIVE", "N", "NEW", "NEW MEMBER", "STILL ACTIVE", "CURRENTLY ACTIVE"}


def persistence_outcome_from_status(value: object) -> str:
    text = str(value or "").strip()
    upper = text.upper()
    if not upper:
        return "Unknown"
    if upper in _ACTIVE_STATUSES:
        return "Active"
    if upper in {"AL", "ALUMNI", "EARLY ALUMNI"} or "EARLY ALUM" in upper:
        return "Early Alumni"
    if upper in {"I", "INACTIVE", "S", "SUSPENDED", "INACTIVE/SUSPENDED"}:
        return "Inactive/Suspended"
    if upper in {"RS", "RESIGNED"} or "RESIGN" in upper:
        return "Resigned"
    if upper in {"RV", "REVOKED"} or "REVOK" in upper:
        return "Revoked"
    if upper in {"D", "DROPPED"} or "DROP" in upper or "WITHDRAW" in upper:
        return "Dropped"
    if upper in {"T", "TRANSFER", "TRANSFERRED", "TRANSFERRED / LEFT INSTITUTION"} or "TRANSFER" in upper:
        return "Transfer"
    if upper in {"G", "GRAD", "GRADUATED", "GRADUATED CONFIRMED"}:
        return "Graduated"
    if upper == "INACTIVE / RESIGNED / SUSPENDED / REVOKED":
        return "Inactive/Suspended"
    if any(token in upper for token in ["UNKNOWN", "UNRESOLVED", "DISAPPEAR", "SOURCE PROBLEM", "ROSTER PROBLEM"]):
        return "Unknown"
    return "Unknown"


def checkpoint_outcome_counts(
    cohort: pd.DataFrame,
    longitudinal: pd.DataFrame,
    checkpoint_sort: int,
    latest_roster_sort: int,
    *,
    presence_start_sort: int | None = None,
    baseline: bool = False,
    graduation_sort_column: str = "_graduation_sort",
    manual_status_column: str = "manual_outcome_status",
    manual_sort_column: str = "_manual_outcome_sort",
) -> Dict[str, int]:
    student_ids = (
        cohort.get("student_id", pd.Series(dtype="object"))
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .drop_duplicates()
        .tolist()
    )
    outcomes = pd.Series("Active" if baseline else "Unknown", index=student_ids, dtype="object")
    if not student_ids:
        return {status: 0 for status in PERSISTENCE_OUTCOME_ORDER}

    presence_start = int(presence_start_sort if presence_start_sort is not None else checkpoint_sort)
    roster = longitudinal.copy()
    if not roster.empty and "student_id" in roster.columns:
        roster["student_id"] = roster["student_id"].fillna("").astype(str).str.strip()
        if "observed_term_sort" in roster.columns:
            roster["_persistence_term_sort"] = pd.to_numeric(roster["observed_term_sort"], errors="coerce")
        else:
            roster["_persistence_term_sort"] = pd.to_numeric(roster.get("term_sort"), errors="coerce")
        roster_present = (
            roster.get("roster_present", pd.Series(False, index=roster.index))
            .fillna(False)
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"true", "1", "yes", "y"})
        )
        roster = roster.loc[
            roster_present
            & roster["student_id"].isin(student_ids)
            & roster["_persistence_term_sort"].notna()
            & roster["_persistence_term_sort"].le(latest_roster_sort)
        ].copy()

        if not roster.empty:
            future_ids = set(
                roster.loc[roster["_persistence_term_sort"].ge(presence_start), "student_id"].tolist()
            )
            if not baseline:
                outcomes.loc[outcomes.index.isin(future_ids)] = "Active"

            known_at_checkpoint = roster.loc[roster["_persistence_term_sort"].le(checkpoint_sort)].copy()
            if not known_at_checkpoint.empty:
                latest_rows = (
                    known_at_checkpoint.sort_values(["student_id", "_persistence_term_sort"])
                    .drop_duplicates(subset=["student_id"], keep="last")
                    .set_index("student_id")
                )
                status_column = next(
                    (column for column in ["org_status_bucket", "org_status_raw"] if column in latest_rows.columns),
                    "",
                )
                if status_column:
                    roster_categories = latest_rows[status_column].map(persistence_outcome_from_status)
                    roster_overrides = roster_categories.loc[~roster_categories.eq("Active")]
                    matching_ids = outcomes.index.intersection(roster_overrides.index)
                    outcomes.loc[matching_ids] = roster_overrides.reindex(matching_ids)

    graduation_sort = pd.to_numeric(
        cohort.get(graduation_sort_column, pd.Series(999999, index=cohort.index)),
        errors="coerce",
    ).fillna(999999)
    graduated_ids = set(
        cohort.loc[graduation_sort.le(checkpoint_sort), "student_id"]
        .fillna("")
        .astype(str)
        .str.strip()
        .tolist()
    )
    outcomes.loc[outcomes.index.isin(graduated_ids)] = "Graduated"

    manual_status = cohort.get(manual_status_column, pd.Series("", index=cohort.index)).fillna("").astype(str).str.strip()
    manual_sort = pd.to_numeric(
        cohort.get(manual_sort_column, pd.Series(999999, index=cohort.index)),
        errors="coerce",
    ).fillna(999999)
    manual_rows = cohort.loc[manual_status.ne("") & manual_sort.le(checkpoint_sort), ["student_id"]].copy()
    if not manual_rows.empty:
        manual_rows["manual_category"] = manual_status.loc[manual_rows.index].map(persistence_outcome_from_status)
        manual_categories = manual_rows.drop_duplicates(subset=["student_id"], keep="last").set_index("student_id")[
            "manual_category"
        ]
        matching_ids = outcomes.index.intersection(manual_categories.index)
        outcomes.loc[matching_ids] = manual_categories.reindex(matching_ids)

    counts = outcomes.value_counts()
    return {status: int(counts.get(status, 0)) for status in PERSISTENCE_OUTCOME_ORDER}
