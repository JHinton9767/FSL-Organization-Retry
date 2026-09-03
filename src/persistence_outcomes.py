from __future__ import annotations

from typing import Dict, Iterable

import pandas as pd

from src.chapter_status_events import chapter_kicked_by_status_event, chapter_status_event_lookup
from src.shared_utils import normalize_chapter_key


CHAPTER_KICKED_OUTCOME = "Chapter Kicked"
CHAPTER_KICKED_MIN_CHECKPOINT_GAP = 10
CHAPTER_KICKED_MIN_RETURN_GAP = 20

PERSISTENCE_OUTCOME_ORDER = [
    "Active",
    "Early Alumni",
    "Inactive/Suspended",
    "Dropped/Resigned",
    "Revoked",
    "Transfer",
    CHAPTER_KICKED_OUTCOME,
    "Unknown",
    "Graduated",
]

_ACTIVE_STATUSES = {
    "A",
    "ACTIVE",
    "N",
    "NEW",
    "NEW MEMBER",
    "STILL ACTIVE",
    "CURRENTLY ACTIVE",
    "STILL ACTIVE / CURRENTLY ACTIVE",
    "STILL ACTIVE/CURRENTLY ACTIVE",
}


def persistence_outcome_from_status(value: object) -> str:
    text = str(value or "").strip()
    upper = text.upper()
    compact = "".join(character for character in upper if character.isalnum())
    if not upper:
        return "Unknown"
    if upper in _ACTIVE_STATUSES or ("STILL ACTIVE" in upper and "CURRENTLY ACTIVE" in upper):
        return "Active"
    if upper in {"AL", "ALUMNI", "EARLY ALUMNI"} or "EARLY ALUM" in upper:
        return "Early Alumni"
    if compact in {"I", "INACTIVE", "S", "SUSPEND", "SUSPENDED", "IS", "INACTIVESUSPEND", "INACTIVESUSPENDED"}:
        return "Inactive/Suspended"
    if upper in {"RS", "RESIGNED"} or "RESIGN" in upper:
        return "Dropped/Resigned"
    if upper in {"RV", "REVOKED"} or "REVOK" in upper:
        return "Revoked"
    if upper in {"D", "DROPPED"} or "DROP" in upper or "WITHDRAW" in upper:
        return "Dropped/Resigned"
    if upper in {"T", "TRANSFER", "TRANSFERRED", "TRANSFERRED / LEFT INSTITUTION"} or "TRANSFER" in upper:
        return "Transfer"
    if upper in {"CK", "CHAPTER KICKED", "KICKED"} or ("CHAPTER" in upper and "KICK" in upper):
        return CHAPTER_KICKED_OUTCOME
    if upper in {"G", "GRAD", "GRADUATED", "GRADUATED CONFIRMED"}:
        return "Graduated"
    if upper == "INACTIVE / RESIGNED / SUSPENDED / REVOKED":
        return "Inactive/Suspended"
    if any(token in upper for token in ["UNKNOWN", "UNRESOLVED", "DISAPPEAR", "SOURCE PROBLEM", "ROSTER PROBLEM"]):
        return "Unknown"
    return "Unknown"


def _sorted_term_values(values: Iterable[object]) -> list[int]:
    terms = pd.to_numeric(pd.Series(list(values), dtype="object"), errors="coerce")
    return sorted({int(value) for value in terms.dropna().tolist() if int(value) < 999999})


def chapter_kicked_at_checkpoint(
    chapter_roster_terms: Dict[str, list[int]],
    chapter_key: str,
    student_last_roster_sort: object,
    checkpoint_sort: int,
    latest_roster_sort: int,
) -> bool:
    """Return True when roster coverage indicates a chapter-level disappearance."""
    key = normalize_chapter_key(chapter_key)
    if not key:
        return False
    student_sort = pd.to_numeric(pd.Series([student_last_roster_sort]), errors="coerce").iloc[0]
    if pd.isna(student_sort):
        return False
    student_sort = int(student_sort)
    if student_sort >= 999999 or int(checkpoint_sort) <= student_sort or int(latest_roster_sort) <= student_sort:
        return False

    terms = chapter_roster_terms.get(key, [])
    if not terms:
        return False
    later_terms = [int(term) for term in terms if int(term) > student_sort]
    if not later_terms:
        return True

    next_term = min(later_terms)
    if next_term > int(checkpoint_sort):
        return (int(checkpoint_sort) - student_sort) >= CHAPTER_KICKED_MIN_CHECKPOINT_GAP
    return (next_term - student_sort) >= CHAPTER_KICKED_MIN_RETURN_GAP


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
    chapter_status_events: pd.DataFrame | None = None,
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
    chapter_event_lookup = chapter_status_event_lookup(chapter_status_events)

    presence_start = int(presence_start_sort if presence_start_sort is not None else checkpoint_sort)
    student_latest_roster_sort_at_checkpoint: Dict[str, int] = {}
    chapter_roster_terms: Dict[str, list[int]] = {}
    roster_source = longitudinal.copy()
    if not roster_source.empty and "student_id" in roster_source.columns:
        roster_source["student_id"] = roster_source["student_id"].fillna("").astype(str).str.strip()
        if "observed_term_sort" in roster_source.columns:
            roster_source["_persistence_term_sort"] = pd.to_numeric(roster_source["observed_term_sort"], errors="coerce")
        else:
            roster_source["_persistence_term_sort"] = pd.to_numeric(roster_source.get("term_sort"), errors="coerce")
        roster_present = (
            roster_source.get("roster_present", pd.Series(False, index=roster_source.index))
            .fillna(False)
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"true", "1", "yes", "y"})
        )
        all_roster = roster_source.loc[
            roster_present
            & roster_source["_persistence_term_sort"].notna()
            & roster_source["_persistence_term_sort"].le(latest_roster_sort)
        ].copy()
        if "chapter" in all_roster.columns:
            all_roster["_chapter_key"] = all_roster["chapter"].map(normalize_chapter_key)
            chapter_roster_terms = {
                str(chapter_key): _sorted_term_values(group["_persistence_term_sort"].tolist())
                for chapter_key, group in all_roster.loc[all_roster["_chapter_key"].ne("")]
                .groupby("_chapter_key", dropna=False)
                if str(chapter_key).strip()
            }

        roster = all_roster.loc[all_roster["student_id"].isin(student_ids)].copy()
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
                student_latest_roster_sort_at_checkpoint = {
                    student_id: int(term_sort)
                    for student_id, term_sort in latest_rows["_persistence_term_sort"].dropna().items()
                    if int(term_sort) < 999999
                }
                status_column = next(
                    (column for column in ["org_status_bucket", "org_status_raw"] if column in latest_rows.columns),
                    "",
                )
                if status_column:
                    roster_categories = latest_rows[status_column].map(persistence_outcome_from_status)
                    roster_overrides = roster_categories.loc[~roster_categories.eq("Active")]
                    matching_ids = outcomes.index.intersection(roster_overrides.index)
                    outcomes.loc[matching_ids] = roster_overrides.reindex(matching_ids)
                    if "chapter" in latest_rows.columns:
                        for student_id, row in latest_rows.iterrows():
                            status_outcome = roster_categories.get(student_id, "Unknown")
                            if status_outcome not in {"Active", "Unknown"}:
                                continue
                            if chapter_kicked_by_status_event(
                                chapter_event_lookup,
                                row.get("chapter", ""),
                                row.get("_persistence_term_sort", 999999),
                                checkpoint_sort,
                                latest_roster_sort,
                            ) or chapter_kicked_at_checkpoint(
                                chapter_roster_terms,
                                row.get("chapter", ""),
                                row.get("_persistence_term_sort", 999999),
                                checkpoint_sort,
                                latest_roster_sort,
                            ):
                                outcomes.loc[student_id] = CHAPTER_KICKED_OUTCOME

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
        manual_rows["manual_sort"] = manual_sort.loc[manual_rows.index]
        if student_latest_roster_sort_at_checkpoint:
            latest_roster_sort = manual_rows["student_id"].map(student_latest_roster_sort_at_checkpoint)
            later_roster_after_chapter_kick = (
                manual_rows["manual_category"].eq(CHAPTER_KICKED_OUTCOME)
                & latest_roster_sort.notna()
                & latest_roster_sort.gt(manual_rows["manual_sort"])
            )
            manual_rows = manual_rows.loc[~later_roster_after_chapter_kick].copy()
        manual_categories = manual_rows.drop_duplicates(subset=["student_id"], keep="last").set_index("student_id")[
            "manual_category"
        ]
        matching_ids = outcomes.index.intersection(manual_categories.index)
        outcomes.loc[matching_ids] = manual_categories.reindex(matching_ids)

    counts = outcomes.value_counts()
    return {status: int(counts.get(status, 0)) for status in PERSISTENCE_OUTCOME_ORDER}
