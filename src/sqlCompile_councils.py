from __future__ import annotations

from typing import Sequence

import pandas as pd

from src.build_master_roster import normalize_chapter_name
from src.sqlCompile_cohort import (
    _chapter_disappearance_events, _normalize_chapter_key, _prepared_roster_inventory,
    _semester_sort, normalize_status_code,
)
from src.sqlCompile_reporting import through_reporting_cutoff


# Owner-supplied council assignments, including historical organizations in this list.
COUNCIL_ORGANIZATIONS = {
    "IFC": (
        "Alpha Sigma Phi", "Delta Sigma Phi", "Delta Tau Delta", "Kappa Alpha Order",
        "Kappa Delta Rho", "Kappa Sigma", "Lambda Chi Alpha", "Phi Delta Theta",
        "Phi Gamma Delta", "Phi Kappa Sigma", "Phi Kappa Tau", "Pi Kappa Alpha",
        "Pi Kappa Phi", "Sigma Alpha Epsilon", "Sigma Chi", "Sigma Nu",
        "Sigma Phi Epsilon", "Theta Chi", "Alpha Epsilon Pi", "Alpha Tau Omega",
        "Kappa Alpha", "Phi Kappa Psi", "Sigma Tau Gamma", "Beta Upsilon Chi",
    ),
    "MGC": (
        "Alpha Sigma Rho Sorority, Inc.", "Kappa Delta Chi, Sorority, Inc.",
        "Lambda Delta Psi Sorority, Inc.", "Omega Delta Phi Fraternity, Inc.",
        "Sigma Delta Lambda Sorority, Inc.", "Sigma Lambda Beta Fraternity, Inc.",
        "Sigma Lambda Gamma Sorority, Inc.", "Alpha Psi Lambda", "Delta Xi Nu",
        "Phi Iota Alpha", "Sigma Iota Alpha", "Omega Phi Gamma",
    ),
    "NPHC": (
        "Alpha Phi Alpha Fraternity, Inc.", "Alpha Kappa Alpha Sorority, Inc.",
        "Kappa Alpha Psi Fraternity, Inc.", "Omega Psi Phi Fraternity, Inc.",
        "Delta Sigma Theta Sorority, Inc.", "Phi Beta Sigma Fraternity, Inc.",
        "Zeta Phi Beta Sorority, Inc.", "Sigma Gamma Rho Sorority, Inc.",
        "Iota Phi Theta Fraternity, Inc.",
    ),
    "PHC": (
        "Alpha Delta Pi", "Alpha Gamma Delta", "Alpha Xi Delta", "Chi Omega",
        "Delta Gamma", "Delta Zeta", "Gamma Phi Beta", "Zeta Tau Alpha",
    ),
    "Other": ("Order of Omega", "Phi Delta Delta"),
}
COUNCILS = tuple(COUNCIL_ORGANIZATIONS)
UNMAPPED_COUNCIL = "Unmapped"
CHAPTER_COUNCILS = {
    normalize_chapter_name(chapter).casefold(): council
    for council, chapters in COUNCIL_ORGANIZATIONS.items() for chapter in chapters
}
ORGANIZATION_REVIEW_COLUMNS = [
    "Organization", "Observed Names", "First Roster Semester", "Last Roster Semester",
    "Roster Students", "Chapter Kicked Evidence", "Evidence Semesters", "Evidence Notes",
    "Records After Cutoff",
]


def council_for_chapter(chapter: object) -> str:
    return CHAPTER_COUNCILS.get(normalize_chapter_name(chapter).casefold(), UNMAPPED_COUNCIL)


def chapters_for_councils(chapters: Sequence[str], councils: Sequence[str] | None) -> list[str]:
    if councils is None:
        return list(chapters)
    selected = set(councils)
    return [chapter for chapter in chapters if council_for_chapter(chapter) in selected]


def _review_key(chapter: object) -> str:
    normalized = normalize_chapter_name(chapter)
    return normalized if normalized not in {"", "Unknown"} else str(chapter or "").strip() or "(Missing chapter)"


def build_unmapped_organization_review(
    compiled: pd.DataFrame, inventory: pd.DataFrame, manual: pd.DataFrame,
    zero_member_periods: pd.DataFrame, reporting_cutoff: str,
) -> pd.DataFrame:
    # Discovery includes later initial rosters; removal evidence respects the reporting cutoff.
    manual = manual.loc[manual["Chapter"].fillna("").astype(str).str.strip().ne("")].copy()
    observations = pd.concat([
        frame.reindex(columns=["Semester", "Chapter"]) for frame in (compiled, inventory, manual)
    ], ignore_index=True).fillna("")
    observations = observations.loc[observations["Chapter"].map(council_for_chapter).eq(UNMAPPED_COUNCIL)].copy()
    if observations.empty:
        return pd.DataFrame(columns=ORGANIZATION_REVIEW_COLUMNS)
    observations["_organization"] = observations["Chapter"].map(_review_key)
    roster = pd.concat([
        compiled.reindex(columns=["Semester", "Chapter", "Student ID"]),
        inventory.reindex(columns=["Semester", "Chapter", "Student ID"]),
    ], ignore_index=True).fillna("")
    roster["_organization"] = roster["Chapter"].map(_review_key)
    evidence = pd.concat([
        through_reporting_cutoff(frame, reporting_cutoff).reindex(columns=["Semester", "Chapter", "Status"])
        for frame in (compiled, manual)
    ], ignore_index=True).fillna("")
    evidence = evidence.loc[evidence["Status"].map(normalize_status_code).eq("CK")].copy()
    evidence["_organization"] = evidence["Chapter"].map(_review_key)
    events = _chapter_disappearance_events(
        _prepared_roster_inventory(through_reporting_cutoff(inventory, reporting_cutoff)), zero_member_periods,
    )
    rows = []
    for organization, observed in observations.groupby("_organization", sort=True):
        chapter_rosters = roster.loc[roster["_organization"].eq(organization)]
        terms = sorted({term for term in chapter_rosters["Semester"] if _semester_sort(term) < 999999}, key=_semester_sort)
        recorded = evidence.loc[evidence["_organization"].eq(organization), "Semester"].tolist()
        inferred = events.get(_normalize_chapter_key(organization), [])
        evidence_terms = sorted(set(recorded + [event["disappearance_semester"] for event in inferred]), key=_semester_sort)
        evidence_types = [label for label, present in [("Recorded CK status", recorded), ("Inferred roster disappearance", inferred)] if present]
        rows.append({
            "Organization": organization,
            "Observed Names": " | ".join(sorted(set(observed["Chapter"].astype(str)) - {""})),
            "First Roster Semester": terms[0] if terms else "",
            "Last Roster Semester": terms[-1] if terms else "",
            "Roster Students": int(chapter_rosters["Student ID"].replace("", pd.NA).nunique()),
            "Chapter Kicked Evidence": "; ".join(evidence_types) or "None through cutoff",
            "Evidence Semesters": ", ".join(evidence_terms),
            "Evidence Notes": " | ".join(dict.fromkeys(event["reason"] for event in inferred)),
            "Records After Cutoff": "Yes" if observed["Semester"].map(_semester_sort).between(_semester_sort(reporting_cutoff) + 1, 999998).any() else "No",
        })
    return pd.DataFrame(rows, columns=ORGANIZATION_REVIEW_COLUMNS).sort_values(
        "Chapter Kicked Evidence", key=lambda column: column.eq("None through cutoff"), kind="stable",
    ).reset_index(drop=True)
