from __future__ import annotations

import pandas as pd

from .normalize import unique_joined


MEMBERSHIP_COLUMNS = [
    "student_id",
    "first_name",
    "last_name",
    "first_fsl_term",
    "first_fsl_term_code",
    "first_fsl_term_sort",
    "first_chapter",
    "first_council",
    "latest_fsl_term",
    "latest_fsl_term_code",
    "latest_status_bucket",
    "chapters_seen",
    "councils_seen",
    "roster_terms_seen",
    "membership_flags",
]


def build_membership_summary(normalized: pd.DataFrame) -> pd.DataFrame:
    roster = normalized.loc[normalized["source_category"].eq("roster")].copy()
    if roster.empty:
        return pd.DataFrame(columns=MEMBERSHIP_COLUMNS)

    roster["term_sort_for_order"] = pd.to_numeric(roster["term_sort"], errors="coerce").fillna(999999)
    rows: list[dict[str, object]] = []
    for student_id, group in roster.groupby("student_id", sort=True):
        ordered = group.sort_values(["term_sort_for_order", "source_file", "row_number"])
        first = ordered.iloc[0]
        latest = ordered.iloc[-1]
        first_sort = first.get("term_sort_for_order")
        first_term_rows = ordered.loc[ordered["term_sort_for_order"].eq(first_sort)]
        flags: list[str] = []
        if first_term_rows["chapter"].dropna().astype(str).nunique() > 1:
            flags.append("multiple_chapters_in_first_fsl_term")
        if ordered["term_code"].dropna().astype(str).eq("").all():
            flags.append("missing_roster_terms")
        rows.append(
            {
                "student_id": student_id,
                "first_name": unique_joined(group["first_name"]),
                "last_name": unique_joined(group["last_name"]),
                "first_fsl_term": first.get("term_label", ""),
                "first_fsl_term_code": first.get("term_code", ""),
                "first_fsl_term_sort": None if pd.isna(first.get("term_sort")) else first.get("term_sort"),
                "first_chapter": first.get("chapter", ""),
                "first_council": first.get("council", ""),
                "latest_fsl_term": latest.get("term_label", ""),
                "latest_fsl_term_code": latest.get("term_code", ""),
                "latest_status_bucket": latest.get("status_bucket", ""),
                "chapters_seen": unique_joined(group["chapter"]),
                "councils_seen": unique_joined(group["council"]),
                "roster_terms_seen": unique_joined(group["term_label"]),
                "membership_flags": "; ".join(flags),
            }
        )
    return pd.DataFrame(rows, columns=MEMBERSHIP_COLUMNS)

