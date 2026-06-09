from __future__ import annotations

import pandas as pd

from .normalize import unique_joined


EVIDENCE_COLUMNS = [
    "student_id",
    "graduation_evidence_found",
    "graduation_term",
    "graduation_term_code",
    "graduation_term_sort",
    "graduation_source_priority",
    "graduation_source_category",
    "graduation_evidence_detail",
    "graduation_evidence_sources",
    "graduation_evidence_flags",
]

SOURCE_PRIORITY = {"roster": 1, "transcript": 2, "graduation": 3, "academic": 4}


def build_graduation_evidence(normalized: pd.DataFrame) -> pd.DataFrame:
    evidence = normalized.loc[normalized["explicit_graduation_evidence"].astype(bool)].copy()
    if evidence.empty:
        return pd.DataFrame(columns=EVIDENCE_COLUMNS)
    evidence["source_priority"] = evidence["source_category"].map(SOURCE_PRIORITY).fillna(99).astype(int)
    evidence["term_sort_for_order"] = pd.to_numeric(evidence["term_sort"], errors="coerce").fillna(999999)
    rows: list[dict[str, object]] = []
    for student_id, group in evidence.groupby("student_id", sort=True):
        ordered = group.sort_values(["source_priority", "term_sort_for_order", "source_file", "row_number"])
        chosen = ordered.iloc[0]
        flags: list[str] = []
        term_count = ordered["term_code"].dropna().astype(str).loc[lambda series: series.ne("")].nunique()
        if term_count > 1:
            flags.append("conflicting_graduation_terms")
        source_count = ordered["source_category"].dropna().astype(str).nunique()
        if source_count > 1:
            flags.append("multiple_graduation_sources")
        rows.append(
            {
                "student_id": student_id,
                "graduation_evidence_found": True,
                "graduation_term": chosen.get("term_label", ""),
                "graduation_term_code": chosen.get("term_code", ""),
                "graduation_term_sort": None if pd.isna(chosen.get("term_sort")) else chosen.get("term_sort"),
                "graduation_source_priority": chosen.get("source_priority", ""),
                "graduation_source_category": chosen.get("source_category", ""),
                "graduation_evidence_detail": chosen.get("evidence_detail", ""),
                "graduation_evidence_sources": unique_joined(ordered["source_file"]),
                "graduation_evidence_flags": "; ".join(flags),
            }
        )
    return pd.DataFrame(rows, columns=EVIDENCE_COLUMNS)

