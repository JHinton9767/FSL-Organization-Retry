from __future__ import annotations

import pandas as pd

from src.shared_utils import clean_text

from .apply_corrections import latest_correction_by_student
from .normalize import normalize_term


FINAL_COLUMNS = [
    "student_id",
    "first_name",
    "last_name",
    "cohort_term",
    "cohort_term_code",
    "cohort_term_sort",
    "chapter",
    "council",
    "latest_fsl_term",
    "latest_status_bucket",
    "graduation_status",
    "graduation_term",
    "graduation_term_code",
    "graduation_term_sort",
    "outcome_source",
    "manual_review_required",
    "manual_review_reason",
]


def _auto_status(row: pd.Series) -> str:
    if bool(row.get("graduation_evidence_found", False)):
        return "Graduated"
    latest = clean_text(row.get("latest_status_bucket")).lower()
    if latest in {"active", "new member"}:
        return "Still Active"
    if latest in {"inactive", "resigned", "suspended", "revoked", "transfer"}:
        return "Not Graduated"
    return "Unknown"


def build_final_outcomes(
    membership: pd.DataFrame,
    evidence: pd.DataFrame,
    manual_queue: pd.DataFrame,
    corrections: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if membership.empty:
        empty = pd.DataFrame(columns=FINAL_COLUMNS)
        return empty, pd.DataFrame(), pd.DataFrame()

    evidence_cols = [
        "student_id",
        "graduation_evidence_found",
        "graduation_term",
        "graduation_term_code",
        "graduation_term_sort",
        "graduation_source_category",
    ]
    merged = membership.merge(evidence[evidence_cols] if not evidence.empty else pd.DataFrame(columns=evidence_cols), on="student_id", how="left")
    merged["graduation_evidence_found"] = merged["graduation_evidence_found"].fillna(False)
    merged["graduation_status"] = merged.apply(_auto_status, axis=1)
    merged["outcome_source"] = merged.apply(
        lambda row: f"automated_{row['graduation_source_category']}" if row["graduation_status"] == "Graduated" else "automated_roster_membership",
        axis=1,
    )
    queue_reasons = manual_queue.loc[manual_queue["student_id"].ne(""), ["student_id", "manual_review_reason"]]
    merged = merged.merge(queue_reasons.drop_duplicates("student_id"), on="student_id", how="left")
    merged["manual_review_required"] = merged["manual_review_reason"].fillna("").ne("")

    applied_rows: list[dict[str, object]] = []
    audit_rows: list[dict[str, object]] = []
    latest = latest_correction_by_student(corrections)
    if not latest.empty:
        correction_map = latest.set_index("student_id").to_dict(orient="index")
        for idx, row in merged.iterrows():
            student_id = row["student_id"]
            correction = correction_map.get(student_id)
            if not correction:
                continue
            before = row.to_dict()
            status = clean_text(correction.get("corrected_graduation_status"))
            if status:
                merged.at[idx, "graduation_status"] = status
            grad_term = clean_text(correction.get("corrected_graduation_term"))
            if grad_term:
                term_code, term_label, term_sort = normalize_term(grad_term)
                merged.at[idx, "graduation_term"] = term_label or grad_term
                merged.at[idx, "graduation_term_code"] = term_code
                merged.at[idx, "graduation_term_sort"] = term_sort
            first_term = clean_text(correction.get("corrected_first_fsl_term"))
            if first_term:
                term_code, term_label, term_sort = normalize_term(first_term)
                merged.at[idx, "first_fsl_term"] = term_label or first_term
                merged.at[idx, "first_fsl_term_code"] = term_code
                merged.at[idx, "first_fsl_term_sort"] = term_sort
            chapter = clean_text(correction.get("corrected_chapter"))
            if chapter:
                merged.at[idx, "first_chapter"] = chapter
            council = clean_text(correction.get("corrected_council"))
            if council:
                merged.at[idx, "first_council"] = council
            merged.at[idx, "outcome_source"] = "manual_correction"
            merged.at[idx, "manual_review_required"] = False
            applied_rows.append(correction)
            audit_rows.append(
                {
                    "student_id": student_id,
                    "before_status": before.get("graduation_status", ""),
                    "after_status": merged.at[idx, "graduation_status"],
                    "correction_reason": correction.get("correction_reason", ""),
                    "reviewer_initials": correction.get("reviewer_initials", ""),
                    "reviewed_date": correction.get("reviewed_date", ""),
                }
            )

    final = pd.DataFrame(
        {
            "student_id": merged["student_id"],
            "first_name": merged["first_name"],
            "last_name": merged["last_name"],
            "cohort_term": merged["first_fsl_term"],
            "cohort_term_code": merged["first_fsl_term_code"],
            "cohort_term_sort": merged["first_fsl_term_sort"],
            "chapter": merged["first_chapter"],
            "council": merged["first_council"],
            "latest_fsl_term": merged["latest_fsl_term"],
            "latest_status_bucket": merged["latest_status_bucket"],
            "graduation_status": merged["graduation_status"],
            "graduation_term": merged["graduation_term"].fillna(""),
            "graduation_term_code": merged["graduation_term_code"].fillna(""),
            "graduation_term_sort": merged["graduation_term_sort"],
            "outcome_source": merged["outcome_source"],
            "manual_review_required": merged["manual_review_required"],
            "manual_review_reason": merged["manual_review_reason"].fillna(""),
        }
    )
    return final[FINAL_COLUMNS], pd.DataFrame(applied_rows), pd.DataFrame(audit_rows)


def add_graduation_windows(final: pd.DataFrame) -> pd.DataFrame:
    result = final.copy()
    cohort_sort = pd.to_numeric(result["cohort_term_sort"], errors="coerce")
    grad_sort = pd.to_numeric(result["graduation_term_sort"], errors="coerce")
    elapsed_semesters = grad_sort - cohort_sort
    result["graduated_confirmed"] = result["graduation_status"].map(lambda value: clean_text(value).lower() == "graduated")
    for years in [4, 5, 6]:
        result[f"graduated_within_{years}yr"] = result["graduated_confirmed"] & elapsed_semesters.le(years * 10)
    result["resolved_outcome"] = result["graduation_status"].map(lambda value: clean_text(value).lower() in {"graduated", "not graduated"})
    return result


def build_rates(final: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    if final.empty:
        return pd.DataFrame()
    data = add_graduation_windows(final)
    rows: list[dict[str, object]] = []
    for keys, group in data.groupby(group_columns, dropna=False, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        total = len(group)
        graduates = int(group["graduated_confirmed"].sum())
        unknown = int((~group["resolved_outcome"]).sum())
        resolved = int(group["resolved_outcome"].sum())
        row = {column: value for column, value in zip(group_columns, keys)}
        row.update(
            {
                "cohort_size": total,
                "confirmed_graduates": graduates,
                "unknown_or_manual_review": unknown,
                "resolved_count": resolved,
                "graduation_rate_conservative": graduates / total if total else 0,
                "graduation_rate_resolved_only": graduates / resolved if resolved else None,
                "graduated_within_4yr": int(group["graduated_within_4yr"].sum()),
                "graduated_within_5yr": int(group["graduated_within_5yr"].sum()),
                "graduated_within_6yr": int(group["graduated_within_6yr"].sum()),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def build_qa_summary(final: pd.DataFrame, invalid_ids: pd.DataFrame, manual_queue: pd.DataFrame, corrections: pd.DataFrame) -> pd.DataFrame:
    data = add_graduation_windows(final) if not final.empty else final
    rows = [
        {"check": "tracked_students", "value": len(final), "notes": "Valid Banner IDs with roster membership"},
        {"check": "invalid_id_rows_excluded", "value": len(invalid_ids), "notes": "Rows excluded before calculation"},
        {"check": "manual_review_queue_rows", "value": len(manual_queue), "notes": "Rows needing human review"},
        {"check": "manual_corrections_loaded", "value": len(corrections), "notes": "Nonblank valid Banner ID corrections"},
    ]
    if not data.empty:
        rows.extend(
            [
                {"check": "confirmed_graduates", "value": int(data["graduated_confirmed"].sum()), "notes": "Explicit evidence only"},
                {"check": "unknown_or_manual_review", "value": int((~data["resolved_outcome"]).sum()), "notes": "Not counted as graduates"},
            ]
        )
    return pd.DataFrame(rows)

