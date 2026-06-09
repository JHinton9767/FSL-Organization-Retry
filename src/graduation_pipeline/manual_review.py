from __future__ import annotations

import pandas as pd

from .config import MANUAL_CORRECTION_COLUMNS


QUEUE_COLUMNS = [
    "student_id",
    "first_name",
    "last_name",
    "first_fsl_term",
    "first_fsl_term_code",
    "first_chapter",
    "first_council",
    "latest_fsl_term",
    "latest_status_bucket",
    "manual_review_reason",
    *MANUAL_CORRECTION_COLUMNS,
]


def build_manual_review_queue(
    membership: pd.DataFrame,
    evidence: pd.DataFrame,
    invalid_ids: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    evidence_ids = set(evidence["student_id"]) if not evidence.empty else set()
    evidence_flags = evidence.set_index("student_id")["graduation_evidence_flags"].to_dict() if not evidence.empty else {}

    for record in membership.to_dict(orient="records"):
        reasons: list[str] = []
        student_id = record["student_id"]
        flags = str(record.get("membership_flags") or "")
        if flags:
            reasons.append(flags)
        if student_id in evidence_flags and evidence_flags[student_id]:
            reasons.append(evidence_flags[student_id])
        if student_id not in evidence_ids:
            latest = str(record.get("latest_status_bucket") or "")
            if latest.lower() not in {"active", "new member"}:
                reasons.append("no_explicit_graduation_evidence_after_roster_membership")
        if reasons:
            row = {
                "student_id": student_id,
                "first_name": record.get("first_name", ""),
                "last_name": record.get("last_name", ""),
                "first_fsl_term": record.get("first_fsl_term", ""),
                "first_fsl_term_code": record.get("first_fsl_term_code", ""),
                "first_chapter": record.get("first_chapter", ""),
                "first_council": record.get("first_council", ""),
                "latest_fsl_term": record.get("latest_fsl_term", ""),
                "latest_status_bucket": record.get("latest_status_bucket", ""),
                "manual_review_reason": "; ".join(dict.fromkeys(reasons)),
            }
            for column in MANUAL_CORRECTION_COLUMNS:
                row[column] = student_id if column == "banner_id" else ""
            rows.append(row)

    if not evidence.empty:
        membership_ids = set(membership["student_id"]) if not membership.empty else set()
        for student_id in sorted(set(evidence["student_id"]) - membership_ids):
            row = {
                "student_id": student_id,
                "first_name": "",
                "last_name": "",
                "first_fsl_term": "",
                "first_fsl_term_code": "",
                "first_chapter": "",
                "first_council": "",
                "latest_fsl_term": "",
                "latest_status_bucket": "",
                "manual_review_reason": "graduation_evidence_without_roster_membership",
            }
            for column in MANUAL_CORRECTION_COLUMNS:
                row[column] = student_id if column == "banner_id" else ""
            rows.append(row)

    if not invalid_ids.empty:
        for _, invalid in invalid_ids.head(500).iterrows():
            row = {
                "student_id": "",
                "first_name": "",
                "last_name": "",
                "first_fsl_term": "",
                "first_fsl_term_code": "",
                "first_chapter": "",
                "first_council": "",
                "latest_fsl_term": "",
                "latest_status_bucket": "",
                "manual_review_reason": f"invalid_id_in_{invalid.get('source_category')}: {invalid.get('student_id_raw')}",
            }
            for column in MANUAL_CORRECTION_COLUMNS:
                row[column] = ""
            rows.append(row)

    return pd.DataFrame(rows, columns=QUEUE_COLUMNS).sort_values(["first_fsl_term_code", "student_id"]).reset_index(drop=True)

