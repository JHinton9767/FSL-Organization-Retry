from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.graduation_pipeline.apply_corrections import load_manual_corrections
from src.graduation_pipeline.config import GraduationPipelineConfig
from src.graduation_pipeline.graduation_evidence import build_graduation_evidence
from src.graduation_pipeline.graduation_rates import build_final_outcomes, build_rates
from src.graduation_pipeline.load_required_fields import load_required_fields
from src.graduation_pipeline.manual_review import build_manual_review_queue
from src.graduation_pipeline.membership_summary import build_membership_summary
from src.graduation_pipeline.normalize import normalize_required_fields
from src.graduation_pipeline.run_pipeline import run
from src.graduation_pipeline.source_inventory import build_source_manifest


def _raw(records: list[dict[str, object]]) -> pd.DataFrame:
    defaults = {
        "source_category": "roster",
        "source_file": "test.csv",
        "source_sheet": "",
        "row_number": 1,
        "student_id_raw": "",
        "student_id": "",
        "first_name": "",
        "last_name": "",
        "term_raw": "",
        "term_code": "",
        "term_label": "",
        "term_sort": "",
        "chapter_raw": "",
        "chapter": "",
        "council": "",
        "status_raw": "",
        "status_bucket": "",
        "graduation_text_raw": "",
        "explicit_graduation_evidence": False,
        "evidence_detail": "",
    }
    return pd.DataFrame([{**defaults, **record} for record in records])


def test_invalid_banner_ids_are_excluded_before_tracking() -> None:
    normalized, invalid = normalize_required_fields(
        _raw(
            [
                {"student_id_raw": "A01234567", "term_raw": "Fall 2019", "status_raw": "Active"},
                {"student_id_raw": "abc123", "term_raw": "Fall 2019", "status_raw": "Active"},
                {"student_id_raw": "", "term_raw": "Fall 2019", "status_raw": "Active"},
            ]
        )
    )

    assert normalized["student_id"].tolist() == ["A01234567"]
    assert len(invalid) == 2


def test_no_disappearance_graduation_inference() -> None:
    normalized, invalid = normalize_required_fields(
        _raw([{"student_id_raw": "A01234567", "term_raw": "Fall 2019", "chapter_raw": "Alpha Beta", "status_raw": "Inactive"}])
    )
    membership = build_membership_summary(normalized)
    evidence = build_graduation_evidence(normalized)
    queue = build_manual_review_queue(membership, evidence, invalid)
    final, _, _ = build_final_outcomes(membership, evidence, queue, pd.DataFrame())

    assert evidence.empty
    assert final.loc[0, "graduation_status"] == "Not Graduated"
    assert "no_explicit_graduation_evidence" in queue.loc[0, "manual_review_reason"]


def test_roster_graduation_is_highest_priority() -> None:
    normalized, _ = normalize_required_fields(
        _raw(
            [
                {
                    "source_category": "roster",
                    "student_id_raw": "A01234567",
                    "term_raw": "Fall 2022",
                    "chapter_raw": "Alpha Beta",
                    "status_raw": "G",
                },
                {
                    "source_category": "transcript",
                    "student_id_raw": "A01234567",
                    "term_raw": "Fall 2023",
                    "graduation_text_raw": "Degree awarded",
                    "evidence_detail": "transcript graduation evidence",
                },
                {
                    "source_category": "graduation",
                    "student_id_raw": "A01234567",
                    "term_raw": "Fall 2021",
                    "status_raw": "Graduated",
                    "graduation_text_raw": "Graduated",
                },
            ]
        )
    )
    evidence = build_graduation_evidence(normalized)

    assert evidence.loc[0, "graduation_source_category"] == "roster"
    assert evidence.loc[0, "graduation_term_code"] == "2022FA"


def test_manual_correction_applies_to_final_outcomes_and_rates() -> None:
    normalized, invalid = normalize_required_fields(
        _raw([{"student_id_raw": "A01234567", "term_raw": "Fall 2019", "chapter_raw": "Alpha Beta", "status_raw": "Inactive"}])
    )
    membership = build_membership_summary(normalized)
    evidence = build_graduation_evidence(normalized)
    queue = build_manual_review_queue(membership, evidence, invalid)
    corrections = pd.DataFrame(
        [
            {
                "banner_id": "A01234567",
                "student_id": "A01234567",
                "corrected_graduation_status": "Graduated",
                "corrected_graduation_term": "Fall 2023",
                "corrected_graduation_term_code": "2023FA",
                "corrected_first_fsl_term": "",
                "corrected_first_fsl_term_code": "",
                "corrected_chapter": "",
                "corrected_council": "",
                "correction_reason": "confirmed in registrar file",
                "reviewer_initials": "JH",
                "reviewed_date": "2026-06-09",
                "notes": "",
                "active": "yes",
            }
        ]
    )
    final, applied, audit = build_final_outcomes(membership, evidence, queue, corrections)
    rates = build_rates(final, ["cohort_term_code", "cohort_term"])

    assert final.loc[0, "graduation_status"] == "Graduated"
    assert final.loc[0, "outcome_source"] == "manual_correction"
    assert len(applied) == 1
    assert audit.loc[0, "before_status"] == "Not Graduated"
    assert rates.loc[0, "confirmed_graduates"] == 1
    assert rates.loc[0, "graduated_within_4yr"] == 1


def test_load_manual_corrections_drops_blank_rows(tmp_path: Path) -> None:
    path = tmp_path / "manual_corrections.csv"
    pd.DataFrame(
        [
            {"banner_id": "", "corrected_graduation_status": "", "active": ""},
            {"banner_id": "not-an-id", "corrected_graduation_status": "Graduated", "active": "yes"},
            {"banner_id": "A01234567", "corrected_graduation_status": "Graduated", "active": "yes"},
        ]
    ).to_csv(path, index=False)

    corrections = load_manual_corrections(path)

    assert corrections["student_id"].tolist() == ["A01234567"]


def test_runner_writes_focused_outputs(tmp_path: Path) -> None:
    rosters = tmp_path / "rosters"
    graduation = tmp_path / "graduation"
    transcripts = tmp_path / "transcripts"
    academic = tmp_path / "academic"
    output = tmp_path / "output" / "graduation"
    cache = tmp_path / "cache"
    manual = tmp_path / "data" / "manual" / "manual_corrections.csv"
    rosters.mkdir()
    graduation.mkdir()
    transcripts.mkdir()
    academic.mkdir()
    pd.DataFrame(
        [
            {"Student ID": "A01234567", "First Name": "Alex", "Last Name": "One", "Term": "Fall 2019", "Chapter": "Alpha Beta", "Status": "Active"},
            {"Student ID": "bad-id", "First Name": "Bad", "Last Name": "Id", "Term": "Fall 2019", "Chapter": "Alpha Beta", "Status": "Active"},
        ]
    ).to_csv(rosters / "fall_2019_roster.csv", index=False)
    pd.DataFrame(
        [{"Student ID": "A01234567", "Graduation Term": "Fall 2023", "Graduation Status": "Graduated"}]
    ).to_csv(graduation / "grads.csv", index=False)

    config = GraduationPipelineConfig(
        config_path=tmp_path / "local_paths.yaml",
        rosters_root=rosters,
        graduation_root=graduation,
        transcript_text_root=transcripts,
        academic_root=academic,
        output_root=output,
        cache_root=cache,
        manual_corrections_path=manual,
    )

    outputs = run(config, refresh_cache=True)
    final = pd.read_csv(outputs["final_student_outcomes"], dtype=str, keep_default_na=False)
    rates = pd.read_csv(outputs["graduation_rates_by_cohort"], dtype=str, keep_default_na=False)
    invalid = pd.read_csv(outputs["invalid_ids"], dtype=str, keep_default_na=False)

    assert final["student_id"].tolist() == ["A01234567"]
    assert final.loc[0, "graduation_status"] == "Graduated"
    assert rates.loc[0, "confirmed_graduates"] == "1"
    assert invalid["student_id_raw"].tolist() == ["bad-id"]
    assert manual.exists()
