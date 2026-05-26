from zipfile import ZipFile

import pandas as pd
import pytest

from app.config_loader import (
    build_manual_corrections_package,
    find_manual_correction_conflicts,
    import_manual_corrections_package,
    ensure_manual_transcript_files,
    load_manual_adjustments,
    load_manual_roster_corrections,
    load_manual_review_queue,
    save_manual_adjustments,
    normalize_manual_roster_corrections,
    prepare_manual_corrections_workspace,
    save_manual_review_queue,
    save_manual_roster_corrections,
)
from app.data_loader import _validate_loaded_tables


def test_canonical_loader_validation_accepts_required_tables() -> None:
    warnings = _validate_loaded_tables(
        "canonical",
        {
            "student_summary": pd.DataFrame({"student_id": ["1"]}),
            "master_longitudinal": pd.DataFrame({"student_id": ["1"], "term_code": ["2024FA"]}),
            "cohort_metrics": pd.DataFrame(
                {
                    "Metric Group": ["Graduation"],
                    "Metric Label": ["Observed Eventual Graduation Rate"],
                    "Cohort": ["Overall"],
                }
            ),
            "qa_checks": pd.DataFrame({"Check Group": ["Schema"], "Check": ["Authoritative tables built"], "Status": ["Pass"]}),
        },
    )
    assert warnings == []


def test_canonical_loader_rejects_noncanonical_dataset_types() -> None:
    with pytest.raises(ValueError, match="Unsupported dataset type"):
        _validate_loaded_tables("processed", {})


def test_manual_roster_corrections_default_student_join_term(tmp_path) -> None:
    path = tmp_path / "manual_roster_corrections.csv"
    corrections = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "last_name": ["Doe"],
            "first_name": ["Jane"],
            "student_join_term": [""],
            "organization_join_term": ["Spring 2026"],
            "organization_name": ["Alpha Sigma Phi"],
            "leaving_organization_term": [""],
            "final_status_term": ["Fall 2026"],
            "final_status": ["Inactive"],
        }
    )

    save_manual_roster_corrections(corrections, path)
    loaded = load_manual_roster_corrections(path)

    assert loaded.loc[0, "student_join_term"] == "Spring 2026"
    assert list(loaded.columns) == [
        "student_id",
        "last_name",
        "first_name",
        "student_join_term",
        "organization_join_term",
        "organization_name",
        "leaving_organization_term",
        "final_status_term",
        "final_status",
        "exclude_from_roster_calculations",
    ]


def test_manual_roster_corrections_accept_exclusion_action(tmp_path) -> None:
    path = tmp_path / "manual_roster_corrections.csv"
    corrections = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "last_name": ["Doe"],
            "first_name": ["Jane"],
            "student_join_term": [""],
            "organization_join_term": ["Spring 2026"],
            "organization_name": ["Alpha Sigma Phi"],
            "leaving_organization_term": [""],
            "final_status_term": [""],
            "final_status": [""],
            "exclude_from_roster_calculations": ["Yes"],
        }
    )

    save_manual_roster_corrections(corrections, path)
    loaded = load_manual_roster_corrections(path)

    assert len(loaded) == 1
    assert loaded.loc[0, "exclude_from_roster_calculations"] == "Yes"


def test_manual_roster_correction_normalizer_removes_deleted_rows() -> None:
    corrections = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002"],
            "last_name": ["Doe", "Smith"],
            "first_name": ["Jane", "John"],
            "organization_join_term": ["Spring 2026", "Spring 2026"],
            "organization_name": ["Alpha Sigma Phi", "Lambda Chi Alpha"],
            "final_status": ["Unknown", "Inactive"],
            "delete_row": ["x", ""],
        }
    )

    normalized = normalize_manual_roster_corrections(corrections)

    assert len(normalized) == 1
    assert normalized.loc[0, "student_id"] == "A00000002"
    assert normalized.loc[0, "student_join_term"] == "Spring 2026"


def test_manual_roster_corrections_create_transcript_template(tmp_path) -> None:
    corrections = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "last_name": ["Doe"],
            "first_name": ["Jane"],
            "student_join_term": [""],
            "organization_join_term": ["Spring 2026"],
            "organization_name": ["Alpha Sigma Phi"],
            "leaving_organization_term": ["Spring 2026"],
            "final_status_term": ["Fall 2026"],
            "final_status": ["Inactive"],
        }
    )

    created = ensure_manual_transcript_files(corrections, tmp_path / "Transcripts")

    assert len(created) == 1
    assert created[0].name == "A00000001_Doe_Jane.txt"
    text = created[0].read_text(encoding="utf-8")
    assert "Organization Join Term: Spring 2026" in text
    assert "--- TRANSCRIPT TEXT ---" in text


def test_manual_workspace_and_package_are_helper_ready(tmp_path) -> None:
    corrections_path = tmp_path / "config" / "manual_roster_corrections.csv"
    transcript_folder = tmp_path / "transcript_text" / "Transcripts"

    workspace = prepare_manual_corrections_workspace(corrections_path, transcript_folder)
    (transcript_folder / "A00000001_Doe_Jane.txt").write_text("Spring 2026\nCredits: 3\n", encoding="utf-8")
    package_bytes = build_manual_corrections_package(corrections_path, transcript_folder)

    assert workspace["corrections_path"].exists()
    assert workspace["transcript_folder"].exists()
    package_path = tmp_path / "manual_package.zip"
    package_path.write_bytes(package_bytes)
    with ZipFile(package_path) as archive:
        assert sorted(archive.namelist()) == [
            "Transcripts/A00000001_Doe_Jane.txt",
            "manual_adjustments.csv",
            "manual_review_queue.csv",
            "manual_roster_corrections.csv",
        ]


def test_manual_correction_conflict_detection() -> None:
    corrections = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000001"],
            "last_name": ["Doe", "Doe"],
            "first_name": ["Jane", "Jane"],
            "student_join_term": ["Spring 2026", "Spring 2026"],
            "organization_join_term": ["Spring 2026", "Spring 2026"],
            "organization_name": ["Alpha Sigma Phi", "Alpha Sigma Phi"],
            "leaving_organization_term": ["Spring 2026", "Spring 2026"],
            "final_status_term": ["Fall 2026", "Fall 2026"],
            "final_status": ["Inactive", "Graduated"],
        }
    )

    conflicts = find_manual_correction_conflicts(corrections)

    assert len(conflicts) == 1
    assert conflicts.loc[0, "student_id"] == "A00000001"


def test_import_manual_package_merges_corrections_and_transcripts(tmp_path, monkeypatch) -> None:
    corrections_path = tmp_path / "config" / "manual_roster_corrections.csv"
    adjustments_path = tmp_path / "config" / "manual_adjustments.csv"
    review_path = tmp_path / "config" / "manual_review_queue.csv"
    transcript_folder = tmp_path / "transcript_text" / "Transcripts"
    monkeypatch.setattr("app.config_loader.MANUAL_ROSTER_CORRECTIONS_PATH", corrections_path)
    monkeypatch.setattr("app.config_loader.MANUAL_ADJUSTMENTS_PATH", adjustments_path)
    monkeypatch.setattr("app.config_loader.MANUAL_REVIEW_QUEUE_PATH", review_path)
    monkeypatch.setattr("app.config_loader.MANUAL_TRANSCRIPTS_PATH", transcript_folder)

    save_manual_roster_corrections(
        pd.DataFrame(
            {
                "student_id": ["A00000001"],
                "last_name": ["Doe"],
                "first_name": ["Jane"],
                "student_join_term": ["Spring 2026"],
                "organization_join_term": ["Spring 2026"],
                "organization_name": ["Alpha Sigma Phi"],
                "leaving_organization_term": ["Spring 2026"],
                "final_status_term": ["Fall 2026"],
                "final_status": ["Inactive"],
            }
        ),
        corrections_path,
    )
    save_manual_review_queue(pd.DataFrame({"review_key": ["A00000001"], "review_status": ["Corrected"]}), review_path)
    save_manual_adjustments(
        pd.DataFrame(
            {
                "student_id": ["A00000001"],
                "normalized_student_id": ["A00000001"],
                "field_to_override": ["final_outcome_bucket"],
                "adjusted_value": ["Inactive"],
            }
        ),
        adjustments_path,
    )
    transcript_folder.mkdir(parents=True, exist_ok=True)
    (transcript_folder / "A00000001_Doe_Jane.txt").write_text("Spring 2026\nCredits: 3\n", encoding="utf-8")
    package_bytes = build_manual_corrections_package(corrections_path, transcript_folder, review_path)

    result = import_manual_corrections_package(package_bytes)

    assert result["incoming_rows"] == 1
    assert result["merged_rows"] == 1
    assert result["transcript_skipped"] == 1
    assert len(load_manual_roster_corrections(corrections_path)) == 1
    assert len(load_manual_adjustments(adjustments_path)) == 1
    assert len(load_manual_review_queue(review_path)) == 1
