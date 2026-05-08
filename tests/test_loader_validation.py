from zipfile import ZipFile

import pandas as pd
import pytest

from app.config_loader import (
    build_manual_corrections_package,
    ensure_manual_transcript_files,
    load_manual_roster_corrections,
    prepare_manual_corrections_workspace,
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
    ]


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
            "manual_roster_corrections.csv",
        ]
