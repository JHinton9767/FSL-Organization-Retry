from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest

from app.config_loader import (
    append_graduation_evidence,
    append_manual_adjustments,
    append_manual_review_actions,
    append_manual_roster_corrections,
    append_outcome_overrides,
    append_roster_exclusions,
    build_manual_corrections_package,
    find_manual_correction_conflicts,
    import_manual_corrections_package,
    ensure_manual_transcript_files,
    dedupe_manual_review_queue_by_cohort,
    graduated_alumni_rows_to_manual_corrections,
    load_graduation_evidence,
    load_manual_adjustments,
    load_manual_roster_corrections,
    load_manual_review_actions,
    load_outcome_overrides,
    load_roster_exclusions,
    normalize_manual_adjustments,
    save_manual_adjustments,
    normalize_manual_roster_corrections,
    prepare_manual_corrections_workspace,
    save_manual_review_actions,
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


def test_manual_roster_corrections_use_organization_join_term_only(tmp_path) -> None:
    path = tmp_path / "manual_roster_corrections.csv"
    corrections = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "last_name": ["Doe"],
            "first_name": ["Jane"],
            "organization_join_term": ["Spring 2026"],
            "organization_name": ["Alpha Sigma Phi"],
            "leaving_organization_term": [""],
            "final_status_term": ["Fall 2026"],
            "final_status": ["Inactive"],
        }
    )

    save_manual_roster_corrections(corrections, path)
    loaded = load_manual_roster_corrections(path)

    assert "student_join_term" not in loaded.columns
    assert list(loaded.columns) == [
        "student_id",
        "last_name",
        "first_name",
        "organization_join_term",
        "organization_name",
        "corrected_organization_name",
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


def test_manual_roster_corrections_accept_corrected_organization_alias(tmp_path) -> None:
    path = tmp_path / "manual_roster_corrections.csv"
    corrections = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "organization_name": ["Alpha Sigma Phi"],
            "new chapter": ["Chi Omega"],
        }
    )

    corrections.to_csv(path, index=False)
    loaded = load_manual_roster_corrections(path)

    assert loaded.loc[0, "organization_name"] == "Alpha Sigma Phi"
    assert loaded.loc[0, "corrected_organization_name"] == "Chi Omega"


def test_append_manual_roster_corrections_only_adds_new_rows(tmp_path) -> None:
    path = tmp_path / "manual_roster_corrections.csv"
    first = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "last_name": ["Doe"],
            "first_name": ["Jane"],
            "organization_join_term": ["Spring 2026"],
            "organization_name": ["Alpha Sigma Phi"],
            "final_status_term": ["Fall 2026"],
            "final_status": ["Inactive"],
        }
    )
    second = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002"],
            "last_name": ["Doe", "Smith"],
            "first_name": ["Jane", "John"],
            "organization_join_term": ["Spring 2026", "Spring 2026"],
            "organization_name": ["Alpha Sigma Phi", "Lambda Chi Alpha"],
            "final_status_term": ["Fall 2026", "Fall 2026"],
            "final_status": ["Inactive", "Unknown"],
        }
    )

    first_result = append_manual_roster_corrections(first, path)
    second_result = append_manual_roster_corrections(second, path)
    loaded = load_manual_roster_corrections(path)

    assert first_result["appended_rows"] == 1
    assert second_result["appended_rows"] == 1
    assert second_result["skipped_rows"] == 1
    assert loaded["student_id"].tolist() == ["A00000001", "A00000002"]


def test_append_manual_adjustments_only_adds_new_rows(tmp_path) -> None:
    path = tmp_path / "manual_adjustments.csv"
    first = pd.DataFrame(
        {
            "adjustment_id": ["adj-1"],
            "student_id": ["A00000001"],
            "normalized_student_id": ["A00000001"],
            "field_to_override": ["final_outcome_bucket"],
            "adjusted_value": ["Inactive"],
        }
    )
    second = pd.DataFrame(
        {
            "adjustment_id": ["adj-1", "adj-2"],
            "student_id": ["A00000001", "A00000002"],
            "normalized_student_id": ["A00000001", "A00000002"],
            "field_to_override": ["final_outcome_bucket", "final_outcome_bucket"],
            "adjusted_value": ["Inactive", "Unknown"],
        }
    )

    first_result = append_manual_adjustments(first, path)
    second_result = append_manual_adjustments(second, path)
    loaded = load_manual_adjustments(path)

    assert first_result["appended_rows"] == 1
    assert second_result["appended_rows"] == 1
    assert second_result["skipped_rows"] == 1
    assert loaded["adjustment_id"].tolist() == ["adj-1", "adj-2"]


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
    assert "student_join_term" not in normalized.columns


def test_manual_roster_correction_normalizer_requires_valid_student_id() -> None:
    corrections = pd.DataFrame(
        {
            "student_id": ["", "1234567", "A12345678", "A00000001", "a00000002"],
            "last_name": ["Blank", "Numeric", "Wrong", "Doe", "Smith"],
            "first_name": ["No", "Bad", "Bad", "Jane", "Alex"],
            "organization_join_term": ["Spring 2026"] * 5,
            "organization_name": ["Alpha Sigma Phi"] * 5,
            "final_status": ["Inactive"] * 5,
        }
    )

    normalized = normalize_manual_roster_corrections(corrections)

    assert normalized["student_id"].tolist() == ["A00000001", "A00000002"]


def test_manual_adjustment_normalizer_requires_valid_student_id() -> None:
    adjustments = pd.DataFrame(
        {
            "student_id": ["", "A12345678", "a00000001"],
            "normalized_student_id": ["", "", ""],
            "field_to_override": ["final_outcome_bucket"] * 3,
            "adjusted_value": ["Inactive", "Graduated", "Inactive"],
        }
    )

    normalized = normalize_manual_adjustments(adjustments)

    assert normalized["student_id"].tolist() == ["A00000001"]
    assert normalized["normalized_student_id"].tolist() == ["A00000001"]


def test_manual_roster_corrections_create_transcript_template(tmp_path) -> None:
    corrections = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "last_name": ["Doe"],
            "first_name": ["Jane"],
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
            "graduation_evidence.csv",
            "manual_adjustments.csv",
            "manual_review_actions.csv",
            "manual_review_queue.csv",
            "manual_roster_corrections.csv",
            "outcome_overrides.csv",
            "roster_exclusions.csv",
        ]


def test_manual_correction_conflict_detection() -> None:
    corrections = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000001"],
            "last_name": ["Doe", "Doe"],
            "first_name": ["Jane", "Jane"],
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


def test_manual_review_queue_keeps_one_row_per_student_cohort() -> None:
    queue = pd.DataFrame(
        {
            "review_key": ["issue-1", "issue-2", "issue-3", "invalid"],
            "student_id": ["A00000001", "A00000001", "A00000001", "not-an-id"],
            "student_name": ["Jane Doe", "Jane Doe", "Jane Doe", "Bad Id"],
            "chapter": ["Alpha Sigma Phi", "Alpha Sigma Phi", "Alpha Sigma Phi", "Alpha Sigma Phi"],
            "join_term": ["Spring 2026", "Spring 2026", "Fall 2026", "Spring 2026"],
            "queue_reason": ["No graduation mention", "Conflicting evidence", "No graduation mention", "Bad ID"],
            "review_notes": ["", "Keep this note", "", ""],
            "review_status": ["Needs Review", "In Progress", "Needs Review", "Needs Review"],
        }
    )

    deduped = dedupe_manual_review_queue_by_cohort(queue)

    assert deduped[["student_id", "join_term"]].values.tolist() == [
        ["A00000001", "Spring 2026"],
        ["A00000001", "Fall 2026"],
    ]
    spring_row = deduped.loc[deduped["join_term"].eq("Spring 2026")].iloc[0]
    assert spring_row["review_status"] == "In Progress"
    assert spring_row["queue_reason"] == "Conflicting evidence; No graduation mention"
    assert spring_row["review_notes"] == "Keep this note"


def test_manual_review_actions_store_only_valid_selected_cohorts(tmp_path) -> None:
    actions_path = tmp_path / "config" / "manual_review_actions.csv"

    result = append_manual_review_actions(
        pd.DataFrame(
            {
                "review_key": ["selected-1", "selected-2", "bad"],
                "student_id": ["A00000001", "A00000001", "not-an-id"],
                "join_term": ["Spring 2026", "Spring 2026", "Spring 2026"],
                "review_status": ["Needs Review", "Corrected", "Needs Review"],
                "review_notes": ["", "selected row was actioned", "bad id"],
            }
        ),
        actions_path,
    )

    saved = load_manual_review_actions(actions_path)

    assert result["incoming_rows"] == 1
    assert len(saved) == 1
    assert saved.loc[0, "student_id"] == "A00000001"
    assert saved.loc[0, "review_status"] == "Corrected"
    assert saved.loc[0, "review_notes"] == "selected row was actioned"


def test_manual_review_actions_permission_error_writes_pending_fallback(tmp_path, monkeypatch) -> None:
    actions_path = tmp_path / "config" / "manual_review_actions.csv"
    original_to_csv = pd.DataFrame.to_csv

    def locked_action_file(self, path_or_buf=None, *args, **kwargs):
        if path_or_buf is not None and Path(path_or_buf) == actions_path:
            raise PermissionError("locked")
        return original_to_csv(self, path_or_buf, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_csv", locked_action_file)

    result = append_manual_review_actions(
        pd.DataFrame(
            {
                "review_key": ["selected-locked"],
                "student_id": ["A00000001"],
                "join_term": ["Spring 2026"],
                "review_status": ["Corrected"],
                "review_notes": ["saved while primary file was locked"],
            }
        ),
        actions_path,
    )
    saved = load_manual_review_actions(actions_path)

    assert result["used_fallback"] is True
    assert Path(result["path"]).name.startswith("manual_review_actions.pending_")
    assert len(saved) == 1
    assert saved.loc[0, "review_key"] == "selected-locked"
    assert saved.loc[0, "review_notes"] == "saved while primary file was locked"


def test_manual_review_actions_empty_file_loads_as_empty_queue(tmp_path) -> None:
    actions_path = tmp_path / "config" / "manual_review_actions.csv"
    actions_path.parent.mkdir(parents=True, exist_ok=True)
    actions_path.write_text("", encoding="utf-8")

    loaded = load_manual_review_actions(actions_path)

    assert loaded.empty
    assert list(loaded.columns)


def test_decision_registries_keep_only_valid_banner_ids(tmp_path) -> None:
    graduation_path = tmp_path / "config" / "graduation_evidence.csv"
    outcomes_path = tmp_path / "config" / "outcome_overrides.csv"
    exclusions_path = tmp_path / "config" / "roster_exclusions.csv"

    append_graduation_evidence(
        pd.DataFrame(
            {
                "student_id": ["a00000001", "bad"],
                "organization_name": ["Alpha Sigma Phi", "Alpha Sigma Phi"],
                "graduation_term": ["Spring 2026", "Spring 2026"],
                "evidence_source": ["Roster alumni list", "Roster alumni list"],
            }
        ),
        graduation_path,
    )
    append_outcome_overrides(
        pd.DataFrame(
            {
                "student_id": ["A00000002", "123"],
                "organization_name": ["Beta", "Beta"],
                "final_status": ["Dropped", "Dropped"],
                "final_status_term": ["Fall 2025", "Fall 2025"],
            }
        ),
        outcomes_path,
    )
    append_roster_exclusions(
        pd.DataFrame(
            {
                "student_id": ["A00000003", "not-an-id"],
                "organization_name": ["Gamma", "Gamma"],
                "term": ["Spring 2024", "Spring 2024"],
                "reason": ["Not a student", "Not a student"],
            }
        ),
        exclusions_path,
    )

    assert load_graduation_evidence(graduation_path)["student_id"].tolist() == ["A00000001"]
    assert load_outcome_overrides(outcomes_path)["student_id"].tolist() == ["A00000002"]
    assert load_roster_exclusions(exclusions_path)["student_id"].tolist() == ["A00000003"]


def test_decision_registry_appends_use_stable_keys(tmp_path) -> None:
    graduation_path = tmp_path / "config" / "graduation_evidence.csv"
    row = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "organization_name": ["Alpha Sigma Phi"],
            "graduation_term": ["Spring 2026"],
            "evidence_source": ["Alumni list"],
            "entered_at": ["2026-01-01T00:00:00"],
        }
    )
    changed_timestamp = row.copy()
    changed_timestamp["entered_at"] = "2026-01-02T00:00:00"

    first = append_graduation_evidence(row, graduation_path)
    second = append_graduation_evidence(changed_timestamp, graduation_path)

    assert first["appended_rows"] == 1
    assert second["appended_rows"] == 0
    assert len(load_graduation_evidence(graduation_path)) == 1


def test_graduated_alumni_batch_builds_manual_corrections_with_defaults() -> None:
    alumni = pd.DataFrame(
        {
            "banner_id": ["a00000001", "not-an-id"],
            "last_name": ["Doe", "Bad"],
            "first_name": ["Jane", "Id"],
        }
    )

    corrections = graduated_alumni_rows_to_manual_corrections(
        alumni,
        default_organization="Alpha Sigma Phi",
        default_graduation_term="Spring 2026",
    )

    assert corrections[["student_id", "organization_name", "final_status_term", "final_status"]].values.tolist() == [
        ["A00000001", "Alpha Sigma Phi", "Spring 2026", "Graduated"]
    ]
    assert corrections.loc[0, "leaving_organization_term"] == "Spring 2026"


def test_graduated_alumni_batch_can_fill_from_summary() -> None:
    alumni = pd.DataFrame({"student_id": ["A00000001"], "graduation_term": ["Fall 2025"]})
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "student_name": ["Jane Doe"],
            "join_term": ["Fall 2021"],
            "chapter": ["Alpha Sigma Phi"],
        }
    )

    corrections = graduated_alumni_rows_to_manual_corrections(alumni, summary=summary)

    assert corrections.loc[0, "first_name"] == "Jane"
    assert corrections.loc[0, "last_name"] == "Doe"
    assert corrections.loc[0, "organization_join_term"] == "Fall 2021"
    assert corrections.loc[0, "organization_name"] == "Alpha Sigma Phi"
    assert corrections.loc[0, "final_status_term"] == "Fall 2025"


def test_graduated_alumni_batch_matches_missing_id_by_exact_name_and_chapter() -> None:
    alumni = pd.DataFrame(
        {
            "student_name": ["Jane Doe"],
            "chapter": ["Alpha Sigma Phi"],
            "initiation_date": ["Fall 1999"],
            "graduation_term": ["Spring 2003"],
        }
    )
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "student_name": ["Jane Doe"],
            "join_term": ["Fall 2021"],
            "chapter": ["Alpha Sigma Phi"],
        }
    )

    corrections = graduated_alumni_rows_to_manual_corrections(alumni, summary=summary)

    assert corrections.loc[0, "student_id"] == "A00000001"
    assert corrections.loc[0, "organization_join_term"] == "Fall 1999"
    assert corrections.loc[0, "final_status_term"] == "Spring 2003"
    assert corrections.loc[0, "final_status"] == "Graduated"


def test_graduated_alumni_batch_skips_ambiguous_name_chapter_matches() -> None:
    alumni = pd.DataFrame({"student_name": ["Jane Doe"], "chapter": ["Alpha Sigma Phi"], "graduation_term": ["Spring 2003"]})
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002"],
            "student_name": ["Jane Doe", "Jane Doe"],
            "chapter": ["Alpha Sigma Phi", "Alpha Sigma Phi"],
        }
    )

    corrections = graduated_alumni_rows_to_manual_corrections(alumni, summary=summary)

    assert corrections.empty


def test_import_manual_package_merges_corrections_and_transcripts(tmp_path, monkeypatch) -> None:
    corrections_path = tmp_path / "config" / "manual_roster_corrections.csv"
    adjustments_path = tmp_path / "config" / "manual_adjustments.csv"
    review_path = tmp_path / "config" / "manual_review_queue.csv"
    actions_path = tmp_path / "config" / "manual_review_actions.csv"
    graduation_path = tmp_path / "config" / "graduation_evidence.csv"
    outcomes_path = tmp_path / "config" / "outcome_overrides.csv"
    exclusions_path = tmp_path / "config" / "roster_exclusions.csv"
    transcript_folder = tmp_path / "transcript_text" / "Transcripts"
    monkeypatch.setattr("app.config_loader.MANUAL_ROSTER_CORRECTIONS_PATH", corrections_path)
    monkeypatch.setattr("app.config_loader.MANUAL_ADJUSTMENTS_PATH", adjustments_path)
    monkeypatch.setattr("app.config_loader.MANUAL_REVIEW_QUEUE_PATH", review_path)
    monkeypatch.setattr("app.config_loader.MANUAL_REVIEW_ACTIONS_PATH", actions_path)
    monkeypatch.setattr("app.config_loader.GRADUATION_EVIDENCE_PATH", graduation_path)
    monkeypatch.setattr("app.config_loader.OUTCOME_OVERRIDES_PATH", outcomes_path)
    monkeypatch.setattr("app.config_loader.ROSTER_EXCLUSIONS_PATH", exclusions_path)
    monkeypatch.setattr("app.config_loader.MANUAL_TRANSCRIPTS_PATH", transcript_folder)

    save_manual_roster_corrections(
        pd.DataFrame(
            {
                "student_id": ["A00000001"],
                "last_name": ["Doe"],
                "first_name": ["Jane"],
                "organization_join_term": ["Spring 2026"],
                "organization_name": ["Alpha Sigma Phi"],
                "leaving_organization_term": ["Spring 2026"],
                "final_status_term": ["Fall 2026"],
                "final_status": ["Inactive"],
            }
        ),
        corrections_path,
    )
    save_manual_review_actions(
        pd.DataFrame(
            {
                "review_key": ["A00000001"],
                "student_id": ["A00000001"],
                "join_term": ["Spring 2026"],
                "review_status": ["Corrected"],
            }
        ),
        actions_path,
    )
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
    append_graduation_evidence(
        pd.DataFrame({"student_id": ["A00000001"], "graduation_term": ["Fall 2026"], "evidence_source": ["Alumni list"]}),
        graduation_path,
    )
    append_outcome_overrides(
        pd.DataFrame({"student_id": ["A00000001"], "final_status": ["Inactive"], "final_status_term": ["Fall 2026"]}),
        outcomes_path,
    )
    append_roster_exclusions(
        pd.DataFrame({"student_id": ["A00000001"], "organization_name": ["Alpha Sigma Phi"], "term": ["Spring 2026"]}),
        exclusions_path,
    )
    transcript_folder.mkdir(parents=True, exist_ok=True)
    (transcript_folder / "A00000001_Doe_Jane.txt").write_text("Spring 2026\nCredits: 3\n", encoding="utf-8")
    package_bytes = build_manual_corrections_package(corrections_path, transcript_folder, review_path, actions_path)

    result = import_manual_corrections_package(package_bytes)

    assert result["incoming_rows"] == 1
    assert result["merged_rows"] == 1
    assert result["transcript_skipped"] == 1
    assert len(load_manual_roster_corrections(corrections_path)) == 1
    assert len(load_manual_adjustments(adjustments_path)) == 1
    assert len(load_manual_review_actions(actions_path)) == 1
    assert len(load_graduation_evidence(graduation_path)) == 1
    assert len(load_outcome_overrides(outcomes_path)) == 1
    assert len(load_roster_exclusions(exclusions_path)) == 1
