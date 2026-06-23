import pandas as pd

from src.build_canonical_pipeline import (
    OUTCOME_GRADUATED_CONFIRMED,
    OUTCOME_INACTIVE_EXIT,
    OUTCOME_NOT_RETAINED,
    OUTCOME_STILL_ACTIVE,
    OUTCOME_TRANSFERRED_LEFT,
    build_generated_manual_review_queue,
    build_input_group_outcome_buckets,
    build_student_longitudinal_tracking,
    build_student_source_appearances,
    validate_outcome_tracking,
)


def _summary(student_id: str, current_active: str = "No", current_roster_term_code: str = "") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "student_id": [student_id],
            "student_name": ["Jane Doe"],
            "current_active_flag": [current_active],
            "current_active_roster_term_code": [current_roster_term_code],
            "org_entry_cohort": ["Fall 2020"],
            "join_term": ["Fall 2020"],
            "graduation_evidence_confirmed": ["No"],
            "outcome_evidence_source": [""],
        }
    )


def _roster(student_id: str, status: str = "Active", chapter: str = "Alpha") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "student_id": [student_id],
            "student_id_raw": [student_id],
            "first_name": ["Jane"],
            "last_name": ["Doe"],
            "email": ["jane@example.edu"],
            "source_file": ["roster.xlsx"],
            "source_sheet": ["Alpha"],
            "term_code": ["2020FA"],
            "term_label": ["Fall 2020"],
            "term_year": [2020],
            "chapter": [chapter],
            "org_status_raw": [status],
            "org_status_bucket": [status],
            "chapter_assignment_notes": [""],
        }
    )


def test_graduation_file_match_creates_confirmed_graduate_bucket() -> None:
    roster = _roster("A00000001")
    graduation = pd.DataFrame(
        {
            "Student ID": ["A00000001"],
            "First Name": ["Jane"],
            "Last Name": ["Doe"],
            "Graduation Term": ["Spring 2024"],
            "Outcome": ["Graduated"],
            "Graduation Source File": ["graduates.csv"],
        }
    )

    appearances = build_student_source_appearances(roster, pd.DataFrame(), graduation, pd.DataFrame(), pd.DataFrame())
    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000001"), pd.DataFrame())

    assert tracking.loc[0, "final_outcome_bucket"] == OUTCOME_GRADUATED_CONFIRMED
    assert tracking.loc[0, "explicit_graduation_evidence"] == "Yes"


def test_disappearance_without_graduation_is_not_graduated() -> None:
    appearances = build_student_source_appearances(_roster("A00000002"), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000002"), pd.DataFrame())

    assert tracking.loc[0, "final_outcome_bucket"] == OUTCOME_NOT_RETAINED
    assert tracking.loc[0, "final_outcome_bucket"] != OUTCOME_GRADUATED_CONFIRMED


def test_latest_loaded_roster_students_are_not_marked_not_retained() -> None:
    roster = _roster("A00000009", status="Unknown")
    appearances = build_student_source_appearances(roster, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000009", current_roster_term_code="2020FA"), pd.DataFrame())

    assert tracking.loc[0, "final_outcome_bucket"] == OUTCOME_STILL_ACTIVE
    assert tracking.loc[0, "final_outcome_bucket"] != OUTCOME_NOT_RETAINED


def test_latest_loaded_roster_explicit_exit_still_counts_as_exit() -> None:
    roster = _roster("A00000010", status="Inactive")
    appearances = build_student_source_appearances(roster, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000010", current_roster_term_code="2020FA"), pd.DataFrame())

    assert tracking.loc[0, "final_outcome_bucket"] == OUTCOME_INACTIVE_EXIT


def test_early_alumni_roster_status_counts_as_non_graduate_exit() -> None:
    roster = _roster("A00000011", status="Early Alumni")
    appearances = build_student_source_appearances(roster, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000011", current_roster_term_code="2020FA"), pd.DataFrame())

    assert tracking.loc[0, "explicit_graduation_evidence"] == "No"
    assert tracking.loc[0, "final_outcome_bucket"] == OUTCOME_INACTIVE_EXIT


def test_transcript_graduation_counts_when_roster_has_no_graduation() -> None:
    roster = _roster("A00000007", status="Active")
    transcript_terms = pd.DataFrame(
        {
            "student_id": ["A00000007"],
            "student_id_raw": ["A00000007"],
            "first_name": ["Jane"],
            "last_name": ["Doe"],
            "source_file": ["A00000007_Doe_Jane.txt"],
            "term_code": ["2024SP"],
            "term_label": ["Spring 2024"],
            "term_year": [2024],
            "summary_graduation_term_code": ["2024SP"],
            "summary_graduation_signal_text": [""],
            "summary_academic_standing": ["Good Standing"],
        }
    )

    appearances = build_student_source_appearances(roster, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), transcript_terms)
    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000007"), pd.DataFrame())

    assert tracking.loc[0, "final_outcome_bucket"] == OUTCOME_GRADUATED_CONFIRMED
    assert tracking.loc[0, "explicit_graduation_evidence"] == "Yes"
    assert "Transcript" in tracking.loc[0, "graduation_evidence_source"]


def test_roster_graduation_source_takes_priority_over_transcript() -> None:
    roster = _roster("A00000008", status="G")
    transcript_terms = pd.DataFrame(
        {
            "student_id": ["A00000008"],
            "student_id_raw": ["A00000008"],
            "first_name": ["Jane"],
            "last_name": ["Doe"],
            "source_file": ["A00000008_Doe_Jane.txt"],
            "term_code": ["2025SP"],
            "term_label": ["Spring 2025"],
            "term_year": [2025],
            "summary_graduation_term_code": ["2025SP"],
            "summary_graduation_signal_text": [""],
            "summary_academic_standing": ["Good Standing"],
        }
    )

    appearances = build_student_source_appearances(roster, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), transcript_terms)
    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000008"), pd.DataFrame())

    assert tracking.loc[0, "final_outcome_bucket"] == OUTCOME_GRADUATED_CONFIRMED
    assert tracking.loc[0, "graduation_evidence_source"] == "Roster status"


def test_source_appearances_exclude_rows_without_valid_student_id() -> None:
    roster = pd.concat([_roster("A00000002"), _roster(""), _roster("jdoe123"), _roster("A12345678")], ignore_index=True)

    appearances = build_student_source_appearances(roster, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    assert appearances["normalized_student_id"].tolist() == ["A00000002"]
    assert appearances["student_id"].tolist() == ["A00000002"]


def test_tracking_excludes_invalid_normalized_student_ids() -> None:
    appearances = pd.DataFrame(
        {
            "student_id": ["A00000002", "jdoe123", "A12345678"],
            "normalized_student_id": ["A00000002", "jdoe123", ""],
            "source_type": ["roster", "roster", "roster"],
            "term_code": ["2020FA", "2020FA", "2020FA"],
            "term": ["Fall 2020", "Fall 2020", "Fall 2020"],
            "source_file": ["roster.xlsx", "roster.xlsx", "roster.xlsx"],
            "source_sheet": ["Alpha", "Alpha", "Alpha"],
            "organization": ["Alpha", "Alpha", "Alpha"],
            "chapter": ["Alpha", "Alpha", "Alpha"],
            "raw_status": ["Active", "Active", "Active"],
            "normalized_status": ["Active", "Active", "Active"],
            "name_raw": ["Jane Doe", "Bad Netid", "Bad Banner"],
            "email_raw": ["", "", ""],
            "banner_id_raw": ["A00000002", "jdoe123", "A12345678"],
            "input_group_id": ["1", "2", "3"],
        }
    )

    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000002"), pd.DataFrame())

    assert tracking["normalized_student_id"].tolist() == ["A00000002"]


def test_latest_active_summary_signal_creates_still_active_bucket() -> None:
    appearances = build_student_source_appearances(_roster("A00000003"), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000003", current_active="Yes"), pd.DataFrame())

    assert tracking.loc[0, "final_outcome_bucket"] == OUTCOME_STILL_ACTIVE


def test_multiple_chapters_same_term_enters_manual_review_queue() -> None:
    roster = pd.concat([_roster("A00000004", chapter="Alpha"), _roster("A00000004", chapter="Beta")], ignore_index=True)
    appearances = build_student_source_appearances(roster, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000004"), pd.DataFrame())
    buckets = build_input_group_outcome_buckets(appearances, tracking)
    queue = build_generated_manual_review_queue(appearances, tracking, buckets)

    assert "multiple_chapters_same_term" in queue["issue_type"].tolist()


def test_manual_adjustment_overrides_automated_outcome_and_validates() -> None:
    appearances = build_student_source_appearances(_roster("A00000005"), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    manual = pd.DataFrame(
        {
            "adjustment_id": ["manual-grad"],
            "student_id": ["A00000005"],
            "normalized_student_id": ["A00000005"],
            "field_to_override": ["final_outcome_bucket"],
            "adjusted_value": ["Graduated Confirmed"],
            "active": ["Yes"],
        }
    )
    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000005"), manual)
    buckets = build_input_group_outcome_buckets(appearances, tracking)
    _, failures = validate_outcome_tracking(tracking, buckets)

    assert tracking.loc[0, "final_outcome_bucket"] == OUTCOME_GRADUATED_CONFIRMED
    assert failures == []


def test_manual_transfer_code_creates_transfer_outcome() -> None:
    appearances = build_student_source_appearances(_roster("A00000012"), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    manual = pd.DataFrame(
        {
            "adjustment_id": ["manual-transfer"],
            "student_id": ["A00000012"],
            "normalized_student_id": ["A00000012"],
            "field_to_override": ["final_outcome_bucket"],
            "adjusted_value": ["T"],
            "active": ["Yes"],
        }
    )

    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000012"), manual)

    assert tracking.loc[0, "final_outcome_bucket"] == OUTCOME_TRANSFERRED_LEFT
    assert tracking.loc[0, "manual_outcome_status"] == "T"


def test_input_group_buckets_count_unique_students_once() -> None:
    roster = pd.concat([_roster("A00000006"), _roster("A00000006")], ignore_index=True)
    graduation = pd.DataFrame(
        {
            "Student ID": ["A00000006"],
            "First Name": ["Jane"],
            "Last Name": ["Doe"],
            "Graduation Term": ["Spring 2024"],
            "Outcome": ["Graduated"],
            "Graduation Source File": ["graduates.csv"],
        }
    )
    appearances = build_student_source_appearances(roster, pd.DataFrame(), graduation, pd.DataFrame(), pd.DataFrame())
    tracking = build_student_longitudinal_tracking(appearances, _summary("A00000006"), pd.DataFrame())
    buckets = build_input_group_outcome_buckets(appearances, tracking)
    roster_bucket = buckets.loc[buckets["source_type"].eq("roster")].iloc[0]

    assert int(roster_bucket["unique_student_count"]) == 1
    assert int(roster_bucket["confirmed_graduated_count"]) == 1
