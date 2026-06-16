from pathlib import Path

import pandas as pd

from src.build_canonical_pipeline import (
    apply_manual_roster_corrections,
    build_current_active_fields,
    graduation_evidence_to_manual_adjustments,
    outcome_overrides_to_manual_adjustments,
    roster_exclusions_to_manual_roster_corrections,
    roster_file_version_details,
    roster_status_bucket,
    should_mark_roster_disappeared_unknown,
)
from src.shared_utils import ROSTER_DISAPPEARED_UNKNOWN
from src.build_master_roster import (
    build_individual_new_member_form_lookup,
    chapter_from_filename,
    extract_person_name_from_label,
    infer_chapter,
    is_individual_new_member_form_pdf,
    normalize_banner_id,
    should_upgrade_to_new_member_status,
    source_context_indicates_new_member,
)


def test_current_active_fields_use_latest_roster_only() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2", "3"],
        }
    )
    roster = pd.DataFrame(
        {
            "student_id": ["1", "2", "2", "3"],
            "term_code": ["2024FA", "2023FA", "2024FA", "2024FA"],
            "org_status_bucket": ["Active", "Active", "Inactive", "New Member"],
            "chapter": ["Alpha", "Beta", "Beta", "Gamma"],
            "source_file": ["fall_2024.xlsx", "fall_2023.xlsx", "fall_2024.xlsx", "fall_2024.xlsx"],
            "source_sheet": ["Alpha", "Beta", "Beta", "Gamma"],
        }
    )
    chapter_mapping = pd.DataFrame(
        {
            "chapter": ["Alpha", "Beta", "Gamma"],
            "chapter_group": ["North", "North", "South"],
            "council": ["IFC", "IFC", "PHC"],
            "org_type": ["Fraternity", "Fraternity", "Sorority"],
            "family": ["Traditional", "Traditional", "Traditional"],
            "custom_group": ["Pilot", "Pilot", "Pilot"],
        }
    )
    result = build_current_active_fields(
        summary,
        roster,
        chapter_mapping,
        settings={"chapter_size_bands": [{"label": "Small", "min": 1, "max": 24}]},
    )
    assert result.loc[0, "current_active_flag"] == "Yes"
    assert result.loc[0, "current_active_chapter"] == "Alpha"
    assert result.loc[1, "current_active_flag"] == "No"
    assert result.loc[1, "current_active_chapter"] == ""
    assert result.loc[2, "current_active_flag"] == "Yes"
    assert result.loc[2, "current_active_chapter"] == "Gamma"
    assert result.loc[0, "current_active_roster_term_code"] == "2024FA"
    assert result.loc[2, "current_active_council"] == "PHC"


def test_current_active_fields_allow_missing_source_metadata_columns() -> None:
    summary = pd.DataFrame({"student_id": ["1", "2"]})
    roster = pd.DataFrame(
        {
            "student_id": ["1", "2"],
            "term_code": ["2024FA", "2024FA"],
            "org_status_bucket": ["Active", "Inactive"],
            "chapter": ["Alpha", "Beta"],
        }
    )
    result = build_current_active_fields(
        summary,
        roster,
        pd.DataFrame(columns=["chapter", "chapter_group", "council", "org_type", "family", "custom_group"]),
        settings={"chapter_size_bands": [{"label": "Small", "min": 1, "max": 24}]},
    )

    assert result.loc[0, "current_active_flag"] == "Yes"
    assert result.loc[0, "current_active_source_file"] == ""
    assert result.loc[0, "current_active_source_sheet"] == ""
    assert result.loc[1, "current_active_flag"] == "No"


def test_infer_chapter_uses_parent_chapter_folder_before_council_or_final_folders() -> None:
    path = Path(r"Copy of Rosters\Spring 2026\IFC\Final\Alpha Sigma Phi\roster.xlsx")

    assert chapter_from_filename(path) == "Unknown"
    assert infer_chapter(path, "Sheet1") == "Alpha Sigma Phi"


def test_roster_file_version_details_reads_initial_and_final_from_folder_context() -> None:
    initial_label, initial_priority = roster_file_version_details(r"Copy of Rosters\Fall 2025\PHC\Initial\chapter.xlsx")
    final_label, final_priority = roster_file_version_details(r"Copy of Rosters\Fall 2025\PHC\Final\chapter.xlsx")

    assert initial_label == "Initial"
    assert initial_priority == 1
    assert final_label == "Final"
    assert final_priority == 3


def test_roster_status_bucket_only_marks_explicit_roster_graduation_codes() -> None:
    assert roster_status_bucket("G", "Member") == "Graduated"
    assert roster_status_bucket("Graduated", "Member") == "Graduated"
    assert roster_status_bucket("Good Standing", "Member") != "Graduated"
    assert roster_status_bucket("AL", "Member") == "Early Alumni"
    assert roster_status_bucket("Alumni", "Member") == "Early Alumni"
    assert roster_status_bucket("Early Alumni", "Member") == "Early Alumni"
    assert roster_status_bucket("T", "Member") == "Transfer"
    assert roster_status_bucket("Transfer", "Member") == "Transfer"


def test_banner_id_normalization_only_keeps_valid_a0_ids() -> None:
    assert normalize_banner_id("a01234567") == "A01234567"
    assert normalize_banner_id("A01234567") == "A01234567"
    assert normalize_banner_id("A01234567.0") == "A01234567"
    assert normalize_banner_id("1234567") == ""
    assert normalize_banner_id("A12345678") == ""
    assert normalize_banner_id("A0123456") == ""
    assert normalize_banner_id("") == ""


def test_manual_roster_corrections_override_status_and_chapter() -> None:
    roster = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002"],
            "first_name": ["Jane", "Alex"],
            "last_name": ["Doe", "Smith"],
            "term_code": ["2026SP", "2026SP"],
            "term_label": ["Spring 2026", "Spring 2026"],
            "chapter": ["Wrong Chapter", "Beta"],
            "chapter_assignment_source": ["original", "original"],
            "chapter_assignment_confidence": ["high", "high"],
            "chapter_assignment_notes": ["", ""],
            "org_status_raw": ["Active", "Active"],
            "org_status_bucket": ["Active", "Active"],
            "new_member_flag": ["No", "No"],
        }
    )
    corrections = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "last_name": [""],
            "first_name": [""],
            "organization_join_term": ["Spring 2026"],
            "organization_name": ["Alpha Sigma Phi"],
            "leaving_organization_term": [""],
            "final_status_term": [""],
            "final_status": [""],
        }
    )

    result = apply_manual_roster_corrections(roster, corrections)

    jane = result.loc[result["student_id"].eq("A00000001")].iloc[0]
    assert jane["chapter"] == "Alpha Sigma Phi"
    assert jane["chapter_assignment_source"] == "manual_roster_correction"
    assert jane["org_status_bucket"] == "New Member"
    assert jane["new_member_flag"] == "Yes"


def test_manual_roster_corrections_mark_between_terms_unknown() -> None:
    roster = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000001", "A00000001"],
            "first_name": ["Jane", "Jane", "Jane"],
            "last_name": ["Doe", "Doe", "Doe"],
            "term_code": ["2025FA", "2026SP", "2026FA"],
            "term_label": ["Fall 2025", "Spring 2026", "Fall 2026"],
            "chapter": ["Alpha Sigma Phi", "Alpha Sigma Phi", "Alpha Sigma Phi"],
            "chapter_assignment_source": ["original", "original", "original"],
            "chapter_assignment_confidence": ["high", "high", "high"],
            "chapter_assignment_notes": ["", "", ""],
            "org_status_raw": ["Active", "Active", "Active"],
            "org_status_bucket": ["Active", "Active", "Active"],
            "new_member_flag": ["No", "No", "No"],
        }
    )
    corrections = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "last_name": [""],
            "first_name": [""],
            "organization_join_term": ["Fall 2025"],
            "organization_name": ["Alpha Sigma Phi"],
            "leaving_organization_term": ["Fall 2025"],
            "final_status_term": ["Fall 2026"],
            "final_status": ["Inactive"],
        }
    )

    result = apply_manual_roster_corrections(roster, corrections)

    spring = result.loc[result["term_code"].eq("2026SP")].iloc[0]
    final = result.loc[result["term_code"].eq("2026FA")].iloc[0]
    assert spring["org_status_bucket"] == "Unknown"
    assert final["org_status_bucket"] == "Inactive"


def test_manual_roster_corrections_exclude_matching_rows_from_roster_calculations() -> None:
    roster = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000001", "A00000001", "A00000002"],
            "first_name": ["Jane", "Jane", "Jane", "Alex"],
            "last_name": ["Doe", "Doe", "Doe", "Smith"],
            "term_code": ["2025FA", "2026SP", "2026FA", "2026SP"],
            "term_label": ["Fall 2025", "Spring 2026", "Fall 2026", "Spring 2026"],
            "chapter": ["Alpha Sigma Phi", "Alpha Sigma Phi", "Beta", "Alpha Sigma Phi"],
            "chapter_assignment_source": ["original", "original", "original", "original"],
            "chapter_assignment_confidence": ["high", "high", "high", "high"],
            "chapter_assignment_notes": ["", "", "", ""],
            "org_status_raw": ["Active", "Active", "Active", "Active"],
            "org_status_bucket": ["Active", "Active", "Active", "Active"],
            "new_member_flag": ["No", "No", "No", "No"],
        }
    )
    corrections = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "last_name": [""],
            "first_name": [""],
            "organization_join_term": ["Fall 2025"],
            "organization_name": ["Alpha Sigma Phi"],
            "leaving_organization_term": [""],
            "final_status_term": ["Spring 2026"],
            "final_status": [""],
            "exclude_from_roster_calculations": ["Yes"],
        }
    )

    result = apply_manual_roster_corrections(roster, corrections)

    assert result["student_id"].tolist() == ["A00000001", "A00000002"]
    assert result.loc[result["student_id"].eq("A00000001"), "chapter"].iloc[0] == "Beta"


def test_decision_registries_convert_to_canonical_adjustments() -> None:
    graduation = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "organization_name": ["Alpha Sigma Phi"],
            "graduation_term": ["Spring 2026"],
            "evidence_source": ["Alumni list"],
            "entered_by": ["JH"],
        }
    )
    outcomes = pd.DataFrame(
        {
            "student_id": ["A00000002", "A00000003"],
            "organization_name": ["Beta", "Gamma"],
            "final_status": ["Dropped", "Early Alumni"],
            "final_status_term": ["Fall 2025", "Spring 2026"],
            "reason": ["Verified by advisor", "AL roster status"],
        }
    )

    grad_adjustments = graduation_evidence_to_manual_adjustments(graduation)
    outcome_adjustments = outcome_overrides_to_manual_adjustments(outcomes)

    assert grad_adjustments.loc[grad_adjustments["field_to_override"].eq("final_outcome_bucket"), "adjusted_value"].tolist() == [
        "Graduated Confirmed"
    ]
    assert grad_adjustments.loc[grad_adjustments["field_to_override"].eq("final_outcome_bucket"), "original_value"].tolist() == [
        "Spring 2026"
    ]
    assert outcome_adjustments.loc[outcome_adjustments["field_to_override"].eq("final_outcome_bucket"), "adjusted_value"].tolist() == [
        "Dropped",
        "Early Alumni",
    ]


def test_roster_exclusion_registry_converts_to_roster_corrections() -> None:
    exclusions = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "organization_name": ["Alpha Sigma Phi"],
            "term": ["Spring 2026"],
            "reason": ["Not a student"],
        }
    )

    corrections = roster_exclusions_to_manual_roster_corrections(exclusions)

    assert corrections.loc[0, "student_id"] == "A00000001"
    assert corrections.loc[0, "organization_name"] == "Alpha Sigma Phi"
    assert corrections.loc[0, "organization_join_term"] == "Spring 2026"
    assert corrections.loc[0, "exclude_from_roster_calculations"] == "Yes"


def test_current_active_fields_prefer_spreadsheet_over_pdf_copy() -> None:
    summary = pd.DataFrame({"student_id": ["1"]})
    roster = pd.DataFrame(
        {
            "student_id": ["1", "1"],
            "term_code": ["2026SP", "2026SP"],
            "org_status_bucket": ["Active", "Active"],
            "chapter": ["Alpha Sigma Phi", "Alpha Sigma Phi"],
            "source_file": ["Copy of Rosters/Spring 2026/IFC/Final/Alpha Sigma Phi/roster.pdf", "Copy of Rosters/Spring 2026/IFC/Final/Alpha Sigma Phi/roster.xlsx"],
            "source_sheet": ["Page 1", "Members"],
        }
    )

    result = build_current_active_fields(
        summary,
        roster,
        pd.DataFrame(columns=["chapter", "chapter_group", "council", "org_type", "family", "custom_group"]),
        settings={"chapter_size_bands": [{"label": "Small", "min": 1, "max": 24}]},
    )

    assert result.loc[0, "current_active_flag"] == "Yes"
    assert result.loc[0, "current_active_source_file"].endswith(".xlsx")


def test_current_active_fields_match_short_chapter_to_official_mapping_name() -> None:
    summary = pd.DataFrame({"student_id": ["1"]})
    roster = pd.DataFrame(
        {
            "student_id": ["1"],
            "term_code": ["2026SP"],
            "org_status_bucket": ["Active"],
            "chapter": ["Kappa Delta Chi"],
            "source_file": ["Copy of Rosters/Spring 2026/NPHC/Final/Kappa Delta Chi/roster.xlsx"],
            "source_sheet": ["Members"],
        }
    )
    chapter_mapping = pd.DataFrame(
        {
            "chapter": ["Kappa Delta Chi, Sorority, Inc."],
            "chapter_group": ["Kappa Delta Chi"],
            "council": ["NPHC"],
            "org_type": ["Sorority"],
            "family": ["NPHC"],
            "custom_group": [""],
        }
    )

    result = build_current_active_fields(
        summary,
        roster,
        chapter_mapping,
        settings={"chapter_size_bands": [{"label": "Small", "min": 1, "max": 24}]},
    )

    assert result.loc[0, "current_active_flag"] == "Yes"
    assert result.loc[0, "current_active_council"] == "NPHC"
    assert result.loc[0, "current_active_org_type"] == "Sorority"


def test_roster_disappeared_unknown_applies_to_non_current_chapter_with_unresolved_outcome() -> None:
    current_active_keys = {"alpha sigma phi", "chi omega"}
    chapter_last_roster_sort = {"legacy chapter": 20223, "alpha sigma phi": 20263}

    assert should_mark_roster_disappeared_unknown(
        "Unknown",
        "Legacy Chapter",
        current_active_keys,
        chapter_last_roster_sort,
        20263,
    )
    assert not should_mark_roster_disappeared_unknown(
        ROSTER_DISAPPEARED_UNKNOWN,
        "Alpha Sigma Phi",
        current_active_keys,
        chapter_last_roster_sort,
        20263,
    )


def test_source_context_indicates_new_member_for_new_member_titled_sheet() -> None:
    path = Path(r"Copy of Rosters\Spring 2026\IFC\Alpha Sigma Phi\Raw Data.xlsx")

    assert source_context_indicates_new_member(path, "New Members")
    assert should_upgrade_to_new_member_status("Active", "", True, False)


def test_infer_chapter_ignores_raw_data_and_council_context_without_named_chapter() -> None:
    path = Path(r"Copy of Rosters\Spring 2026\IFC\Raw Data.xlsx")

    assert chapter_from_filename(path) == "Unknown"
    assert infer_chapter(path, "Raw Data") == ""


def test_individual_person_form_pdf_builds_new_member_evidence() -> None:
    path = Path(r"Copy of Rosters\Spring 2026\IFC\Alpha Sigma Phi\Forms\Jane Doe.pdf")
    lookup = build_individual_new_member_form_lookup([path])

    assert is_individual_new_member_form_pdf(path)
    assert ("2026", "spring 2026", "jane", "doe") in lookup


def test_chapter_named_pdf_is_not_treated_as_person_form() -> None:
    path = Path(r"Copy of Rosters\Spring 2026\IFC\Final\Kappa Alpha Order.pdf")

    assert extract_person_name_from_label(path.stem) is None
    assert not is_individual_new_member_form_pdf(path)
