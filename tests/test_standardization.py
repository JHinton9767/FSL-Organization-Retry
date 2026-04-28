from pathlib import Path

import pandas as pd

from app.standardize import standardize_processed_summary
from src.build_canonical_pipeline import build_current_active_fields, roster_file_version_details, roster_status_bucket
from src.build_master_roster import (
    build_individual_new_member_form_lookup,
    chapter_from_filename,
    infer_chapter,
    is_individual_new_member_form_pdf,
    should_upgrade_to_new_member_status,
    source_context_indicates_new_member,
)


def test_processed_summary_assigns_core_groups() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2"],
            "first_name": ["Alex", "Jamie"],
            "last_name": ["Lee", "Ng"],
            "chapter": ["Alpha", "Alpha"],
            "join_term": ["Fall 2021", "Spring 2022"],
            "latest_membership_status": ["ACTIVE", "TRANSFER"],
            "major": ["Biology", ""],
            "pell_flag": ["Yes", ""],
            "cohort": ["FTFT", "Transfer"],
            "total_earned": [45, 78],
            "avg_term_gpa": [3.1, 2.8],
            "latest_gpa_cum": [3.2, 3.0],
            "graduated": [False, True],
            "graduated_4yr": [False, False],
            "graduated_6yr": [False, True],
            "first_term": ["Fall 2021", "Spring 2022"],
            "first_term_sort": [20213, 20221],
            "last_term_sort": [20223, 20241],
        }
    )
    standardized = standardize_processed_summary(
        summary=summary,
        chapter_mapping=pd.DataFrame(
            {
                "chapter": ["Alpha"],
                "chapter_group": ["North"],
                "council": ["IFC"],
                "org_type": ["Fraternity"],
                "family": ["Traditional"],
                "custom_group": ["Pilot"],
            }
        ),
        settings={
            "high_hours_threshold": 60,
            "chapter_size_bands": [{"label": "Small", "min": 1, "max": 24}],
            "completeness_fields": ["student_id", "chapter", "join_term"],
        },
        status_code_map={"active": ["ACTIVE"], "transfer": ["TRANSFER"], "graduated": ["GRADUATED"], "inactive": [], "suspended": []},
    )
    assert standardized.loc[0, "chapter_group"] == "North"
    assert standardized.loc[0, "pell_group"] == "Pell"
    assert standardized.loc[1, "transfer_group"] == "Transfer"
    assert standardized.loc[1, "high_hours_group"] == "High Hours"


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


def test_processed_summary_does_not_treat_single_letter_g_inside_longer_text_as_graduated() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1"],
            "first_name": ["Alex"],
            "last_name": ["Lee"],
            "chapter": ["Alpha"],
            "join_term": ["Fall 2021"],
            "latest_membership_status": ["Good Standing"],
            "major": ["Biology"],
            "pell_flag": ["Yes"],
            "cohort": ["FTFT"],
            "total_earned": [45],
            "avg_term_gpa": [3.1],
            "latest_gpa_cum": [3.2],
            "graduated": [False],
            "graduated_4yr": [False],
            "graduated_6yr": [False],
            "first_term": ["Fall 2021"],
            "first_term_sort": [20213],
            "last_term_sort": [20223],
        }
    )

    standardized = standardize_processed_summary(
        summary=summary,
        chapter_mapping=pd.DataFrame(columns=["chapter", "chapter_group", "council", "org_type", "family", "custom_group"]),
        settings={
            "high_hours_threshold": 60,
            "chapter_size_bands": [{"label": "Small", "min": 1, "max": 24}],
            "completeness_fields": ["student_id", "chapter", "join_term"],
        },
        status_code_map={"active": ["A"], "transfer": ["T"], "graduated": ["G"], "inactive": [], "suspended": []},
    )

    assert standardized.loc[0, "latest_outcome_bucket"] != "Graduated"


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
