import pandas as pd

from app.standardize import standardize_processed_summary
from src.build_canonical_pipeline import build_current_active_fields


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
