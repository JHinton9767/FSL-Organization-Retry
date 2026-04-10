import pandas as pd

from app.standardize import standardize_processed_summary


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

