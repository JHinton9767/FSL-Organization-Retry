import pandas as pd

from app.standardize import standardize_enhanced_summary


def test_missing_fields_degrade_to_unknown_without_crashing() -> None:
    summary = pd.DataFrame(
        {
            "Student ID": ["1"],
            "Preferred First Name": ["Taylor"],
            "Preferred Last Name": ["Jordan"],
            "Initial Chapter": [""],
            "Latest Chapter": [""],
            "First Observed Organization Term": [""],
            "Organization Entry Cohort": [""],
            "Latest Known Outcome Bucket": [""],
            "Latest Known Roster Status Bucket": [""],
            "Initial Roster Status Bucket": [""],
        }
    )
    standardized = standardize_enhanced_summary(
        summary=summary,
        chapter_mapping=pd.DataFrame(columns=["chapter", "chapter_group", "council", "org_type", "family", "custom_group"]),
        settings={"chapter_size_bands": [], "high_hours_threshold": 60, "completeness_fields": ["student_id", "chapter", "join_term"]},
    )
    assert standardized.loc[0, "status_group"] == "Unknown"
    assert standardized.loc[0, "major_group"] == "Unknown"
    assert standardized.loc[0, "chapter_group"] == "Unassigned"
