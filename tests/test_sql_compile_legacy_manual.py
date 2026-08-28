from pathlib import Path

import pandas as pd

from src.sqlCompile_cohort import read_manual_status_rows
from src.sqlCompile_legacy_manual import (
    import_legacy_manual_decisions,
    legacy_status_to_sql_status,
    load_legacy_manual_decision_rows,
)


def test_legacy_status_to_sql_status_maps_old_dashboard_outcomes() -> None:
    assert legacy_status_to_sql_status("Graduated Confirmed") == "G"
    assert legacy_status_to_sql_status("Chapter Kicked") == "CK"
    assert legacy_status_to_sql_status("Early Alumni") == "AL"
    assert legacy_status_to_sql_status("Resigned") == "RS"
    assert legacy_status_to_sql_status("Inactive/Suspended") == "S"
    assert legacy_status_to_sql_status("Dropped/Inactive") == "D"


def test_load_legacy_manual_decision_rows_converts_old_registry_files(tmp_path: Path) -> None:
    pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "organization_name": ["Alpha Sigma Phi"],
            "organization_join_term": ["Fall 2020"],
            "graduation_term": ["Spring 2024"],
            "evidence_source": ["Alumni list"],
        }
    ).to_csv(tmp_path / "graduation_evidence.csv", index=False)
    pd.DataFrame(
        {
            "student_id": ["A00000002"],
            "organization_name": ["Beta"],
            "organization_join_term": ["Fall 2020"],
            "final_status": ["Resigned"],
            "final_status_term": ["Fall 2021"],
            "reason": ["Verified form"],
        }
    ).to_csv(tmp_path / "outcome_overrides.csv", index=False)
    pd.DataFrame(
        {
            "student_id": ["A00000003"],
            "organization_join_term": ["Spring 2021"],
            "organization_name": ["Gamma"],
            "corrected_organization_name": [""],
            "leaving_organization_term": ["Fall 2021"],
            "final_status_term": ["Spring 2022"],
            "final_status": ["Chapter Kicked"],
            "exclude_from_roster_calculations": [""],
        }
    ).to_csv(tmp_path / "manual_roster_corrections.csv", index=False)
    pd.DataFrame(
        [
            {
                "student_id": "A00000004",
                "field_to_override": "latest_known_chapter",
                "original_value": "",
                "adjusted_value": "Delta",
                "active": "Yes",
            },
            {
                "student_id": "A00000004",
                "field_to_override": "final_outcome_bucket",
                "original_value": "Spring 2023",
                "adjusted_value": "Transfer",
                "reason": "Manual adjustment",
                "active": "Yes",
            },
        ]
    ).to_csv(tmp_path / "manual_adjustments.csv", index=False)
    pd.DataFrame(
        {
            "student_id": ["A00000005"],
            "chapter": ["Epsilon"],
            "join_term": ["Fall 2021"],
            "last_observed_org_term": ["Spring 2022"],
            "latest_outcome_bucket": ["Unknown"],
            "review_status": ["Corrected"],
            "has_manual_correction": ["Yes"],
            "review_notes": ["Saved as Suspended."],
        }
    ).to_csv(tmp_path / "manual_review_actions.pending_20260827.csv", index=False)

    loaded = load_legacy_manual_decision_rows(tmp_path)
    rows = loaded.rows.sort_values("Student ID").reset_index(drop=True)

    assert rows["Student ID"].tolist() == ["A00000001", "A00000002", "A00000003", "A00000004", "A00000005"]
    assert rows["Status"].tolist() == ["G", "RS", "CK", "T", "S"]
    assert rows.loc[rows["Student ID"].eq("A00000004"), "Chapter"].iloc[0] == "Delta"
    assert loaded.converted_counts["graduation_evidence"] == 1
    assert loaded.converted_counts["manual_review_actions"] == 1


def test_load_legacy_manual_decision_rows_auto_detects_manual_check_exports(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    pd.DataFrame(
        {
            "Student ID": ["A00000006", "A00000007"],
            "Chapter": ["Zeta Tau Alpha", "Theta Chi"],
            "Join Term": ["Fall 2021", "Spring 2022"],
            "Last Observed Org Term": ["Spring 2022", "Fall 2022"],
            "Latest Outcome Bucket": ["Unknown", "Unknown"],
            "Review Status": ["Corrected", "Needs Review"],
            "Has Manual Correction": ["Yes", ""],
            "Review Notes": ["Saved as Early Alumni.", ""],
        }
    ).to_csv(config_dir / "Manual checks form.csv", index=False)

    loaded = load_legacy_manual_decision_rows(tmp_path)

    assert loaded.rows.to_dict("records") == [
        {
            "Cohort Semester": "Fall 2021",
            "Cohort Chapter": "Zeta Tau Alpha",
            "Semester": "Spring 2022",
            "Chapter": "Zeta Tau Alpha",
            "Student ID": "A00000006",
            "Status": "AL",
            "Notes": "Imported from legacy Manual checks form.csv. Saved as Early Alumni.",
        }
    ]
    assert loaded.source_counts["manual_review_actions"] == 2
    assert loaded.converted_counts["manual_review_actions"] == 1


def test_import_legacy_manual_decisions_appends_to_sql_compile_manual_file(tmp_path: Path) -> None:
    pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "organization_name": ["Alpha Sigma Phi"],
            "organization_join_term": ["Fall 2020"],
            "final_status": ["Dropped"],
            "final_status_term": ["Spring 2021"],
        }
    ).to_csv(tmp_path / "outcome_overrides.csv", index=False)
    destination = tmp_path / "sqlCompile_manual_status.csv"

    result = import_legacy_manual_decisions(tmp_path, destination)
    rows = read_manual_status_rows(destination)

    assert result.saved_rows == 1
    assert rows.to_dict("records") == [
        {
            "Cohort Semester": "Fall 2020",
            "Cohort Chapter": "Alpha Sigma Phi",
            "Semester": "Spring 2021",
            "Chapter": "Alpha Sigma Phi",
            "Student ID": "A00000001",
            "Status": "D",
            "Notes": "Imported from legacy outcome_overrides.csv.",
        }
    ]
