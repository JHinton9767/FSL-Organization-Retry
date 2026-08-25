import pandas as pd

from src.sqlCompile_dashboard import (
    build_dashboard_rate_table,
    build_manual_entry_template,
    build_outcome_distribution,
    odd_record_editor_to_manual_rows,
)


def test_dashboard_rate_table_uses_resolved_denominator() -> None:
    outcomes = pd.DataFrame(
        [
            {
                "Cohort Semester": "Fall 2025",
                "Student ID": "A01234567",
                "Final Outcome Bucket": "Needs Manual Form Review",
                "Needs Manual Form Review": "Yes",
            },
            {
                "Cohort Semester": "Fall 2025",
                "Student ID": "A01234568",
                "Final Outcome Bucket": "Graduated",
                "Needs Manual Form Review": "No",
            },
            {
                "Cohort Semester": "Fall 2025",
                "Student ID": "A01234569",
                "Final Outcome Bucket": "Active / Still On Roster",
                "Needs Manual Form Review": "No",
            },
            {
                "Cohort Semester": "Fall 2025",
                "Student ID": "A01234570",
                "Final Outcome Bucket": "Resigned",
                "Needs Manual Form Review": "No",
            },
        ]
    )

    rates = build_dashboard_rate_table(outcomes)
    row = rates.iloc[0]

    assert row["Cohort Students"] == 4
    assert row["Resolved Students"] == 3
    assert row["Needs Manual Review"] == 1
    assert row["Persistence Rate"] == 1 / 3
    assert row["Graduation Rate"] == 1 / 3
    assert row["Known Exit Rate"] == 1 / 3


def test_dashboard_manual_entry_template_round_trips_to_manual_rows() -> None:
    review = pd.DataFrame(
        [
            {
                "Cohort Semester": "Fall 2025",
                "Cohort Chapter": "Alpha Sigma Phi",
                "Student ID": "A01234567",
                "Last Known Semester": "Spring 2026",
                "Last Known Chapter": "Alpha Sigma Phi",
                "Last Known Status": "A",
            }
        ]
    )

    template = build_manual_entry_template(review)
    template.loc[0, "Semester"] = "Fall 2026"
    template.loc[0, "Status"] = "RS"
    template.loc[0, "Notes"] = "Form found."

    manual_rows = odd_record_editor_to_manual_rows(template)

    assert manual_rows.to_dict("records") == [
        {
            "Cohort Semester": "Fall 2025",
            "Cohort Chapter": "Alpha Sigma Phi",
            "Semester": "Fall 2026",
            "Chapter": "Alpha Sigma Phi",
            "Student ID": "A01234567",
            "Status": "RS",
            "Notes": "Form found.",
        }
    ]


def test_dashboard_outcome_distribution_counts_by_cohort() -> None:
    outcomes = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2025", "Student ID": "A1", "Final Outcome Bucket": "Graduated"},
            {"Cohort Semester": "Fall 2025", "Student ID": "A2", "Final Outcome Bucket": "Graduated"},
            {"Cohort Semester": "Fall 2025", "Student ID": "A3", "Final Outcome Bucket": "Resigned"},
        ]
    )

    distribution = build_outcome_distribution(outcomes)

    assert distribution.set_index("Final Outcome Bucket").loc["Graduated", "Student Count"] == 2
    assert distribution.set_index("Final Outcome Bucket").loc["Graduated", "Share of Cohort"] == 2 / 3
