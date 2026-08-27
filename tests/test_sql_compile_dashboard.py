import pandas as pd

from src.sqlCompile_dashboard import (
    MANUAL_CHECKER_SELECT_COLUMN,
    build_dashboard_rate_table,
    build_manual_checker_queue,
    build_manual_entry_template,
    build_outcome_distribution,
    build_sql_compile_milestone_dashboard,
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


def test_dashboard_manual_checker_queue_adds_selection_without_affecting_manual_rows() -> None:
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

    queue = build_manual_checker_queue(review)

    assert queue.columns[0] == MANUAL_CHECKER_SELECT_COLUMN
    assert not bool(queue.loc[0, MANUAL_CHECKER_SELECT_COLUMN])

    queue.loc[0, MANUAL_CHECKER_SELECT_COLUMN] = True
    queue.loc[0, "Semester"] = "Fall 2026"
    queue.loc[0, "Status"] = "CK"
    queue.loc[0, "Notes"] = "Chapter removed before next roster."

    manual_rows = odd_record_editor_to_manual_rows(queue)

    assert MANUAL_CHECKER_SELECT_COLUMN not in manual_rows.columns
    assert manual_rows.to_dict("records") == [
        {
            "Cohort Semester": "Fall 2025",
            "Cohort Chapter": "Alpha Sigma Phi",
            "Semester": "Fall 2026",
            "Chapter": "Alpha Sigma Phi",
            "Student ID": "A01234567",
            "Status": "CK",
            "Notes": "Chapter removed before next roster.",
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


def test_dashboard_milestones_support_grouped_semesters_and_status_mapping() -> None:
    timeline = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Fall 2020", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Fall 2021", "Status Code": "A", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Fall 2024", "Status Code": "G", "Source": "manual_status", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A2", "Semester": "Fall 2020", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A2", "Semester": "Fall 2021", "Status Code": "RS", "Source": "manual_status", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A2", "Semester": "Fall 2022", "Status Code": "A", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Spring 2021", "Student ID": "A3", "Semester": "Spring 2021", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Spring 2021", "Student ID": "A3", "Semester": "Spring 2022", "Status Code": "A", "Source": "sqlCompile", "Included In Outcome": "Yes"},
        ]
    )
    outcomes = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha", "Student ID": "A1"},
            {"Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha", "Student ID": "A2"},
            {"Cohort Semester": "Spring 2021", "Cohort Chapter": "Beta", "Student ID": "A3"},
        ]
    )

    dashboard = build_sql_compile_milestone_dashboard(
        timeline,
        outcomes,
        selected_semesters=["Fall 2020", "Spring 2021"],
        selection_label="2 Semesters",
    )
    table = dashboard["table_frame"].set_index("Milestone")
    chart = dashboard["chart_frame"]

    assert dashboard["meta"]["students"] == 3
    assert table["Measured Students"].tolist() == [3] * 7
    assert table.loc["Cohort Year", "Active Count"] == 3
    assert table.loc["1 Year", "Active Count"] == 2
    assert table.loc["1 Year", "Dropped/Resigned Count"] == 1
    assert table.loc["2 Year", "Active Count"] == 1
    assert table.loc["2 Year", "Unknown Count"] == 1
    assert table.loc["4 Year", "Measured Students"] == 3
    assert table.loc["4 Year", "Graduated Count"] == 1
    assert table.loc["4 Year", "Dropped/Resigned Count"] == 1
    assert table.loc["4 Year", "Unknown Count"] == 1
    assert table.loc["6 Year", "Measured Students"] == 3
    assert table.loc["6 Year", "Graduated Count"] == 1
    assert table.loc["6 Year", "Dropped/Resigned Count"] == 1
    assert table.loc["6 Year", "Unknown Count"] == 1
    assert chart.loc[chart["Outcome"].eq("Graduated") & chart["Milestone Sort"].eq(4), "Denominator"].iloc[0] == 3
