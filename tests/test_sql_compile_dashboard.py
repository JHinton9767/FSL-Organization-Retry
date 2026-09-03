import pandas as pd

from src.sqlCompile_dashboard import (
    LAST_KNOWN_STATUS_COLUMNS,
    MANUAL_CHECKER_SELECT_COLUMN,
    PG_CHART_BREAKDOWN_CHAPTER,
    PG_CHART_BREAKDOWN_SEMESTER,
    build_dashboard_rate_table,
    build_last_known_status_template,
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


def test_dashboard_rate_table_counts_inactive_suspended_as_known_exit() -> None:
    outcomes = pd.DataFrame(
        [
            {
                "Cohort Semester": "Fall 2025",
                "Student ID": "A01234567",
                "Final Outcome Bucket": "Inactive/Suspended",
                "Needs Manual Form Review": "No",
            },
            {
                "Cohort Semester": "Fall 2025",
                "Student ID": "A01234568",
                "Final Outcome Bucket": "Suspended",
                "Needs Manual Form Review": "No",
            },
        ]
    )

    row = build_dashboard_rate_table(outcomes).iloc[0]

    assert row["Known Non-Graduate Exits"] == 2
    assert row["Known Exit Rate"] == 1


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


def test_dashboard_last_known_status_template_uses_persistence_buckets() -> None:
    outcomes = pd.DataFrame(
        [
            {
                "Cohort Semester": "Fall 2025",
                "Cohort Chapter": "Alpha Sigma Phi",
                "Student ID": "A1",
                "Student Name": "Active Student",
                "Last Known Semester": "Spring 2026",
                "Last Known Chapter": "Alpha Sigma Phi",
                "Last Known Status": "A",
                "Last Known Status Code": "A",
                "Final Outcome Bucket": "Needs Manual Form Review",
                "Needs Manual Form Review": "Yes",
                "Manual Status Applied": "No",
            },
            {
                "Cohort Semester": "Fall 2025",
                "Cohort Chapter": "Beta Theta Pi",
                "Student ID": "A2",
                "Student Name": "Kicked Student",
                "Last Known Semester": "Fall 2026",
                "Last Known Chapter": "Beta Theta Pi",
                "Last Known Status": "CK",
                "Last Known Status Code": "CK",
                "Final Outcome Bucket": "Chapter Kicked",
                "Needs Manual Form Review": "No",
                "Manual Status Applied": "No",
            },
            {
                "Cohort Semester": "Fall 2025",
                "Cohort Chapter": "Delta Tau Delta",
                "Student ID": "A3",
                "Student Name": "Graduated Student",
                "Last Known Semester": "Spring 2027",
                "Last Known Chapter": "Delta Tau Delta",
                "Last Known Status": "G",
                "Last Known Status Code": "G",
                "Final Outcome Bucket": "Graduated",
                "Needs Manual Form Review": "No",
                "Manual Status Applied": "Yes",
            },
        ]
    )

    template = build_last_known_status_template(outcomes)

    assert template.columns.tolist() == LAST_KNOWN_STATUS_COLUMNS
    assert template.columns.tolist()[3] == "Student Name"
    assert template.set_index("Student ID").loc["A1", "Student Name"] == "Active Student"
    assert template.set_index("Student ID").loc["A1", "Last Known Outcome Bucket"] == "Unknown"
    assert template.set_index("Student ID").loc["A2", "Last Known Outcome Bucket"] == "Chapter Kicked"
    assert template.set_index("Student ID").loc["A3", "Last Known Outcome Bucket"] == "Graduated"
    assert template.set_index("Student ID").loc["A3", "Manual Status Applied"] == "Yes"


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


def test_dashboard_milestones_use_eligibility_by_year_and_carry_forward_terminal_outcomes() -> None:
    timeline = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Fall 2020", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Spring 2026", "Status Code": "G", "Source": "manual_status", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A2", "Semester": "Fall 2020", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A2", "Semester": "Fall 2021", "Status Code": "RS", "Source": "manual_status", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A2", "Semester": "Fall 2022", "Status Code": "A", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2025", "Student ID": "A3", "Semester": "Fall 2025", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2025", "Student ID": "A3", "Semester": "Spring 2026", "Status Code": "A", "Source": "sqlCompile", "Included In Outcome": "Yes"},
        ]
    )
    outcomes = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha", "Student ID": "A1"},
            {"Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha", "Student ID": "A2"},
            {"Cohort Semester": "Fall 2025", "Cohort Chapter": "Beta", "Student ID": "A3"},
        ]
    )

    dashboard = build_sql_compile_milestone_dashboard(
        timeline,
        outcomes,
        selected_semesters=["Fall 2020", "Fall 2025"],
        selection_label="2 Semesters",
    )
    table = dashboard["table_frame"].set_index("Milestone")
    chart = dashboard["chart_frame"]

    assert dashboard["meta"]["students"] == 3
    assert table.index.tolist() == ["1 Year", "2 Year", "3 Year", "4 Year", "5 Year", "6 Year"]
    assert table.loc["1 Year", "Measured Students"] == 3
    assert table.loc["2 Year", "Measured Students"] == 2
    assert table.loc["3 Year", "Measured Students"] == 2
    assert table.loc["4 Year", "Measured Students"] == 2
    assert table.loc["5 Year", "Measured Students"] == 2
    assert table.loc["6 Year", "Measured Students"] == 2
    assert table.loc["1 Year", "Active Count"] == 3
    assert table.loc["2 Year", "Active Count"] == 1
    assert table.loc["2 Year", "Dropped/Resigned Count"] == 1
    assert table.loc["4 Year", "Active Count"] == 1
    assert table.loc["4 Year", "Dropped/Resigned Count"] == 1
    assert table.loc["6 Year", "Graduated Count"] == 1
    assert table.loc["6 Year", "Dropped/Resigned Count"] == 1
    assert table.loc["6 Year", "Active Count"] == 0
    assert table.loc["6 Year", "Future Students"] == 1
    assert table.loc["6 Year", "Milestone Status"] == "Partially Future"
    assert chart.loc[chart["Outcome"].eq("Graduated") & chart["Milestone Sort"].eq(6), "Denominator"].iloc[0] == 2
    assert chart.loc[chart["Outcome"].eq("Graduated") & chart["Milestone Sort"].eq(6), "Share"].iloc[0] == 1 / 2
    assert chart.loc[chart["Outcome"].eq("Graduated") & chart["Milestone Sort"].eq(6), "Future Students"].iloc[0] == 1
    assert chart.loc[chart["Outcome"].eq("Future") & chart["Milestone Sort"].eq(6)].empty


def test_dashboard_milestones_marks_unmeasured_recent_cohorts_as_future() -> None:
    timeline = pd.DataFrame(
        [
            {
                "Cohort Semester": "Fall 2025",
                "Student ID": "A3",
                "Semester": "Fall 2025",
                "Status Code": "N",
                "Source": "sqlCompile",
                "Included In Outcome": "Yes",
            },
            {
                "Cohort Semester": "Fall 2025",
                "Student ID": "A3",
                "Semester": "Spring 2026",
                "Status Code": "A",
                "Source": "sqlCompile",
                "Included In Outcome": "Yes",
            },
        ]
    )
    outcomes = pd.DataFrame(
        [{"Cohort Semester": "Fall 2025", "Cohort Chapter": "Beta", "Student ID": "A3"}]
    )

    dashboard = build_sql_compile_milestone_dashboard(
        timeline,
        outcomes,
        selected_semesters=["Fall 2025"],
        selection_label="Fall 2025",
    )
    table = dashboard["table_frame"].set_index("Milestone")
    chart = dashboard["chart_frame"]
    chart_table = dashboard["chart_table_frame"]

    assert table.index.tolist() == ["1 Year", "2 Year", "3 Year", "4 Year", "5 Year", "6 Year"]
    assert table.loc["1 Year", "Milestone Status"] == "Measured"
    assert table.loc["1 Year", "Measured Students"] == 1
    assert table.loc["2 Year", "Milestone Status"] == "Future"
    assert table.loc["2 Year", "Measured Students"] == 0
    assert table.loc["2 Year", "Future Students"] == 1
    future_bar = chart.loc[chart["Outcome"].eq("Future") & chart["Milestone Sort"].eq(2)].iloc[0]
    assert future_bar["Share"] == 1
    assert future_bar["Count"] == 1
    assert dashboard["meta"]["max_milestone"] == "1 Year"
    assert chart_table.loc[chart_table["Milestone"].eq("6 Year"), "Milestone Status"].iloc[0] == "Future"


def test_dashboard_milestones_can_filter_to_selected_chapters() -> None:
    timeline = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Fall 2020", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Fall 2021", "Status Code": "A", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A2", "Semester": "Fall 2020", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A2", "Semester": "Spring 2024", "Status Code": "G", "Source": "manual_status", "Included In Outcome": "Yes"},
        ]
    )
    outcomes = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha", "Student ID": "A1", "Final Outcome Bucket": "Active / Still On Roster", "Needs Manual Form Review": "No"},
            {"Cohort Semester": "Fall 2020", "Cohort Chapter": "Beta", "Student ID": "A2", "Final Outcome Bucket": "Graduated", "Needs Manual Form Review": "No"},
        ]
    )

    dashboard = build_sql_compile_milestone_dashboard(
        timeline,
        outcomes,
        selected_semesters=["Fall 2020"],
        selected_chapters=["Beta"],
        selection_label="Fall 2020",
    )
    chart_table = dashboard["chart_table_frame"]
    rates = build_dashboard_rate_table(outcomes, group_columns=["Cohort Semester", "Cohort Chapter"])

    assert dashboard["meta"]["students"] == 1
    assert chart_table["Chart Group"].unique().tolist() == ["1 Year", "2 Year", "3 Year", "4 Year", "5 Year", "6 Year"]
    assert dashboard["table_frame"].set_index("Milestone").loc["4 Year", "Graduated Count"] == 1
    assert rates.columns.tolist()[:2] == ["Cohort Semester", "Cohort Chapter"]
    assert set(rates["Cohort Chapter"]) == {"Alpha", "Beta"}


def test_dashboard_overall_chart_uses_only_selected_semesters() -> None:
    timeline = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Fall 2020", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Spring 2024", "Status Code": "G", "Source": "manual_status", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Spring 2021", "Student ID": "A2", "Semester": "Spring 2021", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Spring 2021", "Student ID": "A2", "Semester": "Spring 2024", "Status Code": "RS", "Source": "manual_status", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2021", "Student ID": "A3", "Semester": "Fall 2021", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2021", "Student ID": "A3", "Semester": "Spring 2025", "Status Code": "T", "Source": "manual_status", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2022", "Student ID": "A4", "Semester": "Fall 2022", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2022", "Student ID": "A4", "Semester": "Spring 2025", "Status Code": "G", "Source": "manual_status", "Included In Outcome": "Yes"},
        ]
    )
    outcomes = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha", "Student ID": "A1"},
            {"Cohort Semester": "Spring 2021", "Cohort Chapter": "Beta", "Student ID": "A2"},
            {"Cohort Semester": "Fall 2021", "Cohort Chapter": "Gamma", "Student ID": "A3"},
            {"Cohort Semester": "Fall 2022", "Cohort Chapter": "Delta", "Student ID": "A4"},
        ]
    )

    dashboard = build_sql_compile_milestone_dashboard(
        timeline,
        outcomes,
        selected_semesters=["Fall 2020", "Spring 2021", "Fall 2021"],
        selection_label="3 Semesters",
        chart_milestone_offsets=[1, 2, 3, 4, 5, 6],
    )

    assert dashboard["meta"]["students"] == 3
    assert dashboard["table_frame"].set_index("Milestone").loc["1 Year", "Measured Students"] == 3
    chart = dashboard["chart_frame"]
    assert chart["Cohort Students"].max() == 3
    assert not chart["Chart Group"].astype(str).str.contains("Fall 2022").any()


def test_dashboard_can_chart_selected_milestone_by_semester_joined() -> None:
    timeline = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Fall 2020", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Spring 2021", "Status Code": "G", "Source": "manual_status", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2025", "Student ID": "A2", "Semester": "Fall 2025", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2025", "Student ID": "A2", "Semester": "Spring 2026", "Status Code": "A", "Source": "sqlCompile", "Included In Outcome": "Yes"},
        ]
    )
    outcomes = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha", "Student ID": "A1"},
            {"Cohort Semester": "Fall 2025", "Cohort Chapter": "Beta", "Student ID": "A2"},
        ]
    )

    dashboard = build_sql_compile_milestone_dashboard(
        timeline,
        outcomes,
        selected_semesters=["Fall 2020", "Fall 2025"],
        selection_label="2 Semesters",
        chart_breakdown=PG_CHART_BREAKDOWN_SEMESTER,
        chart_milestone_offsets=[4],
    )
    chart = dashboard["chart_frame"]
    chart_table = dashboard["chart_table_frame"]

    assert chart["Chart Group"].drop_duplicates().tolist() == ["Fall 2020", "Fall 2025"]
    assert chart.loc[chart["Chart Group"].eq("Fall 2020"), "Outcome"].tolist() == ["Graduated"]
    assert chart.loc[chart["Chart Group"].eq("Fall 2025"), "Outcome"].tolist() == ["Future"]
    assert chart_table["Milestone"].unique().tolist() == ["4 Year"]


def test_dashboard_can_chart_selected_milestone_by_chapter_joined() -> None:
    timeline = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Fall 2020", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A1", "Semester": "Spring 2024", "Status Code": "G", "Source": "manual_status", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A2", "Semester": "Fall 2020", "Status Code": "N", "Source": "sqlCompile", "Included In Outcome": "Yes"},
            {"Cohort Semester": "Fall 2020", "Student ID": "A2", "Semester": "Spring 2024", "Status Code": "RS", "Source": "manual_status", "Included In Outcome": "Yes"},
        ]
    )
    outcomes = pd.DataFrame(
        [
            {"Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha", "Student ID": "A1"},
            {"Cohort Semester": "Fall 2020", "Cohort Chapter": "Beta", "Student ID": "A2"},
        ]
    )

    dashboard = build_sql_compile_milestone_dashboard(
        timeline,
        outcomes,
        selected_semesters=["Fall 2020"],
        selection_label="Fall 2020",
        chart_breakdown=PG_CHART_BREAKDOWN_CHAPTER,
        chart_milestone_offsets=[4],
    )
    chart = dashboard["chart_frame"]

    assert chart["Chart Group"].drop_duplicates().tolist() == ["Alpha", "Beta"]
    assert chart.loc[chart["Chart Group"].eq("Alpha"), "Outcome"].tolist() == ["Graduated"]
    assert chart.loc[chart["Chart Group"].eq("Beta"), "Outcome"].tolist() == ["Dropped/Resigned"]
