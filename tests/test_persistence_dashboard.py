import pandas as pd

from app.analysis import build_persistence_dashboard, filter_persistence_population, persistence_cohort_options


def test_filter_persistence_population_supports_council_and_org_type_distinctions() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2", "3", "4", "5"],
            "join_term": ["Fall 2019", "Fall 2019", "Fall 2019", "Fall 2019", "Fall 2020"],
            "council": ["IFC", "PHC", "MCG", "NPHC", "IFC"],
            "org_type": ["Fraternity", "Sorority", "Sorority", "Fraternity", "Fraternity"],
        }
    )

    all_students = filter_persistence_population(summary, "Fall 2019", "ALL")
    mgc_students = filter_persistence_population(summary, "Fall 2019", "MGC")
    fraternity_students = filter_persistence_population(summary, "Fall 2019", "FRA")
    sorority_students = filter_persistence_population(summary, "Fall 2019", "SOR")

    assert all_students["student_id"].tolist() == ["1", "2", "3", "4"]
    assert mgc_students["student_id"].tolist() == ["3"]
    assert fraternity_students["student_id"].tolist() == ["1", "4"]
    assert sorority_students["student_id"].tolist() == ["2", "3"]


def test_persistence_cohort_options_include_academic_year_totals_after_matching_spring() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2", "3", "4"],
            "join_term": ["Fall 2015", "Spring 2016", "Fall 2016", "Spring 2017"],
        }
    )

    options = persistence_cohort_options(summary)

    assert options == ["Fall 2015", "Spring 2016", "Fall 2015 Total", "Fall 2016", "Spring 2017", "Fall 2016 Total"]


def test_persistence_cohort_options_use_organization_join_term_only() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2", "3"],
            "school_entry_term": ["Fall 2019", "", "Spring 2020"],
            "join_term": ["Fall 2020", "Fall 2021", "Fall 2021"],
        }
    )

    options = persistence_cohort_options(summary)

    assert options == ["Fall 2020", "Fall 2021"]


def test_filter_persistence_population_supports_academic_year_total_cohorts() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2", "3", "4"],
            "join_term": ["Fall 2015", "Spring 2016", "Fall 2016", "Spring 2017"],
            "council": ["IFC", "PHC", "IFC", "PHC"],
            "org_type": ["Fraternity", "Sorority", "Fraternity", "Sorority"],
        }
    )

    total_students = filter_persistence_population(summary, "Fall 2015 Total", "ALL")
    fraternity_total = filter_persistence_population(summary, "Fall 2015 Total", "FRA")

    assert total_students["student_id"].tolist() == ["1", "2"]
    assert fraternity_total["student_id"].tolist() == ["1"]


def test_build_persistence_dashboard_uses_explicit_graduation_and_roster_checkpoint_retention() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2", "3"],
            "join_term": ["Fall 2019", "Fall 2019", "Fall 2019"],
            "council": ["IFC", "IFC", "IFC"],
            "org_type": ["Fraternity", "Fraternity", "Fraternity"],
            "is_graduated": [True, False, False],
            "graduation_term": ["Fall 2023", "", ""],
            "graduation_term_code": ["2023FA", "", ""],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["1", "2", "1", "2"],
            "observed_term": ["Fall 2020", "Fall 2020", "Fall 2023", "Fall 2023"],
            "observed_term_sort": [20203, 20203, 20233, 20233],
            "academic_present": ["Yes", "Yes", "Yes", "Yes"],
            "roster_present": ["No", "No", "No", "Yes"],
        }
    )

    dashboard = build_persistence_dashboard(summary, longitudinal, "Fall 2019", "ALL")
    table = dashboard["table_frame"]
    four_year = table.loc[table["Milestone"].eq("4 Year")].iloc[0]

    assert dashboard["meta"]["students"] == 3
    assert four_year["Term"] == "Fall 2023"
    assert four_year["Graduated Count"] == 1
    assert four_year["Active Count"] == 1
    assert four_year["Unknown Count"] == 1
    assert four_year["Graduated"] == 1 / 3
    assert four_year["Active"] == 1 / 3
    assert four_year["Unknown"] == 1 / 3


def test_build_persistence_dashboard_counts_roster_presence_as_retained() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2", "3"],
            "school_entry_term": ["Fall 2019", "Fall 2019", "Fall 2019"],
            "join_term": ["Fall 2020", "Fall 2020", "Fall 2020"],
            "council": ["IFC", "IFC", "IFC"],
            "org_type": ["Fraternity", "Fraternity", "Fraternity"],
            "is_graduated": [True, False, False],
            "graduation_term": ["Fall 2024", "", ""],
            "graduation_term_code": ["2024FA", "", ""],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["1", "2", "1", "2"],
            "observed_term": ["Fall 2020", "Fall 2020", "Fall 2024", "Fall 2024"],
            "observed_term_sort": [20203, 20203, 20243, 20243],
            "academic_present": ["Yes", "No", "Yes", "No"],
            "roster_present": ["No", "Yes", "No", "Yes"],
        }
    )

    dashboard = build_persistence_dashboard(summary, longitudinal, "Fall 2020", "ALL")
    table = dashboard["table_frame"]
    four_year = table.loc[table["Milestone"].eq("4 Year")].iloc[0]

    assert dashboard["meta"]["students"] == 3
    assert four_year["Graduated Count"] == 1
    assert four_year["Active Count"] == 1
    assert four_year["Unknown Count"] == 1


def test_build_persistence_dashboard_supports_academic_year_total_checkpoints() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2", "3"],
            "join_term": ["Fall 2015", "Spring 2016", "Spring 2016"],
            "council": ["IFC", "IFC", "IFC"],
            "org_type": ["Fraternity", "Fraternity", "Fraternity"],
            "is_graduated": [True, False, False],
            "graduation_term": ["Fall 2019", "", ""],
            "graduation_term_code": ["2019FA", "", ""],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["1", "2", "1", "2"],
            "observed_term": ["Fall 2016", "Spring 2017", "Fall 2019", "Spring 2020"],
            "observed_term_sort": [20163, 20171, 20193, 20201],
            "academic_present": ["Yes", "Yes", "Yes", "Yes"],
            "roster_present": ["No", "Yes", "No", "Yes"],
        }
    )

    dashboard = build_persistence_dashboard(summary, longitudinal, "Fall 2015 Total", "ALL")
    table = dashboard["table_frame"]
    four_year = table.loc[table["Milestone"].eq("4 Year")].iloc[0]

    assert dashboard["meta"]["students"] == 3
    assert four_year["Term"] == "Fall 2019 Total"
    assert four_year["Graduated Count"] == 1
    assert four_year["Active Count"] == 1
    assert four_year["Unknown Count"] == 1


def test_build_persistence_dashboard_uses_later_roster_presence_and_skips_after_latest_roster() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2"],
            "join_term": ["Fall 2019", "Fall 2019"],
            "council": ["IFC", "IFC"],
            "org_type": ["Fraternity", "Fraternity"],
            "is_graduated": [False, False],
            "graduation_term": ["", ""],
            "graduation_term_code": ["", ""],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["1", "999"],
            "observed_term": ["Spring 2024", "Spring 2024"],
            "observed_term_sort": [20241, 20241],
            "academic_present": ["No", "No"],
            "roster_present": ["Yes", "Yes"],
        }
    )

    dashboard = build_persistence_dashboard(summary, longitudinal, "Fall 2019", "ALL")
    table = dashboard["table_frame"]
    four_year = table.loc[table["Milestone"].eq("4 Year")].iloc[0]

    assert four_year["Active Count"] == 1
    assert four_year["Unknown Count"] == 1
    assert not table["Milestone"].eq("5 Year").any()


def test_build_persistence_dashboard_uses_eight_roster_categories_then_manual_override() -> None:
    student_ids = [f"A0000000{index}" for index in range(1, 10)]
    summary = pd.DataFrame(
        {
            "student_id": student_ids,
            "join_term": ["Fall 2020"] * 9,
            "council": ["IFC"] * 9,
            "org_type": ["Fraternity"] * 9,
            "is_graduated": [False] * 8 + [True],
            "graduation_term": [""] * 8 + ["Fall 2024"],
            "graduation_term_code": [""] * 8 + ["2024FA"],
            "manual_outcome_status": ["", "", "", "", "", "Dropped", "", "", ""],
            "manual_outcome_term": ["", "", "", "", "", "Fall 2024", "", "", ""],
            "last_observed_org_term_code": ["2024FA"] * 9,
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": student_ids,
            "observed_term": ["Fall 2024"] * 9,
            "observed_term_sort": [20243] * 9,
            "roster_present": ["Yes"] * 9,
            "org_status_bucket": [
                "Active",
                "Early Alumni",
                "Inactive",
                "Resigned",
                "Revoked",
                "Active",
                "Transfer",
                "H",
                "Graduated",
            ],
        }
    )

    dashboard = build_persistence_dashboard(summary, longitudinal, "Fall 2020", "ALL")
    four_year = dashboard["table_frame"].loc[
        dashboard["table_frame"]["Milestone"].eq("4 Year")
    ].iloc[0]

    for outcome in [
        "Active",
        "Early Alumni",
        "Inactive/Suspended",
        "Dropped/Resigned",
        "Revoked",
        "Transfer",
        "Unknown",
        "Graduated",
    ]:
        expected = 2 if outcome == "Dropped/Resigned" else 1
        assert four_year[f"{outcome} Count"] == expected
