import pandas as pd

from app.analysis import build_chapter_health_dashboard, build_advisor_intervention_queue, chapter_health_options


def test_chapter_health_options_include_historical_chapters() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1"],
            "initial_chapter": ["Current Chapter"],
            "latest_chapter": ["Current Chapter"],
            "current_active_chapter": ["Current Chapter"],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["1", "2"],
            "chapter": ["Current Chapter", "Historical Chapter"],
        }
    )

    options = chapter_health_options(summary, longitudinal)

    assert "Current Chapter" in options
    assert "Historical Chapter" in options


def test_build_chapter_health_dashboard_surfaces_roster_disappeared_unknown_cases() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2", "3"],
            "student_name": ["Grad Student", "Unknown Student", "Active Student"],
            "initial_chapter": ["Alpha", "Alpha", "Alpha"],
            "latest_chapter": ["Alpha", "Alpha", "Alpha"],
            "chapter": ["Alpha", "Alpha", "Alpha"],
            "chapter_group": ["Group A", "Group A", "Group A"],
            "council": ["IFC", "IFC", "IFC"],
            "org_type": ["Fraternity", "Fraternity", "Fraternity"],
            "family": ["FRA", "FRA", "FRA"],
            "join_term": ["Fall 2019", "Fall 2019", "Fall 2020"],
            "is_graduated": [True, False, False],
            "is_resolved_outcome": [True, False, False],
            "is_active_outcome": [False, False, True],
            "is_unknown_outcome": [False, True, False],
            "is_known_non_graduate_exit": [False, False, False],
            "current_active_chapter": ["", "", "Alpha"],
            "current_active_flag": ["No", "No", "Yes"],
            "current_active_roster_term": ["", "", "Spring 2026"],
            "retained_next_fall_measurable": ["Yes", "Yes", "Yes"],
            "retained_next_fall": ["Yes", "No", "Yes"],
            "first_year_avg_term_gpa": [3.2, 2.8, 3.0],
            "average_cumulative_gpa": [3.1, 2.7, 3.05],
            "latest_outcome_bucket": ["Graduated", "Roster Dissapeared/Unknown", "Active/Unknown"],
            "roster_disappeared_unknown_flag": ["No", "Yes", "No"],
            "outcome_evidence_source": ["Roster status", "", "Current or active signal only"],
            "data_completeness_rate": [1.0, 0.75, 0.9],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["1", "2", "3", "3"],
            "chapter": ["Alpha", "Alpha", "Alpha", "Alpha"],
            "observed_year": [2020, 2020, 2021, 2026],
            "observed_term": ["Fall 2020", "Fall 2020", "Fall 2021", "Spring 2026"],
            "observed_term_sort": [20203, 20203, 20213, 20261],
            "roster_present": ["Yes", "Yes", "Yes", "Yes"],
            "academic_present": ["Yes", "No", "Yes", "Yes"],
            "term_gpa": [3.2, None, 3.0, 3.1],
            "cumulative_gpa": [3.1, None, 3.0, 3.05],
        }
    )

    dashboard = build_chapter_health_dashboard(summary, longitudinal, "Alpha")

    assert dashboard["meta"]["council"] == "IFC"
    assert dashboard["meta"]["is_currently_active"] is True
    assert dashboard["kpis"]["current_active_members"] == 1
    assert dashboard["kpis"]["students_entering_chapter"] == 3
    assert dashboard["kpis"]["roster_disappeared_unknown"] == 1
    assert dashboard["kpis"]["resolved_graduation_rate"] == 1.0
    assert not dashboard["yearly_trend"].empty
    assert not dashboard["outcome_breakdown"].empty
    assert "Roster Dissapeared/Unknown" in dashboard["outcome_breakdown"]["Outcome"].tolist()
    assert "Unknown Student" in dashboard["review_students"]["Student Name"].tolist()


def test_build_chapter_health_dashboard_builds_risk_flags_for_low_gpa_and_unknown_share() -> None:
    summary = pd.DataFrame(
        {
            "student_id": [str(value) for value in range(1, 11)],
            "student_name": [f"Student {value}" for value in range(1, 11)],
            "initial_chapter": ["Alpha"] * 10,
            "latest_chapter": ["Alpha"] * 10,
            "chapter": ["Alpha"] * 10,
            "chapter_group": ["Group A"] * 10,
            "council": ["IFC"] * 10,
            "org_type": ["Fraternity"] * 10,
            "family": ["FRA"] * 10,
            "join_term": ["Fall 2019"] * 10,
            "is_graduated": [True, True, False, False, False, False, False, False, False, False],
            "is_resolved_outcome": [True, True, False, False, False, False, False, False, False, False],
            "is_active_outcome": [False, False, True, True, False, False, False, False, False, False],
            "is_unknown_outcome": [False, False, False, False, True, True, True, True, False, False],
            "is_known_non_graduate_exit": [False] * 10,
            "current_active_chapter": ["", "", "Alpha", "Alpha", "", "", "", "", "", ""],
            "current_active_flag": ["No", "No", "Yes", "Yes", "No", "No", "No", "No", "No", "No"],
            "current_active_roster_term": ["", "", "Spring 2026", "Spring 2026", "", "", "", "", "", ""],
            "retained_next_fall_measurable": ["Yes"] * 10,
            "retained_next_fall": ["Yes", "Yes", "Yes", "Yes", "No", "No", "No", "No", "No", "No"],
            "first_year_avg_term_gpa": [2.3, 2.4, 2.2, 2.5, 2.4, 2.5, 2.6, 2.4, 2.5, 2.4],
            "average_cumulative_gpa": [2.2, 2.3, 2.1, 2.4, 2.3, 2.4, 2.5, 2.2, 2.4, 2.3],
            "latest_outcome_bucket": [
                "Graduated",
                "Graduated",
                "Active/Unknown",
                "Active/Unknown",
                "Roster Dissapeared/Unknown",
                "Unknown",
                "Unknown",
                "Unknown",
                "Unknown",
                "Unknown",
            ],
            "roster_disappeared_unknown_flag": ["No", "No", "No", "No", "Yes", "No", "No", "No", "No", "No"],
            "outcome_evidence_source": ["Roster status", "Roster status", "", "", "", "", "", "", "", ""],
            "data_completeness_rate": [0.7] * 10,
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": [str(value) for value in range(1, 11)],
            "chapter": ["Alpha"] * 10,
            "observed_year": [2020] * 5 + [2026] * 5,
            "observed_term": ["Fall 2020"] * 5 + ["Spring 2026"] * 5,
            "observed_term_sort": [20203] * 5 + [20261] * 5,
            "roster_present": ["Yes"] * 10,
            "academic_present": ["Yes"] * 10,
            "term_gpa": [2.3] * 10,
            "cumulative_gpa": [2.3] * 10,
        }
    )

    dashboard = build_chapter_health_dashboard(summary, longitudinal, "Alpha")
    flags = dashboard["risk_flags"]

    assert not flags.empty
    assert "High unresolved outcome share" in flags["Flag"].tolist()
    assert "Low first-year GPA" in flags["Flag"].tolist()


def test_build_advisor_intervention_queue_prioritizes_active_students_with_low_gpa() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2", "3", "4"],
            "student_name": ["High Risk", "Medium Risk", "Monitor Risk", "Inactive Student"],
            "current_active_flag": ["Yes", "Yes", "Yes", "No"],
            "current_active_chapter": ["Alpha", "Alpha", "Beta", ""],
            "current_active_council": ["IFC", "IFC", "PHC", ""],
            "latest_chapter": ["Alpha", "Alpha", "Beta", "Gamma"],
            "council": ["IFC", "IFC", "PHC", "MGC"],
            "join_term": ["Fall 2022", "Fall 2021", "Spring 2023", "Fall 2020"],
            "average_cumulative_gpa": [1.9, 2.45, 3.1, 2.0],
            "first_year_avg_term_gpa": [2.2, 2.65, 3.0, 2.1],
            "data_completeness_rate": [0.95, 0.85, 0.65, 0.9],
            "latest_outcome_bucket": ["Active/Unknown", "Active/Unknown", "Unknown", "Unknown"],
            "is_unknown_outcome": [False, False, True, True],
        }
    )

    dashboard = build_advisor_intervention_queue(summary)
    queue = dashboard["queue"]
    chapter_rollup = dashboard["chapter_rollup"]

    assert dashboard["meta"]["current_active_students"] == 3
    assert dashboard["meta"]["flagged_students"] == 3
    assert queue.iloc[0]["Student Name"] == "High Risk"
    assert queue.iloc[0]["Priority"] == "High"
    assert "low cumulative GPA" in queue.iloc[1]["Risk Flags"]
    assert chapter_rollup.iloc[0]["Current Chapter"] == "Alpha"
