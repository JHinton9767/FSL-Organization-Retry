import pandas as pd

from app.analysis import build_chapter_health_dashboard, chapter_health_options


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
