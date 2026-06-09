import pandas as pd

from src.build_canonical_pipeline import build_cohort_status_over_time


def _status_rows(table: pd.DataFrame, cohort: str, checkpoint: str) -> pd.DataFrame:
    rows = table.loc[table["cohort_term"].eq(cohort) & table["checkpoint"].eq(checkpoint)].copy()
    return rows.set_index("status")


def test_cohort_status_over_time_tracks_retained_graduated_and_not_retained() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002", "A00000003"],
            "school_entry_term": ["Fall 2019", "Fall 2019", "Fall 2019"],
            "join_term": ["Fall 2019", "Fall 2019", "Fall 2019"],
            "is_graduated": [True, False, False],
            "graduation_term_code": ["2023FA", "", ""],
            "graduation_term": ["Fall 2023", "", ""],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002"],
            "term_code": ["2023FA", "2023FA"],
            "observed_term_sort": [20233, 20233],
            "academic_present": ["Yes", "Yes"],
            "roster_present": ["No", "No"],
        }
    )

    table = build_cohort_status_over_time(summary, longitudinal)

    baseline = _status_rows(table, "Fall 2019", "Cohort Year")
    four_year = _status_rows(table, "Fall 2019", "4 Year")
    assert int(baseline.loc["Retained", "student_count"]) == 3
    assert int(baseline.loc["Graduated", "student_count"]) == 0
    assert int(baseline.loc["Not Retained", "student_count"]) == 0
    assert int(four_year.loc["Retained", "student_count"]) == 1
    assert int(four_year.loc["Graduated", "student_count"]) == 1
    assert int(four_year.loc["Not Retained", "student_count"]) == 1
    assert four_year.loc["Graduated", "share"] == 1 / 3


def test_cohort_status_over_time_uses_organization_join_term() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "school_entry_term_code": ["2018FA"],
            "school_entry_term": ["Fall 2018"],
            "join_term_code": ["2020FA"],
            "join_term": ["Fall 2020"],
            "is_graduated": [False],
        }
    )

    table = build_cohort_status_over_time(summary, pd.DataFrame())

    assert table["cohort_term"].unique().tolist() == ["Fall 2020"]
    assert table["cohort_basis"].unique().tolist() == ["organization_join_term"]


def test_cohort_status_over_time_does_not_count_unconfirmed_graduation() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "join_term": ["Fall 2019"],
            "is_graduated": [False],
            "graduated_eventual": ["Yes"],
            "graduation_term_code": ["2023FA"],
            "graduation_term": ["Fall 2023"],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "term_code": ["2023FA"],
            "observed_term_sort": [20233],
            "academic_present": ["No"],
            "roster_present": ["No"],
        }
    )

    table = build_cohort_status_over_time(summary, longitudinal)
    four_year = _status_rows(table, "Fall 2019", "4 Year")

    assert int(four_year.loc["Graduated", "student_count"]) == 0
    assert int(four_year.loc["Not Retained", "student_count"]) == 1
