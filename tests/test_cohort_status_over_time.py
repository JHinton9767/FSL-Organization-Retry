import pandas as pd

from src.build_canonical_pipeline import build_cohort_status_over_time


def _status_rows(table: pd.DataFrame, cohort: str, checkpoint: str) -> pd.DataFrame:
    rows = table.loc[table["cohort_term"].eq(cohort) & table["checkpoint"].eq(checkpoint)].copy()
    return rows.set_index("status")


def test_cohort_status_over_time_tracks_active_graduated_and_unknown() -> None:
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
            "roster_present": ["No", "Yes"],
        }
    )

    table = build_cohort_status_over_time(summary, longitudinal)

    baseline = _status_rows(table, "Fall 2019", "Cohort Year")
    four_year = _status_rows(table, "Fall 2019", "4 Year")
    assert int(baseline.loc["Active", "student_count"]) == 3
    assert int(baseline.loc["Graduated", "student_count"]) == 0
    assert int(baseline.loc["Unknown", "student_count"]) == 0
    assert int(four_year.loc["Active", "student_count"]) == 1
    assert int(four_year.loc["Graduated", "student_count"]) == 1
    assert int(four_year.loc["Unknown", "student_count"]) == 1
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
            "student_id": ["A00000001", "A99999999"],
            "term_code": ["2023FA", "2023FA"],
            "observed_term_sort": [20233, 20233],
            "academic_present": ["No", "No"],
            "roster_present": ["No", "Yes"],
        }
    )

    table = build_cohort_status_over_time(summary, longitudinal)
    four_year = _status_rows(table, "Fall 2019", "4 Year")

    assert int(four_year.loc["Graduated", "student_count"]) == 0
    assert int(four_year.loc["Unknown", "student_count"]) == 1


def test_cohort_status_over_time_uses_later_roster_presence_and_skips_future_checkpoints() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002"],
            "join_term": ["Fall 2019", "Fall 2019"],
            "is_graduated": [False, False],
            "graduation_term_code": ["", ""],
            "graduation_term": ["", ""],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["A00000001", "A99999999"],
            "term_code": ["2024SP", "2024SP"],
            "observed_term_sort": [20241, 20241],
            "academic_present": ["No", "No"],
            "roster_present": ["Yes", "Yes"],
        }
    )

    table = build_cohort_status_over_time(summary, longitudinal, max_years=6)
    four_year = _status_rows(table, "Fall 2019", "4 Year")

    assert int(four_year.loc["Active", "student_count"]) == 1
    assert int(four_year.loc["Unknown", "student_count"]) == 1
    assert not table["checkpoint"].eq("5 Year").any()


def test_cohort_status_over_time_uses_latest_full_roster_marker_over_partial_future_roster() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002"],
            "join_term": ["Fall 2020", "Fall 2020"],
            "is_graduated": [False, False],
            "graduation_term_code": ["", ""],
            "graduation_term": ["", ""],
            "current_active_roster_term_code": ["2026SP", "2026SP"],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002"],
            "term_code": ["2026SP", "2026FA"],
            "observed_term_sort": [20261, 20263],
            "roster_present": ["Yes", "Yes"],
            "chapter": ["Alpha", "Beta"],
            "org_status_bucket": ["Active", "Active"],
        }
    )

    table = build_cohort_status_over_time(summary, longitudinal, max_years=6)

    assert table["checkpoint"].tolist()[-9:] == ["5 Year"] * 9
    assert not table["checkpoint"].eq("6 Year").any()


def test_cohort_status_over_time_marks_chapter_kicked_from_roster_gap() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002"],
            "join_term": ["Fall 2020", "Fall 2020"],
            "is_graduated": [False, False],
            "graduation_term_code": ["", ""],
            "graduation_term": ["", ""],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002", "A00000002"],
            "term_code": ["2020FA", "2020FA", "2021FA"],
            "observed_term_sort": [20203, 20203, 20213],
            "roster_present": ["Yes", "Yes", "Yes"],
            "chapter": ["Alpha", "Beta", "Beta"],
            "org_status_bucket": ["Active", "Active", "Active"],
        }
    )

    table = build_cohort_status_over_time(summary, longitudinal, max_years=1)
    one_year = _status_rows(table, "Fall 2020", "1 Year")

    assert int(one_year.loc["Chapter Kicked", "student_count"]) == 1
    assert int(one_year.loc["Unknown", "student_count"]) == 0
    assert int(one_year.loc["Active", "student_count"]) == 1


def test_cohort_status_over_time_marks_chapter_kicked_from_confirmed_event() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002"],
            "join_term": ["Fall 2020", "Fall 2020"],
            "is_graduated": [False, False],
            "graduation_term_code": ["", ""],
            "graduation_term": ["", ""],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002", "A00000002"],
            "term_code": ["2020FA", "2020FA", "2021FA"],
            "observed_term_sort": [20203, 20203, 20213],
            "roster_present": ["Yes", "Yes", "Yes"],
            "chapter": ["Alpha", "Beta", "Beta"],
            "org_status_bucket": ["Active", "Active", "Active"],
        }
    )
    chapter_status_events = pd.DataFrame(
        {
            "chapter": ["Alpha"],
            "event_type": ["Chapter Kicked"],
            "effective_term": ["Fall 2020"],
            "confidence": ["Confirmed"],
            "active": ["Yes"],
        }
    )

    table = build_cohort_status_over_time(
        summary,
        longitudinal,
        max_years=1,
        chapter_status_events=chapter_status_events,
    )
    one_year = _status_rows(table, "Fall 2020", "1 Year")

    assert int(one_year.loc["Chapter Kicked", "student_count"]) == 1
    assert int(one_year.loc["Active", "student_count"]) == 1


def test_cohort_status_over_time_later_roster_supersedes_manual_chapter_kicked() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001"],
            "join_term": ["Fall 2020"],
            "is_graduated": [False],
            "graduation_term_code": [""],
            "graduation_term": [""],
            "manual_outcome_status": ["Chapter Kicked"],
            "manual_outcome_term": ["Fall 2020"],
        }
    )
    longitudinal = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000001"],
            "term_code": ["2020FA", "2021FA"],
            "observed_term_sort": [20203, 20213],
            "roster_present": ["Yes", "Yes"],
            "chapter": ["Alpha", "Beta"],
            "org_status_bucket": ["Active", "Active"],
        }
    )

    table = build_cohort_status_over_time(summary, longitudinal, max_years=1)
    one_year = _status_rows(table, "Fall 2020", "1 Year")

    assert int(one_year.loc["Active", "student_count"]) == 1
    assert int(one_year.loc["Chapter Kicked", "student_count"]) == 0
