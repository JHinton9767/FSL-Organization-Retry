import pandas as pd

from app.analysis import summarize_metric_by_group
from app.metrics_engine import ALL_STUDENTS_LABEL, RESOLVED_OUTCOMES_ONLY_LABEL, compute_metric_views
from app.models import MetricDefinition
from app.status_framework import build_outcome_resolution_fields
from src.shared_utils import ROSTER_DISAPPEARED_UNKNOWN


def test_compute_metric_views_preserves_full_and_adds_resolved_only() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1", "2", "3"],
            "graduated_eventual": [True, False, False],
            "graduated_eventual_measurable": [True, True, True],
            "resolved_outcomes_only_flag": [True, False, False],
        }
    )
    metric = MetricDefinition(
        key="grad",
        display_name="Grad",
        category="Graduation",
        kind="rate_bool",
        source_table="summary",
        numerator_field="graduated_eventual",
        denominator_field="graduated_eventual_measurable",
        format="percent",
    )

    views = compute_metric_views(frame, metric)
    assert views["all"]["denominator"] == 3
    assert views["resolved_only"]["denominator"] == 1
    assert views["all"]["value"] == (1 / 3)
    assert views["resolved_only"]["value"] == 1.0
    assert views["excluded_active_unknown_n"] == 2


def test_graduation_requires_confirmed_evidence() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1", "2", "3"],
            "latest_outcome_bucket": ["Graduated", "Graduated", "Unknown"],
            "latest_roster_status_bucket": ["Unknown", "Graduated", "Unknown"],
            "active_flag": ["No", "No", "No"],
            "graduated_eventual": ["Yes", "Yes", "No"],
            "graduation_term_code": ["", "2024SP", ""],
            "outcome_evidence_source": ["", "Roster status", ""],
            "source_logic": ["canonical_pipeline", "canonical_pipeline", "canonical_pipeline"],
        }
    )

    result = build_outcome_resolution_fields(frame, {})

    assert result.loc[0, "outcome_resolution_group"] == "Truly Unknown / Unresolved"
    assert bool(result.loc[0, "is_graduated"]) is False
    assert bool(result.loc[0, "graduation_status_without_evidence"]) is True
    assert result.loc[1, "outcome_resolution_group"] == "Graduated"
    assert bool(result.loc[1, "is_graduated"]) is True


def test_academic_graduation_term_alone_does_not_count() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1"],
            "latest_outcome_bucket": ["Graduated"],
            "latest_roster_status_bucket": ["Unknown"],
            "active_flag": ["No"],
            "graduated_eventual": ["Yes"],
            "graduation_term_code": ["2024SP"],
            "outcome_evidence_source": ["Academic graduation term"],
            "source_logic": ["canonical_pipeline"],
        }
    )

    result = build_outcome_resolution_fields(frame, {})

    assert result.loc[0, "outcome_resolution_group"] == "Truly Unknown / Unresolved"
    assert bool(result.loc[0, "is_graduated"]) is False
    assert bool(result.loc[0, "graduation_status_without_evidence"]) is True


def test_graduation_list_alone_does_not_count_without_roster_confirmation() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1"],
            "latest_outcome_bucket": ["Graduated"],
            "latest_roster_status_bucket": ["Unknown"],
            "active_flag": ["No"],
            "graduated_eventual": ["Yes"],
            "graduation_term_code": ["2024SP"],
            "outcome_evidence_source": ["Graduation list only; no Copy of Rosters confirmation"],
            "source_logic": ["canonical_pipeline"],
        }
    )

    result = build_outcome_resolution_fields(frame, {})

    assert result.loc[0, "outcome_resolution_group"] == "Truly Unknown / Unresolved"
    assert bool(result.loc[0, "is_graduated"]) is False
    assert bool(result.loc[0, "graduation_status_without_evidence"]) is True


def test_unconfirmed_graduation_flags_do_not_count_without_explicit_evidence() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1", "2"],
            "latest_outcome_bucket": ["Graduated", "Graduated"],
            "latest_roster_status_bucket": ["Unknown", "Graduated"],
            "active_flag": ["No", "No"],
            "graduated_eventual": [True, True],
            "outcome_evidence_source": ["Processed graduation flag (unconfirmed)", ""],
            "source_logic": ["unconfirmed_pipeline", "unconfirmed_pipeline"],
        }
    )

    result = build_outcome_resolution_fields(frame, {})

    assert result.loc[0, "outcome_resolution_group"] == "Truly Unknown / Unresolved"
    assert bool(result.loc[0, "is_graduated"]) is False
    assert result.loc[1, "outcome_resolution_group"] == "Graduated"
    assert bool(result.loc[1, "is_graduated"]) is True


def test_alumni_or_undergraduate_text_does_not_create_graduation() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1", "2"],
            "latest_outcome_bucket": ["Graduated", "Unknown"],
            "latest_roster_status_bucket": ["Alumni", "Unknown"],
            "academic_status_raw": ["Undergraduate", "Degree seeking undergraduate"],
            "active_flag": ["No", "No"],
            "graduated_eventual": [True, False],
            "outcome_evidence_source": ["", ""],
            "source_logic": ["canonical_pipeline", "canonical_pipeline"],
        }
    )

    result = build_outcome_resolution_fields(frame, {})

    assert result.loc[0, "outcome_resolution_group"] == "Truly Unknown / Unresolved"
    assert bool(result.loc[0, "is_graduated"]) is False
    assert bool(result.loc[0, "graduation_status_without_evidence"]) is True
    assert result.loc[1, "outcome_resolution_group"] == "Truly Unknown / Unresolved"
    assert bool(result.loc[1, "is_graduated"]) is False


def test_roster_disappeared_unknown_is_not_treated_as_still_active() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1"],
            "latest_outcome_bucket": [ROSTER_DISAPPEARED_UNKNOWN],
            "latest_roster_status_bucket": ["Active"],
            "active_flag": ["Yes"],
            "outcome_evidence_source": ["Chapter roster disappeared from the currently active chapter list; no later explicit student outcome was observed."],
        }
    )

    result = build_outcome_resolution_fields(frame, {})

    assert result.loc[0, "outcome_resolution_group"] == "Truly Unknown / Unresolved"
    assert bool(result.loc[0, "is_unknown_outcome"]) is True
    assert bool(result.loc[0, "is_active_outcome"]) is False


def test_group_summary_can_rank_on_resolved_only_denominator() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1", "2", "3", "4"],
            "chapter": ["A", "A", "B", "B"],
            "graduated_eventual": [True, False, True, False],
            "graduated_eventual_measurable": [True, True, True, True],
            "resolved_outcomes_only_flag": [True, False, True, True],
        }
    )
    metric = MetricDefinition(
        key="grad",
        display_name="Grad",
        category="Graduation",
        kind="rate_bool",
        source_table="summary",
        numerator_field="graduated_eventual",
        denominator_field="graduated_eventual_measurable",
        format="percent",
    )

    all_summary = summarize_metric_by_group(frame, metric, "chapter", min_n=2, population_label=ALL_STUDENTS_LABEL)
    resolved_summary = summarize_metric_by_group(
        frame,
        metric,
        "chapter",
        min_n=2,
        population_label=RESOLVED_OUTCOMES_ONLY_LABEL,
    )

    assert all_summary["Group"].tolist() == ["A", "B"]
    assert resolved_summary["Group"].tolist() == ["B"]


def test_graduation_metric_counts_unique_students() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1", "1", "2"],
            "graduated_eventual": [True, True, False],
            "graduated_eventual_measurable": [True, True, True],
            "resolved_outcomes_only_flag": [True, True, True],
        }
    )
    metric = MetricDefinition(
        key="grad",
        display_name="Grad",
        category="Graduation",
        kind="rate_bool",
        source_table="summary",
        numerator_field="graduated_eventual",
        denominator_field="graduated_eventual_measurable",
        format="percent",
    )

    views = compute_metric_views(frame, metric)

    assert views["all"]["numerator"] == 1
    assert views["all"]["denominator"] == 2
    assert views["all"]["value"] == 0.5
