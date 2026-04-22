import pandas as pd

from app.analysis import summarize_metric_by_group
from app.metrics_engine import ALL_STUDENTS_LABEL, RESOLVED_OUTCOMES_ONLY_LABEL, compute_metric_views
from app.models import MetricDefinition
from app.standardize import standardize_processed_summary
from app.status_framework import build_outcome_resolution_fields


def test_processed_summary_builds_resolved_outcome_flags() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["1", "2", "3", "4"],
            "first_name": ["Alex", "Jamie", "Taylor", "Morgan"],
            "last_name": ["Lee", "Ng", "Jordan", "Patel"],
            "chapter": ["Alpha", "Alpha", "Beta", "Beta"],
            "join_term": ["Fall 2021", "Fall 2021", "Spring 2022", "Spring 2022"],
            "latest_membership_status": ["ACTIVE", "TRANSFER", "", ""],
            "major": ["Biology", "History", "Math", "English"],
            "pell_flag": ["Yes", "No", "", ""],
            "cohort": ["FTFT", "Transfer", "", ""],
            "total_earned": [45, 78, 12, 30],
            "avg_term_gpa": [3.1, 2.8, 3.5, 2.9],
            "latest_gpa_cum": [3.2, 3.0, 3.8, 3.1],
            "graduated": [False, False, True, False],
            "graduated_4yr": [False, False, False, False],
            "graduated_6yr": [False, False, True, False],
            "first_term": ["Fall 2021", "Fall 2021", "Spring 2022", "Spring 2022"],
            "first_term_sort": [20213, 20213, 20221, 20221],
            "last_term_sort": [20223, 20241, 20221, 20221],
        }
    )

    standardized = standardize_processed_summary(
        summary=summary,
        chapter_mapping=pd.DataFrame(columns=["chapter", "chapter_group", "council", "org_type", "family", "custom_group"]),
        settings={
            "high_hours_threshold": 60,
            "chapter_size_bands": [{"label": "Small", "min": 1, "max": 24}],
            "completeness_fields": ["student_id", "chapter", "join_term"],
            "outcome_resolution": {
                "priority_order": ["Graduated", "Known Non-Graduate Exit", "Still Active", "Unknown", "Other / Unmapped"],
                "group_patterns": {
                    "Graduated": ["\\bGRADUAT"],
                    "Known Non-Graduate Exit": ["\\bTRANSFER\\b"],
                    "Still Active": ["\\bACTIVE\\b"],
                    "Unknown": ["\\bUNKNOWN\\b"],
                    "Other / Unmapped": [],
                },
                "resolved_only_excluded_groups": ["Still Active", "Unknown", "Other / Unmapped"],
            },
        },
        status_code_map={"active": ["ACTIVE"], "transfer": ["TRANSFER"], "graduated": ["GRADUATED"], "inactive": [], "suspended": []},
    )

    assert standardized.loc[0, "outcome_resolution_group"] == "Still Active"
    assert bool(standardized.loc[0, "resolved_outcome_excluded_flag"]) is True
    assert standardized.loc[1, "outcome_resolution_group"] == "Resolved Non-Graduate Exit"
    assert bool(standardized.loc[1, "resolved_outcomes_only_flag"]) is True
    assert standardized.loc[2, "outcome_resolution_group"] == "Graduated"
    assert standardized.loc[3, "outcome_resolution_group"] == "Unknown"


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
            "latest_outcome_bucket": ["Graduated", "Graduated", "No Further Observation"],
            "latest_roster_status_bucket": ["Unknown", "Unknown", "Unknown"],
            "active_flag": ["No", "No", "No"],
            "graduated_eventual": ["Yes", "Yes", "No"],
            "graduation_term_code": ["", "2024SP", ""],
            "outcome_evidence_source": ["", "Academic graduation term", ""],
            "source_logic": ["canonical_pipeline", "canonical_pipeline", "canonical_pipeline"],
        }
    )

    result = build_outcome_resolution_fields(frame, {})

    assert result.loc[0, "outcome_resolution_group"] == "Truly Unknown / Unresolved"
    assert bool(result.loc[0, "is_graduated"]) is False
    assert bool(result.loc[0, "graduation_status_without_evidence"]) is True
    assert result.loc[1, "outcome_resolution_group"] == "Graduated"
    assert bool(result.loc[1, "is_graduated"]) is True


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
