import pandas as pd

from app.metrics_engine import available_metrics, compute_metric, compute_metric_views
from app.models import MetricDefinition


def test_rate_metric_uses_observed_rows_when_denominator_missing() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1", "2", "3"],
            "risk_flag": [True, False, pd.NA],
        }
    )
    metric = MetricDefinition(
        key="risk",
        display_name="Risk",
        category="QA",
        kind="rate_bool",
        source_table="summary",
        numerator_field="risk_flag",
        format="percent",
    )
    result = compute_metric(frame, metric)
    assert result["numerator"] == 1
    assert result["denominator"] == 2
    assert result["value"] == 0.5


def test_mean_metric_ignores_missing_values() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1", "2", "3"],
            "average_cumulative_gpa": [3.0, None, 4.0],
        }
    )
    metric = MetricDefinition(
        key="gpa",
        display_name="GPA",
        category="GPA",
        kind="mean",
        source_table="summary",
        value_field="average_cumulative_gpa",
        format="decimal",
    )
    result = compute_metric(frame, metric)
    assert result["denominator"] == 2
    assert result["value"] == 3.5


def test_current_active_metric_ignores_resolved_only_split() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1", "2", "3"],
            "current_active_flag": ["Yes", "No", "Yes"],
            "is_resolved_outcome": [False, True, False],
            "is_active_outcome": [True, False, True],
            "is_unknown_outcome": [False, False, False],
            "is_graduated": [False, False, False],
            "is_known_non_graduate_exit": [False, True, False],
        }
    )
    metric = MetricDefinition(
        key="active_member_count",
        display_name="Current Active Members (Most Recent Roster)",
        category="Coverage",
        kind="sum_bool",
        source_table="summary",
        value_field="current_active_flag",
        format="integer",
    )
    result = compute_metric_views(frame, metric)
    assert result["all"]["value"] == 2
    assert result["resolved_only"]["value"] == 2


def test_sum_bool_metric_is_available_from_value_field() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1", "2"],
            "current_active_flag": ["Yes", "No"],
        }
    )
    metric = MetricDefinition(
        key="active_member_count",
        display_name="Current Active Members (Most Recent Roster)",
        category="Coverage",
        kind="sum_bool",
        source_table="summary",
        value_field="current_active_flag",
        format="integer",
    )
    assert available_metrics([metric], frame, pd.DataFrame()) == [metric]
