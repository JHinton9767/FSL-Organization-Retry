import pandas as pd

from app.metrics_engine import compute_metric
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

