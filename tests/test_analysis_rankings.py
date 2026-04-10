import pandas as pd

from app.analysis import summarize_metric_by_group
from app.models import MetricDefinition


def test_group_summary_respects_min_n() -> None:
    frame = pd.DataFrame(
        {
            "student_id": ["1", "2", "3", "4"],
            "chapter": ["A", "A", "B", "C"],
            "graduated_eventual": [True, False, True, True],
            "graduated_eventual_measurable": [True, True, True, True],
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
    result = summarize_metric_by_group(frame, metric, "chapter", min_n=2)
    assert result["Group"].tolist() == ["A"]

