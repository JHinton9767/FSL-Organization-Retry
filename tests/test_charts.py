import pandas as pd

from app.charts import persistence_milestone_chart


def test_persistence_milestone_chart_keeps_legend_below_cohort_labels() -> None:
    frame = pd.DataFrame(
        [
            {
                "Milestone": "1 Year<br>12 Semesters<br>n=8,471",
                "Milestone Sort": 1,
                "Outcome": "Active",
                "Share": 0.79,
                "Count": 6749,
                "Cohort Students": 8471,
                "Eligible Students": 8471,
                "Future Students": 0,
                "Label": "Active<br>79.7%<br>(n=6,749)",
            },
            {
                "Milestone": "1 Year<br>12 Semesters<br>n=8,471",
                "Milestone Sort": 1,
                "Outcome": "Graduated",
                "Share": 0.21,
                "Count": 1722,
                "Cohort Students": 8471,
                "Eligible Students": 8471,
                "Future Students": 0,
                "Label": "Graduated<br>20.3%<br>(n=1,722)",
            },
        ]
    )

    chart = persistence_milestone_chart(frame, title="1-6 Year Outcome Rates", xaxis_title="Milestone")

    assert chart.layout.legend.orientation == "h"
    assert chart.layout.legend.y <= -0.2
    assert chart.layout.margin.b >= 180
