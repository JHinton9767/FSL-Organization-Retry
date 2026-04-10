import pandas as pd
import pytest

from app.legacy_bridge import _validate_loaded_tables


def test_processed_loader_validation_requires_master_dataset() -> None:
    with pytest.raises(ValueError, match="Required table missing: master_dataset"):
        _validate_loaded_tables(
            "processed",
            {
                "student_summary": pd.DataFrame({"student_id": ["1"]}),
            },
        )


def test_enhanced_loader_validation_warns_when_longitudinal_missing() -> None:
    warnings = _validate_loaded_tables(
        "enhanced",
        {
            "student_summary": pd.DataFrame({"Student ID": ["1"]}),
            "cohort_metrics": pd.DataFrame(
                {
                    "Metric Group": ["Graduation"],
                    "Metric Label": ["Observed 6-Year Graduation Rate"],
                    "Cohort": ["Fall 2020"],
                }
            ),
        },
    )
    assert warnings == ["Master_Longitudinal was not available, so observed-term trend views are limited."]
