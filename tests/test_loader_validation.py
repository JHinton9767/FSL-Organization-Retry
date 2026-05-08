import pandas as pd
import pytest

from app.data_loader import _validate_loaded_tables


def test_canonical_loader_validation_accepts_required_tables() -> None:
    warnings = _validate_loaded_tables(
        "canonical",
        {
            "student_summary": pd.DataFrame({"student_id": ["1"]}),
            "master_longitudinal": pd.DataFrame({"student_id": ["1"], "term_code": ["2024FA"]}),
            "cohort_metrics": pd.DataFrame(
                {
                    "Metric Group": ["Graduation"],
                    "Metric Label": ["Observed Eventual Graduation Rate"],
                    "Cohort": ["Overall"],
                }
            ),
            "qa_checks": pd.DataFrame({"Check Group": ["Schema"], "Check": ["Authoritative tables built"], "Status": ["Pass"]}),
        },
    )
    assert warnings == []


def test_canonical_loader_rejects_noncanonical_dataset_types() -> None:
    with pytest.raises(ValueError, match="Unsupported dataset type"):
        _validate_loaded_tables("processed", {})
