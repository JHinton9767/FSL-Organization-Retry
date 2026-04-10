from pathlib import Path

import pandas as pd

from app.legacy_bridge import _combine_uploaded_sources
from src.greek_life_pipeline import build_alias_lookup


def test_uploaded_sources_apply_column_aliases(tmp_path: Path) -> None:
    csv_path = tmp_path / "academic_one.csv"
    parquet_path = tmp_path / "academic_two.parquet"

    pd.DataFrame(
        {
            "Student ID": ["1"],
            "First Name": ["Alex"],
            "Last Name": ["Lee"],
            "Term GPA": [3.4],
        }
    ).to_csv(csv_path, index=False)

    pd.DataFrame(
        {
            "banner id": ["2"],
            "given name": ["Jamie"],
            "surname": ["Ng"],
            "overall gpa": [3.8],
        }
    ).to_parquet(parquet_path, index=False)

    combined = _combine_uploaded_sources([csv_path, parquet_path], "academic", build_alias_lookup())
    assert {"student_id", "first_name", "last_name"}.issubset(set(combined.columns))
    assert "gpa_term" in combined.columns or "gpa_cum" in combined.columns

