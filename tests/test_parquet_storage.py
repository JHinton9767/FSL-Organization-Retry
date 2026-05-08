from pathlib import Path

import pandas as pd

from app.data_loader import CANONICAL_REQUIRED_FILES, _read_canonical_tables
from src.build_canonical_pipeline import write_frame


def _minimal_canonical_frame(table_key: str) -> pd.DataFrame:
    if table_key == "master_longitudinal":
        return pd.DataFrame({"student_id": ["A00000001"], "term_code": ["2026SP"]})
    if table_key == "cohort_metrics":
        return pd.DataFrame({"Metric Group": ["Graduation"], "Metric Label": ["Rate"], "Cohort": ["Overall"]})
    if table_key == "qa_checks":
        return pd.DataFrame({"Check Group": ["Schema"], "Check": ["Loaded"], "Status": ["Pass"]})
    return pd.DataFrame({"student_id": ["A00000001"]})


def test_write_frame_publishes_csv_and_parquet(tmp_path: Path) -> None:
    path = tmp_path / "student_summary.csv"
    frame = pd.DataFrame({"student_id": ["A00000001"], "average_gpa": [3.25]})

    write_frame(path, frame)

    assert path.exists()
    assert path.with_suffix(".parquet").exists()
    loaded = pd.read_parquet(path.with_suffix(".parquet"))
    assert loaded.to_dict(orient="records") == frame.to_dict(orient="records")


def test_canonical_loader_reads_parquet_outputs(tmp_path: Path) -> None:
    for filename, table_key in CANONICAL_REQUIRED_FILES.items():
        _minimal_canonical_frame(table_key).to_parquet(tmp_path / filename, index=False)

    tables = _read_canonical_tables(tmp_path)

    assert sorted(tables) == sorted(CANONICAL_REQUIRED_FILES.values())
    assert tables["student_summary"].loc[0, "student_id"] == "A00000001"


def test_canonical_loader_falls_back_to_csv_outputs(tmp_path: Path) -> None:
    for filename, table_key in CANONICAL_REQUIRED_FILES.items():
        csv_name = Path(filename).with_suffix(".csv").name
        _minimal_canonical_frame(table_key).to_csv(tmp_path / csv_name, index=False)

    tables = _read_canonical_tables(tmp_path)

    assert sorted(tables) == sorted(CANONICAL_REQUIRED_FILES.values())
    assert tables["master_longitudinal"].loc[0, "term_code"] == "2026SP"
