import sqlite3
from pathlib import Path

import pandas as pd

from src.sqlCompile import write_sqlite
from src.sqlCompile_cohort import (
    MANUAL_STATUS_COLUMNS,
    build_new_member_cohort_report,
    build_new_member_cohort_tables,
)


def test_new_member_cohort_flags_last_active_rows_for_manual_review() -> None:
    compiled = pd.DataFrame(
        [
            {"Semester": "Fall 2025", "Chapter": "Alpha Sigma Phi", "Student ID": "A01234567", "Status": "N"},
            {"Semester": "Spring 2026", "Chapter": "Alpha Sigma Phi", "Student ID": "A01234567", "Status": "A"},
            {"Semester": "Fall 2025", "Chapter": "Delta Zeta", "Student ID": "A01234568", "Status": "N"},
            {"Semester": "Spring 2026", "Chapter": "Delta Zeta", "Student ID": "A01234568", "Status": "D"},
            {"Semester": "Fall 2025", "Chapter": "Gamma Phi Beta", "Student ID": "A01234569", "Status": "N"},
            {"Semester": "Spring 2026", "Chapter": "Gamma Phi Beta", "Student ID": "A01234569", "Status": "A"},
            {"Semester": "Fall 2026", "Chapter": "Gamma Phi Beta", "Student ID": "A01234569", "Status": "A"},
            {"Semester": "Fall 2025", "Chapter": "Kappa Delta", "Student ID": "A01234570", "Status": "N"},
            {"Semester": "Spring 2026", "Chapter": "Kappa Delta", "Student ID": "A01234570", "Status": "G"},
        ]
    )
    manual = pd.DataFrame(
        [
            {
                "Cohort Semester": "Fall 2025",
                "Cohort Chapter": "Gamma Phi Beta",
                "Semester": "Fall 2026",
                "Chapter": "Gamma Phi Beta",
                "Student ID": "A01234569",
                "Status": "RS",
                "Notes": "Manual form found.",
            }
        ],
        columns=MANUAL_STATUS_COLUMNS,
    )

    timeline, outcomes, review, summary, selected = build_new_member_cohort_tables(
        compiled,
        manual,
        cohort_semesters=["Fall 2025"],
    )

    assert selected == ["Fall 2025"]
    gamma_timeline = timeline.loc[timeline["Student ID"].eq("A01234569")]
    assert len(gamma_timeline) == 3
    assert gamma_timeline.iloc[-1]["Source"] == "manual_status"

    indexed = outcomes.set_index("Student ID")
    assert indexed.loc["A01234567", "Needs Manual Form Review"] == "Yes"
    assert indexed.loc["A01234567", "Final Outcome Bucket"] == "Needs Manual Form Review"
    assert indexed.loc["A01234568", "Final Outcome Bucket"] == "Dropped/Inactive"
    assert indexed.loc["A01234569", "Final Outcome Bucket"] == "Resigned"
    assert indexed.loc["A01234569", "Manual Status Applied"] == "Yes"
    assert indexed.loc["A01234570", "Final Outcome Bucket"] == "Graduated"

    assert review["Student ID"].tolist() == ["A01234567"]

    summary_lookup = summary.set_index(["Metric", "Outcome Bucket"])
    assert summary_lookup.loc[("Manual Review", "Needs Manual Form Review"), "Student Count"] == 1
    assert summary_lookup.loc[("Resolved Size", "Resolved Students"), "Student Count"] == 3
    assert summary_lookup.loc[("Known Non-Graduate Exit Rate", "Known Non-Graduate Exit"), "Student Count"] == 2
    assert summary_lookup.loc[("Graduation Rate", "Graduated"), "Rate of Resolved"] == 1 / 3


def test_new_member_cohort_report_writes_sqlite_tables_and_csvs(tmp_path: Path) -> None:
    compiled = pd.DataFrame(
        [
            {"Semester": "Fall 2025", "Chapter": "Alpha Sigma Phi", "Student ID": "A01234567", "Status": "N"},
            {"Semester": "Spring 2026", "Chapter": "Alpha Sigma Phi", "Student ID": "A01234567", "Status": "A"},
        ]
    )
    database = tmp_path / "sqlCompile.sqlite"
    manual_status = tmp_path / "manual.csv"
    output_dir = tmp_path / "reports"

    write_sqlite(compiled, database)

    result = build_new_member_cohort_report(
        database_path=database,
        cohort_semesters=["Fall 2025"],
        manual_status_file=manual_status,
        output_dir=output_dir,
    )

    assert result.review_rows == 1
    assert manual_status.exists()
    assert (result.output_dir / "new_member_timeline.csv").exists()
    assert (result.output_dir / "new_member_outcomes.csv").exists()
    assert (result.output_dir / "new_member_form_review.csv").exists()
    assert (result.output_dir / "new_member_rate_summary.csv").exists()

    with sqlite3.connect(database) as connection:
        review_count = connection.execute("SELECT COUNT(*) FROM new_member_form_review").fetchone()[0]
        summary_count = connection.execute("SELECT COUNT(*) FROM new_member_rate_summary").fetchone()[0]

    assert review_count == 1
    assert summary_count > 0


def test_new_member_cohort_tables_can_build_all_semesters() -> None:
    compiled = pd.DataFrame(
        [
            {"Semester": "Fall 2025", "Chapter": "Alpha Sigma Phi", "Student ID": "A01234567", "Status": "N"},
            {"Semester": "Spring 2026", "Chapter": "Alpha Sigma Phi", "Student ID": "A01234567", "Status": "D"},
            {"Semester": "Spring 2026", "Chapter": "Delta Zeta", "Student ID": "A01234568", "Status": "N"},
            {"Semester": "Fall 2026", "Chapter": "Delta Zeta", "Student ID": "A01234568", "Status": "G"},
        ]
    )

    _, outcomes, _, summary, selected = build_new_member_cohort_tables(
        compiled,
        pd.DataFrame(columns=MANUAL_STATUS_COLUMNS),
        all_cohorts=True,
    )

    assert selected == ["Fall 2025", "Spring 2026"]
    assert outcomes["Cohort Semester"].tolist() == ["Fall 2025", "Spring 2026"]
    assert set(summary["Cohort Semester"]) == {"Fall 2025", "Spring 2026"}
