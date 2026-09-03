import sqlite3
from pathlib import Path

import pandas as pd

from src.sqlCompile import ROSTER_INVENTORY_COLUMNS, write_sqlite
from src.sqlCompile_cohort import (
    MANUAL_STATUS_COLUMNS,
    ZERO_MEMBER_PERIOD_COLUMNS,
    build_new_member_cohort_report,
    build_new_member_cohort_tables,
    normalize_status_code,
    outcome_bucket,
    write_report_csvs,
)


def _inventory(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=ROSTER_INVENTORY_COLUMNS)


def test_inactive_suspended_status_variants_share_one_outcome_bucket() -> None:
    statuses = ["I", "Inactive", "S", "Suspended", "I/S", "I / S", "Inactive/Suspended"]

    buckets = {outcome_bucket(normalize_status_code(status), needs_manual_review=False) for status in statuses}

    assert buckets == {"Inactive/Suspended"}


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


def test_new_member_cohort_marks_students_lost_with_disappeared_chapter() -> None:
    compiled = pd.DataFrame(
        [
            {"Semester": "Fall 2025", "Chapter": "Alpha Sigma Phi", "Student ID": "A1", "Status": "N"},
            {"Semester": "Fall 2025", "Chapter": "Alpha Sigma Phi", "Student ID": "A2", "Status": "N"},
            {"Semester": "Fall 2025", "Chapter": "Beta Theta Pi", "Student ID": "B1", "Status": "N"},
            {"Semester": "Spring 2026", "Chapter": "Alpha Sigma Phi", "Student ID": "A1", "Status": "A"},
            {"Semester": "Spring 2026", "Chapter": "Alpha Sigma Phi", "Student ID": "A2", "Status": "A"},
            {"Semester": "Spring 2026", "Chapter": "Beta Theta Pi", "Student ID": "B1", "Status": "A"},
            {"Semester": "Fall 2026", "Chapter": "Beta Theta Pi", "Student ID": "B1", "Status": "A"},
        ]
    )
    inventory = _inventory(
        [
            {"Semester": "Fall 2025", "Chapter": "Alpha Sigma Phi", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Fall 2025/Final/Alpha.xlsx", "Source Sheet": "Roster", "Student Rows": 2},
            {"Semester": "Fall 2025", "Chapter": "Beta Theta Pi", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Fall 2025/Final/Beta.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
            {"Semester": "Spring 2026", "Chapter": "Alpha Sigma Phi", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Spring 2026/Final/Alpha.xlsx", "Source Sheet": "Roster", "Student Rows": 2},
            {"Semester": "Spring 2026", "Chapter": "Beta Theta Pi", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Spring 2026/Final/Beta.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
            {"Semester": "Fall 2026", "Chapter": "Beta Theta Pi", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Fall 2026/Final/Beta.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
        ]
    )

    timeline, outcomes, review, summary, _ = build_new_member_cohort_tables(
        compiled,
        pd.DataFrame(columns=MANUAL_STATUS_COLUMNS),
        roster_inventory=inventory,
        cohort_semesters=["Fall 2025"],
    )

    indexed = outcomes.set_index("Student ID")
    assert indexed.loc["A1", "Final Outcome Bucket"] == "Chapter Kicked"
    assert indexed.loc["A2", "Final Outcome Bucket"] == "Chapter Kicked"
    assert indexed.loc["A1", "Needs Manual Form Review"] == "No"
    assert indexed.loc["B1", "Needs Manual Form Review"] == "Yes"
    assert review["Student ID"].tolist() == ["B1"]
    assert set(timeline.loc[timeline["Status Code"].eq("CK"), "Student ID"]) == {"A1", "A2"}

    summary_lookup = summary.set_index(["Metric", "Outcome Bucket"])
    assert summary_lookup.loc[("Outcome Bucket Rate", "Chapter Kicked"), "Student Count"] == 2
    assert summary_lookup.loc[("Known Non-Graduate Exit Rate", "Known Non-Graduate Exit"), "Student Count"] == 2


def test_new_member_cohort_marks_midsemester_chapter_disappearance_from_roster_passes() -> None:
    compiled = pd.DataFrame(
        [
            {"Semester": "Fall 2025", "Chapter": "Alpha Sigma Phi", "Student ID": "A1", "Status": "N"},
            {"Semester": "Fall 2025", "Chapter": "Beta Theta Pi", "Student ID": "B1", "Status": "N"},
        ]
    )
    inventory = _inventory(
        [
            {"Semester": "Fall 2025", "Chapter": "Alpha Sigma Phi", "Roster Pass": "Initial", "Roster Pass Priority": 1, "Source File": "Fall 2025/Initial/Alpha.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
            {"Semester": "Fall 2025", "Chapter": "Beta Theta Pi", "Roster Pass": "Initial", "Roster Pass Priority": 1, "Source File": "Fall 2025/Initial/Beta.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
            {"Semester": "Fall 2025", "Chapter": "Beta Theta Pi", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Fall 2025/Final/Beta.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
        ]
    )

    _, outcomes, review, _, _ = build_new_member_cohort_tables(
        compiled,
        pd.DataFrame(columns=MANUAL_STATUS_COLUMNS),
        roster_inventory=inventory,
        cohort_semesters=["Fall 2025"],
    )

    indexed = outcomes.set_index("Student ID")
    assert indexed.loc["A1", "Final Outcome Bucket"] == "Chapter Kicked"
    assert indexed.loc["B1", "Final Outcome Bucket"] == "New Member / No Later Status"
    assert review.empty


def test_new_member_cohort_marks_students_lost_during_internal_chapter_gap() -> None:
    compiled = pd.DataFrame(
        [
            {"Semester": "Spring 2017", "Chapter": "Delta Tau Delta", "Student ID": "A1", "Status": "N"},
            {"Semester": "Fall 2017", "Chapter": "Delta Tau Delta", "Student ID": "A1", "Status": "A"},
            {"Semester": "Spring 2018", "Chapter": "Beta Theta Pi", "Student ID": "B1", "Status": "N"},
            {"Semester": "Fall 2018", "Chapter": "Beta Theta Pi", "Student ID": "B1", "Status": "A"},
            {"Semester": "Spring 2019", "Chapter": "Delta Tau Delta", "Student ID": "D2", "Status": "N"},
        ]
    )
    inventory = _inventory(
        [
            {"Semester": "Spring 2017", "Chapter": "Delta Tau Delta", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Spring 2017/Final/Delta.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
            {"Semester": "Fall 2017", "Chapter": "Delta Tau Delta", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Fall 2017/Final/Delta.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
            {"Semester": "Spring 2018", "Chapter": "Beta Theta Pi", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Spring 2018/Final/Beta.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
            {"Semester": "Fall 2018", "Chapter": "Beta Theta Pi", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Fall 2018/Final/Beta.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
            {"Semester": "Spring 2019", "Chapter": "Delta Tau Delta", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Spring 2019/Final/Delta.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
        ]
    )

    timeline, outcomes, review, _, _ = build_new_member_cohort_tables(
        compiled,
        pd.DataFrame(columns=MANUAL_STATUS_COLUMNS),
        roster_inventory=inventory,
        zero_member_periods=pd.DataFrame(columns=ZERO_MEMBER_PERIOD_COLUMNS),
        cohort_semesters=["Spring 2017"],
    )

    indexed = outcomes.set_index("Student ID")
    assert indexed.loc["A1", "Final Outcome Bucket"] == "Chapter Kicked"
    assert indexed.loc["A1", "Needs Manual Form Review"] == "No"
    assert review.empty
    chapter_kicked_row = timeline.loc[timeline["Student ID"].eq("A1") & timeline["Status Code"].eq("CK")].iloc[0]
    assert chapter_kicked_row["Semester"] == "Spring 2018"


def test_new_member_cohort_does_not_mark_zero_member_gap_as_chapter_kicked() -> None:
    compiled = pd.DataFrame(
        [
            {"Semester": "Spring 2017", "Chapter": "Alpha Kappa Alpha", "Student ID": "A1", "Status": "N"},
            {"Semester": "Fall 2017", "Chapter": "Alpha Kappa Alpha", "Student ID": "A1", "Status": "A"},
            {"Semester": "Spring 2018", "Chapter": "Beta Theta Pi", "Student ID": "B1", "Status": "N"},
            {"Semester": "Fall 2018", "Chapter": "Beta Theta Pi", "Student ID": "B1", "Status": "A"},
            {"Semester": "Spring 2019", "Chapter": "Alpha Kappa Alpha", "Student ID": "A2", "Status": "N"},
        ]
    )
    inventory = _inventory(
        [
            {"Semester": "Spring 2017", "Chapter": "Alpha Kappa Alpha", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Spring 2017/Final/AKA.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
            {"Semester": "Fall 2017", "Chapter": "Alpha Kappa Alpha", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Fall 2017/Final/AKA.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
            {"Semester": "Spring 2018", "Chapter": "Beta Theta Pi", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Spring 2018/Final/Beta.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
            {"Semester": "Fall 2018", "Chapter": "Beta Theta Pi", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Fall 2018/Final/Beta.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
            {"Semester": "Spring 2019", "Chapter": "Alpha Kappa Alpha", "Roster Pass": "Final", "Roster Pass Priority": 3, "Source File": "Spring 2019/Final/AKA.xlsx", "Source Sheet": "Roster", "Student Rows": 1},
        ]
    )
    zero_member_periods = pd.DataFrame(
        [
            {
                "Chapter": "Alpha Kappa Alpha",
                "Start Semester": "Spring 2018",
                "End Semester": "Fall 2018",
                "Notes": "Recognized chapter with no active members.",
            }
        ],
        columns=ZERO_MEMBER_PERIOD_COLUMNS,
    )

    timeline, outcomes, review, _, _ = build_new_member_cohort_tables(
        compiled,
        pd.DataFrame(columns=MANUAL_STATUS_COLUMNS),
        roster_inventory=inventory,
        zero_member_periods=zero_member_periods,
        cohort_semesters=["Spring 2017"],
    )

    indexed = outcomes.set_index("Student ID")
    assert indexed.loc["A1", "Final Outcome Bucket"] == "Needs Manual Form Review"
    assert indexed.loc["A1", "Needs Manual Form Review"] == "Yes"
    assert review["Student ID"].tolist() == ["A1"]
    assert timeline.loc[timeline["Student ID"].eq("A1") & timeline["Status Code"].eq("CK")].empty


def test_report_csv_writer_uses_timestamped_fallback_when_csv_is_locked(tmp_path: Path, monkeypatch) -> None:
    frame = pd.DataFrame([{"value": "x"}])
    original_to_csv = pd.DataFrame.to_csv

    def flaky_to_csv(self, path_or_buf=None, *args, **kwargs):
        if Path(path_or_buf).name == "new_member_rate_summary.csv":
            raise PermissionError("locked")
        return original_to_csv(self, path_or_buf, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_csv", flaky_to_csv)

    output_dir, csv_paths, warnings = write_report_csvs(
        tmp_path,
        ["Fall 2025"],
        timeline=frame,
        outcomes=frame,
        review=frame,
        summary=frame,
    )

    assert output_dir == tmp_path / "fall_2025"
    assert csv_paths["timeline"] == output_dir / "new_member_timeline.csv"
    assert csv_paths["summary"].name.startswith("new_member_rate_summary_")
    assert csv_paths["summary"].exists()
    assert warnings


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
