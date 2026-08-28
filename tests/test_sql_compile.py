import sqlite3
from pathlib import Path

from openpyxl import Workbook

from src.sqlCompile import (
    OUTPUT_COLUMNS,
    ROSTER_INVENTORY_TABLE,
    STUDENT_NAME_TABLE,
    build_sql_compile_frame,
    sqlCompile,
)


def _write_roster(path: Path, title: str, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Roster"
    sheet.append([title, "", "", "", ""])
    sheet.append(["Last Name", "First Name", "Banner ID", "Position", "Status"])
    for row in rows:
        sheet.append(row)
    workbook.save(path)


def test_sql_compile_resolves_statuses_by_semester_student_id(tmp_path: Path) -> None:
    root = tmp_path / "Rosters"
    _write_roster(
        root / "Fall 2025" / "Alpha Sigma Phi roster.xlsx",
        "Alpha Sigma Phi Fall 2025 Roster",
        [
            ["Rivera", "Ana", "A01234567", "Member", "A"],
            ["Patel", "Nia", "A01234568", "Member", "A"],
        ],
    )
    _write_roster(
        root / "Fall 2025" / "Delta Zeta roster.xlsx",
        "Delta Zeta Fall 2025 Roster",
        [
            ["Rivera", "Ana", "A01234567", "Member", "D"],
            ["Patel", "Nia", "A01234568", "Member", "N"],
        ],
    )
    _write_roster(
        root / "Spring 2026" / "Alpha Sigma Phi roster.xlsx",
        "Alpha Sigma Phi Spring 2026 Roster",
        [["Rivera", "Ana", "A01234567", "Member", "A"]],
    )

    frame, issues, source_file_count = build_sql_compile_frame([root])

    assert source_file_count == 3
    assert issues.empty
    assert frame.columns.tolist() == OUTPUT_COLUMNS
    assert frame.to_dict("records") == [
        {"Semester": "Fall 2025", "Chapter": "Delta Zeta", "Student ID": "A01234567", "Status": "D"},
        {"Semester": "Fall 2025", "Chapter": "Delta Zeta", "Student ID": "A01234568", "Status": "N"},
        {"Semester": "Spring 2026", "Chapter": "Alpha Sigma Phi", "Student ID": "A01234567", "Status": "A"},
    ]


def test_sql_compile_uses_initial_updated_final_as_same_semester_tie_breaker(tmp_path: Path) -> None:
    root = tmp_path / "Rosters"
    _write_roster(
        root / "Fall 2025" / "Initial" / "Alpha Sigma Phi roster.xlsx",
        "Alpha Sigma Phi Fall 2025 Initial Roster",
        [
            ["Rivera", "Ana", "A01234567", "Member", "A"],
            ["Patel", "Nia", "A01234568", "Member", "D"],
            ["Chen", "Leo", "A01234569", "Member", "D"],
        ],
    )
    _write_roster(
        root / "Fall 2025" / "Updated" / "Alpha Sigma Phi roster.xlsx",
        "Alpha Sigma Phi Fall 2025 Updated Roster",
        [
            ["Rivera", "Ana", "A01234567", "Member", "N"],
            ["Patel", "Nia", "A01234568", "Member", "A"],
            ["Chen", "Leo", "A01234569", "Member", "RS"],
        ],
    )
    _write_roster(
        root / "Fall 2025" / "Final" / "Alpha Sigma Phi roster.xlsx",
        "Alpha Sigma Phi Fall 2025 Final Roster",
        [
            ["Rivera", "Ana", "A01234567", "Member", "A"],
            ["Patel", "Nia", "A01234568", "Member", "A"],
            ["Chen", "Leo", "A01234569", "Member", "S"],
        ],
    )

    frame, issues, source_file_count = build_sql_compile_frame([root])

    assert source_file_count == 3
    assert issues.empty
    assert frame.to_dict("records") == [
        {"Semester": "Fall 2025", "Chapter": "Alpha Sigma Phi", "Student ID": "A01234567", "Status": "N"},
        {"Semester": "Fall 2025", "Chapter": "Alpha Sigma Phi", "Student ID": "A01234568", "Status": "D"},
        {"Semester": "Fall 2025", "Chapter": "Alpha Sigma Phi", "Student ID": "A01234569", "Status": "S"},
    ]


def test_sql_compile_writes_sqlite_table_with_requested_columns(tmp_path: Path) -> None:
    root = tmp_path / "Rosters"
    _write_roster(
        root / "Fall 2025" / "Alpha Sigma Phi roster.xlsx",
        "Alpha Sigma Phi Fall 2025 Roster",
        [["Rivera", "Ana", "A01234567", "Member", "RS"]],
    )
    output = tmp_path / "sqlCompile.sqlite"

    result = sqlCompile(input_roots=[root], output_path=output)

    assert result.output_path == output.resolve()
    assert result.row_count == 1
    with sqlite3.connect(output) as connection:
        columns = [row[1] for row in connection.execute('PRAGMA table_info("sqlCompile")')]
        rows = connection.execute('SELECT "Semester", "Chapter", "Student ID", "Status" FROM "sqlCompile"').fetchall()
        inventory_rows = connection.execute(f'SELECT COUNT(*) FROM "{ROSTER_INVENTORY_TABLE}"').fetchone()[0]
        name_rows = connection.execute(
            f'SELECT "Student ID", "Student Name" FROM "{STUDENT_NAME_TABLE}"'
        ).fetchall()

    assert columns == OUTPUT_COLUMNS
    assert rows == [("Fall 2025", "Alpha Sigma Phi", "A01234567", "RS")]
    assert inventory_rows == 1
    assert name_rows == [("A01234567", "Ana Rivera")]
