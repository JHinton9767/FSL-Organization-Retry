import sqlite3
from pathlib import Path

from openpyxl import Workbook

from src.sqlCompile import OUTPUT_COLUMNS, build_sql_compile_frame, sqlCompile


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

    assert columns == OUTPUT_COLUMNS
    assert rows == [("Fall 2025", "Alpha Sigma Phi", "A01234567", "RS")]
