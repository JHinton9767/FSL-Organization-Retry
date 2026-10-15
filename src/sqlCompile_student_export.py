"""Export the complete observed FSL population without applying outcome filters.

Uses only the Python standard library so the request list can be built on the
data computer without installing the dashboard or spreadsheet dependencies.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sqlite3
import sys
import tempfile
from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATABASE = ROOT / "output" / "sqlCompile" / "sqlCompile.sqlite"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "outgoing" / "fsl_student_list"
DEFAULT_NAME_CHOICES = ROOT / "config" / "sqlCompile_duplicate_name_resolutions.csv"
NAME_TABLE = "sqlCompile_student_names"
OBSERVATION_TABLE = "sqlCompile_student_name_observations"
NEW_MEMBER_TABLE = "sqlCompile_new_member_observations"
LIST_COLUMNS = ["Student ID", "Student Name"]
REVIEW_COLUMNS = [*LIST_COLUMNS, "Review Reason", "Observed Names"]


@dataclass(frozen=True)
class StudentListExport:
    list_path: Path
    review_path: Path
    student_count: int
    review_count: int
    missing_name_count: int
    conflicting_name_count: int
    warnings: tuple[str, ...]


def _resolve_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    return (path if path.is_absolute() else ROOT / path).resolve()


def _text(value: object) -> str:
    return " ".join(str(value).split()) if value is not None else ""


def _student_id(value: object) -> str:
    return _text(value).upper()


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _read_name_choices(path: Path, *, required: bool) -> dict[str, str]:
    if not path.exists() and not required:
        return {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not set(LIST_COLUMNS).issubset(reader.fieldnames or []):
            raise ValueError(f"Name choices file needs Student ID and Student Name columns: {path}")
        choices = {}
        for row in reader:
            student_id, name = _student_id(row["Student ID"]), _text(row["Student Name"])
            if student_id and name:
                # Match the dashboard's latest nonblank saved decision per ID.
                choices[student_id] = name
        return choices


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    """Replace each completed UTF-8 CSV atomically, including empty review lists."""
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8-sig", newline="", dir=path.parent,
            suffix=".tmp", delete=False,
        ) as handle:
            temporary = Path(handle.name)
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def export_student_list(
    database_path: str | Path = DEFAULT_DATABASE,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    *,
    table_name: str = "sqlCompile",
    name_choices_path: str | Path | None = None,
) -> StudentListExport:
    database = _resolve_path(database_path)
    destination = _resolve_path(output_dir)
    choices_path = _resolve_path(name_choices_path or DEFAULT_NAME_CHOICES)
    if not database.is_file():
        raise FileNotFoundError(f"SQL database not found: {database}. Run this on the computer with the populated database.")

    student_ids: set[str] = set()
    preferred: dict[str, set[str]] = defaultdict(set)
    observed: dict[str, Counter] = defaultdict(Counter)
    warnings: list[str] = []
    blank_rows = 0
    # One read-only transaction gives all tables the same SQLite snapshot.
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
        connection.execute("BEGIN")
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if table_name not in tables:
            raise ValueError(f"Table {table_name!r} is missing from {database}.")
        for table in dict.fromkeys([table_name, NAME_TABLE, OBSERVATION_TABLE, NEW_MEMBER_TABLE]):
            if table not in tables:
                continue
            identifier = _quote_identifier(table)
            columns = {row[1] for row in connection.execute(f"PRAGMA table_info({identifier})")}
            required_columns = {"Student ID"}
            if table in {NAME_TABLE, OBSERVATION_TABLE}:
                required_columns.add("Student Name")
            if not required_columns.issubset(columns):
                raise ValueError(f"Table {table!r} is missing required columns: {', '.join(sorted(required_columns - columns))}.")
            name_column = '"Student Name"' if "Student Name" in columns else "NULL"
            count_column = '"Observation Count"' if table == OBSERVATION_TABLE and "Observation Count" in columns else "1"
            for raw_id, raw_name, raw_count in connection.execute(
                f'SELECT "Student ID", {name_column}, {count_column} FROM {identifier}'
            ):
                student_id, name = _student_id(raw_id), _text(raw_name)
                if not student_id:
                    blank_rows += 1
                    continue
                student_ids.add(student_id)
                if name:
                    try:
                        count = max(1, int(raw_count))
                    except (ValueError, TypeError, OverflowError):
                        count = 1
                    observed[student_id][name] += count
                    if table == NAME_TABLE:
                        preferred[student_id].add(name)
        if OBSERVATION_TABLE not in tables:
            warnings.append("Historical name variations are unavailable in this database. Recompile the rosters to check all observed names.")
        if NAME_TABLE not in tables:
            warnings.append("The compiled name lookup is missing. Recompile the rosters to populate names; IDs without names remain in the list.")

    if not student_ids:
        raise ValueError("The SQL database contains no student IDs. No export was written. Use the populated database on the data computer.")
    choices = _read_name_choices(choices_path, required=name_choices_path is not None)
    rows: list[dict[str, str]] = []
    reviews: list[dict[str, str]] = []
    missing_names = conflicting_names = 0
    for student_id in sorted(student_ids):
        counts = observed[student_id]
        names = sorted(counts, key=lambda name: (-counts[name], name.casefold(), name))
        compiled_names = sorted(preferred[student_id], key=lambda name: (-counts[name], name.casefold(), name))
        name = choices.get(student_id) or next(iter(compiled_names or names), "")
        row = {"Student ID": student_id, "Student Name": name}
        rows.append(row)
        reasons = []
        if not name:
            missing_names += 1
            reasons.append("Missing name")
        # Ignore case/spacing only. Do not silently equate different spellings,
        # initials, punctuation, or diacritics when matching student identities.
        if len({item.casefold() for item in names}) > 1 and student_id not in choices:
            conflicting_names += 1
            reasons.append("Conflicting names; displayed name is provisional")
        if not re.fullmatch(r"A0\d{7}", student_id):
            reasons.append("Unexpected student ID format; retained for review")
        if reasons:
            reviews.append({**row, "Review Reason": "; ".join(reasons), "Observed Names": " | ".join(names)})
    if blank_rows:
        warnings.append(f"Skipped {blank_rows:,} source rows with no student ID.")
    if missing_names:
        warnings.append(f"{missing_names:,} students have no name. Their IDs are included in the main list.")
    if conflicting_names:
        warnings.append(f"{conflicting_names:,} students have unresolved name variations. Check the review file before sharing.")

    list_path = destination / "fsl_student_list.csv"
    review_path = destination / "fsl_student_name_review.csv"
    if {list_path, review_path} & {database, choices_path}:
        raise ValueError("The output paths would replace an input file. Choose a different output directory.")
    destination.mkdir(parents=True, exist_ok=True)
    _write_csv(review_path, REVIEW_COLUMNS, reviews)
    _write_csv(list_path, LIST_COLUMNS, rows)
    return StudentListExport(
        list_path, review_path, len(rows), len(reviews), missing_names,
        conflicting_names, tuple(warnings),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export every unique FSL student ID and name from sqlCompile, across all semesters and statuses.")
    parser.add_argument("--database", default=str(DEFAULT_DATABASE), help="Populated sqlCompile SQLite file.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Folder for the main list and name review CSVs.")
    parser.add_argument("--table", default="sqlCompile", help="Base roster table name.")
    parser.add_argument("--name-choices", default=None, help="Saved Student ID / Student Name corrections CSV. Defaults to config/sqlCompile_duplicate_name_resolutions.csv when present.")
    args = parser.parse_args(argv)
    try:
        result = export_student_list(
            args.database, args.output_dir, table_name=args.table,
            name_choices_path=args.name_choices,
        )
    except (OSError, sqlite3.Error, ValueError, csv.Error) as exc:
        print(f"Export failed: {exc}", file=sys.stderr)
        return 1
    print(f"Unique students: {result.student_count:,}")
    print(f"Main list: {result.list_path}")
    print(f"Name/ID review: {result.review_path} ({result.review_count:,} students)")
    for warning in result.warnings:
        print(f"WARNING: {warning}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
