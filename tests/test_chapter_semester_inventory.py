from __future__ import annotations

import csv
from pathlib import Path

from scripts.build_chapter_semester_inventory import build_chapter_semester_exports


ROSTER_COLUMNS = [
    "student_id",
    "student_id_raw",
    "term_code",
    "term_label",
    "chapter",
    "chapter_raw",
    "org_status_raw",
    "org_status_bucket",
    "org_position_raw",
    "source_file",
]


def _write_roster(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=ROSTER_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_chapter_semester_inventory_filters_invalid_ids_and_excluded_positions(tmp_path: Path) -> None:
    roster_path = tmp_path / "canonical" / "latest" / "roster_term.csv"
    _write_roster(
        roster_path,
        [
            {
                "student_id": "A00000001",
                "term_code": "2025FA",
                "term_label": "Fall 2025",
                "chapter": "Beta Chapter",
                "org_status_bucket": "Active",
                "org_position_raw": "Member",
                "source_file": r"S:\Rosters\Fall 2025\Beta.xlsx",
            },
            {
                "student_id": "A00000002",
                "term_code": "2025FA",
                "term_label": "Fall 2025",
                "chapter": "Alpha Chapter",
                "org_status_bucket": "AL",
                "org_position_raw": "Member",
                "source_file": r"S:\Rosters\Fall 2025\Alpha.xlsx",
            },
            {
                "student_id": "not-an-id",
                "term_code": "2025FA",
                "term_label": "Fall 2025",
                "chapter": "Gamma Chapter",
                "org_status_bucket": "Active",
                "org_position_raw": "Member",
            },
            {
                "student_id": "A00000003",
                "term_code": "2025FA",
                "term_label": "Fall 2025",
                "chapter": "Alpha Chapter",
                "org_status_bucket": "Active",
                "org_position_raw": "Advisor",
            },
            {
                "student_id": "A00000004",
                "term_code": "2026SP",
                "term_label": "Spring 2026",
                "chapter": "Alpha Chapter",
                "org_status_bucket": "T",
                "org_position_raw": "Member",
            },
        ],
    )

    result = build_chapter_semester_exports(roster_path, tmp_path / "exports")
    inventory = _read_csv(result.inventory_path)

    assert result.source_rows == 5
    assert result.valid_rows == 3
    assert result.invalid_id_rows == 1
    assert result.excluded_position_rows == 1
    assert [(row["term_label"], row["chapter"]) for row in inventory] == [
        ("Fall 2025", "Alpha Chapter"),
        ("Fall 2025", "Beta Chapter"),
        ("Spring 2026", "Alpha Chapter"),
    ]
    assert inventory[0]["unique_valid_banner_ids"] == "1"
    assert inventory[0]["early_alumni_count"] == "1"
    assert inventory[0]["source_files"] == "Alpha.xlsx"
    assert inventory[2]["transfer_count"] == "1"


def test_chapter_lifecycle_template_flags_gaps_between_seen_terms(tmp_path: Path) -> None:
    roster_path = tmp_path / "canonical" / "latest" / "roster_term.csv"
    _write_roster(
        roster_path,
        [
            {
                "student_id": "A00000001",
                "term_code": "2024FA",
                "term_label": "Fall 2024",
                "chapter": "Alpha Chapter",
                "org_status_bucket": "Active",
            },
            {
                "student_id": "A00000002",
                "term_code": "2025SP",
                "term_label": "Spring 2025",
                "chapter": "Beta Chapter",
                "org_status_bucket": "Active",
            },
            {
                "student_id": "A00000003",
                "term_code": "2025FA",
                "term_label": "Fall 2025",
                "chapter": "Alpha Chapter",
                "org_status_bucket": "Graduated",
            },
        ],
    )

    result = build_chapter_semester_exports(roster_path, tmp_path / "exports")
    lifecycle = {row["chapter"]: row for row in _read_csv(result.lifecycle_review_path)}
    matrix = {row["chapter"]: row for row in _read_csv(result.matrix_path)}

    assert lifecycle["Alpha Chapter"]["possible_gap_count_between_first_and_last_seen"] == "1"
    assert lifecycle["Alpha Chapter"]["possible_roster_gaps_between_first_and_last_seen"] == "Spring 2025"
    assert matrix["Alpha Chapter"]["Fall 2024"] == "1"
    assert matrix["Alpha Chapter"]["Spring 2025"] == ""
    assert matrix["Alpha Chapter"]["Fall 2025"] == "1"
