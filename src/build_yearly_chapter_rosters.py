from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from openpyxl import Workbook

from src.build_master_roster import autosize_columns, is_excluded_chapter, style_header
from src.build_member_tenure_report import DEFAULT_MASTER_WORKBOOK, load_master_roster


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = ROOT / "Yearly"


@dataclass(frozen=True)
class ChapterMember:
    chapter: str
    last_name: str
    first_name: str
    banner_id: str

    def as_list(self) -> List[str]:
        return [self.last_name, self.first_name, self.banner_id]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build yearly chapter roster workbooks from Master_FSL_Roster.xlsx. "
            "Each year gets its own workbook, with one sheet per chapter."
        )
    )
    parser.add_argument(
        "--master",
        default=str(DEFAULT_MASTER_WORKBOOK),
        help="Path to Master_FSL_Roster.xlsx. Default: Master_FSL_Roster.xlsx next to the code.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Folder where yearly workbooks will be written. Default: Yearly",
    )
    return parser.parse_args()


def safe_sheet_name(value: str) -> str:
    cleaned = value.strip() or "Unknown"
    for char in '[]:*?/\\':
        cleaned = cleaned.replace(char, "")
    return cleaned[:31] or "Unknown"


def dedupe_members(rows: Iterable[ChapterMember]) -> List[ChapterMember]:
    best_rows: Dict[Tuple[str, str, str, str], ChapterMember] = {}
    for row in rows:
        key = (
            row.chapter.lower(),
            row.banner_id.lower(),
            row.last_name.lower(),
            row.first_name.lower(),
        )
        existing = best_rows.get(key)
        if existing is None:
            best_rows[key] = row
            continue

        existing_has_banner = bool(existing.banner_id)
        current_has_banner = bool(row.banner_id)
        if current_has_banner and not existing_has_banner:
            best_rows[key] = row

    return sorted(
        best_rows.values(),
        key=lambda item: (
            item.last_name.lower(),
            item.first_name.lower(),
            item.banner_id.lower(),
        ),
    )


def choose_preferred_member(existing: ChapterMember, candidate: ChapterMember) -> ChapterMember:
    existing_unknown = existing.chapter.lower() == "unknown"
    candidate_unknown = candidate.chapter.lower() == "unknown"
    if existing_unknown and not candidate_unknown:
        return candidate
    return existing


def dedupe_chapter_members(rows: Iterable[ChapterMember]) -> List[ChapterMember]:
    best_rows: Dict[Tuple[str, str, str, str], ChapterMember] = {}
    for row in rows:
        if row.banner_id:
            key = (
                row.chapter.lower(),
                "banner",
                row.banner_id.lower(),
                "",
            )
        else:
            key = (
                row.chapter.lower(),
                "name",
                row.last_name.lower(),
                row.first_name.lower(),
            )

        existing = best_rows.get(key)
        if existing is None:
            best_rows[key] = row
            continue
        best_rows[key] = choose_preferred_member(existing, row)

    return list(best_rows.values())


def rows_to_yearly_chapters(master_rows) -> Dict[str, Dict[str, List[ChapterMember]]]:
    grouped: Dict[str, Dict[str, List[ChapterMember]]] = defaultdict(lambda: defaultdict(list))

    for row in master_rows:
        academic_year = row.academic_year.strip()
        chapter = row.chapter.strip() or "Unknown"
        if not academic_year or academic_year.lower() == "unknown":
            continue
        if is_excluded_chapter(chapter):
            continue
        if not any([row.last_name, row.first_name, row.banner_id]):
            continue

        grouped[academic_year][chapter].append(
            ChapterMember(
                chapter=chapter,
                last_name=row.last_name,
                first_name=row.first_name,
                banner_id=row.banner_id,
            )
        )

    return grouped


def write_year_workbook(academic_year: str, chapter_rows: Dict[str, Sequence[ChapterMember]], output_path: Path) -> None:
    wb = Workbook()
    wb.remove(wb.active)

    for chapter in sorted(chapter_rows):
        ws = wb.create_sheet(title=safe_sheet_name(chapter))
        ws.append(["Last Name", "First Name", "Banner ID"])
        style_header(ws)
        for member in dedupe_chapter_members(chapter_rows[chapter]):
            ws.append(member.as_list())
        ws.freeze_panes = "A2"
        autosize_columns(ws)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)


def build_yearly_chapter_rosters(master_path: Path, output_dir: Path) -> None:
    master_rows = load_master_roster(master_path)
    if not master_rows:
        raise FileNotFoundError(f"No usable roster rows were found in {master_path}.")

    grouped = rows_to_yearly_chapters(master_rows)
    if not grouped:
        raise FileNotFoundError(f"No yearly chapter rows were found in {master_path}.")

    output_dir.mkdir(parents=True, exist_ok=True)
    for academic_year in sorted(grouped):
        write_year_workbook(
            academic_year=academic_year,
            chapter_rows=grouped[academic_year],
            output_path=output_dir / f"{academic_year}.xlsx",
        )


def main() -> None:
    args = parse_args()
    master_path = Path(args.master).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    build_yearly_chapter_rosters(master_path=master_path, output_dir=output_dir)
    print(f"Yearly chapter rosters created in: {output_dir}")


if __name__ == "__main__":
    main()
