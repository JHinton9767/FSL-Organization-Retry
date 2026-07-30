from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.chapter_semester_inventory import build_chapter_semester_exports
from src.path_config import DEFAULT_CONFIG_PATH, load_path_config


def resolve_canonical_root(config: str | None, canonical_dir: str | None) -> Path:
    if canonical_dir:
        return Path(canonical_dir).expanduser().resolve()
    if config or DEFAULT_CONFIG_PATH.exists():
        return load_path_config(config).output_root
    return (ROOT / "output" / "canonical").resolve()


def resolve_roster_path(canonical_root: Path, roster_term: str | None) -> Path:
    if roster_term:
        return Path(roster_term).expanduser().resolve()
    candidates = [
        canonical_root / "latest" / "roster_term.csv",
        canonical_root / "roster_term.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError(
        "Could not find roster_term.csv. Run the canonical pipeline first, or pass "
        "--roster-term PATH_TO_ROSTER_TERM_CSV."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build chapter-by-semester roster presence exports from canonical roster_term.csv."
    )
    parser.add_argument("--config", help="Path to config/local_paths.yaml.")
    parser.add_argument("--canonical-dir", help="Canonical output root or a specific latest/run folder.")
    parser.add_argument("--roster-term", help="Direct path to roster_term.csv.")
    parser.add_argument(
        "--output-dir",
        help="Directory for chapter_semester_inventory.csv, chapter_semester_matrix.csv, and chapter_lifecycle_review_template.csv.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    canonical_root = resolve_canonical_root(args.config, args.canonical_dir)
    roster_path = resolve_roster_path(canonical_root, args.roster_term)
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else roster_path.parent
    result = build_chapter_semester_exports(roster_path, output_dir)

    print(f"Read roster rows: {result.source_rows}")
    print(f"Valid roster rows used: {result.valid_rows}")
    print(f"Skipped invalid/missing Banner ID rows: {result.invalid_id_rows}")
    print(f"Skipped Advisor/Greek Staff rows: {result.excluded_position_rows}")
    print(f"Terms: {result.term_count}")
    print(f"Chapters: {result.chapter_count}")
    print(f"Wrote: {result.inventory_path}")
    print(f"Wrote: {result.matrix_path}")
    print(f"Wrote: {result.lifecycle_review_path}")


if __name__ == "__main__":
    main()
