from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.path_config import load_path_config, validate_path_config
RAW_KEYWORDS = (
    "copy of rosters",
    "rosters/",
    "rosters\\",
    "raw",
    "grade",
    "graduation",
    "reference",
    "snapshot",
    "student",
)
RAW_SUFFIXES = {".pdf", ".xlsx", ".xls", ".xlsm", ".xlsb", ".csv", ".docx", ".parquet", ".db", ".sqlite"}
LARGE_FILE_BYTES = 5 * 1024 * 1024
PATH_WARNING_LENGTH = 240


def _git_lines(args: Iterable[str]) -> List[str]:
    result = subprocess.run(["git", *args], cwd=ROOT, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        return []
    return [line for line in result.stdout.splitlines() if line.strip()]


def _looks_raw(path_text: str) -> bool:
    lowered = path_text.lower()
    suffix = Path(path_text).suffix.lower()
    if suffix in {".py", ".md", ".json", ".toml", ".bat", ".pq"}:
        return False
    return suffix in RAW_SUFFIXES or any(keyword in lowered for keyword in RAW_KEYWORDS)


def main() -> int:
    parser = argparse.ArgumentParser(description="Report repo hygiene risks without deleting or untracking anything.")
    parser.add_argument("--config", default=None, help="Optional path config to validate.")
    args = parser.parse_args()

    tracked = _git_lines(["ls-files"])
    ignored = _git_lines(["status", "--ignored", "--short"])

    large_tracked = []
    raw_tracked = []
    long_paths = []
    for relative in tracked:
        path = ROOT / relative
        if path.exists() and path.is_file() and path.stat().st_size >= LARGE_FILE_BYTES:
            large_tracked.append((relative, path.stat().st_size))
        if _looks_raw(relative):
            raw_tracked.append(relative)
        if len(str(path)) >= PATH_WARNING_LENGTH:
            long_paths.append((relative, len(str(path))))

    print("Repository hygiene report")
    print("=========================")
    print(f"Tracked files: {len(tracked):,}")
    print(f"Ignored entries shown by Git: {len(ignored):,}")

    print("\nLarge tracked files (>5 MB)")
    if large_tracked:
        for relative, size in sorted(large_tracked, key=lambda item: item[1], reverse=True):
            print(f"- {relative} ({size / (1024 * 1024):.1f} MB)")
    else:
        print("- none")

    print("\nRaw/private-looking tracked files")
    if raw_tracked:
        for relative in raw_tracked:
            print(f"- {relative}")
    else:
        print("- none")

    print("\nTracked paths near Windows path limit")
    if long_paths:
        for relative, length in sorted(long_paths, key=lambda item: item[1], reverse=True):
            print(f"- {length} chars: {relative}")
    else:
        print("- none")

    print("\nPath config")
    try:
        paths = load_path_config(args.config)
        print(f"- config: {paths.config_path}")
        print(f"- using example config: {paths.used_example_config}")
        print(f"- raw_data_root: {paths.raw_data_root} ({'exists' if paths.raw_data_root.exists() else 'missing'})")
        print(f"- output_root: {paths.output_root} ({'exists' if paths.output_root.exists() else 'missing'})")
        print(f"- cache_root: {paths.cache_root} ({'exists' if paths.cache_root.exists() else 'missing'})")
        issues = validate_path_config(paths, required_source_keys=["raw_data_root", "rosters_root", "grade_reports_root"])
        if issues:
            for issue in issues:
                print(f"- {issue}")
        else:
            print("- path checks passed")
    except Exception as exc:
        print(f"- path config error: {exc}")

    print("\nThis script is report-only. It does not delete, move, or untrack files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
