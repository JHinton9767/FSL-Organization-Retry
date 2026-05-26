from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.io_utils import canonicalize_column, normalize_text
from src.path_config import load_path_config


VALID_BANNER_ID_RE = re.compile(r"^A0\d{7}$")
SUPPORTED_FILE_SUFFIXES = {".csv", ".xlsx", ".xls", ".xlsm"}
DEFAULT_OUTPUT_DIR = ROOT / "data" / "outgoing" / "banner_id_batches"
DEFAULT_BATCH_SIZE = 999

BANNER_ID_HEADER_ALIASES = {
    "bannerid",
    "studentid",
    "id",
    "uniqueid",
    "txstid",
    "texasstateid",
    "banner",
    "bannernumber",
}

MASTER_COLUMNS = [
    "Banner ID",
    "first_seen_source_type",
    "first_seen_file",
    "first_seen_sheet",
    "source_count",
    "seen_in_roster",
    "seen_in_academic",
    "seen_in_grade_report",
    "seen_in_graduation",
    "seen_in_snapshot",
    "seen_in_reference",
    "seen_in_canonical_output",
    "source_files",
]

REJECTED_COLUMNS = [
    "raw_value",
    "normalized_value",
    "source_file",
    "sheet_name",
    "column_name",
    "rejection_reason",
]

MANUAL_REVIEW_COLUMNS = [
    "Banner ID",
    "reason",
    "evidence",
    "source_file",
    "first_seen_term",
    "last_seen_term",
    "suggested_review_bucket",
]

PLACEHOLDER_SCHEMAS = {
    "returned_academic_ids_by_report.csv": [
        "report_file",
        "sheet_name",
        "Banner ID",
        "term",
        "academic_year",
        "has_meaningful_academic_data",
        "first_nonblank_column",
    ],
    "missing_from_returned_reports.csv": [
        "Banner ID",
        "requested_source_count",
        "report_file",
        "missing_reason",
    ],
    "first_academic_appearance_by_banner_id.csv": [
        "Banner ID",
        "first_academic_report_file",
        "first_academic_sheet",
        "first_academic_term",
        "first_meaningful_academic_field",
    ],
    "last_academic_appearance_by_banner_id.csv": [
        "Banner ID",
        "last_academic_report_file",
        "last_academic_sheet",
        "last_academic_term",
        "last_meaningful_academic_field",
    ],
    "unchanged_academic_record_flags.csv": [
        "Banner ID",
        "flag_reason",
        "first_report_file",
        "last_report_file",
        "unchanged_field_count",
        "suggested_review_bucket",
    ],
}


@dataclass
class BannerRecord:
    banner_id: str
    first_seen_source_type: str = ""
    first_seen_file: str = ""
    first_seen_sheet: str = ""
    source_keys: set[str] = field(default_factory=set)
    source_files: set[str] = field(default_factory=set)
    source_types: set[str] = field(default_factory=set)

    def add_source(self, source_type: str, source_file: str, sheet_name: str, column_name: str) -> None:
        if not self.first_seen_file:
            self.first_seen_source_type = source_type
            self.first_seen_file = source_file
            self.first_seen_sheet = sheet_name
        self.source_keys.add(f"{source_file}|{sheet_name}|{column_name}")
        self.source_files.add(source_file)
        self.source_types.add(source_type)


@dataclass
class ScanResult:
    records: Dict[str, BannerRecord] = field(default_factory=dict)
    rejected_rows: List[dict[str, str]] = field(default_factory=list)
    scanned_files: set[str] = field(default_factory=set)
    skipped_files: List[dict[str, str]] = field(default_factory=list)
    scanned_roots: List[str] = field(default_factory=list)


def normalize_header_for_banner_id(value: object) -> str:
    text = canonicalize_column(value)
    return re.sub(r"[^a-z0-9]", "", text)


def is_banner_id_header(value: object) -> bool:
    return normalize_header_for_banner_id(value) in BANNER_ID_HEADER_ALIASES


def normalize_candidate_banner_id(value: object) -> str:
    text = normalize_text(value).upper()
    return re.sub(r"\s+", "", text)


def banner_id_rejection_reason(raw_value: object, normalized_value: str) -> str:
    if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)) or not normalize_text(raw_value):
        return "blank_or_null"
    if len(normalized_value) != 9:
        return "wrong_length"
    if not normalized_value.startswith("A0"):
        return "wrong_prefix"
    if not normalized_value[2:].isdigit():
        return "non_numeric_suffix"
    return "not_banner_id_format"


def validate_banner_id(value: object) -> tuple[Optional[str], Optional[str], str]:
    normalized = normalize_candidate_banner_id(value)
    if VALID_BANNER_ID_RE.fullmatch(normalized):
        return normalized, None, normalized
    return None, banner_id_rejection_reason(value, normalized), normalized


def split_batches(ids: Sequence[str], batch_size: int = DEFAULT_BATCH_SIZE) -> List[List[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    return [list(ids[index : index + batch_size]) for index in range(0, len(ids), batch_size)]


def source_type_for_path(path: Path) -> str:
    lowered = str(path).lower()
    if "output" in path.parts or "canonical" in lowered:
        return "canonical_output"
    if "graduation" in lowered:
        return "graduation"
    if "snapshot" in lowered or "current_snapshot" in lowered:
        return "snapshot"
    if "reference" in lowered or "benchmark" in lowered:
        return "reference"
    if "grade" in lowered or "academic" in lowered or "gpa" in lowered:
        return "academic"
    if "roster" in lowered:
        return "roster"
    return "reference"


def should_skip_file(path: Path) -> bool:
    return (
        path.name.startswith("~$")
        or path.name.startswith(".")
        or any(part.startswith(".") for part in path.parts)
        or path.suffix.lower() not in SUPPORTED_FILE_SUFFIXES
    )


def iter_candidate_files(roots: Iterable[Path]) -> List[Path]:
    files: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        if root.is_file():
            if not should_skip_file(root):
                files.add(root.resolve())
            continue
        for path in root.rglob("*"):
            if path.is_file() and not should_skip_file(path):
                files.add(path.resolve())
    return sorted(files, key=lambda item: str(item).lower())


def _stringify_source_file(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def _candidate_column_indexes(header_values: Sequence[object]) -> List[int]:
    return [index for index, value in enumerate(header_values) if is_banner_id_header(value)]


def _detect_header_rows(frame: pd.DataFrame, max_scan_rows: int = 25) -> List[int]:
    rows: List[int] = []
    limit = min(len(frame), max_scan_rows)
    for row_index in range(limit):
        values = frame.iloc[row_index].tolist()
        if _candidate_column_indexes(values):
            rows.append(row_index)
    return rows


def _sections_from_raw_frame(raw_frame: pd.DataFrame) -> List[tuple[int, int, List[object], List[int]]]:
    if raw_frame.empty:
        return []
    header_rows = _detect_header_rows(raw_frame)
    if not header_rows:
        return []
    sections: List[tuple[int, int, List[object], List[int]]] = []
    for position, header_row in enumerate(header_rows):
        next_header = header_rows[position + 1] if position + 1 < len(header_rows) else len(raw_frame)
        headers = raw_frame.iloc[header_row].tolist()
        candidate_indexes = _candidate_column_indexes(headers)
        sections.append((header_row + 1, next_header, headers, candidate_indexes))
    return sections


def _read_csv_raw(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, header=None, dtype=object, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(path, header=None, dtype=object, encoding="latin1")


def _read_workbook_raw(path: Path) -> Dict[str, pd.DataFrame]:
    return pd.read_excel(path, sheet_name=None, header=None, dtype=object)


def _scan_raw_frame(result: ScanResult, frame: pd.DataFrame, path: Path, sheet_name: str, source_type: str) -> None:
    source_file = _stringify_source_file(path)
    for start_row, end_row, headers, indexes in _sections_from_raw_frame(frame):
        for column_index in indexes:
            column_name = normalize_text(headers[column_index]) or f"column_{column_index + 1}"
            for row_index in range(start_row, end_row):
                raw_value = frame.iat[row_index, column_index] if column_index < len(frame.columns) else ""
                valid_id, rejection_reason, normalized = validate_banner_id(raw_value)
                if valid_id:
                    record = result.records.setdefault(valid_id, BannerRecord(valid_id))
                    record.add_source(source_type, source_file, sheet_name, column_name)
                else:
                    result.rejected_rows.append(
                        {
                            "raw_value": normalize_text(raw_value),
                            "normalized_value": normalized,
                            "source_file": source_file,
                            "sheet_name": sheet_name,
                            "column_name": column_name,
                            "rejection_reason": rejection_reason or "not_banner_id_format",
                        }
                    )


def scan_file(result: ScanResult, path: Path, verbose: bool = False) -> None:
    source_type = source_type_for_path(path)
    source_file = _stringify_source_file(path)
    try:
        if path.suffix.lower() == ".csv":
            _scan_raw_frame(result, _read_csv_raw(path), path, "CSV", source_type)
        else:
            for sheet_name, frame in _read_workbook_raw(path).items():
                _scan_raw_frame(result, frame, path, str(sheet_name), source_type)
        result.scanned_files.add(source_file)
        if verbose:
            print(f"scanned: {source_file}")
    except Exception as exc:
        result.skipped_files.append({"source_file": source_file, "reason": f"{type(exc).__name__}: {exc}"})
        if verbose:
            print(f"skipped: {source_file} ({exc})")


def _unique_existing(paths: Iterable[Path]) -> List[Path]:
    seen: set[Path] = set()
    existing: List[Path] = []
    for path in paths:
        resolved = path.resolve()
        if resolved.exists() and resolved not in seen:
            seen.add(resolved)
            existing.append(resolved)
    return existing


def build_source_roots(include_raw: bool, include_canonical: bool, config_path: Optional[str] = None) -> List[Path]:
    roots: List[Path] = []
    try:
        paths = load_path_config(config_path)
        if include_raw:
            roots.extend(
                [
                    paths.rosters_root,
                    paths.roster_inbox_root,
                    paths.grade_reports_root,
                    paths.transcript_text_root,
                    paths.graduation_root,
                    paths.snapshot_root,
                    paths.reference_root,
                    paths.membership_reference_root,
                    paths.gpa_reference_root,
                    paths.gpa_benchmark_root,
                ]
            )
        if include_canonical:
            roots.extend([paths.output_root / "latest", paths.output_root])
    except Exception:
        if include_canonical:
            roots.extend([ROOT / "output" / "canonical" / "latest", ROOT / "output" / "canonical"])

    if include_raw:
        roots.extend(
            [
                ROOT / "data" / "inbox",
                ROOT / "data" / "raw",
                ROOT / "Copy of Rosters",
                ROOT / "Rosters",
            ]
        )
    if include_canonical:
        roots.extend([ROOT / "output" / "canonical" / "latest", ROOT / "output" / "canonical"])
    return _unique_existing(roots)


def records_to_master_frame(records: Dict[str, BannerRecord]) -> pd.DataFrame:
    rows: List[dict[str, object]] = []
    for banner_id in sorted(records):
        record = records[banner_id]
        rows.append(
            {
                "Banner ID": banner_id,
                "first_seen_source_type": record.first_seen_source_type,
                "first_seen_file": record.first_seen_file,
                "first_seen_sheet": record.first_seen_sheet,
                "source_count": len(record.source_keys),
                "seen_in_roster": "Yes" if "roster" in record.source_types else "No",
                "seen_in_academic": "Yes" if "academic" in record.source_types else "No",
                "seen_in_grade_report": "Yes" if "academic" in record.source_types else "No",
                "seen_in_graduation": "Yes" if "graduation" in record.source_types else "No",
                "seen_in_snapshot": "Yes" if "snapshot" in record.source_types else "No",
                "seen_in_reference": "Yes" if "reference" in record.source_types else "No",
                "seen_in_canonical_output": "Yes" if "canonical_output" in record.source_types else "No",
                "source_files": "; ".join(sorted(record.source_files)),
            }
        )
    return pd.DataFrame(rows, columns=MASTER_COLUMNS)


def _clear_previous_outputs(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for path in output_dir.glob("banner_ids_batch_*.csv"):
        path.unlink()
    for path in output_dir.glob("banner_ids_batch_*.txt"):
        path.unlink()


def write_outputs(result: ScanResult, output_dir: Path, batch_size: int, dry_run: bool = False) -> dict[str, object]:
    master = records_to_master_frame(result.records)
    batches = split_batches(master["Banner ID"].tolist(), batch_size=batch_size) if not master.empty else []
    rejected = pd.DataFrame(result.rejected_rows, columns=REJECTED_COLUMNS)
    skipped = pd.DataFrame(result.skipped_files, columns=["source_file", "reason"])
    summary = {
        "total_valid_unique_banner_ids": int(len(master)),
        "total_batches": int(len(batches)),
        "batch_size_limit": int(batch_size),
        "total_rejected_values": int(len(rejected)),
        "output_folder": str(output_dir),
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "scanned_file_count": int(len(result.scanned_files)),
        "scanned_source_roots": result.scanned_roots,
        "skipped_file_count": int(len(result.skipped_files)),
        "errors_or_skipped_files": result.skipped_files,
    }
    if dry_run:
        return summary

    _clear_previous_outputs(output_dir)
    master.to_csv(output_dir / "banner_ids_master.csv", index=False)
    master.to_csv(output_dir / "requested_banner_ids_master.csv", index=False)
    rejected.to_csv(output_dir / "rejected_banner_id_values.csv", index=False)
    skipped.to_csv(output_dir / "skipped_or_unreadable_files.csv", index=False)

    for batch_number, batch_ids in enumerate(batches, start=1):
        stem = f"banner_ids_batch_{batch_number:03d}"
        pd.DataFrame({"Banner ID": batch_ids}).to_csv(output_dir / f"{stem}.csv", index=False)
        (output_dir / f"{stem}.txt").write_text("\n".join(batch_ids) + ("\n" if batch_ids else ""), encoding="utf-8")

    pd.DataFrame(columns=MANUAL_REVIEW_COLUMNS).to_csv(output_dir / "manual_review_candidates.csv", index=False)
    for filename, columns in PLACEHOLDER_SCHEMAS.items():
        pd.DataFrame(columns=columns).to_csv(output_dir / filename, index=False)

    (output_dir / "banner_id_batch_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    pd.DataFrame([summary | {"scanned_source_roots": "; ".join(summary["scanned_source_roots"]), "errors_or_skipped_files": json.dumps(summary["errors_or_skipped_files"])}]).to_csv(
        output_dir / "banner_id_batch_summary.csv",
        index=False,
    )
    return summary


def build_banner_id_batches(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    batch_size: int = DEFAULT_BATCH_SIZE,
    include_raw: bool = True,
    include_canonical: bool = True,
    config_path: Optional[str] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> tuple[ScanResult, dict[str, object]]:
    roots = build_source_roots(include_raw=include_raw, include_canonical=include_canonical, config_path=config_path)
    result = ScanResult(scanned_roots=[str(path) for path in roots])
    for path in iter_candidate_files(roots):
        scan_file(result, path, verbose=verbose)
    summary = write_outputs(result, output_dir=output_dir, batch_size=batch_size, dry_run=dry_run)
    return result, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build strict Banner ID request batches for Academic Report pulls.")
    parser.add_argument("--config", default=None, help="Optional config/local_paths.yaml path.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--include-canonical", action="store_true", help="Scan canonical output files. If no include flags are provided, raw and canonical are both scanned.")
    parser.add_argument("--include-raw", action="store_true", help="Scan configured/raw source folders. If no include flags are provided, raw and canonical are both scanned.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    include_raw = args.include_raw or not args.include_canonical
    include_canonical = args.include_canonical or not args.include_raw
    _, summary = build_banner_id_batches(
        output_dir=Path(args.output_dir).expanduser().resolve(),
        batch_size=args.batch_size,
        include_raw=include_raw,
        include_canonical=include_canonical,
        config_path=args.config,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    print(json.dumps(summary, indent=2))
    if args.dry_run:
        print("Dry run only; no files were written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
