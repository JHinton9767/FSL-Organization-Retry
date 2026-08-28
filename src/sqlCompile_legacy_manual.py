from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping

import pandas as pd

from src.build_master_roster import normalize_banner_id
from src.path_config import ROOT
from src.shared_utils import clean_text
from src.sqlCompile_cohort import (
    DEFAULT_MANUAL_STATUS_PATH,
    MANUAL_STATUS_COLUMNS,
    append_manual_status_rows,
    completed_manual_status_rows,
    normalize_status_code,
)


LEGACY_MANUAL_FILE_NAMES = {
    "manual_roster_corrections": "manual_roster_corrections.csv",
    "graduation_evidence": "graduation_evidence.csv",
    "outcome_overrides": "outcome_overrides.csv",
    "manual_adjustments": "manual_adjustments.csv",
    "manual_review_queue": "manual_review_queue.csv",
    "manual_review_actions": "manual_review_actions.csv",
}
AUTO_SOURCE_NAME = "auto"
LEGACY_FILE_EXTENSIONS = {".csv", ".xlsx", ".xlsm", ".xls"}
LEGACY_FILE_NAME_HINTS = {
    "manualreview",
    "manualcheck",
    "manualchecker",
    "manualrosterchange",
    "manualrostercorrection",
    "outcomeoverride",
    "graduationevidence",
    "manualadjustment",
}
SQL_STATUS_CODES = {"A", "N", "D", "G", "RS", "RV", "S", "T", "AL", "H", "CK"}
OUTCOME_FIELD_NAMES = {
    "final_outcome_bucket",
    "latest_outcome_bucket",
    "outcome_bucket",
    "final_status",
    "status",
}


@dataclass(frozen=True)
class LegacyManualDecisionLoad:
    rows: pd.DataFrame
    source_counts: dict[str, int]
    converted_counts: dict[str, int]
    searched_paths: dict[str, list[Path]]

    @property
    def skipped_counts(self) -> dict[str, int]:
        return {
            source: max(self.source_counts.get(source, 0) - self.converted_counts.get(source, 0), 0)
            for source in self.source_counts
        }


@dataclass(frozen=True)
class LegacyManualImportResult:
    manual_status_path: Path
    loaded: LegacyManualDecisionLoad
    saved_rows: int


def _normalized_column_name(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean_text(value).lower())


def _column_lookup(row: pd.Series) -> dict[str, str]:
    return {_normalized_column_name(column): str(column) for column in row.index}


def _row_value(row: pd.Series, *aliases: str) -> str:
    lookup = _column_lookup(row)
    for alias in aliases:
        column = lookup.get(_normalized_column_name(alias))
        if column is not None:
            return clean_text(row.get(column, ""))
    return ""


def _first_value(row: pd.Series, *aliases: str) -> str:
    for alias in aliases:
        value = _row_value(row, alias)
        if value:
            return value
    return ""


def _first_banner_id(row: pd.Series, *aliases: str) -> str:
    for alias in aliases:
        value = normalize_banner_id(_row_value(row, alias))
        if value:
            return value
    return ""


def _is_truthy(value: object) -> bool:
    return clean_text(value).lower() in {"yes", "y", "true", "1", "x", "applied"}


def legacy_status_to_sql_status(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""

    direct = normalize_status_code(text)
    if direct in SQL_STATUS_CODES:
        return direct

    upper = text.upper()
    if "CHAPTER" in upper and "KICK" in upper:
        return "CK"
    if "GRAD" in upper or "DEGREE" in upper:
        return "G"
    if "EARLY" in upper and "ALUM" in upper:
        return "AL"
    if "REVOK" in upper:
        return "RV"
    if "SUSPEND" in upper:
        return "S"
    if "TRANSFER" in upper:
        return "T"
    if "RESIGN" in upper and "DROPPED/RESIGNED" not in upper:
        return "RS"
    if any(token in upper for token in ["DROP", "INACTIVE", "WITHDRAW", "LEFT", "REMOVE", "DISMISS", "EXPEL"]):
        return "D"
    if "NEW MEMBER" in upper:
        return "N"
    if "ACTIVE" in upper or "CURRENT" in upper:
        return "A"
    return ""


def _note(source_name: str, *parts: object) -> str:
    details = [clean_text(part) for part in parts if clean_text(part)]
    prefix = f"Imported from legacy {source_name}."
    return f"{prefix} {' | '.join(details)}" if details else prefix


def _manual_row(
    *,
    source_name: str,
    student_id: object,
    status: object,
    semester: object,
    chapter: object = "",
    cohort_semester: object = "",
    cohort_chapter: object = "",
    notes: object = "",
) -> dict[str, str]:
    chapter_text = clean_text(chapter)
    cohort_chapter_text = clean_text(cohort_chapter)
    return {
        "Cohort Semester": clean_text(cohort_semester),
        "Cohort Chapter": cohort_chapter_text or chapter_text,
        "Semester": clean_text(semester),
        "Chapter": chapter_text or cohort_chapter_text,
        "Student ID": normalize_banner_id(clean_text(student_id)),
        "Status": legacy_status_to_sql_status(status),
        "Notes": _note(source_name, notes),
    }


def _dedupe_manual_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=MANUAL_STATUS_COLUMNS)
    rows = completed_manual_status_rows(frame)
    if rows.empty:
        return pd.DataFrame(columns=MANUAL_STATUS_COLUMNS)
    rows = rows.drop_duplicates(
        subset=["Cohort Semester", "Cohort Chapter", "Semester", "Chapter", "Student ID"],
        keep="last",
    )
    return rows.loc[:, MANUAL_STATUS_COLUMNS].reset_index(drop=True)


def _convert_manual_roster_corrections(frame: pd.DataFrame, source_name: str) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for _, row in frame.iterrows():
        if _is_truthy(_row_value(row, "exclude_from_roster_calculations")) and not _row_value(row, "final_status"):
            continue
        rows.append(
            _manual_row(
                source_name=source_name,
                student_id=_first_banner_id(row, "student_id", "student id", "normalized_student_id"),
                cohort_semester=_row_value(row, "organization_join_term"),
                cohort_chapter=_first_value(row, "organization_name", "corrected_organization_name"),
                semester=_first_value(row, "final_status_term", "leaving_organization_term", "organization_join_term"),
                chapter=_first_value(row, "corrected_organization_name", "organization_name"),
                status=_row_value(row, "final_status"),
            )
        )
    return _dedupe_manual_rows(pd.DataFrame(rows, columns=MANUAL_STATUS_COLUMNS))


def _convert_graduation_evidence(frame: pd.DataFrame, source_name: str) -> pd.DataFrame:
    rows = [
        _manual_row(
            source_name=source_name,
            student_id=_first_banner_id(row, "student_id", "student id", "normalized_student_id"),
            cohort_semester=_row_value(row, "organization_join_term"),
            cohort_chapter=_row_value(row, "organization_name"),
            semester=_row_value(row, "graduation_term"),
            chapter=_row_value(row, "organization_name"),
            status="G",
            notes=_first_value(row, "evidence_source", "notes", "reason"),
        )
        for _, row in frame.iterrows()
    ]
    return _dedupe_manual_rows(pd.DataFrame(rows, columns=MANUAL_STATUS_COLUMNS))


def _convert_outcome_overrides(frame: pd.DataFrame, source_name: str) -> pd.DataFrame:
    rows = [
        _manual_row(
            source_name=source_name,
            student_id=_first_banner_id(row, "student_id", "student id", "normalized_student_id"),
            cohort_semester=_row_value(row, "organization_join_term"),
            cohort_chapter=_row_value(row, "organization_name"),
            semester=_first_value(row, "final_status_term", "status_term", "term"),
            chapter=_row_value(row, "organization_name"),
            status=_first_value(row, "final_status", "status", "outcome", "outcome_bucket"),
            notes=_first_value(row, "reason", "evidence_source", "notes"),
        )
        for _, row in frame.iterrows()
    ]
    return _dedupe_manual_rows(pd.DataFrame(rows, columns=MANUAL_STATUS_COLUMNS))


def _active_manual_adjustments(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    active = frame.get("active", pd.Series("Yes", index=frame.index)).fillna("Yes").astype(str).str.strip().str.lower()
    return frame.loc[~active.isin({"no", "n", "false", "0", "inactive"})].copy()


def _convert_manual_adjustments(frame: pd.DataFrame, source_name: str) -> pd.DataFrame:
    active = _active_manual_adjustments(frame)
    if active.empty:
        return pd.DataFrame(columns=MANUAL_STATUS_COLUMNS)

    chapter_lookup: dict[str, str] = {}
    for _, row in active.iterrows():
        field = _row_value(row, "field_to_override", "field")
        if "chapter" not in field.lower():
            continue
        student_id = _first_banner_id(row, "student_id", "normalized_student_id")
        chapter = _row_value(row, "adjusted_value")
        if student_id and chapter:
            chapter_lookup[student_id] = chapter

    rows: list[dict[str, str]] = []
    for _, row in active.iterrows():
        field = _row_value(row, "field_to_override", "field").lower()
        if field not in OUTCOME_FIELD_NAMES:
            continue
        student_id = _first_banner_id(row, "student_id", "normalized_student_id")
        rows.append(
            _manual_row(
                source_name=source_name,
                student_id=student_id,
                semester=_first_value(row, "original_value", "term", "final_status_term", "manual_adjusted_term"),
                chapter=chapter_lookup.get(student_id, ""),
                status=_row_value(row, "adjusted_value"),
                notes=_first_value(row, "reason", "evidence", "reviewer"),
            )
        )
    return _dedupe_manual_rows(pd.DataFrame(rows, columns=MANUAL_STATUS_COLUMNS))


def _status_from_review_note(row: pd.Series) -> str:
    note = _first_value(row, "review_notes", "reviewer_notes", "notes")
    match = re.search(r"\bsaved\s+as\s+([^.;|]+)", note, flags=re.IGNORECASE)
    return match.group(1).strip() if match else ""


def _convert_manual_review_queue(frame: pd.DataFrame, source_name: str) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for _, row in frame.iterrows():
        adjusted_status = _first_value(row, "manual_adjusted_outcome", "manual_outcome", "adjusted_value") or _status_from_review_note(row)
        if not adjusted_status:
            has_correction = _row_value(row, "has_manual_correction").lower() in {"yes", "true", "1"}
            corrected = _row_value(row, "review_status").lower() == "corrected"
            adjusted_status = _row_value(row, "latest_outcome_bucket") if has_correction and corrected else ""
        rows.append(
            _manual_row(
                source_name=source_name,
                student_id=_first_banner_id(row, "student_id", "normalized_student_id", "banner id"),
                cohort_semester=_first_value(row, "join_term", "organization_join_term"),
                cohort_chapter=_first_value(row, "chapter", "organization"),
                semester=_first_value(row, "manual_adjusted_term", "final_status_term", "last_observed_org_term", "term"),
                chapter=_first_value(row, "manual_adjusted_org", "chapter", "organization"),
                status=adjusted_status,
                notes=_first_value(row, "review_notes", "reviewer_notes", "queue_reason", "issue_description"),
            )
        )
    return _dedupe_manual_rows(pd.DataFrame(rows, columns=MANUAL_STATUS_COLUMNS))


CONVERTERS: Mapping[str, Callable[[pd.DataFrame, str], pd.DataFrame]] = {
    "manual_roster_corrections": _convert_manual_roster_corrections,
    "graduation_evidence": _convert_graduation_evidence,
    "outcome_overrides": _convert_outcome_overrides,
    "manual_adjustments": _convert_manual_adjustments,
    "manual_review_queue": _convert_manual_review_queue,
    "manual_review_actions": _convert_manual_review_queue,
}


def _source_name_from_path(path: Path) -> str:
    lowered = path.name.lower()
    if lowered.startswith("manual_review_actions.pending_"):
        return "manual_review_actions"
    for source_name, file_name in LEGACY_MANUAL_FILE_NAMES.items():
        if lowered == file_name.lower() or lowered == Path(file_name).stem.lower():
            return source_name
    return ""


def _source_hint_from_path(path: Path) -> str:
    source_name = _source_name_from_path(path)
    if source_name:
        return source_name
    stem = _normalized_column_name(path.stem)
    if path.suffix.lower() not in LEGACY_FILE_EXTENSIONS:
        return ""
    if "graduationevidence" in stem:
        return "graduation_evidence"
    if "outcomeoverride" in stem:
        return "outcome_overrides"
    if "manualadjustment" in stem:
        return "manual_adjustments"
    if "manualrostercorrection" in stem or "manualrosterchange" in stem:
        return "manual_roster_corrections"
    if any(hint in stem for hint in LEGACY_FILE_NAME_HINTS):
        return AUTO_SOURCE_NAME
    return ""


def _infer_source_name_from_columns(frame: pd.DataFrame) -> str:
    if frame.empty:
        return ""
    columns = {_normalized_column_name(column) for column in frame.columns}
    if {"reviewstatus", "hasmanualcorrection"} & columns or "queuereason" in columns or "reviewkey" in columns:
        return "manual_review_actions"
    if "graduationterm" in columns:
        return "graduation_evidence"
    if {"fieldtooverride", "adjustedvalue"}.issubset(columns):
        return "manual_adjustments"
    if "finalstatus" in columns and {"correctedorganizationname", "excludefromrostercalculations"} & columns:
        return "manual_roster_corrections"
    if "finalstatus" in columns or "outcomebucket" in columns:
        return "outcome_overrides"
    return ""


def _legacy_search_directories(base: Path) -> list[Path]:
    if base.is_file():
        return []
    candidates = [
        base,
        base / "config",
        base / "output" / "canonical" / "latest",
    ]
    run_root = base / "output" / "canonical"
    if run_root.exists():
        candidates.extend(sorted(run_root.glob("run_*"), key=lambda path: path.stat().st_mtime if path.exists() else 0, reverse=True))

    seen: set[Path] = set()
    directories: list[Path] = []
    for candidate in candidates:
        if not candidate.is_dir():
            continue
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        directories.append(candidate)
    return directories


def _legacy_source_paths(legacy_path: str | Path) -> dict[str, list[Path]]:
    base = Path(legacy_path)
    if base.is_file():
        source_name = _source_hint_from_path(base) or AUTO_SOURCE_NAME
        return {source_name: [base]}

    paths: dict[str, list[Path]] = {source_name: [] for source_name in LEGACY_MANUAL_FILE_NAMES}
    paths[AUTO_SOURCE_NAME] = []
    seen: set[Path] = set()
    for directory in _legacy_search_directories(base):
        for source_name, file_name in LEGACY_MANUAL_FILE_NAMES.items():
            candidates = [directory / file_name]
            if source_name == "manual_review_actions":
                candidates.extend(sorted(directory.glob("manual_review_actions.pending_*.csv")))
            for candidate in candidates:
                resolved = candidate.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                paths[source_name].append(candidate)

        for candidate in directory.iterdir():
            if not candidate.is_file():
                continue
            source_name = _source_hint_from_path(candidate)
            if not source_name:
                continue
            resolved = candidate.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            paths.setdefault(source_name, []).append(candidate)
    return paths


def _read_legacy_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        if path.stat().st_size == 0:
            return pd.DataFrame()
    except OSError:
        return pd.DataFrame()

    try:
        if path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
            return pd.read_excel(path, dtype=str).fillna("")
        return pd.read_csv(path, dtype=str).fillna("")
    except (OSError, pd.errors.EmptyDataError, UnicodeDecodeError):
        return pd.DataFrame()


def load_legacy_manual_decision_rows(legacy_path: str | Path = ROOT / "config") -> LegacyManualDecisionLoad:
    discovered_paths = _legacy_source_paths(legacy_path)
    searched_paths = {
        source_name: list(paths)
        for source_name, paths in discovered_paths.items()
        if source_name != AUTO_SOURCE_NAME
    }
    converted_frames: list[pd.DataFrame] = []
    source_counts: dict[str, int] = {source_name: 0 for source_name in LEGACY_MANUAL_FILE_NAMES}
    converted_counts: dict[str, int] = {source_name: 0 for source_name in LEGACY_MANUAL_FILE_NAMES}

    for source_name, paths in discovered_paths.items():
        for path in paths:
            frame = _read_legacy_file(path)
            if frame.empty:
                continue
            actual_source_name = source_name if source_name != AUTO_SOURCE_NAME else _infer_source_name_from_columns(frame)
            converter = CONVERTERS.get(actual_source_name)
            if converter is None:
                continue
            if source_name == AUTO_SOURCE_NAME:
                searched_paths.setdefault(actual_source_name, [])
                if path not in searched_paths[actual_source_name]:
                    searched_paths[actual_source_name].append(path)
            source_counts[actual_source_name] = source_counts.get(actual_source_name, 0) + int(len(frame))
            converted = converter(frame, path.name)
            converted_counts[actual_source_name] = converted_counts.get(actual_source_name, 0) + int(len(converted))
            if not converted.empty:
                converted_frames.append(converted)

    combined = pd.concat(converted_frames, ignore_index=True) if converted_frames else pd.DataFrame(columns=MANUAL_STATUS_COLUMNS)
    return LegacyManualDecisionLoad(
        rows=_dedupe_manual_rows(combined),
        source_counts=source_counts,
        converted_counts=converted_counts,
        searched_paths=searched_paths,
    )


def import_legacy_manual_decisions(
    legacy_path: str | Path = ROOT / "config",
    manual_status_file: str | Path = DEFAULT_MANUAL_STATUS_PATH,
) -> LegacyManualImportResult:
    loaded = load_legacy_manual_decision_rows(legacy_path)
    destination, saved_rows = append_manual_status_rows(loaded.rows, manual_status_file)
    return LegacyManualImportResult(
        manual_status_path=destination,
        loaded=loaded,
        saved_rows=saved_rows,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Import finished legacy dashboard manual decisions into sqlCompile manual status rows.")
    parser.add_argument("--legacy-path", default=str(ROOT), help="Legacy project root, config folder, canonical output folder, or one legacy CSV/XLSX file.")
    parser.add_argument("--manual-status-file", default=str(DEFAULT_MANUAL_STATUS_PATH), help="Destination sqlCompile manual status CSV.")
    parser.add_argument("--preview-output", default="", help="Optional CSV path where importable rows should be written for review.")
    parser.add_argument("--dry-run", action="store_true", help="Preview importable rows without appending them.")
    args = parser.parse_args(argv)

    loaded = load_legacy_manual_decision_rows(args.legacy_path)
    print(f"Legacy path scanned: {Path(args.legacy_path)}")
    print(f"Importable sqlCompile manual rows: {len(loaded.rows):,}")
    for source_name in LEGACY_MANUAL_FILE_NAMES:
        checked = len(loaded.searched_paths.get(source_name, []))
        print(
            f"{source_name}: {loaded.converted_counts.get(source_name, 0):,} converted "
            f"from {loaded.source_counts.get(source_name, 0):,} source row(s) across {checked:,} checked file(s)"
        )
    if args.preview_output:
        preview_path = Path(args.preview_output).expanduser()
        if not preview_path.is_absolute():
            preview_path = Path.cwd() / preview_path
        preview_path.parent.mkdir(parents=True, exist_ok=True)
        loaded.rows.to_csv(preview_path, index=False)
        print(f"Preview rows written to: {preview_path}")
    if args.dry_run:
        return 0

    destination, saved_rows = append_manual_status_rows(loaded.rows, args.manual_status_file)
    print(f"Manual status file: {destination}")
    print(f"Rows appended/replaced: {saved_rows:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
