from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

from app.config_loader import load_chapter_mapping, load_dataset_manifest, stringify_notes
from app.io_utils import ROOT
from app.models import AnalysisBundle, DataFileStatus, DataSourceStatus, DatasetVersion, MetricDefinition
from src.shared_utils import apply_chapter_mapping_overrides


CANONICAL_REQUIRED_FILES = {
    "roster_term.parquet": "roster_term",
    "academic_term.parquet": "academic_term",
    "master_longitudinal.parquet": "master_longitudinal",
    "student_summary.parquet": "student_summary",
    "cohort_metrics.parquet": "cohort_metrics",
    "qa_checks.parquet": "qa_checks",
}
CANONICAL_OPTIONAL_FILES = [
    "identity_exceptions.parquet",
    "term_exceptions.parquet",
    "status_exceptions.parquet",
    "chapter_conflicts.parquet",
    "outcome_exceptions.parquet",
    "missing_evidence_cases.parquet",
    "unresolved_chapter_review.parquet",
    "graduation_status_audit.parquet",
    "student_source_appearances.parquet",
    "student_longitudinal_tracking.parquet",
    "input_group_outcome_buckets.parquet",
    "yearly_unique_id_checklist.parquet",
    "manual_review_queue.parquet",
    "transcript_term_summary.parquet",
    "transcript_course_detail.parquet",
    "transcript_parse_audit.parquet",
    "transcript_parse_issues.parquet",
]
MANUAL_CORRECTION_REQUIRED_FILES = {
    "student_summary.parquet": "student_summary",
    "roster_term.parquet": "roster_term",
    "qa_checks.parquet": "qa_checks",
}
MANUAL_CORRECTION_OPTIONAL_FILES = [
    "identity_exceptions.parquet",
    "term_exceptions.parquet",
    "status_exceptions.parquet",
    "chapter_conflicts.parquet",
    "outcome_exceptions.parquet",
    "missing_evidence_cases.parquet",
    "unresolved_chapter_review.parquet",
    "graduation_status_audit.parquet",
    "student_source_appearances.parquet",
    "student_longitudinal_tracking.parquet",
    "input_group_outcome_buckets.parquet",
    "yearly_unique_id_checklist.parquet",
    "manual_review_queue.parquet",
    "transcript_parse_audit.parquet",
    "transcript_parse_issues.parquet",
]


def _iso_mtime(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")


def _latest_run_folder(root: Path, prefix: str) -> Optional[Path]:
    if not root.exists():
        return None
    candidates = [path for path in root.iterdir() if path.is_dir() and path.name.startswith(prefix)]
    if not candidates:
        latest = root / "latest"
        return latest if latest.exists() else None
    return sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True)[0]


def _status_from_path(label: str, path: Path, required: bool) -> DataFileStatus:
    return DataFileStatus(
        label=label,
        path=path,
        required=required,
        exists=path.exists(),
        loaded=False,
        row_count=None,
        last_modified=_iso_mtime(path),
        warning="" if path.exists() or not required else "Missing required file",
    )


def _table_key_from_filename(filename: str) -> str:
    return Path(filename).stem


def _preferred_canonical_path(folder: Path, filename: str) -> Path:
    path = folder / filename
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        csv_path = path.with_suffix(".csv")
        return path if path.exists() or not csv_path.exists() else csv_path
    if suffix == ".csv":
        parquet_path = path.with_suffix(".parquet")
        return parquet_path if parquet_path.exists() else path
    return path


def _read_canonical_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def scan_preloaded_sources() -> List[DataSourceStatus]:
    manifest = load_dataset_manifest()
    statuses: List[DataSourceStatus] = []

    for priority, source_key in enumerate(manifest.get("priority", [])):
        source_cfg = manifest.get("sources", {}).get(source_key, {})
        if source_key != "canonical":
            continue
        label = source_cfg.get("label", "Canonical Analytics Run")
        root = ROOT / source_cfg.get("root", "output/canonical")
        selected = _latest_run_folder(root, source_cfg.get("run_prefix", "run_"))
        warnings: List[str] = []
        files: List[DataFileStatus] = []

        if not root.exists():
            warnings.append(f"Folder not found: {root}")
        elif selected is None:
            warnings.append(f"No canonical run folders were found under {root}")
        else:
            for filename in source_cfg.get("required_files", list(CANONICAL_REQUIRED_FILES)):
                files.append(_status_from_path(filename, _preferred_canonical_path(selected, filename), True))
            for filename in source_cfg.get("optional_files", CANONICAL_OPTIONAL_FILES):
                files.append(_status_from_path(filename, _preferred_canonical_path(selected, filename), False))

        available = selected is not None and all(item.exists for item in files if item.required)
        if selected is not None and not available:
            missing = [item.label for item in files if item.required and not item.exists]
            warnings.append("Missing required files: " + ", ".join(missing))

        statuses.append(
            DataSourceStatus(
                source_key="canonical",
                label=label,
                priority=priority,
                root_path=root,
                selected_path=selected,
                available=available,
                files=files,
                warnings=warnings,
            )
        )

    if statuses:
        return statuses

    root = ROOT / "output" / "canonical"
    selected = _latest_run_folder(root, "run_")
    files = [
        _status_from_path(filename, _preferred_canonical_path(selected or root, filename), True)
        for filename in CANONICAL_REQUIRED_FILES
    ]
    return [
        DataSourceStatus(
            source_key="canonical",
            label="Canonical Analytics Run",
            priority=0,
            root_path=root,
            selected_path=selected,
            available=selected is not None and all(item.exists for item in files),
            files=files,
            warnings=[] if selected is not None else [f"No canonical run folders were found under {root}"],
        )
    ]


def discover_dataset_versions() -> List[DatasetVersion]:
    versions: List[DatasetVersion] = []
    for status in scan_preloaded_sources():
        if not status.available or status.selected_path is None:
            continue
        versions.append(
            DatasetVersion(
                key=f"canonical::{status.selected_path}",
                label=f"{status.label} - {status.selected_path.name}",
                dataset_type="canonical",
                root_path=status.selected_path,
                created_at=_iso_mtime(status.selected_path),
                notes=status.warnings,
            )
        )
    return versions


def select_default_dataset(versions: List[DatasetVersion]) -> Optional[DatasetVersion]:
    return versions[0] if versions else None


def _validate_loaded_tables(bundle_kind: str, tables: Dict[str, pd.DataFrame]) -> List[str]:
    if bundle_kind != "canonical":
        raise ValueError(f"Unsupported dataset type: {bundle_kind}. The app only loads canonical analytics runs.")

    requirements = {
        "student_summary": ["student_id"],
        "master_longitudinal": ["student_id", "term_code"],
        "cohort_metrics": ["Metric Group", "Metric Label", "Cohort"],
        "qa_checks": ["Check Group", "Check", "Status"],
    }
    missing_messages: List[str] = []
    for table_name, required_columns in requirements.items():
        frame = tables.get(table_name)
        if frame is None:
            missing_messages.append(f"Required table missing: {table_name}")
            continue
        missing_columns = [column for column in required_columns if column not in frame.columns]
        if missing_columns:
            missing_messages.append(f"{table_name} is missing required columns: {', '.join(missing_columns)}")

    if missing_messages:
        raise ValueError("Dataset validation failed. " + " | ".join(missing_messages))
    return []


def _validate_manual_correction_tables(tables: Dict[str, pd.DataFrame]) -> List[str]:
    frame = tables.get("student_summary")
    if frame is None or "student_id" not in frame.columns:
        raise ValueError("Manual Corrections Mode requires student_summary with a student_id column.")
    return []


def _loaded_status(label: str, path: Path, required: bool, frame: Optional[pd.DataFrame] = None) -> DataFileStatus:
    return DataFileStatus(
        label=label,
        path=path,
        required=required,
        exists=path.exists(),
        loaded=frame is not None and path.exists(),
        row_count=None if frame is None else int(len(frame)),
        last_modified=_iso_mtime(path),
        warning="" if path.exists() or not required else "Missing required file",
    )


def _build_data_status(version: DatasetVersion, tables: Dict[str, pd.DataFrame]) -> List[DataFileStatus]:
    statuses: List[DataFileStatus] = []
    for filename, table_key in CANONICAL_REQUIRED_FILES.items():
        statuses.append(_loaded_status(filename, _preferred_canonical_path(version.root_path, filename), True, tables.get(table_key)))
    statuses.append(_loaded_status("canonical_schema.json", version.root_path / "canonical_schema.json", True, None))
    for filename in CANONICAL_OPTIONAL_FILES:
        statuses.append(_loaded_status(filename, _preferred_canonical_path(version.root_path, filename), False, tables.get(_table_key_from_filename(filename))))
    return statuses


def _read_canonical_tables(
    folder: Path,
    required_files: Optional[Dict[str, str]] = None,
    optional_files: Optional[List[str]] = None,
) -> Dict[str, pd.DataFrame]:
    required = required_files or CANONICAL_REQUIRED_FILES
    optional = optional_files if optional_files is not None else CANONICAL_OPTIONAL_FILES
    tables = {
        table_key: _read_canonical_table(_preferred_canonical_path(folder, filename))
        for filename, table_key in required.items()
    }
    for filename in optional:
        path = _preferred_canonical_path(folder, filename)
        if path.exists():
            tables[_table_key_from_filename(filename)] = _read_canonical_table(path)
    return tables


def _build_manual_correction_data_status(version: DatasetVersion, tables: Dict[str, pd.DataFrame]) -> List[DataFileStatus]:
    statuses: List[DataFileStatus] = []
    for filename, table_key in MANUAL_CORRECTION_REQUIRED_FILES.items():
        statuses.append(_loaded_status(filename, _preferred_canonical_path(version.root_path, filename), True, tables.get(table_key)))
    for filename in MANUAL_CORRECTION_OPTIONAL_FILES:
        statuses.append(_loaded_status(filename, _preferred_canonical_path(version.root_path, filename), False, tables.get(_table_key_from_filename(filename))))
    return statuses


def load_analysis_bundle(
    version: DatasetVersion,
    metric_definitions: List[MetricDefinition],
    settings: Dict[str, object],
    status_code_map: Dict[str, Iterable[str]],
    chapter_mapping_path: Optional[Path] = None,
) -> AnalysisBundle:
    if version.dataset_type != "canonical":
        raise ValueError(f"Unsupported dataset type: {version.dataset_type}. The app only loads canonical analytics runs.")

    chapter_mapping = load_chapter_mapping(chapter_mapping_path)
    tables = _read_canonical_tables(version.root_path)
    validation_warnings = _validate_loaded_tables("canonical", tables)

    summary = tables["student_summary"].copy()
    longitudinal = tables.get("master_longitudinal", pd.DataFrame()).copy()
    notes = ["Loaded canonical analytics bundle directly."]
    if not chapter_mapping.empty:
        summary = apply_chapter_mapping_overrides(summary, chapter_mapping, chapter_column="chapter")
        summary = apply_chapter_mapping_overrides(
            summary,
            chapter_mapping,
            chapter_column="current_active_chapter",
            output_prefix="current_active_",
        )
        notes.append("Applied configured chapter-to-council and org-type overrides to the canonical bundle.")

    metadata = {
        "bundle_kind": "canonical",
        "available_campus_baseline": bool("is_fsl_member" in summary.columns and (~summary["is_fsl_member"].fillna(True)).any()),
        "raw_tables": sorted(tables.keys()),
        "validation_warnings": validation_warnings,
    }
    return AnalysisBundle(
        version=version,
        summary=summary,
        longitudinal=longitudinal,
        tables=tables,
        metric_definitions=list(metric_definitions),
        notes=stringify_notes(notes + validation_warnings + version.notes),
        metadata=metadata,
        data_status=_build_data_status(version, tables),
    )


def load_manual_corrections_bundle(
    version: DatasetVersion,
    metric_definitions: List[MetricDefinition],
    settings: Dict[str, object],
    chapter_mapping_path: Optional[Path] = None,
) -> AnalysisBundle:
    if version.dataset_type != "canonical":
        raise ValueError(f"Unsupported dataset type: {version.dataset_type}. The app only loads canonical analytics runs.")

    chapter_mapping = load_chapter_mapping(chapter_mapping_path)
    tables = _read_canonical_tables(
        version.root_path,
        required_files=MANUAL_CORRECTION_REQUIRED_FILES,
        optional_files=MANUAL_CORRECTION_OPTIONAL_FILES,
    )
    validation_warnings = _validate_manual_correction_tables(tables)

    summary = tables["student_summary"].copy()
    if not chapter_mapping.empty:
        summary = apply_chapter_mapping_overrides(summary, chapter_mapping, chapter_column="chapter")
        summary = apply_chapter_mapping_overrides(
            summary,
            chapter_mapping,
            chapter_column="current_active_chapter",
            output_prefix="current_active_",
        )

    metadata = {
        "bundle_kind": "canonical_manual_corrections",
        "manual_corrections_mode": True,
        "raw_tables": sorted(tables.keys()),
        "validation_warnings": validation_warnings,
    }
    notes = ["Loaded lightweight Manual Corrections bundle. Large analytics tables were not loaded."]
    return AnalysisBundle(
        version=version,
        summary=summary,
        longitudinal=pd.DataFrame(),
        tables=tables,
        metric_definitions=list(metric_definitions),
        notes=stringify_notes(notes + validation_warnings + version.notes),
        metadata=metadata,
        data_status=_build_manual_correction_data_status(version, tables),
    )
