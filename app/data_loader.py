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
    "roster_term.csv": "roster_term",
    "academic_term.csv": "academic_term",
    "master_longitudinal.csv": "master_longitudinal",
    "student_summary.csv": "student_summary",
    "cohort_metrics.csv": "cohort_metrics",
    "qa_checks.csv": "qa_checks",
}
CANONICAL_OPTIONAL_FILES = [
    "identity_exceptions.csv",
    "term_exceptions.csv",
    "status_exceptions.csv",
    "chapter_conflicts.csv",
    "outcome_exceptions.csv",
    "missing_evidence_cases.csv",
    "unresolved_chapter_review.csv",
    "graduation_status_audit.csv",
    "transcript_term_summary.csv",
    "transcript_course_detail.csv",
    "transcript_parse_audit.csv",
    "transcript_parse_issues.csv",
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
                files.append(_status_from_path(filename, selected / filename, True))
            for filename in source_cfg.get("optional_files", CANONICAL_OPTIONAL_FILES):
                files.append(_status_from_path(filename, selected / filename, False))

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
        _status_from_path(filename, (selected or root) / filename, True)
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
        statuses.append(_loaded_status(filename, version.root_path / filename, True, tables.get(table_key)))
    statuses.append(_loaded_status("canonical_schema.json", version.root_path / "canonical_schema.json", True, None))
    for filename in CANONICAL_OPTIONAL_FILES:
        statuses.append(_loaded_status(filename, version.root_path / filename, False, tables.get(filename.replace(".csv", ""))))
    return statuses


def _read_canonical_tables(folder: Path) -> Dict[str, pd.DataFrame]:
    tables = {
        table_key: pd.read_csv(folder / filename)
        for filename, table_key in CANONICAL_REQUIRED_FILES.items()
    }
    for filename in CANONICAL_OPTIONAL_FILES:
        path = folder / filename
        if path.exists():
            tables[filename.replace(".csv", "")] = pd.read_csv(path)
    return tables


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
