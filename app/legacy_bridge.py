from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from app.config_loader import load_chapter_mapping, load_dataset_manifest, stringify_notes
from app.io_utils import ROOT, read_tabular_file
from app.models import AnalysisBundle, DataFileStatus, DataSourceStatus, DatasetVersion, MetricDefinition
from app.standardize import (
    merge_longitudinal_rollups,
    standardize_enhanced_longitudinal,
    standardize_enhanced_summary,
    standardize_processed_longitudinal,
    standardize_processed_summary,
    standardize_snapshot_summary,
)
from src.build_executive_report import DEFAULT_ENHANCED_ROOT, load_latest_bundle
from src.greek_life_pipeline import (
    build_alias_lookup,
    build_master_dataset,
    build_metrics,
    normalize_academic_records,
    normalize_roster_records,
    standardize_columns,
)


METRICS_ROOT = ROOT / "output" / "metrics"


def _iso_mtime(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")


def _latest_run_folder(root: Path, prefix: str) -> Optional[Path]:
    if not root.exists():
        return None
    candidates = [path for path in root.iterdir() if path.is_dir() and path.name.startswith(prefix)]
    if not candidates:
        return None
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


def _status_from_glob(label: str, base: Path, pattern: str, required: bool) -> DataFileStatus:
    matches = sorted(base.glob(pattern), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)
    path = matches[0] if matches else base / pattern
    return _status_from_path(label, path, required)


def scan_preloaded_sources() -> List[DataSourceStatus]:
    manifest = load_dataset_manifest()
    statuses: List[DataSourceStatus] = []

    for priority, source_key in enumerate(manifest.get("priority", [])):
        source_cfg = manifest.get("sources", {}).get(source_key, {})
        label = source_cfg.get("label", source_key.replace("_", " ").title())
        mode = source_cfg.get("mode", "fixed")
        warnings: List[str] = []
        files: List[DataFileStatus] = []

        if mode == "latest_run":
            root = ROOT / source_cfg.get("root", "")
            run_prefix = source_cfg.get("run_prefix", "run_")
            selected = _latest_run_folder(root, run_prefix)
            if not root.exists():
                warnings.append(f"Folder not found: {root}")
            elif selected is None:
                warnings.append(f"No run folders were found under {root}")

            if selected is not None:
                for filename in source_cfg.get("required_files", []):
                    files.append(_status_from_path(filename, selected / filename, True))
                for filename in source_cfg.get("optional_files", []):
                    if "*" in filename or "?" in filename:
                        files.append(_status_from_glob(filename, selected, filename, False))
                    else:
                        files.append(_status_from_path(filename, selected / filename, False))
            available = selected is not None and all(item.exists for item in files if item.required)
            if selected is not None and not available:
                missing = [item.label for item in files if item.required and not item.exists]
                warnings.append("Missing required files: " + ", ".join(missing))
            statuses.append(
                DataSourceStatus(
                    source_key=source_key,
                    label=label,
                    priority=priority,
                    root_path=root,
                    selected_path=selected,
                    available=available,
                    files=files,
                    warnings=warnings,
                )
            )
            continue

        root = ROOT
        selected = ROOT
        for file_cfg in source_cfg.get("files", []):
            path = ROOT / file_cfg["path"]
            files.append(_status_from_path(file_cfg.get("label", Path(file_cfg["path"]).name), path, bool(file_cfg.get("required", False))))
        available = all(item.exists for item in files if item.required)
        if not available:
            missing = [item.label for item in files if item.required and not item.exists]
            warnings.append("Missing required files: " + ", ".join(missing))
        statuses.append(
            DataSourceStatus(
                source_key=source_key,
                label=label,
                priority=priority,
                root_path=root,
                selected_path=selected,
                available=available,
                files=files,
                warnings=warnings,
            )
        )

    return statuses


def discover_dataset_versions() -> List[DatasetVersion]:
    versions: List[DatasetVersion] = []
    for status in scan_preloaded_sources():
        if not status.available or status.selected_path is None:
            continue
        label = status.label if status.source_key == "processed" else f"{status.label} - {status.selected_path.name}"
        versions.append(
            DatasetVersion(
                key=f"{status.source_key}::{status.selected_path}",
                label=label,
                dataset_type=status.source_key,
                root_path=status.selected_path,
                created_at=_iso_mtime(status.selected_path),
                notes=status.warnings,
            )
        )
    return versions


def select_default_dataset(versions: List[DatasetVersion]) -> Optional[DatasetVersion]:
    return versions[0] if versions else None


def _parse_enhanced_source_from_methodology(path: Path) -> Optional[Path]:
    if not path.exists():
        return None
    match = re.search(r"Enhanced analytics source:\s+`([^`]+)`", path.read_text(encoding="utf-8", errors="ignore"))
    if not match:
        return None
    candidate = Path(match.group(1)).expanduser()
    return candidate if candidate.exists() else None


def _base_metric_definitions(definitions: List[MetricDefinition]) -> List[MetricDefinition]:
    return list(definitions)


def _load_current_snapshot_tables(folder: Path) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    tables = {
        "snapshot_augmented_student_summary": pd.read_csv(folder / "snapshot_augmented_student_summary.csv"),
        "snapshot_augmented_cohort_metrics": pd.read_csv(folder / "snapshot_augmented_cohort_metrics.csv"),
        "snapshot_augmented_chapter_metrics": pd.read_csv(folder / "snapshot_augmented_chapter_metrics.csv"),
        "snapshot_merge_qa": pd.read_csv(folder / "snapshot_merge_qa.csv"),
    }
    notes: List[str] = ["Using snapshot-augmented student summary and chapter metrics."]
    enhanced_source = _parse_enhanced_source_from_methodology(folder / "methodology.md")
    if enhanced_source:
        notes.append(f"Linked enhanced source found at {enhanced_source}.")
        enhanced_bundle = load_latest_bundle(
            enhanced_root=DEFAULT_ENHANCED_ROOT,
            explicit_folder=enhanced_source,
            explicit_workbook=None,
        )
        tables.update(enhanced_bundle.tables)
        notes.extend(enhanced_bundle.caveats)
    else:
        notes.append("No linked enhanced source folder was found, so observed-term trends may be limited.")
    return tables, notes


def _validate_loaded_tables(bundle_kind: str, tables: Dict[str, pd.DataFrame]) -> List[str]:
    requirements = {
        "current_snapshot": {
            "snapshot_augmented_student_summary": ["Student ID"],
            "snapshot_augmented_cohort_metrics": ["Metric Group", "Metric Label", "Cohort"],
            "snapshot_augmented_chapter_metrics": ["Chapter", "Students"],
        },
        "enhanced": {
            "student_summary": ["Student ID"],
            "cohort_metrics": ["Metric Group", "Metric Label", "Cohort"],
        },
        "processed": {
            "student_summary": ["student_id"],
            "master_dataset": ["student_id", "term"],
        },
    }
    warnings: List[str] = []
    missing_messages: List[str] = []

    for table_name, required_columns in requirements.get(bundle_kind, {}).items():
        frame = tables.get(table_name)
        if frame is None:
            missing_messages.append(f"Required table missing: {table_name}")
            continue
        missing_columns = [column for column in required_columns if column not in frame.columns]
        if missing_columns:
            missing_messages.append(f"{table_name} is missing required columns: {', '.join(missing_columns)}")

    if missing_messages:
        raise ValueError("Dataset validation failed. " + " | ".join(missing_messages))

    if bundle_kind in {"current_snapshot", "enhanced"} and "master_longitudinal" not in tables:
        warnings.append("Master_Longitudinal was not available, so observed-term trend views are limited.")

    return warnings


def _loaded_status(label: str, path: Path, required: bool, frame: Optional[pd.DataFrame] = None, warning: str = "") -> DataFileStatus:
    return DataFileStatus(
        label=label,
        path=path,
        required=required,
        exists=path.exists(),
        loaded=frame is not None and path.exists(),
        row_count=None if frame is None else int(len(frame)),
        last_modified=_iso_mtime(path),
        warning=warning,
    )


def _build_data_status(version: DatasetVersion, tables: Dict[str, pd.DataFrame]) -> List[DataFileStatus]:
    statuses: List[DataFileStatus] = []

    if version.dataset_type == "current_snapshot":
        snapshot_map = {
            "snapshot_augmented_student_summary.csv": "snapshot_augmented_student_summary",
            "snapshot_augmented_cohort_metrics.csv": "snapshot_augmented_cohort_metrics",
            "snapshot_augmented_chapter_metrics.csv": "snapshot_augmented_chapter_metrics",
            "snapshot_merge_qa.csv": "snapshot_merge_qa",
        }
        for filename, table_key in snapshot_map.items():
            path = version.root_path / filename
            statuses.append(_loaded_status(filename, path, True, tables.get(table_key)))
        methodology_path = version.root_path / "methodology.md"
        statuses.append(_loaded_status("methodology.md", methodology_path, False, None))

        enhanced_source = _parse_enhanced_source_from_methodology(methodology_path)
        if enhanced_source:
            for filename, table_key in [
                ("student_summary.csv", "student_summary"),
                ("cohort_metrics.csv", "cohort_metrics"),
                ("master_longitudinal.csv", "master_longitudinal"),
                ("metric_definitions.csv", "metric_definitions"),
                ("qa_checks.csv", "qa_checks"),
            ]:
                statuses.append(_loaded_status(f"linked:{filename}", enhanced_source / filename, False, tables.get(table_key)))
        return statuses

    if version.dataset_type == "enhanced":
        for filename, table_key, required in [
            ("student_summary.csv", "student_summary", True),
            ("cohort_metrics.csv", "cohort_metrics", True),
            ("master_longitudinal.csv", "master_longitudinal", False),
            ("metric_definitions.csv", "metric_definitions", False),
            ("qa_checks.csv", "qa_checks", False),
            ("methodology.md", "", False),
        ]:
            statuses.append(_loaded_status(filename, version.root_path / filename, required, tables.get(table_key) if table_key else None))
        return statuses

    for filename, table_key, required, path in [
        ("student_summary.csv", "student_summary", True, ROOT / "data" / "processed" / "student_summary.csv"),
        ("master_dataset.csv", "master_dataset", True, ROOT / "data" / "processed" / "master_dataset.csv"),
        ("graduation_rates.csv", "graduation_rates", False, METRICS_ROOT / "graduation_rates.csv"),
        ("retention_rates.csv", "retention_rates", False, METRICS_ROOT / "retention_rates.csv"),
        ("gpa_trends.csv", "gpa_trends", False, METRICS_ROOT / "gpa_trends.csv"),
        ("credit_momentum.csv", "credit_momentum", False, METRICS_ROOT / "credit_momentum.csv"),
        ("standing_distribution.csv", "standing_distribution", False, METRICS_ROOT / "standing_distribution.csv"),
    ]:
        statuses.append(_loaded_status(filename, path, required, tables.get(table_key)))
    return statuses


def load_analysis_bundle(
    version: DatasetVersion,
    metric_definitions: List[MetricDefinition],
    settings: Dict[str, object],
    status_code_map: Dict[str, Iterable[str]],
    chapter_mapping_path: Optional[Path] = None,
) -> AnalysisBundle:
    chapter_mapping = load_chapter_mapping(chapter_mapping_path)

    if version.dataset_type == "current_snapshot":
        tables, notes = _load_current_snapshot_tables(version.root_path)
        bundle_kind = "current_snapshot"
    elif version.dataset_type == "enhanced":
        bundle = load_latest_bundle(
            enhanced_root=DEFAULT_ENHANCED_ROOT,
            explicit_folder=version.root_path,
            explicit_workbook=None,
        )
        tables = bundle.tables
        notes = bundle.caveats
        bundle_kind = "enhanced"
    elif version.dataset_type == "processed":
        tables = {
            "student_summary": pd.read_csv(ROOT / "data" / "processed" / "student_summary.csv"),
            "master_dataset": pd.read_csv(ROOT / "data" / "processed" / "master_dataset.csv"),
        }
        for metric_path in sorted(METRICS_ROOT.glob("*.csv")):
            tables[metric_path.stem] = pd.read_csv(metric_path)
        notes = ["Loaded processed pipeline tables from the fixed local project folders."]
        bundle_kind = "processed"
    else:
        raise ValueError(f"Unsupported dataset type: {version.dataset_type}")

    validation_warnings = _validate_loaded_tables(bundle_kind, tables)

    if bundle_kind == "current_snapshot":
        raw_summary = tables["snapshot_augmented_student_summary"].copy()
        raw_longitudinal = tables.get("master_longitudinal", pd.DataFrame())
        summary = standardize_snapshot_summary(raw_summary, chapter_mapping, settings)
        longitudinal = standardize_enhanced_longitudinal(raw_longitudinal, chapter_mapping) if not raw_longitudinal.empty else pd.DataFrame()
    elif bundle_kind == "enhanced":
        raw_summary = tables["student_summary"].copy()
        raw_longitudinal = tables.get("master_longitudinal", pd.DataFrame())
        summary = standardize_enhanced_summary(raw_summary, chapter_mapping, settings)
        longitudinal = standardize_enhanced_longitudinal(raw_longitudinal, chapter_mapping) if not raw_longitudinal.empty else pd.DataFrame()
    else:
        raw_summary = tables["student_summary"].copy()
        raw_longitudinal = tables.get("master_dataset", pd.DataFrame())
        summary = standardize_processed_summary(raw_summary, chapter_mapping, settings, status_code_map)
        longitudinal = standardize_processed_longitudinal(raw_longitudinal, chapter_mapping) if not raw_longitudinal.empty else pd.DataFrame()

    summary = merge_longitudinal_rollups(summary, longitudinal)
    data_status = _build_data_status(version, tables)
    all_notes = stringify_notes(notes + validation_warnings + version.notes)

    metadata = {
        "bundle_kind": bundle_kind,
        "available_campus_baseline": bool("is_fsl_member" in summary.columns and (~summary["is_fsl_member"].fillna(True)).any()),
        "raw_tables": sorted(tables.keys()),
        "validation_warnings": validation_warnings,
    }
    return AnalysisBundle(
        version=version,
        summary=summary,
        longitudinal=longitudinal,
        tables=tables,
        metric_definitions=_base_metric_definitions(metric_definitions),
        notes=all_notes,
        metadata=metadata,
        data_status=data_status,
    )


def _combine_uploaded_sources(paths: Iterable[Path], source_type: str, alias_lookup: Dict[str, str]) -> pd.DataFrame:
    frames = []
    for path in paths:
        frame = read_tabular_file(path)
        frame = standardize_columns(frame, alias_lookup)
        frame["source_file"] = path.name
        frame["source_type"] = source_type
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def build_processed_tables_from_local_raw(academic_paths: Iterable[Path], roster_paths: Iterable[Path]) -> Dict[str, pd.DataFrame]:
    alias_lookup = build_alias_lookup()
    academic_raw = _combine_uploaded_sources(academic_paths, "academic", alias_lookup)
    roster_raw = _combine_uploaded_sources(roster_paths, "roster", alias_lookup)
    academic_df = normalize_academic_records(academic_raw)
    roster_df = normalize_roster_records(roster_raw)
    master_df, student_summary = build_master_dataset(academic_df, roster_df)
    metrics = build_metrics(master_df, student_summary)
    return {"student_summary": student_summary, "master_dataset": master_df, **metrics}
