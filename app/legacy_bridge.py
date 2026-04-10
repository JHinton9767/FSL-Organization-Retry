from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from app.config_loader import load_chapter_mapping, stringify_notes
from app.io_utils import ROOT, read_tabular_file, safe_slug, write_dataframe_cache
from app.models import AnalysisBundle, DatasetVersion, MetricDefinition
from app.standardize import (
    merge_longitudinal_rollups,
    standardize_enhanced_longitudinal,
    standardize_enhanced_summary,
    standardize_processed_longitudinal,
    standardize_processed_summary,
    standardize_snapshot_summary,
)
from src.build_current_snapshot_analytics import (
    build_augmented_cohort_metrics,
    build_chapter_metrics,
    build_definitions_table as build_snapshot_definitions_table,
    build_qa_table,
    load_combined_snapshot_table,
    merge_augmented_summary,
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


ENHANCED_ROOT = ROOT / "output" / "enhanced_metrics"
SNAPSHOT_ROOT = ROOT / "output" / "current_snapshot_metrics"
PROCESSED_ROOT = ROOT / "data" / "processed"
METRICS_ROOT = ROOT / "output" / "metrics"
APP_SESSIONS_ROOT = PROCESSED_ROOT / "app_sessions"


def discover_dataset_versions() -> List[DatasetVersion]:
    versions: List[DatasetVersion] = []

    if SNAPSHOT_ROOT.exists():
        for folder in sorted([path for path in SNAPSHOT_ROOT.iterdir() if path.is_dir()], key=lambda item: item.stat().st_mtime, reverse=True):
            if (folder / "snapshot_augmented_student_summary.csv").exists():
                versions.append(
                    DatasetVersion(
                        key=f"snapshot::{folder}",
                        label=f"Current Snapshot Run - {folder.name}",
                        dataset_type="current_snapshot",
                        root_path=folder,
                        created_at=datetime.fromtimestamp(folder.stat().st_mtime).isoformat(timespec="seconds"),
                    )
                )

    if ENHANCED_ROOT.exists():
        for folder in sorted([path for path in ENHANCED_ROOT.iterdir() if path.is_dir()], key=lambda item: item.stat().st_mtime, reverse=True):
            if (folder / "student_summary.csv").exists():
                versions.append(
                    DatasetVersion(
                        key=f"enhanced::{folder}",
                        label=f"Enhanced Run - {folder.name}",
                        dataset_type="enhanced",
                        root_path=folder,
                        created_at=datetime.fromtimestamp(folder.stat().st_mtime).isoformat(timespec="seconds"),
                    )
                )

    if (PROCESSED_ROOT / "student_summary.csv").exists() and (PROCESSED_ROOT / "master_dataset.csv").exists():
        versions.append(
            DatasetVersion(
                key=f"processed::{PROCESSED_ROOT}",
                label="Processed Pipeline Tables",
                dataset_type="processed",
                root_path=PROCESSED_ROOT,
                created_at=datetime.fromtimestamp((PROCESSED_ROOT / "student_summary.csv").stat().st_mtime).isoformat(timespec="seconds"),
            )
        )

    APP_SESSIONS_ROOT.mkdir(parents=True, exist_ok=True)
    for folder in sorted([path for path in APP_SESSIONS_ROOT.iterdir() if path.is_dir()], key=lambda item: item.stat().st_mtime, reverse=True):
        manifest_path = folder / "manifest.json"
        if not manifest_path.exists():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        versions.append(
            DatasetVersion(
                key=f"session::{folder}",
                label=manifest.get("label", folder.name),
                dataset_type="app_session",
                root_path=folder,
                created_at=manifest.get("created_at", ""),
                notes=manifest.get("notes", []),
            )
        )

    return versions


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
    methodology_path = folder / "methodology.md"
    enhanced_source = _parse_enhanced_source_from_methodology(methodology_path)
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


def _load_bundle_from_workbook(path: Path) -> Tuple[str, Dict[str, pd.DataFrame], List[str]]:
    if "organization_entry_analytics_enhanced_" in path.name.lower():
        bundle = load_latest_bundle(
            enhanced_root=DEFAULT_ENHANCED_ROOT,
            explicit_folder=None,
            explicit_workbook=path,
        )
        return "enhanced", bundle.tables, bundle.caveats

    if "organization_entry_snapshot_augmented_" in path.name.lower():
        tables = {
            "snapshot_augmented_student_summary": pd.read_excel(path, sheet_name="Augmented_Summary"),
            "snapshot_augmented_cohort_metrics": pd.read_excel(path, sheet_name="Cohort_Metrics"),
            "snapshot_augmented_chapter_metrics": pd.read_excel(path, sheet_name="Chapter_Metrics"),
            "snapshot_merge_qa": pd.read_excel(path, sheet_name="Snapshot_QA"),
        }
        return "current_snapshot", tables, ["Snapshot workbook upload did not include a linked enhanced source folder."]

    return "", {}, []


def _classify_precomputed_table(path: Path, frame: pd.DataFrame) -> str:
    columns = {str(column).strip() for column in frame.columns}
    lowered = path.name.lower()

    if "snapshot_augmented_student_summary" in lowered or "Augmented Latest Outcome Bucket" in columns:
        return "snapshot_augmented_student_summary"
    if "snapshot_augmented_cohort_metrics" in lowered:
        return "snapshot_augmented_cohort_metrics"
    if "snapshot_augmented_chapter_metrics" in lowered:
        return "snapshot_augmented_chapter_metrics"
    if "snapshot_merge_qa" in lowered:
        return "snapshot_merge_qa"
    if {"Student ID", "Preferred First Name", "Organization Entry Cohort"}.issubset(columns):
        return "student_summary"
    if {"Student ID", "Term", "Relative Term Index From Org Entry"}.issubset(columns):
        return "master_longitudinal"
    if {"Metric Group", "Metric Label", "Cohort"}.issubset(columns):
        return "cohort_metrics"
    if {"Check", "Value"}.issubset(columns):
        return "qa_checks"
    if {"student_id", "chapter", "graduated_4yr"}.issubset(columns):
        return "processed_student_summary"
    if {"student_id", "term", "chapter"}.issubset(columns):
        return "processed_master_dataset"
    return ""


def _save_manifest(session_root: Path, manifest: Dict[str, object]) -> None:
    (session_root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def save_uploaded_files(session_root: Path, category: str, uploads: Iterable[object]) -> List[Path]:
    category_root = session_root / "uploads" / category
    category_root.mkdir(parents=True, exist_ok=True)
    saved_paths: List[Path] = []
    for uploaded in uploads:
        path = category_root / Path(uploaded.name).name
        path.write_bytes(bytes(uploaded.getbuffer()))
        saved_paths.append(path)
    return saved_paths


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


def _load_app_session_manifest(version: DatasetVersion) -> Dict[str, object]:
    return json.loads((version.root_path / "manifest.json").read_text(encoding="utf-8"))


def _standardized_bundle_from_tables(
    version: DatasetVersion,
    bundle_kind: str,
    tables: Dict[str, pd.DataFrame],
    metric_definitions: List[MetricDefinition],
    chapter_mapping: pd.DataFrame,
    settings: Dict[str, object],
    status_code_map: Dict[str, Iterable[str]],
    notes: List[str],
) -> AnalysisBundle:
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

    metadata = {
        "bundle_kind": bundle_kind,
        "available_campus_baseline": bool("is_fsl_member" in summary.columns and (~summary["is_fsl_member"].fillna(True)).any()),
        "raw_tables": sorted(tables.keys()),
    }
    return AnalysisBundle(
        version=version,
        summary=summary,
        longitudinal=longitudinal,
        tables=tables,
        metric_definitions=_base_metric_definitions(metric_definitions),
        notes=stringify_notes(notes),
        metadata=metadata,
    )


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
        return _standardized_bundle_from_tables(version, "current_snapshot", tables, metric_definitions, chapter_mapping, settings, status_code_map, notes)

    if version.dataset_type == "enhanced":
        bundle = load_latest_bundle(
            enhanced_root=DEFAULT_ENHANCED_ROOT,
            explicit_folder=version.root_path,
            explicit_workbook=None,
        )
        return _standardized_bundle_from_tables(version, "enhanced", bundle.tables, metric_definitions, chapter_mapping, settings, status_code_map, bundle.caveats)

    if version.dataset_type == "processed":
        tables = {
            "student_summary": pd.read_csv(version.root_path / "student_summary.csv"),
            "master_dataset": pd.read_csv(version.root_path / "master_dataset.csv"),
        }
        for metric_path in sorted(METRICS_ROOT.glob("*.csv")):
            tables[metric_path.stem] = pd.read_csv(metric_path)
        notes = ["Loaded base processed pipeline tables from data/processed and output/metrics."]
        return _standardized_bundle_from_tables(version, "processed", tables, metric_definitions, chapter_mapping, settings, status_code_map, notes)

    manifest = _load_app_session_manifest(version)
    tables = {
        table_name: read_tabular_file(version.root_path / relative_path)
        for table_name, relative_path in manifest.get("tables", {}).items()
    }
    notes = manifest.get("notes", [])
    mapping_override = version.root_path / manifest["chapter_mapping_path"] if manifest.get("chapter_mapping_path") else chapter_mapping_path
    chapter_mapping = load_chapter_mapping(mapping_override)
    return _standardized_bundle_from_tables(
        version,
        manifest.get("bundle_kind", "processed"),
        tables,
        metric_definitions,
        chapter_mapping,
        settings,
        status_code_map,
        notes,
    )


def _relative_or_absolute(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def process_uploaded_session(
    label: str,
    academic_paths: List[Path],
    roster_paths: List[Path],
    precomputed_paths: List[Path],
    snapshot_paths: List[Path],
    chapter_mapping_path: Optional[Path],
    session_root: Optional[Path] = None,
) -> DatasetVersion:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_root = session_root or (APP_SESSIONS_ROOT / f"{safe_slug(label)}_{timestamp}")
    (session_root / "processed").mkdir(parents=True, exist_ok=True)

    manifest: Dict[str, object] = {
        "label": label,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "bundle_kind": "processed",
        "notes": [],
        "tables": {},
        "source_files": {
            "academic": [_relative_or_absolute(path, session_root) for path in academic_paths],
            "roster": [_relative_or_absolute(path, session_root) for path in roster_paths],
            "precomputed": [_relative_or_absolute(path, session_root) for path in precomputed_paths],
            "snapshot": [_relative_or_absolute(path, session_root) for path in snapshot_paths],
        },
    }
    if chapter_mapping_path:
        manifest["chapter_mapping_path"] = _relative_or_absolute(chapter_mapping_path, session_root)

    workbook_bundle_kind = ""
    workbook_tables: Dict[str, pd.DataFrame] = {}
    workbook_notes: List[str] = []
    for path in precomputed_paths:
        kind, tables, notes = _load_bundle_from_workbook(path)
        if kind:
            workbook_bundle_kind = kind
            workbook_tables = tables
            workbook_notes = notes
            break

    tables: Dict[str, pd.DataFrame] = {}
    bundle_kind = "processed"
    notes: List[str] = []

    if workbook_bundle_kind:
        bundle_kind = workbook_bundle_kind
        tables = workbook_tables
        notes.extend(workbook_notes)
    elif precomputed_paths:
        recognized: Dict[str, pd.DataFrame] = {}
        for path in precomputed_paths:
            frame = read_tabular_file(path)
            key = _classify_precomputed_table(path, frame)
            if key:
                recognized[key] = frame
        if "snapshot_augmented_student_summary" in recognized:
            bundle_kind = "current_snapshot"
            tables = recognized
            notes.append("Uploaded precomputed snapshot-augmented tables were loaded directly.")
        elif "student_summary" in recognized:
            bundle_kind = "enhanced"
            tables = recognized
            notes.append("Uploaded precomputed enhanced analytics tables were loaded directly.")
        elif "processed_student_summary" in recognized:
            bundle_kind = "processed"
            tables = {
                "student_summary": recognized["processed_student_summary"],
                "master_dataset": recognized.get("processed_master_dataset", pd.DataFrame()),
            }
            notes.append("Uploaded precomputed processed pipeline tables were loaded directly.")

    if bundle_kind == "enhanced" and snapshot_paths and "student_summary" in tables and "master_longitudinal" in tables:
        snapshot = load_combined_snapshot_table(snapshot_paths)
        augmented_summary = merge_augmented_summary(tables["student_summary"], tables["master_longitudinal"], snapshot)
        tables["snapshot_augmented_student_summary"] = augmented_summary
        tables["snapshot_augmented_cohort_metrics"] = build_augmented_cohort_metrics(augmented_summary)
        tables["snapshot_augmented_chapter_metrics"] = build_chapter_metrics(augmented_summary)
        tables["snapshot_merge_qa"] = build_qa_table(augmented_summary, snapshot)
        tables["snapshot_definitions"] = build_snapshot_definitions_table()
        bundle_kind = "current_snapshot"
        notes.append("Uploaded snapshot files were additively merged onto the uploaded enhanced tables.")

    if not tables and academic_paths and roster_paths:
        alias_lookup = build_alias_lookup()
        academic_raw = _combine_uploaded_sources(academic_paths, "academic", alias_lookup)
        roster_raw = _combine_uploaded_sources(roster_paths, "roster", alias_lookup)
        academic_df = normalize_academic_records(academic_raw)
        roster_df = normalize_roster_records(roster_raw)
        master_df, student_summary = build_master_dataset(academic_df, roster_df)
        metrics = build_metrics(master_df, student_summary)
        tables = {"student_summary": student_summary, "master_dataset": master_df, **metrics}
        notes.append("A new processed session was built from uploaded academic and roster files.")
        if snapshot_paths:
            notes.append(
                "Snapshot files were staged, but additive snapshot augmentation currently requires enhanced-style summary and longitudinal tables."
            )

    if not tables:
        raise FileNotFoundError(
            "No usable dataset could be built from the uploaded files. Provide academic + roster files or recognized precomputed tables."
        )

    if bundle_kind == "current_snapshot":
        persist_map = {
            "snapshot_augmented_student_summary": "processed/snapshot_augmented_student_summary.csv",
            "snapshot_augmented_cohort_metrics": "processed/snapshot_augmented_cohort_metrics.csv",
            "snapshot_augmented_chapter_metrics": "processed/snapshot_augmented_chapter_metrics.csv",
            "snapshot_merge_qa": "processed/snapshot_merge_qa.csv",
            "master_longitudinal": "processed/master_longitudinal.csv",
            "cohort_metrics": "processed/cohort_metrics.csv",
            "metric_definitions": "processed/metric_definitions.csv",
            "qa_checks": "processed/qa_checks.csv",
        }
    elif bundle_kind == "enhanced":
        persist_map = {
            "student_summary": "processed/student_summary.csv",
            "master_longitudinal": "processed/master_longitudinal.csv",
            "cohort_metrics": "processed/cohort_metrics.csv",
            "metric_definitions": "processed/metric_definitions.csv",
            "qa_checks": "processed/qa_checks.csv",
        }
    else:
        persist_map = {
            "student_summary": "processed/student_summary.csv",
            "master_dataset": "processed/master_dataset.csv",
            "graduation_rates": "processed/graduation_rates.csv",
            "retention_rates": "processed/retention_rates.csv",
            "gpa_trends": "processed/gpa_trends.csv",
            "credit_momentum": "processed/credit_momentum.csv",
            "standing_distribution": "processed/standing_distribution.csv",
        }

    persisted: Dict[str, str] = {}
    for table_name, relative_path in persist_map.items():
        if table_name not in tables or tables[table_name].empty and table_name not in {"student_summary", "master_dataset", "master_longitudinal"}:
            continue
        csv_path = session_root / relative_path
        parquet_path = csv_path.with_suffix(".parquet")
        write_dataframe_cache(tables[table_name], csv_path, parquet_path)
        persisted[table_name] = str(csv_path.relative_to(session_root))

    manifest["bundle_kind"] = bundle_kind
    manifest["notes"] = notes
    manifest["tables"] = persisted
    _save_manifest(session_root, manifest)

    return DatasetVersion(
        key=f"session::{session_root}",
        label=label,
        dataset_type="app_session",
        root_path=session_root,
        created_at=manifest["created_at"],
        notes=notes,
    )
