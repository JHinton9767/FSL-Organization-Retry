from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional

import pandas as pd

from app.io_utils import ROOT, canonical_headers, normalize_text, read_tabular_file
from app.models import MetricDefinition
from app.status_framework import DEFAULT_OUTCOME_RESOLUTION_CONFIG


CONFIG_DIR = ROOT / "config"
APP_SETTINGS_PATH = CONFIG_DIR / "app_settings.json"
METRIC_CATALOG_PATH = CONFIG_DIR / "metric_catalog.json"
STATUS_CODE_MAP_PATH = CONFIG_DIR / "status_code_map.json"
DATASET_MANIFEST_PATH = CONFIG_DIR / "dataset_manifest.json"
DEFAULT_CHAPTER_GROUPS_PATH = CONFIG_DIR / "chapter_groups.csv"
EXAMPLE_CHAPTER_GROUPS_PATH = CONFIG_DIR / "chapter_groups.example.csv"
MANUAL_CHAPTER_ASSIGNMENTS_PATH = CONFIG_DIR / "manual_chapter_assignments.csv"
MANUAL_ROSTER_CORRECTIONS_PATH = CONFIG_DIR / "manual_roster_corrections.csv"
TRANSCRIPT_TEXT_ROOT = ROOT / "data" / "inbox" / "transcript_text"
MANUAL_TRANSCRIPTS_PATH = TRANSCRIPT_TEXT_ROOT / "Transcripts"


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_settings() -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "default_min_sample_size": 5,
        "max_min_sample_size": 50,
        "high_hours_threshold": 60,
        "chapter_size_bands": [
            {"label": "Small (1-24)", "min": 1, "max": 24},
            {"label": "Medium (25-49)", "min": 25, "max": 49},
            {"label": "Large (50-99)", "min": 50, "max": 99},
            {"label": "Very Large (100+)", "min": 100, "max": None},
        ],
        "secondary_organizations": [
            "Phi Delta Chi",
            "Alpha Phi Omega",
            "Delta Sigma Pi",
            "Alpha Kappa Psi",
            "Gamma Sigma Alpha",
            "Rho Lambda",
            "Order of Omega",
        ],
        "completeness_fields": [
            "student_id",
            "chapter",
            "join_term",
            "latest_outcome_bucket",
            "average_cumulative_gpa",
            "total_cumulative_hours",
        ],
        "outcome_resolution": deepcopy(DEFAULT_OUTCOME_RESOLUTION_CONFIG),
    }
    loaded = load_json(APP_SETTINGS_PATH, {})
    defaults.update(loaded)
    return defaults


def load_status_code_map() -> Dict[str, List[str]]:
    return load_json(
        STATUS_CODE_MAP_PATH,
        {
            "active": ["A", "N", "T", "MEMBER", "COUNCIL", "ACTIVE", "NEW MEMBER"],
            "graduated": ["G", "GRAD", "GRADUATED"],
            "inactive": ["INACTIVE", "DROPPED", "RESIGNED", "REVOKED", "REMOVED"],
            "suspended": ["SUSPENDED"],
            "transfer": ["TRANSFER"],
        },
    )


def load_metric_catalog() -> List[MetricDefinition]:
    definitions: List[MetricDefinition] = []
    for item in load_json(METRIC_CATALOG_PATH, []):
        definitions.append(MetricDefinition(**item))
    return definitions


def load_dataset_manifest() -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "priority": ["canonical"],
        "sources": {
            "canonical": {
                "label": "Canonical Analytics Run",
                "root": "output/canonical",
                "mode": "latest_run",
                "run_prefix": "run_",
                "required_files": [
                    "roster_term.parquet",
                    "academic_term.parquet",
                    "master_longitudinal.parquet",
                    "student_summary.parquet",
                    "cohort_metrics.parquet",
                    "qa_checks.parquet",
                    "canonical_schema.json",
                ],
                "optional_files": [
                    "identity_exceptions.parquet",
                    "term_exceptions.parquet",
                    "status_exceptions.parquet",
                    "chapter_conflicts.parquet",
                    "outcome_exceptions.parquet",
                    "missing_evidence_cases.parquet",
                    "unresolved_chapter_review.parquet",
                    "graduation_status_audit.parquet",
                    "transcript_term_summary.parquet",
                    "transcript_course_detail.parquet",
                    "transcript_parse_audit.parquet",
                    "transcript_parse_issues.parquet",
                ],
            },
        },
    }
    loaded = load_json(DATASET_MANIFEST_PATH, {})
    defaults.update(loaded)
    return defaults


def _standardize_chapter_mapping(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["chapter", "chapter_group", "council", "org_type", "family", "custom_group"])

    header_map = dict(zip(frame.columns, canonical_headers(frame.columns)))
    renamed = frame.rename(columns=header_map).copy()

    alias_map = {
        "chapter": ["chapter", "organization", "org", "chapter_name"],
        "chapter_group": ["chapter_group", "group", "group_name", "custom_group"],
        "council": ["council", "council_name", "family"],
        "org_type": ["org_type", "organization_type", "fraternity_sorority", "type"],
        "family": ["family", "organization_family"],
        "custom_group": ["custom_group", "user_group", "custom_segment"],
    }

    resolved: Dict[str, str] = {}
    for target, aliases in alias_map.items():
        source = next((column for column in renamed.columns if column in aliases), None)
        if source:
            resolved[target] = source

    if "chapter" not in resolved:
        return pd.DataFrame(columns=["chapter", "chapter_group", "council", "org_type", "family", "custom_group"])

    standardized = pd.DataFrame()
    for target in ["chapter", "chapter_group", "council", "org_type", "family", "custom_group"]:
        source = resolved.get(target)
        standardized[target] = renamed[source] if source else ""

    standardized = standardized.fillna("").astype(str)
    standardized["chapter"] = standardized["chapter"].str.strip()
    standardized = standardized.loc[standardized["chapter"].ne("")].drop_duplicates(subset=["chapter"])
    return standardized.reset_index(drop=True)


def load_chapter_mapping(path: Optional[Path] = None) -> pd.DataFrame:
    candidate_paths = []
    if path:
        candidate_paths.append(path)
    candidate_paths.extend([DEFAULT_CHAPTER_GROUPS_PATH, EXAMPLE_CHAPTER_GROUPS_PATH])

    for candidate in candidate_paths:
        if candidate.exists():
            return _standardize_chapter_mapping(read_tabular_file(candidate))

    return pd.DataFrame(columns=["chapter", "chapter_group", "council", "org_type", "family", "custom_group"])


def load_manual_chapter_assignments(path: Optional[Path] = None) -> pd.DataFrame:
    candidate = path or MANUAL_CHAPTER_ASSIGNMENTS_PATH
    columns = [
        "student_id",
        "first_name",
        "last_name",
        "chapter_override",
        "notes",
    ]
    if not candidate.exists():
        return pd.DataFrame(columns=columns)

    frame = read_tabular_file(candidate)
    if frame.empty:
        return pd.DataFrame(columns=columns)

    header_map = dict(zip(frame.columns, canonical_headers(frame.columns)))
    renamed = frame.rename(columns=header_map).copy()
    alias_map = {
        "student_id": ["student_id", "student id", "banner id", "banner"],
        "first_name": ["first_name", "first name"],
        "last_name": ["last_name", "last name"],
        "chapter_override": ["chapter_override", "chapter", "chapter name", "organization", "organization name"],
        "notes": ["notes", "note", "comment", "comments"],
    }

    resolved: Dict[str, str] = {}
    for target, aliases in alias_map.items():
        source = next((column for column in renamed.columns if column in aliases), None)
        if source:
            resolved[target] = source

    standardized = pd.DataFrame()
    for column in columns:
        source = resolved.get(column)
        standardized[column] = renamed[source] if source else ""

    standardized = standardized.fillna("").astype(str)
    for column in ["student_id", "first_name", "last_name", "chapter_override", "notes"]:
        standardized[column] = standardized[column].str.strip()
    standardized = standardized.loc[
        standardized["chapter_override"].ne("")
        & (
            standardized["student_id"].ne("")
            | standardized["first_name"].ne("")
            | standardized["last_name"].ne("")
        )
    ].copy()
    return standardized.reset_index(drop=True)


MANUAL_ROSTER_CORRECTION_COLUMNS = [
    "student_id",
    "last_name",
    "first_name",
    "student_join_term",
    "organization_join_term",
    "organization_name",
    "leaving_organization_term",
    "final_status_term",
    "final_status",
]


def empty_manual_roster_corrections() -> pd.DataFrame:
    return pd.DataFrame(columns=MANUAL_ROSTER_CORRECTION_COLUMNS)


def _default_student_join_term(frame: pd.DataFrame) -> pd.DataFrame:
    if {"student_join_term", "organization_join_term"}.issubset(frame.columns):
        missing_student_join = frame["student_join_term"].fillna("").astype(str).str.strip().eq("")
        frame.loc[missing_student_join, "student_join_term"] = frame.loc[missing_student_join, "organization_join_term"]
    return frame


def load_manual_roster_corrections(path: Optional[Path] = None) -> pd.DataFrame:
    candidate = path or MANUAL_ROSTER_CORRECTIONS_PATH
    if not candidate.exists():
        return empty_manual_roster_corrections()

    frame = read_tabular_file(candidate)
    if frame.empty:
        return empty_manual_roster_corrections()

    header_map = dict(zip(frame.columns, canonical_headers(frame.columns)))
    renamed = frame.rename(columns=header_map).copy()
    alias_map = {
        "student_id": ["student_id", "student id", "banner id", "banner"],
        "last_name": ["last_name", "last name"],
        "first_name": ["first_name", "first name"],
        "student_join_term": ["student_join_term", "student join term", "school_entry_term", "school entry term"],
        "organization_join_term": ["organization_join_term", "organization join term", "org join term", "join_term", "term_code", "term code", "term_label", "term label", "term"],
        "organization_name": ["organization_name", "organization name", "chapter_override", "chapter override", "chapter", "new chapter", "organization"],
        "leaving_organization_term": ["leaving_organization_term", "leaving organization term", "last_org_term", "last observed org term"],
        "final_status_term": ["final_status_term", "final status term", "graduation_term", "status term"],
        "final_status": ["final_status", "final status", "status_override", "status override", "status", "member status", "membership status"],
    }

    resolved: Dict[str, str] = {}
    for target, aliases in alias_map.items():
        source = next((column for column in renamed.columns if column in aliases), None)
        if source:
            resolved[target] = source

    standardized = pd.DataFrame()
    for column in MANUAL_ROSTER_CORRECTION_COLUMNS:
        source = resolved.get(column)
        standardized[column] = renamed[source] if source else ""

    standardized = standardized.fillna("").astype(str)
    for column in MANUAL_ROSTER_CORRECTION_COLUMNS:
        standardized[column] = standardized[column].str.strip()
    standardized = _default_student_join_term(standardized)

    delete_source = next((column for column in renamed.columns if column in {"delete_row", "delete row", "x", "delete"}), None)
    delete_mask = (
        renamed[delete_source].fillna("").astype(str).str.strip().str.lower().isin({"yes", "y", "true", "1", "x", "delete"})
        if delete_source
        else pd.Series(False, index=standardized.index)
    )
    has_identity = (
        standardized["student_id"].ne("")
        | standardized["first_name"].ne("")
        | standardized["last_name"].ne("")
    )
    has_action = (
        standardized["organization_join_term"].ne("")
        | standardized["organization_name"].ne("")
        | standardized["leaving_organization_term"].ne("")
        | standardized["final_status_term"].ne("")
        | standardized["final_status"].ne("")
    )
    return standardized.loc[has_identity & has_action & ~delete_mask].reset_index(drop=True)


def save_manual_roster_corrections(frame: pd.DataFrame, path: Optional[Path] = None) -> Path:
    candidate = path or MANUAL_ROSTER_CORRECTIONS_PATH
    candidate.parent.mkdir(parents=True, exist_ok=True)
    if frame is None or frame.empty:
        cleaned = empty_manual_roster_corrections()
    else:
        cleaned = frame.copy()
        delete_mask = (
            cleaned.get("delete_row", pd.Series("", index=cleaned.index))
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"yes", "y", "true", "1", "x", "delete"})
        )
        for column in MANUAL_ROSTER_CORRECTION_COLUMNS:
            if column not in cleaned.columns:
                cleaned[column] = ""
        cleaned = cleaned[MANUAL_ROSTER_CORRECTION_COLUMNS].fillna("").astype(str)
        for column in MANUAL_ROSTER_CORRECTION_COLUMNS:
            cleaned[column] = cleaned[column].str.strip()
        cleaned = _default_student_join_term(cleaned)
        has_identity = cleaned["student_id"].ne("") | cleaned["first_name"].ne("") | cleaned["last_name"].ne("")
        has_action = (
            cleaned["organization_join_term"].ne("")
            | cleaned["organization_name"].ne("")
            | cleaned["leaving_organization_term"].ne("")
            | cleaned["final_status_term"].ne("")
            | cleaned["final_status"].ne("")
        )
        cleaned = cleaned.loc[has_identity & has_action & ~delete_mask].reset_index(drop=True)
    cleaned.to_csv(candidate, index=False)
    return candidate


def _manual_transcript_filename_part(value: object, fallback: str) -> str:
    text = normalize_text(value) or fallback
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_")
    return text or fallback


def manual_transcript_path_for_correction(row: pd.Series, folder: Optional[Path] = None) -> Path:
    target_folder = folder or MANUAL_TRANSCRIPTS_PATH
    student_id = _manual_transcript_filename_part(row.get("student_id", ""), "NoStudentID")
    last_name = _manual_transcript_filename_part(row.get("last_name", ""), "UnknownLast")
    first_name = _manual_transcript_filename_part(row.get("first_name", ""), "UnknownFirst")
    return target_folder / f"{student_id}_{last_name}_{first_name}.txt"


def manual_transcript_template(row: pd.Series) -> str:
    return "\n".join(
        [
            f"Student ID: {normalize_text(row.get('student_id', ''))}",
            f"Name: {normalize_text(row.get('first_name', ''))} {normalize_text(row.get('last_name', ''))}".strip(),
            f"Student Join Term: {normalize_text(row.get('student_join_term', ''))}",
            f"Organization Join Term: {normalize_text(row.get('organization_join_term', ''))}",
            f"Organization Name: {normalize_text(row.get('organization_name', ''))}",
            f"Leaving Organization Term: {normalize_text(row.get('leaving_organization_term', ''))}",
            f"Final Status Term: {normalize_text(row.get('final_status_term', ''))}",
            f"Final Status: {normalize_text(row.get('final_status', ''))}",
            "",
            "Paste transcript text below. Use term headers such as Spring 2024, then course rows, then the Term at a glance block.",
            "Transcript text is academic evidence only; it does not create a graduation outcome unless graduation is explicitly stated.",
            "",
            "--- TRANSCRIPT TEXT ---",
            "",
        ]
    )


def ensure_manual_transcript_files(corrections: pd.DataFrame, folder: Optional[Path] = None) -> List[Path]:
    if corrections is None or corrections.empty:
        return []

    target_folder = folder or MANUAL_TRANSCRIPTS_PATH
    target_folder.mkdir(parents=True, exist_ok=True)

    frame = corrections.copy()
    for column in MANUAL_ROSTER_CORRECTION_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame = frame[MANUAL_ROSTER_CORRECTION_COLUMNS].fillna("").astype(str)
    for column in MANUAL_ROSTER_CORRECTION_COLUMNS:
        frame[column] = frame[column].str.strip()
    frame = _default_student_join_term(frame)

    created: List[Path] = []
    seen: set[Path] = set()
    for _, row in frame.iterrows():
        has_identity = bool(normalize_text(row.get("student_id", "")) or normalize_text(row.get("first_name", "")) or normalize_text(row.get("last_name", "")))
        if not has_identity:
            continue
        path = manual_transcript_path_for_correction(row, target_folder)
        if path in seen:
            continue
        seen.add(path)
        if path.exists():
            continue
        path.write_text(manual_transcript_template(row), encoding="utf-8")
        created.append(path)
    return created


def stringify_notes(values: List[str]) -> List[str]:
    return [normalize_text(value) for value in values if normalize_text(value)]
