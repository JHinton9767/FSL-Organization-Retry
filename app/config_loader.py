from __future__ import annotations

from io import BytesIO
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd

from app.io_utils import ROOT, canonical_headers, normalize_text, read_tabular_file
from app.models import MetricDefinition
from app.status_framework import DEFAULT_OUTCOME_RESOLUTION_CONFIG
from src.path_config import load_path_config


CONFIG_DIR = ROOT / "config"
APP_SETTINGS_PATH = CONFIG_DIR / "app_settings.json"
METRIC_CATALOG_PATH = CONFIG_DIR / "metric_catalog.json"
STATUS_CODE_MAP_PATH = CONFIG_DIR / "status_code_map.json"
DATASET_MANIFEST_PATH = CONFIG_DIR / "dataset_manifest.json"
DEFAULT_CHAPTER_GROUPS_PATH = CONFIG_DIR / "chapter_groups.csv"
EXAMPLE_CHAPTER_GROUPS_PATH = CONFIG_DIR / "chapter_groups.example.csv"
MANUAL_CHAPTER_ASSIGNMENTS_PATH = CONFIG_DIR / "manual_chapter_assignments.csv"
MANUAL_ROSTER_CORRECTIONS_PATH = CONFIG_DIR / "manual_roster_corrections.csv"
MANUAL_ADJUSTMENTS_PATH = CONFIG_DIR / "manual_adjustments.csv"
MANUAL_REVIEW_QUEUE_PATH = CONFIG_DIR / "manual_review_queue.csv"


def _configured_transcript_text_root() -> Path:
    try:
        return load_path_config().transcript_text_root
    except Exception:
        return ROOT / "data" / "inbox" / "transcript_text"


TRANSCRIPT_TEXT_ROOT = _configured_transcript_text_root()
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
                    "student_source_appearances.parquet",
                    "student_longitudinal_tracking.parquet",
                    "input_group_outcome_buckets.parquet",
                    "yearly_unique_id_checklist.parquet",
                    "manual_review_queue.parquet",
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
    "exclude_from_roster_calculations",
]
MANUAL_ADJUSTMENT_COLUMNS = [
    "adjustment_id",
    "student_id",
    "normalized_student_id",
    "adjustment_type",
    "field_to_override",
    "original_value",
    "adjusted_value",
    "reason",
    "evidence",
    "source_file",
    "source_sheet",
    "reviewer",
    "created_at",
    "active",
]
MANUAL_REVIEW_QUEUE_COLUMNS = [
    "review_key",
    "student_id",
    "last_name",
    "first_name",
    "student_name",
    "chapter",
    "join_term",
    "last_observed_org_term",
    "latest_outcome_bucket",
    "outcome_resolution_group",
    "academic_year",
    "term",
    "organization",
    "issue_type",
    "outcome_bucket",
    "priority",
    "input_group_id",
    "source_file",
    "source_sheet",
    "evidence_summary",
    "suggested_action",
    "queue_reason",
    "assigned_to",
    "review_status",
    "needs_transcript",
    "review_notes",
    "has_manual_correction",
    "transcript_file_exists",
    "updated_at",
]
REVIEW_STATUS_OPTIONS = ["Needs Review", "In Progress", "Waiting on Transcript", "Corrected", "Skipped / No Change", "Blocked"]
MANUAL_ROW_DELETE_VALUES = {"yes", "y", "true", "1", "x", "delete"}
MANUAL_ACTION_VALUES = {"yes", "y", "true", "1", "x", "remove", "delete", "exclude"}


def empty_manual_roster_corrections() -> pd.DataFrame:
    return pd.DataFrame(columns=MANUAL_ROSTER_CORRECTION_COLUMNS)


def empty_manual_review_queue() -> pd.DataFrame:
    return pd.DataFrame(columns=MANUAL_REVIEW_QUEUE_COLUMNS)


def empty_manual_adjustments() -> pd.DataFrame:
    return pd.DataFrame(columns=MANUAL_ADJUSTMENT_COLUMNS)


def _manual_adjustment_id(row: pd.Series) -> str:
    if normalize_text(row.get("adjustment_id", "")):
        return normalize_text(row.get("adjustment_id", ""))
    key = "\u241f".join(normalize_text(row.get(column, "")) for column in MANUAL_ADJUSTMENT_COLUMNS if column != "adjustment_id")
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"adj_{digest}"


def normalize_manual_adjustments(frame: Optional[pd.DataFrame]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return empty_manual_adjustments()

    cleaned = frame.copy()
    header_map = dict(zip(cleaned.columns, canonical_headers(cleaned.columns)))
    cleaned = cleaned.rename(columns=header_map)
    alias_map = {
        "adjustment_id": ["adjustment_id", "adjustment id", "id"],
        "student_id": ["student_id", "student id", "banner id", "banner"],
        "normalized_student_id": ["normalized_student_id", "normalized student id", "normalized id"],
        "adjustment_type": ["adjustment_type", "adjustment type", "type"],
        "field_to_override": ["field_to_override", "field to override", "field", "override field"],
        "original_value": ["original_value", "original value", "old value"],
        "adjusted_value": ["adjusted_value", "adjusted value", "new value", "value"],
        "reason": ["reason", "notes", "note", "reviewer_notes", "reviewer notes"],
        "evidence": ["evidence", "evidence_summary", "evidence summary"],
        "source_file": ["source_file", "source file"],
        "source_sheet": ["source_sheet", "source sheet"],
        "reviewer": ["reviewer", "reviewed_by", "reviewed by"],
        "created_at": ["created_at", "created at", "reviewed_at", "reviewed at"],
        "active": ["active", "enabled", "use", "apply"],
    }
    resolved: Dict[str, str] = {}
    for target, aliases in alias_map.items():
        source = next((column for column in cleaned.columns if column in aliases), None)
        if source:
            resolved[target] = source

    standardized = pd.DataFrame(index=cleaned.index)
    for column in MANUAL_ADJUSTMENT_COLUMNS:
        source = resolved.get(column)
        standardized[column] = cleaned[source] if source else ""
    standardized = standardized.fillna("").astype(str)
    for column in MANUAL_ADJUSTMENT_COLUMNS:
        standardized[column] = standardized[column].str.strip()
    standardized["normalized_student_id"] = standardized["normalized_student_id"].where(
        standardized["normalized_student_id"].ne(""),
        standardized["student_id"],
    )
    standardized["adjustment_type"] = standardized["adjustment_type"].where(
        standardized["adjustment_type"].ne(""),
        "outcome_override",
    )
    standardized["field_to_override"] = standardized["field_to_override"].where(
        standardized["field_to_override"].ne(""),
        "final_outcome_bucket",
    )
    standardized["created_at"] = standardized["created_at"].where(
        standardized["created_at"].ne(""),
        datetime.now().isoformat(timespec="seconds"),
    )
    standardized["active"] = standardized["active"].where(standardized["active"].ne(""), "Yes")
    has_identity = standardized["normalized_student_id"].ne("") | standardized["student_id"].ne("")
    has_action = standardized["field_to_override"].ne("") & standardized["adjusted_value"].ne("")
    standardized = standardized.loc[has_identity & has_action].copy()
    if standardized.empty:
        return empty_manual_adjustments()
    standardized["adjustment_id"] = standardized.apply(_manual_adjustment_id, axis=1)
    return standardized.drop_duplicates(subset=["adjustment_id"], keep="last").reset_index(drop=True)


def load_manual_adjustments(path: Optional[Path] = None) -> pd.DataFrame:
    candidate = path or MANUAL_ADJUSTMENTS_PATH
    if not candidate.exists():
        return empty_manual_adjustments()
    frame = read_tabular_file(candidate)
    return normalize_manual_adjustments(frame)


def save_manual_adjustments(frame: pd.DataFrame, path: Optional[Path] = None) -> Path:
    candidate = path or MANUAL_ADJUSTMENTS_PATH
    candidate.parent.mkdir(parents=True, exist_ok=True)
    cleaned = normalize_manual_adjustments(frame)
    cleaned.to_csv(candidate, index=False)
    return candidate


def append_manual_adjustments(frame: pd.DataFrame, path: Optional[Path] = None) -> Dict[str, object]:
    candidate = path or MANUAL_ADJUSTMENTS_PATH
    candidate.parent.mkdir(parents=True, exist_ok=True)
    incoming = normalize_manual_adjustments(frame)
    if incoming.empty:
        if not candidate.exists():
            empty_manual_adjustments().to_csv(candidate, index=False)
        return {"path": candidate, "incoming_rows": 0, "appended_rows": 0, "skipped_rows": 0}

    existing_ids: set[str] = set()
    if candidate.exists() and candidate.stat().st_size:
        try:
            existing = pd.read_csv(candidate, usecols=["adjustment_id"], dtype=str)
            existing_ids = set(existing["adjustment_id"].fillna("").astype(str).str.strip())
        except (ValueError, pd.errors.EmptyDataError):
            existing_ids = set()

    incoming = incoming.loc[~incoming["adjustment_id"].isin(existing_ids)].copy()
    skipped = int(len(normalize_manual_adjustments(frame)) - len(incoming))
    if incoming.empty:
        if not candidate.exists():
            empty_manual_adjustments().to_csv(candidate, index=False)
        return {"path": candidate, "incoming_rows": skipped, "appended_rows": 0, "skipped_rows": skipped}

    write_header = not candidate.exists() or candidate.stat().st_size == 0
    incoming.to_csv(candidate, mode="a", header=write_header, index=False)
    return {"path": candidate, "incoming_rows": skipped + len(incoming), "appended_rows": len(incoming), "skipped_rows": skipped}


def _default_student_join_term(frame: pd.DataFrame) -> pd.DataFrame:
    if {"student_join_term", "organization_join_term"}.issubset(frame.columns):
        missing_student_join = frame["student_join_term"].fillna("").astype(str).str.strip().eq("")
        frame.loc[missing_student_join, "student_join_term"] = frame.loc[missing_student_join, "organization_join_term"]
    return frame


def normalize_manual_roster_corrections(frame: Optional[pd.DataFrame]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return empty_manual_roster_corrections()

    cleaned = frame.copy()
    delete_mask = (
        cleaned.get("delete_row", pd.Series("", index=cleaned.index))
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .isin(MANUAL_ROW_DELETE_VALUES)
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
        | cleaned["exclude_from_roster_calculations"].str.lower().isin(MANUAL_ACTION_VALUES)
    )
    return cleaned.loc[has_identity & has_action & ~delete_mask].reset_index(drop=True)


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
        "exclude_from_roster_calculations": [
            "exclude_from_roster_calculations",
            "exclude from roster calculations",
            "exclude_from_roster",
            "exclude from roster",
            "remove_from_roster",
            "remove from roster",
            "delete_from_roster",
            "delete from roster",
            "ignore_roster_row",
            "ignore roster row",
            "exclude",
        ],
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

    delete_source = next((column for column in renamed.columns if column in {"delete_row", "delete row", "x", "delete"}), None)
    if delete_source:
        standardized["delete_row"] = renamed[delete_source]
    return normalize_manual_roster_corrections(standardized)


def save_manual_roster_corrections(frame: pd.DataFrame, path: Optional[Path] = None) -> Path:
    candidate = path or MANUAL_ROSTER_CORRECTIONS_PATH
    candidate.parent.mkdir(parents=True, exist_ok=True)
    cleaned = normalize_manual_roster_corrections(frame)
    cleaned.to_csv(candidate, index=False)
    return candidate


def append_manual_roster_corrections(frame: pd.DataFrame, path: Optional[Path] = None) -> Dict[str, object]:
    candidate = path or MANUAL_ROSTER_CORRECTIONS_PATH
    candidate.parent.mkdir(parents=True, exist_ok=True)
    incoming = normalize_manual_roster_corrections(frame)
    if incoming.empty:
        if not candidate.exists():
            empty_manual_roster_corrections().to_csv(candidate, index=False)
        return {"path": candidate, "incoming_rows": 0, "appended_rows": 0, "skipped_rows": 0}

    incoming_keys = incoming[MANUAL_ROSTER_CORRECTION_COLUMNS].fillna("").astype(str).agg("\u241f".join, axis=1)
    existing_keys: set[str] = set()
    if candidate.exists() and candidate.stat().st_size:
        try:
            existing = pd.read_csv(candidate, dtype=str)
            for column in MANUAL_ROSTER_CORRECTION_COLUMNS:
                if column not in existing.columns:
                    existing[column] = ""
            existing_keys = set(existing[MANUAL_ROSTER_CORRECTION_COLUMNS].fillna("").astype(str).agg("\u241f".join, axis=1))
        except pd.errors.EmptyDataError:
            existing_keys = set()

    append_mask = ~incoming_keys.isin(existing_keys)
    to_append = incoming.loc[append_mask].copy()
    skipped = int((~append_mask).sum())
    if to_append.empty:
        if not candidate.exists():
            empty_manual_roster_corrections().to_csv(candidate, index=False)
        return {"path": candidate, "incoming_rows": len(incoming), "appended_rows": 0, "skipped_rows": skipped}

    write_header = not candidate.exists() or candidate.stat().st_size == 0
    to_append.to_csv(candidate, mode="a", header=write_header, index=False)
    return {"path": candidate, "incoming_rows": len(incoming), "appended_rows": len(to_append), "skipped_rows": skipped}


def load_manual_review_queue(path: Optional[Path] = None) -> pd.DataFrame:
    candidate = path or MANUAL_REVIEW_QUEUE_PATH
    if not candidate.exists():
        return empty_manual_review_queue()
    frame = read_tabular_file(candidate)
    if frame.empty:
        return empty_manual_review_queue()
    header_map = dict(zip(frame.columns, canonical_headers(frame.columns)))
    frame = frame.rename(columns=header_map).copy()
    for column in MANUAL_REVIEW_QUEUE_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame = frame[MANUAL_REVIEW_QUEUE_COLUMNS].fillna("").astype(str)
    for column in MANUAL_REVIEW_QUEUE_COLUMNS:
        frame[column] = frame[column].str.strip()
    return frame.loc[frame["review_key"].ne("")].drop_duplicates(subset=["review_key"], keep="last").reset_index(drop=True)


def save_manual_review_queue(frame: pd.DataFrame, path: Optional[Path] = None) -> Path:
    candidate = path or MANUAL_REVIEW_QUEUE_PATH
    candidate.parent.mkdir(parents=True, exist_ok=True)
    if frame is None or frame.empty:
        cleaned = empty_manual_review_queue()
    else:
        cleaned = frame.copy()
        for column in MANUAL_REVIEW_QUEUE_COLUMNS:
            if column not in cleaned.columns:
                cleaned[column] = ""
        cleaned = cleaned[MANUAL_REVIEW_QUEUE_COLUMNS].fillna("").astype(str)
        for column in MANUAL_REVIEW_QUEUE_COLUMNS:
            cleaned[column] = cleaned[column].str.strip()
        cleaned = cleaned.loc[cleaned["review_key"].ne("")].drop_duplicates(subset=["review_key"], keep="last").reset_index(drop=True)
    cleaned.to_csv(candidate, index=False)
    return candidate


def prepare_manual_corrections_workspace(
    corrections_path: Optional[Path] = None,
    transcript_folder: Optional[Path] = None,
    review_queue_path: Optional[Path] = None,
) -> Dict[str, Path]:
    correction_file = corrections_path or MANUAL_ROSTER_CORRECTIONS_PATH
    adjustments_file = correction_file.parent / MANUAL_ADJUSTMENTS_PATH.name
    transcripts = transcript_folder or MANUAL_TRANSCRIPTS_PATH
    review_queue = review_queue_path or (correction_file.parent / MANUAL_REVIEW_QUEUE_PATH.name)
    correction_file.parent.mkdir(parents=True, exist_ok=True)
    transcripts.mkdir(parents=True, exist_ok=True)
    review_queue.parent.mkdir(parents=True, exist_ok=True)
    if not correction_file.exists():
        empty_manual_roster_corrections().to_csv(correction_file, index=False)
    if not adjustments_file.exists():
        empty_manual_adjustments().to_csv(adjustments_file, index=False)
    if not review_queue.exists():
        empty_manual_review_queue().to_csv(review_queue, index=False)
    return {
        "corrections_path": correction_file,
        "adjustments_path": adjustments_file,
        "transcript_folder": transcripts,
        "review_queue_path": review_queue,
    }


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
            f"Exclude From Roster Calculations: {normalize_text(row.get('exclude_from_roster_calculations', ''))}",
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


def build_manual_corrections_package(
    corrections_path: Optional[Path] = None,
    transcript_folder: Optional[Path] = None,
    review_queue_path: Optional[Path] = None,
) -> bytes:
    workspace = prepare_manual_corrections_workspace(corrections_path, transcript_folder, review_queue_path)
    correction_file = workspace["corrections_path"]
    adjustments_file = workspace["adjustments_path"]
    transcripts = workspace["transcript_folder"]
    review_queue = workspace["review_queue_path"]
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.write(correction_file, arcname="manual_roster_corrections.csv")
        if adjustments_file.exists():
            archive.write(adjustments_file, arcname="manual_adjustments.csv")
        if review_queue.exists():
            archive.write(review_queue, arcname="manual_review_queue.csv")
        for path in sorted(transcripts.glob("*.txt")):
            archive.write(path, arcname=f"Transcripts/{path.name}")
    buffer.seek(0)
    return buffer.read()


def manual_correction_identity_key(frame: pd.DataFrame) -> pd.Series:
    student_id = frame.get("student_id", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip().str.upper()
    first_name = frame.get("first_name", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip().str.lower()
    last_name = frame.get("last_name", pd.Series("", index=frame.index)).fillna("").astype(str).str.strip().str.lower()
    return student_id.where(student_id.ne(""), last_name + "|" + first_name)


def find_manual_correction_conflicts(corrections: pd.DataFrame) -> pd.DataFrame:
    if corrections is None or corrections.empty:
        return pd.DataFrame(columns=["identity_key", "student_id", "last_name", "first_name", "conflicting_rows"])
    frame = corrections.copy()
    for column in MANUAL_ROSTER_CORRECTION_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame = frame[MANUAL_ROSTER_CORRECTION_COLUMNS].fillna("").astype(str)
    frame["_identity_key"] = manual_correction_identity_key(frame)
    frame["_action_key"] = frame[MANUAL_ROSTER_CORRECTION_COLUMNS].agg("|".join, axis=1)
    conflicts = frame.groupby("_identity_key", dropna=False)["_action_key"].nunique().reset_index(name="action_count")
    conflicts = conflicts.loc[conflicts["action_count"].gt(1) & conflicts["_identity_key"].ne("")]
    rows: List[dict] = []
    for identity_key in conflicts["_identity_key"]:
        matches = frame.loc[frame["_identity_key"].eq(identity_key)]
        first = matches.iloc[0]
        rows.append(
            {
                "identity_key": identity_key,
                "student_id": first.get("student_id", ""),
                "last_name": first.get("last_name", ""),
                "first_name": first.get("first_name", ""),
                "conflicting_rows": int(len(matches)),
            }
        )
    return pd.DataFrame(rows)


def merge_manual_corrections(incoming: pd.DataFrame, path: Optional[Path] = None) -> Dict[str, object]:
    existing = load_manual_roster_corrections(path)
    incoming_clean = save_and_reload_manual_corrections(incoming)
    combined = pd.concat([existing, incoming_clean], ignore_index=True)
    before = len(existing)
    combined = combined.drop_duplicates(subset=MANUAL_ROSTER_CORRECTION_COLUMNS, keep="last").reset_index(drop=True)
    save_manual_roster_corrections(combined, path)
    conflicts = find_manual_correction_conflicts(combined)
    return {"existing_rows": before, "incoming_rows": len(incoming_clean), "merged_rows": len(combined), "conflicts": conflicts}


def save_and_reload_manual_corrections(frame: pd.DataFrame) -> pd.DataFrame:
    temp_path = MANUAL_ROSTER_CORRECTIONS_PATH.parent / ".manual_roster_corrections_import_tmp.csv"
    save_manual_roster_corrections(frame, temp_path)
    loaded = load_manual_roster_corrections(temp_path)
    try:
        temp_path.unlink()
    except OSError:
        pass
    return loaded


def import_manual_corrections_package(package_bytes: bytes) -> Dict[str, object]:
    prepare_manual_corrections_workspace()
    transcript_imported = 0
    transcript_skipped = 0
    review_rows = 0
    correction_result: Dict[str, object] = {
        "existing_rows": len(load_manual_roster_corrections()),
        "incoming_rows": 0,
        "merged_rows": len(load_manual_roster_corrections()),
        "conflicts": pd.DataFrame(),
    }
    with ZipFile(BytesIO(package_bytes)) as archive:
        names = set(archive.namelist())
        if "manual_roster_corrections.csv" in names:
            incoming = pd.read_csv(archive.open("manual_roster_corrections.csv"))
            correction_result = merge_manual_corrections(incoming)
        if "manual_review_queue.csv" in names:
            incoming_queue = pd.read_csv(archive.open("manual_review_queue.csv"))
            combined_queue = pd.concat([load_manual_review_queue(), incoming_queue], ignore_index=True)
            save_manual_review_queue(combined_queue)
            review_rows = len(load_manual_review_queue())
        if "manual_adjustments.csv" in names:
            incoming_adjustments = pd.read_csv(archive.open("manual_adjustments.csv"))
            combined_adjustments = pd.concat([load_manual_adjustments(), incoming_adjustments], ignore_index=True)
            save_manual_adjustments(combined_adjustments)
        for name in sorted(names):
            if not name.lower().startswith("transcripts/") or not name.lower().endswith(".txt"):
                continue
            target = MANUAL_TRANSCRIPTS_PATH / Path(name).name
            content = archive.read(name)
            if target.exists() and target.read_bytes() == content:
                transcript_skipped += 1
                continue
            if target.exists():
                base = target.with_suffix("")
                suffix = 1
                candidate = target
                while candidate.exists():
                    candidate = base.parent / f"{base.name}_imported_{suffix}.txt"
                    suffix += 1
                target = candidate
            target.write_bytes(content)
            transcript_imported += 1
    return {
        **correction_result,
        "review_rows": review_rows,
        "transcript_imported": transcript_imported,
        "transcript_skipped": transcript_skipped,
    }


def stringify_notes(values: List[str]) -> List[str]:
    return [normalize_text(value) for value in values if normalize_text(value)]
