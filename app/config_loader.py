from __future__ import annotations

from io import BytesIO
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Callable, Dict, List, Optional
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd

from app.io_utils import ROOT, canonical_headers, normalize_text, parse_term_label, read_tabular_file
from app.models import MetricDefinition
from app.status_framework import DEFAULT_OUTCOME_RESOLUTION_CONFIG
from src.build_master_roster import normalize_banner_id, normalize_chapter_name
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
MANUAL_REVIEW_ACTIONS_PATH = CONFIG_DIR / "manual_review_actions.csv"
GRADUATION_EVIDENCE_PATH = CONFIG_DIR / "graduation_evidence.csv"
OUTCOME_OVERRIDES_PATH = CONFIG_DIR / "outcome_overrides.csv"
ROSTER_EXCLUSIONS_PATH = CONFIG_DIR / "roster_exclusions.csv"


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
                    "cohort_status_over_time.parquet",
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
    standardized["student_id"] = standardized["student_id"].map(normalize_banner_id)
    standardized = standardized.loc[
        standardized["chapter_override"].ne("")
        & standardized["student_id"].ne("")
    ].copy()
    return standardized.reset_index(drop=True)


MANUAL_ROSTER_CORRECTION_COLUMNS = [
    "student_id",
    "last_name",
    "first_name",
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
GRADUATION_EVIDENCE_COLUMNS = [
    "student_id",
    "organization_name",
    "organization_join_term",
    "graduation_term",
    "evidence_source",
    "notes",
    "entered_by",
    "entered_at",
]
OUTCOME_OVERRIDE_COLUMNS = [
    "student_id",
    "organization_name",
    "organization_join_term",
    "final_status",
    "final_status_term",
    "reason",
    "evidence_source",
    "entered_by",
    "entered_at",
]
ROSTER_EXCLUSION_COLUMNS = [
    "student_id",
    "organization_name",
    "term",
    "source_file",
    "source_sheet",
    "reason",
    "entered_by",
    "entered_at",
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


def empty_graduation_evidence() -> pd.DataFrame:
    return pd.DataFrame(columns=GRADUATION_EVIDENCE_COLUMNS)


def empty_outcome_overrides() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTCOME_OVERRIDE_COLUMNS)


def empty_roster_exclusions() -> pd.DataFrame:
    return pd.DataFrame(columns=ROSTER_EXCLUSION_COLUMNS)


def _read_optional_tabular(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        if path.stat().st_size == 0:
            return pd.DataFrame()
    except OSError:
        return pd.DataFrame()
    try:
        return read_tabular_file(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def manual_review_queue_cohort_key(row: pd.Series) -> str:
    student_id = normalize_banner_id(row.get("student_id", ""))
    if not student_id:
        return ""
    join_term = normalize_text(row.get("join_term", ""))
    term_parts = parse_term_label(join_term)
    term_key = normalize_text(term_parts.get("code", "")) or join_term.lower()
    if not term_key:
        term_key = "unknown"
    return f"{student_id}|{term_key}"


def _manual_queue_status_rank(value: object) -> int:
    status = normalize_text(value).lower()
    ranks = {
        "corrected": 0,
        "in progress": 1,
        "waiting on transcript": 2,
        "needs review": 3,
        "blocked": 4,
        "skipped / no change": 5,
    }
    return ranks.get(status, 6)


def _unique_joined(values: pd.Series) -> str:
    seen: list[str] = []
    for value in values:
        text = normalize_text(value)
        if text and text not in seen:
            seen.append(text)
    return "; ".join(seen)


def dedupe_manual_review_queue_by_cohort(frame: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Keep one valid Banner ID row per organization-join cohort."""
    if frame is None or frame.empty:
        return empty_manual_review_queue()

    cleaned = frame.copy()
    for column in MANUAL_REVIEW_QUEUE_COLUMNS:
        if column not in cleaned.columns:
            cleaned[column] = ""
    cleaned = cleaned[MANUAL_REVIEW_QUEUE_COLUMNS].fillna("").astype(str)
    cleaned["student_id"] = cleaned["student_id"].map(normalize_banner_id)
    cleaned = cleaned.loc[cleaned["student_id"].ne("")].copy()
    if cleaned.empty:
        return empty_manual_review_queue()

    cleaned["_cohort_key"] = cleaned.apply(manual_review_queue_cohort_key, axis=1)
    cleaned = cleaned.loc[cleaned["_cohort_key"].ne("")].copy()
    if cleaned.empty:
        return empty_manual_review_queue()

    cleaned["_original_order"] = range(len(cleaned))
    cleaned["_status_rank"] = cleaned["review_status"].map(_manual_queue_status_rank)
    cleaned["_join_sort"] = cleaned["join_term"].map(lambda value: parse_term_label(value)["sort_value"])
    cleaned = cleaned.sort_values(["_cohort_key", "_status_rank", "_original_order"])

    merged_rows: list[dict[str, object]] = []
    combine_columns = {
        "issue_type",
        "evidence_summary",
        "suggested_action",
        "queue_reason",
        "review_notes",
        "source_file",
        "source_sheet",
        "input_group_id",
    }
    yes_columns = {"needs_transcript", "has_manual_correction", "transcript_file_exists"}
    for _, group in cleaned.groupby("_cohort_key", sort=False):
        base = group.iloc[0][MANUAL_REVIEW_QUEUE_COLUMNS].to_dict()
        for column in MANUAL_REVIEW_QUEUE_COLUMNS:
            if column in combine_columns:
                base[column] = _unique_joined(group[column])
            elif column in yes_columns:
                lowered = group[column].map(lambda value: normalize_text(value).lower())
                if lowered.isin({"yes", "staged", "true", "1", "y"}).any():
                    base[column] = "Staged" if column == "has_manual_correction" and lowered.eq("staged").any() else "Yes"
                elif not normalize_text(base.get(column, "")):
                    base[column] = _unique_joined(group[column])
            elif not normalize_text(base.get(column, "")):
                base[column] = _unique_joined(group[column])
        merged_rows.append(base)

    result = pd.DataFrame(merged_rows, columns=MANUAL_REVIEW_QUEUE_COLUMNS)
    result["_join_sort"] = result["join_term"].map(lambda value: parse_term_label(value)["sort_value"])
    return (
        result.sort_values(["_join_sort", "student_id", "review_key"])
        .drop(columns=["_join_sort"], errors="ignore")
        .reset_index(drop=True)
    )


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
    standardized["student_id"] = standardized["student_id"].map(normalize_banner_id)
    standardized["normalized_student_id"] = standardized["normalized_student_id"].map(normalize_banner_id)
    standardized["normalized_student_id"] = standardized["normalized_student_id"].where(
        standardized["normalized_student_id"].ne(""),
        standardized["student_id"],
    )
    standardized["student_id"] = standardized["student_id"].where(
        standardized["student_id"].ne(""),
        standardized["normalized_student_id"],
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
    has_identity = standardized["normalized_student_id"].ne("")
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
    incoming_count = len(incoming)
    existing = load_manual_adjustments(candidate) if candidate.exists() else empty_manual_adjustments()
    if incoming.empty:
        save_manual_adjustments(existing, candidate)
        return {"path": candidate, "incoming_rows": 0, "appended_rows": 0, "skipped_rows": 0}

    existing_ids = set(existing["adjustment_id"].fillna("").astype(str).str.strip()) if not existing.empty else set()
    to_append = incoming.loc[~incoming["adjustment_id"].isin(existing_ids)].copy()
    skipped = int(incoming_count - len(to_append))
    if to_append.empty:
        save_manual_adjustments(existing, candidate)
        return {"path": candidate, "incoming_rows": incoming_count, "appended_rows": 0, "skipped_rows": skipped}

    combined = pd.concat([existing, to_append], ignore_index=True) if not existing.empty else to_append
    save_manual_adjustments(combined, candidate)
    return {"path": candidate, "incoming_rows": incoming_count, "appended_rows": len(to_append), "skipped_rows": skipped}


def _standardize_registry_frame(frame: Optional[pd.DataFrame], columns: list[str], alias_map: Dict[str, list[str]]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    cleaned = frame.copy()
    header_map = dict(zip(cleaned.columns, canonical_headers(cleaned.columns)))
    cleaned = cleaned.rename(columns=header_map)
    resolved: Dict[str, str] = {}
    for target, aliases in alias_map.items():
        source = next((column for column in cleaned.columns if column in aliases), None)
        if source:
            resolved[target] = source
    standardized = pd.DataFrame(index=cleaned.index)
    for column in columns:
        source = resolved.get(column)
        standardized[column] = cleaned[source] if source else ""
    standardized = standardized.fillna("").astype(str)
    for column in columns:
        standardized[column] = standardized[column].str.strip()
    standardized["student_id"] = standardized["student_id"].map(normalize_banner_id)
    if "organization_name" in standardized.columns:
        standardized["organization_name"] = standardized["organization_name"].map(lambda value: normalize_chapter_name(normalize_text(value)))
    if "entered_at" in standardized.columns:
        standardized["entered_at"] = standardized["entered_at"].where(
            standardized["entered_at"].ne(""),
            datetime.now().isoformat(timespec="seconds"),
        )
    return standardized


def normalize_graduation_evidence(frame: Optional[pd.DataFrame]) -> pd.DataFrame:
    aliases = {
        "student_id": ["student_id", "student id", "banner id", "banner", "a_number", "a number"],
        "organization_name": ["organization_name", "organization name", "chapter", "organization", "org"],
        "organization_join_term": ["organization_join_term", "organization join term", "join_term", "join term", "initiation_date", "initiation date"],
        "graduation_term": ["graduation_term", "graduation term", "grad_term", "grad term", "final_status_term", "final status term"],
        "evidence_source": ["evidence_source", "evidence source", "source", "proof", "evidence"],
        "notes": ["notes", "note", "review_notes", "review notes"],
        "entered_by": ["entered_by", "entered by", "reviewer", "reviewed_by", "reviewed by"],
        "entered_at": ["entered_at", "entered at", "created_at", "created at", "reviewed_at", "reviewed at"],
    }
    standardized = _standardize_registry_frame(frame, GRADUATION_EVIDENCE_COLUMNS, aliases)
    if standardized.empty:
        return empty_graduation_evidence()
    has_action = standardized["graduation_term"].ne("") | standardized["evidence_source"].ne("") | standardized["notes"].ne("")
    standardized = standardized.loc[standardized["student_id"].ne("") & has_action].copy()
    if standardized.empty:
        return empty_graduation_evidence()
    return standardized.drop_duplicates(subset=GRADUATION_EVIDENCE_COLUMNS, keep="last").reset_index(drop=True)


def normalize_outcome_overrides(frame: Optional[pd.DataFrame]) -> pd.DataFrame:
    aliases = {
        "student_id": ["student_id", "student id", "banner id", "banner", "a_number", "a number"],
        "organization_name": ["organization_name", "organization name", "chapter", "organization", "org"],
        "organization_join_term": ["organization_join_term", "organization join term", "join_term", "join term", "initiation_date", "initiation date"],
        "final_status": ["final_status", "final status", "status", "outcome", "outcome_bucket", "outcome bucket"],
        "final_status_term": ["final_status_term", "final status term", "status_term", "status term", "graduation_term", "graduation term"],
        "reason": ["reason", "notes", "note", "review_notes", "review notes"],
        "evidence_source": ["evidence_source", "evidence source", "source", "proof", "evidence"],
        "entered_by": ["entered_by", "entered by", "reviewer", "reviewed_by", "reviewed by"],
        "entered_at": ["entered_at", "entered at", "created_at", "created at", "reviewed_at", "reviewed at"],
    }
    standardized = _standardize_registry_frame(frame, OUTCOME_OVERRIDE_COLUMNS, aliases)
    if standardized.empty:
        return empty_outcome_overrides()
    standardized = standardized.loc[standardized["student_id"].ne("") & standardized["final_status"].ne("")].copy()
    if standardized.empty:
        return empty_outcome_overrides()
    return standardized.drop_duplicates(subset=OUTCOME_OVERRIDE_COLUMNS, keep="last").reset_index(drop=True)


def normalize_roster_exclusions(frame: Optional[pd.DataFrame]) -> pd.DataFrame:
    aliases = {
        "student_id": ["student_id", "student id", "banner id", "banner", "a_number", "a number"],
        "organization_name": ["organization_name", "organization name", "chapter", "organization", "org"],
        "term": ["term", "term_label", "term label", "term_code", "term code", "organization_join_term", "organization join term"],
        "source_file": ["source_file", "source file"],
        "source_sheet": ["source_sheet", "source sheet"],
        "reason": ["reason", "notes", "note", "review_notes", "review notes"],
        "entered_by": ["entered_by", "entered by", "reviewer", "reviewed_by", "reviewed by"],
        "entered_at": ["entered_at", "entered at", "created_at", "created at", "reviewed_at", "reviewed at"],
    }
    standardized = _standardize_registry_frame(frame, ROSTER_EXCLUSION_COLUMNS, aliases)
    if standardized.empty:
        return empty_roster_exclusions()
    has_scope = (
        standardized["organization_name"].ne("")
        | standardized["term"].ne("")
        | standardized["source_file"].ne("")
        | standardized["source_sheet"].ne("")
    )
    standardized = standardized.loc[standardized["student_id"].ne("") & has_scope].copy()
    if standardized.empty:
        return empty_roster_exclusions()
    return standardized.drop_duplicates(subset=ROSTER_EXCLUSION_COLUMNS, keep="last").reset_index(drop=True)


def _load_registry(path: Optional[Path], default_path: Path, normalizer: Callable[[Optional[pd.DataFrame]], pd.DataFrame]) -> pd.DataFrame:
    candidate = path or default_path
    return normalizer(_read_optional_tabular(candidate))


def _save_registry(frame: pd.DataFrame, path: Optional[Path], default_path: Path, normalizer: Callable[[Optional[pd.DataFrame]], pd.DataFrame]) -> Path:
    candidate = path or default_path
    candidate.parent.mkdir(parents=True, exist_ok=True)
    normalizer(frame).to_csv(candidate, index=False)
    return candidate


def _append_registry(
    frame: pd.DataFrame,
    path: Optional[Path],
    default_path: Path,
    normalizer: Callable[[Optional[pd.DataFrame]], pd.DataFrame],
    key_columns: Optional[list[str]] = None,
) -> Dict[str, object]:
    candidate = path or default_path
    candidate.parent.mkdir(parents=True, exist_ok=True)
    incoming = normalizer(frame)
    incoming_count = len(incoming)
    existing = normalizer(_read_optional_tabular(candidate)) if candidate.exists() else normalizer(None)
    if incoming.empty:
        existing.to_csv(candidate, index=False)
        return {"path": candidate, "incoming_rows": 0, "appended_rows": 0, "skipped_rows": 0}
    columns = [column for column in (key_columns or incoming.columns.tolist()) if column in incoming.columns]
    incoming_keys = incoming[columns].fillna("").astype(str).agg("\u241f".join, axis=1)
    existing_keys = set(existing[columns].fillna("").astype(str).agg("\u241f".join, axis=1)) if not existing.empty else set()
    to_append = incoming.loc[~incoming_keys.isin(existing_keys)].copy()
    skipped = int(incoming_count - len(to_append))
    combined = pd.concat([existing, to_append], ignore_index=True) if not existing.empty else to_append
    normalizer(combined).to_csv(candidate, index=False)
    return {"path": candidate, "incoming_rows": incoming_count, "appended_rows": len(to_append), "skipped_rows": skipped}


def load_graduation_evidence(path: Optional[Path] = None) -> pd.DataFrame:
    return _load_registry(path, GRADUATION_EVIDENCE_PATH, normalize_graduation_evidence)


def save_graduation_evidence(frame: pd.DataFrame, path: Optional[Path] = None) -> Path:
    return _save_registry(frame, path, GRADUATION_EVIDENCE_PATH, normalize_graduation_evidence)


def append_graduation_evidence(frame: pd.DataFrame, path: Optional[Path] = None) -> Dict[str, object]:
    return _append_registry(
        frame,
        path,
        GRADUATION_EVIDENCE_PATH,
        normalize_graduation_evidence,
        ["student_id", "organization_name", "organization_join_term", "graduation_term", "evidence_source"],
    )


def load_outcome_overrides(path: Optional[Path] = None) -> pd.DataFrame:
    return _load_registry(path, OUTCOME_OVERRIDES_PATH, normalize_outcome_overrides)


def save_outcome_overrides(frame: pd.DataFrame, path: Optional[Path] = None) -> Path:
    return _save_registry(frame, path, OUTCOME_OVERRIDES_PATH, normalize_outcome_overrides)


def append_outcome_overrides(frame: pd.DataFrame, path: Optional[Path] = None) -> Dict[str, object]:
    return _append_registry(
        frame,
        path,
        OUTCOME_OVERRIDES_PATH,
        normalize_outcome_overrides,
        ["student_id", "organization_name", "organization_join_term", "final_status", "final_status_term"],
    )


def load_roster_exclusions(path: Optional[Path] = None) -> pd.DataFrame:
    return _load_registry(path, ROSTER_EXCLUSIONS_PATH, normalize_roster_exclusions)


def save_roster_exclusions(frame: pd.DataFrame, path: Optional[Path] = None) -> Path:
    return _save_registry(frame, path, ROSTER_EXCLUSIONS_PATH, normalize_roster_exclusions)


def append_roster_exclusions(frame: pd.DataFrame, path: Optional[Path] = None) -> Dict[str, object]:
    return _append_registry(
        frame,
        path,
        ROSTER_EXCLUSIONS_PATH,
        normalize_roster_exclusions,
        ["student_id", "organization_name", "term", "source_file", "source_sheet"],
    )


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
    cleaned["student_id"] = cleaned["student_id"].map(normalize_banner_id)

    has_identity = cleaned["student_id"].ne("")
    has_action = (
        cleaned["organization_join_term"].ne("")
        | cleaned["organization_name"].ne("")
        | cleaned["leaving_organization_term"].ne("")
        | cleaned["final_status_term"].ne("")
        | cleaned["final_status"].ne("")
        | cleaned["exclude_from_roster_calculations"].str.lower().isin(MANUAL_ACTION_VALUES)
    )
    return cleaned.loc[has_identity & has_action & ~delete_mask].reset_index(drop=True)


def _alumni_name_key(first_name: object = "", last_name: object = "", full_name: object = "") -> str:
    first = normalize_text(first_name)
    last = normalize_text(last_name)
    full = normalize_text(full_name)
    if not full:
        full = f"{first} {last}".strip()
    return re.sub(r"[^a-z0-9]+", " ", full.lower()).strip()


def _summary_student_name_key(row: pd.Series) -> str:
    return _alumni_name_key(
        row.get("first_name", ""),
        row.get("last_name", ""),
        row.get("student_name", ""),
    )


def _summary_chapter_values(row: pd.Series) -> list[str]:
    chapters: list[str] = []
    for column in ["current_active_chapter", "latest_chapter", "chapter", "initial_chapter"]:
        chapter = normalize_chapter_name(normalize_text(row.get(column, "")))
        if chapter and chapter not in chapters:
            chapters.append(chapter)
    return chapters


def _graduated_alumni_summary_match_lookup(summary: Optional[pd.DataFrame]) -> dict[tuple[str, str], pd.Series]:
    if summary is None or summary.empty or "student_id" not in summary.columns:
        return {}

    candidates: dict[tuple[str, str], list[pd.Series]] = {}
    working = summary.copy()
    working["_student_id"] = working["student_id"].map(normalize_banner_id)
    working = working.loc[working["_student_id"].ne("")].copy()
    for _, row in working.iterrows():
        name_key = _summary_student_name_key(row)
        if not name_key:
            continue
        for chapter in _summary_chapter_values(row):
            candidates.setdefault((name_key, chapter), []).append(row)

    return {key: rows[0] for key, rows in candidates.items() if len({row["_student_id"] for row in rows}) == 1}


def graduated_alumni_rows_to_manual_corrections(
    frame: Optional[pd.DataFrame],
    default_organization: str = "",
    default_graduation_term: str = "",
    summary: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return empty_manual_roster_corrections()

    source = frame.copy()
    header_map = dict(zip(source.columns, canonical_headers(source.columns)))
    renamed = source.rename(columns=header_map).fillna("").astype(str)
    alias_map = {
        "student_id": ["student_id", "banner_id", "banner", "plid", "student_number", "a_number", "a"],
        "last_name": ["last_name", "lastname", "last"],
        "first_name": ["first_name", "firstname", "first"],
        "student_name": ["student_name", "name", "full_name", "member_name", "alumni_name"],
        "organization_join_term": [
            "organization_join_term",
            "org_join_term",
            "join_term",
            "joined",
            "pledge_term",
            "initiation_date",
            "initiation",
            "initiated",
            "initiation_term",
            "initiation_semester",
        ],
        "organization_name": ["organization_name", "organization", "chapter", "org", "chapter_name"],
        "final_status_term": ["final_status_term", "graduation_term", "grad_term", "graduated_term", "graduation_semester"],
    }

    standardized = pd.DataFrame(index=renamed.index)
    for target, aliases in alias_map.items():
        source_column = next((column for column in renamed.columns if column in aliases), None)
        standardized[target] = renamed[source_column] if source_column else ""

    summary_lookup = pd.DataFrame()
    if summary is not None and not summary.empty and "student_id" in summary.columns:
        summary_lookup = summary.copy()
        summary_lookup["_student_id"] = summary_lookup["student_id"].map(normalize_banner_id)
        summary_lookup = summary_lookup.loc[summary_lookup["_student_id"].ne("")].drop_duplicates("_student_id", keep="first")
        summary_lookup = summary_lookup.set_index("_student_id")
    name_chapter_lookup = _graduated_alumni_summary_match_lookup(summary)

    default_organization = normalize_text(default_organization)
    default_organization_normalized = normalize_chapter_name(default_organization)
    default_graduation_term = normalize_text(default_graduation_term)
    rows: list[dict[str, object]] = []
    for _, row in standardized.iterrows():
        student_id = normalize_banner_id(row.get("student_id", ""))
        row_organization = normalize_text(row.get("organization_name")) or default_organization
        row_chapter_key = normalize_chapter_name(row_organization) or default_organization_normalized
        row_name_key = _alumni_name_key(row.get("first_name"), row.get("last_name"), row.get("student_name"))
        if not student_id and row_name_key and row_chapter_key:
            matched = name_chapter_lookup.get((row_name_key, row_chapter_key))
            if matched is not None:
                student_id = normalize_banner_id(matched.get("_student_id", matched.get("student_id", "")))
        if not student_id:
            continue
        summary_row = summary_lookup.loc[student_id] if not summary_lookup.empty and student_id in summary_lookup.index else pd.Series(dtype="object")
        first_name = normalize_text(row.get("first_name")) or normalize_text(summary_row.get("first_name", ""))
        last_name = normalize_text(row.get("last_name")) or normalize_text(summary_row.get("last_name", ""))
        full_name = normalize_text(row.get("student_name")) or normalize_text(summary_row.get("student_name", ""))
        if (not first_name or not last_name) and full_name:
            parts = full_name.split()
            if not first_name and parts:
                first_name = parts[0]
            if not last_name and len(parts) > 1:
                last_name = parts[-1]
        organization = (
            row_organization
            or default_organization
            or normalize_text(summary_row.get("current_active_chapter", ""))
            or normalize_text(summary_row.get("latest_chapter", ""))
            or normalize_text(summary_row.get("chapter", ""))
        )
        join_term = normalize_text(row.get("organization_join_term")) or normalize_text(summary_row.get("join_term", ""))
        grad_term = normalize_text(row.get("final_status_term")) or default_graduation_term
        rows.append(
            {
                "student_id": student_id,
                "last_name": last_name,
                "first_name": first_name,
                "organization_join_term": join_term,
                "organization_name": organization,
                "leaving_organization_term": grad_term,
                "final_status_term": grad_term,
                "final_status": "Graduated",
                "exclude_from_roster_calculations": "",
            }
        )

    return normalize_manual_roster_corrections(pd.DataFrame(rows))


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
    incoming_count = len(incoming)
    existing = load_manual_roster_corrections(candidate) if candidate.exists() else empty_manual_roster_corrections()
    if incoming.empty:
        save_manual_roster_corrections(existing, candidate)
        return {"path": candidate, "incoming_rows": 0, "appended_rows": 0, "skipped_rows": 0}

    incoming_keys = incoming[MANUAL_ROSTER_CORRECTION_COLUMNS].fillna("").astype(str).agg("\u241f".join, axis=1)
    existing_keys = set(existing[MANUAL_ROSTER_CORRECTION_COLUMNS].fillna("").astype(str).agg("\u241f".join, axis=1)) if not existing.empty else set()

    append_mask = ~incoming_keys.isin(existing_keys)
    to_append = incoming.loc[append_mask].copy()
    skipped = int((~append_mask).sum())
    if to_append.empty:
        save_manual_roster_corrections(existing, candidate)
        return {"path": candidate, "incoming_rows": incoming_count, "appended_rows": 0, "skipped_rows": skipped}

    combined = pd.concat([existing, to_append], ignore_index=True) if not existing.empty else to_append
    save_manual_roster_corrections(combined, candidate)
    return {"path": candidate, "incoming_rows": incoming_count, "appended_rows": len(to_append), "skipped_rows": skipped}


def load_manual_review_queue(path: Optional[Path] = None) -> pd.DataFrame:
    candidate = path or MANUAL_REVIEW_QUEUE_PATH
    frame = _read_manual_review_file(candidate)
    if frame.empty:
        return empty_manual_review_queue()
    return _normalize_manual_review_frame(frame)


def _read_manual_review_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        return empty_manual_review_queue()
    try:
        if path.stat().st_size == 0:
            return empty_manual_review_queue()
    except OSError:
        return empty_manual_review_queue()
    try:
        return read_tabular_file(path)
    except pd.errors.EmptyDataError:
        return empty_manual_review_queue()


def _normalize_manual_review_frame(frame: Optional[pd.DataFrame]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return empty_manual_review_queue()
    cleaned = frame.copy()
    header_map = dict(zip(cleaned.columns, canonical_headers(cleaned.columns)))
    cleaned = cleaned.rename(columns=header_map)
    for column in MANUAL_REVIEW_QUEUE_COLUMNS:
        if column not in cleaned.columns:
            cleaned[column] = ""
    cleaned = cleaned[MANUAL_REVIEW_QUEUE_COLUMNS].fillna("").astype(str)
    for column in MANUAL_REVIEW_QUEUE_COLUMNS:
        cleaned[column] = cleaned[column].str.strip()
    cleaned = cleaned.loc[cleaned["review_key"].ne("")].drop_duplicates(subset=["review_key"], keep="last").reset_index(drop=True)
    return dedupe_manual_review_queue_by_cohort(cleaned)


def save_manual_review_queue(frame: pd.DataFrame, path: Optional[Path] = None) -> Path:
    candidate = path or MANUAL_REVIEW_QUEUE_PATH
    candidate.parent.mkdir(parents=True, exist_ok=True)
    cleaned = _normalize_manual_review_frame(frame)
    cleaned.to_csv(candidate, index=False)
    return candidate


def load_manual_review_actions(path: Optional[Path] = None) -> pd.DataFrame:
    candidate = path or MANUAL_REVIEW_ACTIONS_PATH
    frame = _read_manual_review_file(candidate)
    return _normalize_manual_review_frame(frame)


def save_manual_review_actions(frame: pd.DataFrame, path: Optional[Path] = None) -> Path:
    candidate = path or MANUAL_REVIEW_ACTIONS_PATH
    candidate.parent.mkdir(parents=True, exist_ok=True)
    cleaned = _normalize_manual_review_frame(frame)
    cleaned.to_csv(candidate, index=False)
    return candidate


def append_manual_review_actions(frame: pd.DataFrame, path: Optional[Path] = None) -> Dict[str, object]:
    candidate = path or MANUAL_REVIEW_ACTIONS_PATH
    candidate.parent.mkdir(parents=True, exist_ok=True)
    incoming = _normalize_manual_review_frame(frame)
    incoming_count = len(incoming)
    existing = load_manual_review_actions(candidate) if candidate.exists() else empty_manual_review_queue()
    if incoming.empty:
        save_manual_review_actions(existing, candidate)
        return {"path": candidate, "incoming_rows": 0, "saved_rows": len(existing)}
    combined = pd.concat([existing, incoming], ignore_index=True) if not existing.empty else incoming
    save_manual_review_actions(combined, candidate)
    saved = load_manual_review_actions(candidate)
    return {"path": candidate, "incoming_rows": incoming_count, "saved_rows": len(saved)}


def prepare_manual_corrections_workspace(
    corrections_path: Optional[Path] = None,
    transcript_folder: Optional[Path] = None,
    review_queue_path: Optional[Path] = None,
    review_actions_path: Optional[Path] = None,
    graduation_evidence_path: Optional[Path] = None,
    outcome_overrides_path: Optional[Path] = None,
    roster_exclusions_path: Optional[Path] = None,
) -> Dict[str, Path]:
    correction_file = corrections_path or MANUAL_ROSTER_CORRECTIONS_PATH
    adjustments_file = correction_file.parent / MANUAL_ADJUSTMENTS_PATH.name
    transcripts = transcript_folder or MANUAL_TRANSCRIPTS_PATH
    review_queue = review_queue_path or (correction_file.parent / MANUAL_REVIEW_QUEUE_PATH.name)
    review_actions = review_actions_path or (correction_file.parent / MANUAL_REVIEW_ACTIONS_PATH.name)
    graduation_evidence = graduation_evidence_path or (correction_file.parent / GRADUATION_EVIDENCE_PATH.name)
    outcome_overrides = outcome_overrides_path or (correction_file.parent / OUTCOME_OVERRIDES_PATH.name)
    roster_exclusions = roster_exclusions_path or (correction_file.parent / ROSTER_EXCLUSIONS_PATH.name)
    correction_file.parent.mkdir(parents=True, exist_ok=True)
    transcripts.mkdir(parents=True, exist_ok=True)
    for path in [review_queue, review_actions, graduation_evidence, outcome_overrides, roster_exclusions]:
        path.parent.mkdir(parents=True, exist_ok=True)
    if not correction_file.exists():
        empty_manual_roster_corrections().to_csv(correction_file, index=False)
    if not adjustments_file.exists():
        empty_manual_adjustments().to_csv(adjustments_file, index=False)
    if not review_queue.exists():
        empty_manual_review_queue().to_csv(review_queue, index=False)
    if not review_actions.exists():
        empty_manual_review_queue().to_csv(review_actions, index=False)
    if not graduation_evidence.exists():
        empty_graduation_evidence().to_csv(graduation_evidence, index=False)
    if not outcome_overrides.exists():
        empty_outcome_overrides().to_csv(outcome_overrides, index=False)
    if not roster_exclusions.exists():
        empty_roster_exclusions().to_csv(roster_exclusions, index=False)
    return {
        "corrections_path": correction_file,
        "adjustments_path": adjustments_file,
        "transcript_folder": transcripts,
        "review_queue_path": review_queue,
        "review_actions_path": review_actions,
        "graduation_evidence_path": graduation_evidence,
        "outcome_overrides_path": outcome_overrides,
        "roster_exclusions_path": roster_exclusions,
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

    created: List[Path] = []
    seen: set[Path] = set()
    for _, row in frame.iterrows():
        if not normalize_banner_id(row.get("student_id", "")):
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
    review_actions_path: Optional[Path] = None,
) -> bytes:
    workspace = prepare_manual_corrections_workspace(corrections_path, transcript_folder, review_queue_path, review_actions_path)
    correction_file = workspace["corrections_path"]
    adjustments_file = workspace["adjustments_path"]
    transcripts = workspace["transcript_folder"]
    review_queue = workspace["review_queue_path"]
    review_actions = workspace["review_actions_path"]
    graduation_evidence = workspace["graduation_evidence_path"]
    outcome_overrides = workspace["outcome_overrides_path"]
    roster_exclusions = workspace["roster_exclusions_path"]
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.write(correction_file, arcname="manual_roster_corrections.csv")
        if adjustments_file.exists():
            archive.write(adjustments_file, arcname="manual_adjustments.csv")
        if graduation_evidence.exists():
            archive.write(graduation_evidence, arcname="graduation_evidence.csv")
        if outcome_overrides.exists():
            archive.write(outcome_overrides, arcname="outcome_overrides.csv")
        if roster_exclusions.exists():
            archive.write(roster_exclusions, arcname="roster_exclusions.csv")
        if review_actions.exists():
            archive.write(review_actions, arcname="manual_review_actions.csv")
        if review_queue.exists():
            archive.write(review_queue, arcname="manual_review_queue.csv")
        for path in sorted(transcripts.glob("*.txt")):
            archive.write(path, arcname=f"Transcripts/{path.name}")
    buffer.seek(0)
    return buffer.read()


def manual_correction_identity_key(frame: pd.DataFrame) -> pd.Series:
    student_id = frame.get("student_id", pd.Series("", index=frame.index)).map(normalize_banner_id)
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
    graduation_evidence_rows = 0
    outcome_override_rows = 0
    roster_exclusion_rows = 0
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
        if "manual_review_actions.csv" in names:
            incoming_actions = pd.read_csv(archive.open("manual_review_actions.csv"))
            append_manual_review_actions(incoming_actions)
            review_rows = len(load_manual_review_actions())
        if "manual_review_queue.csv" in names:
            incoming_queue = pd.read_csv(archive.open("manual_review_queue.csv"))
            append_manual_review_actions(incoming_queue)
            review_rows = len(load_manual_review_actions())
        if "graduation_evidence.csv" in names:
            incoming_graduation = pd.read_csv(archive.open("graduation_evidence.csv"))
            append_graduation_evidence(incoming_graduation)
            graduation_evidence_rows = len(load_graduation_evidence())
        if "outcome_overrides.csv" in names:
            incoming_outcomes = pd.read_csv(archive.open("outcome_overrides.csv"))
            append_outcome_overrides(incoming_outcomes)
            outcome_override_rows = len(load_outcome_overrides())
        if "roster_exclusions.csv" in names:
            incoming_exclusions = pd.read_csv(archive.open("roster_exclusions.csv"))
            append_roster_exclusions(incoming_exclusions)
            roster_exclusion_rows = len(load_roster_exclusions())
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
        "graduation_evidence_rows": graduation_evidence_rows,
        "outcome_override_rows": outcome_override_rows,
        "roster_exclusion_rows": roster_exclusion_rows,
        "transcript_imported": transcript_imported,
        "transcript_skipped": transcript_skipped,
    }


def stringify_notes(values: List[str]) -> List[str]:
    return [normalize_text(value) for value in values if normalize_text(value)]
