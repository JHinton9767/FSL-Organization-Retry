from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from app.io_utils import ROOT, canonical_headers, normalize_text, read_tabular_file
from app.models import MetricDefinition


CONFIG_DIR = ROOT / "config"
APP_SETTINGS_PATH = CONFIG_DIR / "app_settings.json"
METRIC_CATALOG_PATH = CONFIG_DIR / "metric_catalog.json"
STATUS_CODE_MAP_PATH = CONFIG_DIR / "status_code_map.json"
DATASET_MANIFEST_PATH = CONFIG_DIR / "dataset_manifest.json"
DEFAULT_CHAPTER_GROUPS_PATH = CONFIG_DIR / "chapter_groups.csv"
EXAMPLE_CHAPTER_GROUPS_PATH = CONFIG_DIR / "chapter_groups.example.csv"
MANUAL_CHAPTER_ASSIGNMENTS_PATH = CONFIG_DIR / "manual_chapter_assignments.csv"


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
        "outcome_resolution": {
            "priority_order": [
                "Graduated",
                "Resolved Non-Graduate Exit",
                "Still Active",
                "Truly Unknown / Unresolved",
                "Other / Unmapped",
            ],
            "group_patterns": {
                "Graduated": [
                    "\\bGRADUATED\\b",
                    "\\bGRAD\\b",
                    "DEGREE AWARDED",
                    "AWARDED DEGREE",
                    "DEGREE CONFER",
                    "CONFERRED DEGREE",
                ],
                "Resolved Non-Graduate Exit": [
                    "\\bINACTIVE\\b",
                    "\\bLEFT\\b",
                    "\\bRESIGN",
                    "\\bREVOK",
                    "\\bSUSPEND",
                    "\\bTRANSFER\\b",
                    "\\bDROP",
                    "\\bREMOVE",
                    "\\bWITHDRAW",
                    "\\bTERMINAT",
                    "\\bDISMISS",
                    "\\bEXPEL",
                ],
                "Still Active": [
                    "\\bSTILL ACTIVE\\b",
                    "\\bACTIVE\\b",
                    "\\bCURRENT\\b",
                    "\\bMEMBER\\b",
                    "\\bNEW MEMBER\\b",
                    "\\bCOUNCIL\\b",
                    "\\bENROLLED\\b",
                ],
                "Truly Unknown / Unresolved": [
                    "\\bUNKNOWN\\b",
                    "\\bUNRESOLVED\\b",
                    "\\bPENDING\\b",
                    "\\bNOT KNOWN\\b",
                    "\\bMISSING\\b",
                    "\\bUNMAPPED\\b",
                    "\\bNO OUTCOME\\b",
                    "\\bNO FURTHER OBSERVATION\\b",
                    "\\bACTIVE\\/UNKNOWN\\b",
                ],
                "Other / Unmapped": [],
            },
            "resolved_only_excluded_groups": [
                "Still Active",
                "Truly Unknown / Unresolved",
                "Other / Unmapped",
            ],
        },
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
                    "roster_term.csv",
                    "academic_term.csv",
                    "master_longitudinal.csv",
                    "student_summary.csv",
                    "cohort_metrics.csv",
                    "qa_checks.csv",
                    "canonical_schema.json",
                ],
                "optional_files": [
                    "identity_exceptions.csv",
                    "term_exceptions.csv",
                    "status_exceptions.csv",
                    "chapter_conflicts.csv",
                    "outcome_exceptions.csv",
                    "missing_evidence_cases.csv",
                    "unresolved_chapter_review.csv",
                    "graduation_status_audit.csv",
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


def stringify_notes(values: List[str]) -> List[str]:
    return [normalize_text(value) for value in values if normalize_text(value)]
