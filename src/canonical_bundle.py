from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CANONICAL_ROOT = ROOT / "output" / "canonical"
REQUIRED_FILES = {
    "roster_term": "roster_term.csv",
    "academic_term": "academic_term.csv",
    "master_longitudinal": "master_longitudinal.csv",
    "student_summary": "student_summary.csv",
    "cohort_metrics": "cohort_metrics.csv",
    "qa_checks": "qa_checks.csv",
}
OPTIONAL_FILES = {
    "identity_exceptions": "identity_exceptions.csv",
    "term_exceptions": "term_exceptions.csv",
    "status_exceptions": "status_exceptions.csv",
    "chapter_conflicts": "chapter_conflicts.csv",
    "outcome_exceptions": "outcome_exceptions.csv",
    "missing_evidence_cases": "missing_evidence_cases.csv",
    "unresolved_chapter_review": "unresolved_chapter_review.csv",
    "graduation_status_audit": "graduation_status_audit.csv",
}


@dataclass(frozen=True)
class CanonicalBundle:
    output_folder: Path
    tables: Dict[str, pd.DataFrame]
    schema: dict


def latest_canonical_folder(root: Path) -> Optional[Path]:
    if not root.exists():
        return None
    candidates = [path for path in root.iterdir() if path.is_dir() and path.name.startswith("run_")]
    if not candidates:
        latest = root / "latest"
        return latest if latest.exists() else None
    return sorted(candidates)[-1]


def load_canonical_bundle(
    canonical_root: Path = DEFAULT_CANONICAL_ROOT,
    explicit_folder: Optional[Path] = None,
) -> CanonicalBundle:
    folder = explicit_folder.expanduser().resolve() if explicit_folder else latest_canonical_folder(canonical_root.expanduser().resolve())
    if folder is None or not folder.exists():
        raise FileNotFoundError(
            f"No canonical output folder was found under {canonical_root}. Run py run_canonical_pipeline.py first."
        )

    tables: Dict[str, pd.DataFrame] = {}
    missing = []
    for key, filename in REQUIRED_FILES.items():
        path = folder / filename
        if not path.exists():
            missing.append(filename)
            continue
        tables[key] = pd.read_csv(path)
    if missing:
        raise FileNotFoundError(
            "Canonical bundle is incomplete. Missing required files: " + ", ".join(missing)
        )

    for key, filename in OPTIONAL_FILES.items():
        path = folder / filename
        if path.exists():
            tables[key] = pd.read_csv(path)

    schema_path = folder / "canonical_schema.json"
    if not schema_path.exists():
        raise FileNotFoundError(f"Canonical schema missing from {folder}")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    return CanonicalBundle(output_folder=folder, tables=tables, schema=schema)
