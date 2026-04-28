from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENHANCED_ROOT = ROOT / "output" / "enhanced_metrics"


@dataclass
class SourceBundle:
    enhanced_folder: Path
    enhanced_workbook: Path
    tables: Dict[str, pd.DataFrame]
    sources_used: List[str]
    sources_ignored: List[str]
    caveats: List[str]


def load_latest_bundle(
    enhanced_root: Path,
    explicit_folder: Path | None = None,
    explicit_workbook: Path | None = None,
) -> SourceBundle:
    folder = explicit_folder.expanduser().resolve() if explicit_folder else None
    if explicit_workbook:
        workbook = explicit_workbook.expanduser().resolve()
        folder = workbook.parent
    else:
        if folder is None:
            root = enhanced_root.expanduser().resolve()
            candidates = [path for path in root.iterdir()] if root.exists() else []
            runs = sorted(path for path in candidates if path.is_dir() and path.name.startswith("run_"))
            if not runs:
                raise FileNotFoundError(f"No enhanced analytics runs found under {root}")
            folder = runs[-1]
        matches = sorted(folder.glob("organization_entry_analytics_enhanced_*.xlsx"))
        workbook = matches[-1] if matches else folder / "organization_entry_analytics_enhanced.xlsx"

    tables: Dict[str, pd.DataFrame] = {}
    for filename, key in [
        ("student_summary.csv", "student_summary"),
        ("cohort_metrics.csv", "cohort_metrics"),
        ("master_longitudinal.csv", "master_longitudinal"),
        ("metric_definitions.csv", "metric_definitions"),
        ("qa_checks.csv", "qa_checks"),
        ("outcome_segments.csv", "outcome_segments"),
        ("status_mapping.csv", "status_mapping"),
        ("change_log.csv", "change_log"),
    ]:
        path = folder / filename
        if path.exists():
            tables[key] = pd.read_csv(path)

    caveats: List[str] = []
    if "master_longitudinal" not in tables:
        caveats.append("Master_Longitudinal was not available, so observed-term trend views are limited.")
    return SourceBundle(
        enhanced_folder=folder,
        enhanced_workbook=workbook,
        tables=tables,
        sources_used=[str(path) for path in folder.glob("*.csv")],
        sources_ignored=[],
        caveats=caveats,
    )
