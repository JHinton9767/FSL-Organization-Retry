from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class DatasetVersion:
    key: str
    label: str
    dataset_type: str
    root_path: Path
    created_at: str = ""
    notes: List[str] = field(default_factory=list)


@dataclass
class DataFileStatus:
    label: str
    path: Path
    required: bool
    exists: bool
    loaded: bool = False
    row_count: Optional[int] = None
    last_modified: str = ""
    warning: str = ""


@dataclass
class DataSourceStatus:
    source_key: str
    label: str
    priority: int
    root_path: Path
    selected_path: Optional[Path]
    available: bool
    files: List[DataFileStatus] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class MetricDefinition:
    key: str
    display_name: str
    category: str
    kind: str
    source_table: str
    value_field: str = ""
    numerator_field: str = ""
    denominator_field: str = ""
    format: str = "decimal"
    higher_is_better: bool = True
    min_sample_size: int = 5
    description: str = ""
    logic_source: str = ""
    numerator_label: str = ""
    denominator_label: str = ""
    notes: str = ""
    limitations: str = ""
    available_when: List[str] = field(default_factory=list)


@dataclass
class AnalysisBundle:
    version: DatasetVersion
    summary: pd.DataFrame
    longitudinal: pd.DataFrame
    tables: Dict[str, pd.DataFrame]
    metric_definitions: List[MetricDefinition]
    notes: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    data_status: List[DataFileStatus] = field(default_factory=list)
