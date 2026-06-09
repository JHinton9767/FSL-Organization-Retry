from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from src.path_config import ROOT, load_path_config


MANUAL_CORRECTION_COLUMNS = [
    "banner_id",
    "corrected_graduation_status",
    "corrected_graduation_term",
    "corrected_first_fsl_term",
    "corrected_chapter",
    "corrected_council",
    "correction_reason",
    "reviewer_initials",
    "reviewed_date",
    "notes",
    "active",
]


@dataclass(frozen=True)
class GraduationPipelineConfig:
    config_path: Path
    rosters_root: Path
    graduation_root: Path
    transcript_text_root: Path
    academic_root: Path
    output_root: Path
    cache_root: Path
    manual_corrections_path: Path


def load_graduation_config(config_path: Optional[str | Path] = None) -> GraduationPipelineConfig:
    """Load machine-specific paths and narrow them to graduation outputs."""
    try:
        paths = load_path_config(config_path)
        config_file = paths.config_path
        rosters_root = paths.roster_inbox_root
        graduation_root = paths.graduation_root
        transcript_text_root = paths.transcript_text_root
        academic_root = paths.grade_reports_root
        base_output = paths.output_root.parent if paths.output_root.name == "canonical" else paths.output_root
        base_cache = paths.cache_root.parent if paths.cache_root.name == "_source_cache" else paths.cache_root
    except Exception:
        config_file = ROOT / "config" / "local_paths.yaml"
        rosters_root = ROOT / "data" / "inbox" / "rosters"
        graduation_root = ROOT / "data" / "inbox" / "graduation"
        transcript_text_root = ROOT / "data" / "inbox" / "transcript_text"
        academic_root = ROOT / "data" / "inbox" / "academic"
        base_output = ROOT / "output"
        base_cache = ROOT / "output" / "_cache"

    output_root = base_output / "graduation"
    cache_root = base_cache / "graduation"
    manual_path = ROOT / "data" / "manual" / "manual_corrections.csv"
    output_root.mkdir(parents=True, exist_ok=True)
    cache_root.mkdir(parents=True, exist_ok=True)
    manual_path.parent.mkdir(parents=True, exist_ok=True)
    return GraduationPipelineConfig(
        config_path=config_file,
        rosters_root=rosters_root,
        graduation_root=graduation_root,
        transcript_text_root=transcript_text_root,
        academic_root=academic_root,
        output_root=output_root,
        cache_root=cache_root,
        manual_corrections_path=manual_path,
    )

