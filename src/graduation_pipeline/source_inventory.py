from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import GraduationPipelineConfig


SUPPORTED_TABULAR_SUFFIXES = {".csv", ".xlsx", ".xlsm", ".xls"}
SUPPORTED_TEXT_SUFFIXES = {".txt"}


def _iter_files(root: Path, suffixes: set[str]) -> Iterable[Path]:
    if not root.exists():
        return []
    return (path for path in root.rglob("*") if path.is_file() and path.suffix.lower() in suffixes)


def _small_file_hash(path: Path, max_bytes: int = 10_000_000) -> str:
    try:
        if path.stat().st_size > max_bytes:
            return ""
        return hashlib.sha1(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def build_source_manifest(config: GraduationPipelineConfig) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    roots = [
        ("roster", config.rosters_root, SUPPORTED_TABULAR_SUFFIXES),
        ("graduation", config.graduation_root, SUPPORTED_TABULAR_SUFFIXES),
        ("academic", config.academic_root, SUPPORTED_TABULAR_SUFFIXES),
        ("transcript", config.transcript_text_root, SUPPORTED_TEXT_SUFFIXES),
    ]
    for category, root, suffixes in roots:
        for path in _iter_files(root, suffixes):
            try:
                stat = path.stat()
            except OSError:
                continue
            rows.append(
                {
                    "source_category": category,
                    "source_file": str(path),
                    "source_suffix": path.suffix.lower(),
                    "size_bytes": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                    "sha1_if_small": _small_file_hash(path),
                }
            )
    return pd.DataFrame(rows).sort_values(["source_category", "source_file"]).reset_index(drop=True)


def manifest_digest(manifest: pd.DataFrame) -> str:
    if manifest.empty:
        return hashlib.sha1(b"empty").hexdigest()
    cols = ["source_category", "source_file", "size_bytes", "mtime_ns", "sha1_if_small"]
    payload = manifest[cols].to_json(orient="records", date_format="iso").encode("utf-8")
    return hashlib.sha1(payload).hexdigest()

