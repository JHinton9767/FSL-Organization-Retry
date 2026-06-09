from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.build_master_roster import normalize_chapter_name
from src.shared_utils import clean_text

from .normalize import banner_id_from_text, contains_graduation_text, empty_required_frame, infer_term_from_path, normalize_term


HEADER_ALIASES = {
    "student_id_raw": {"student id", "banner id", "banner", "plid", "student number", "id"},
    "first_name": {"first name", "firstname", "first"},
    "last_name": {"last name", "lastname", "last"},
    "term_raw": {"term", "semester", "join term", "organization join term", "graduation term", "grad term"},
    "chapter_raw": {"chapter", "organization", "org", "organization name"},
    "council": {"council"},
    "status_raw": {"status", "member status", "roster status", "outcome", "graduation status"},
    "graduation_text_raw": {"graduation", "graduation text", "degree status", "degree", "notes"},
}


def _canonical_header(value: object) -> str:
    return clean_text(value).lower().replace("_", " ").replace("-", " ")


def _map_headers(headers: list[object]) -> dict[str, int]:
    mapped: dict[str, int] = {}
    canonical_headers = [_canonical_header(value) for value in headers]
    for target, aliases in HEADER_ALIASES.items():
        for idx, header in enumerate(canonical_headers):
            if header in aliases and target not in mapped:
                mapped[target] = idx
    return mapped


def _find_header_row(frame: pd.DataFrame) -> tuple[int, dict[str, int]]:
    sample = frame.head(30)
    best_index = 0
    best_map: dict[str, int] = {}
    best_score = -1
    for index, row in sample.iterrows():
        header_map = _map_headers(list(row))
        score = len(header_map)
        if "student_id_raw" in header_map:
            score += 3
        if score > best_score:
            best_index = int(index)
            best_map = header_map
            best_score = score
    return best_index, best_map


def _read_tabular(path: Path) -> list[tuple[str, pd.DataFrame]]:
    suffix = path.suffix.lower()
    try:
        if suffix == ".csv":
            return [("", pd.read_csv(path, header=None, dtype=str, keep_default_na=False))]
        sheets = pd.read_excel(path, sheet_name=None, header=None, dtype=str, keep_default_na=False)
        return list(sheets.items())
    except Exception:
        return []


def _record_from_row(category: str, path: Path, sheet: str, row_number: int, row: pd.Series, header_map: dict[str, int]) -> dict[str, object]:
    def cell(field: str) -> str:
        idx = header_map.get(field)
        return clean_text(row.iloc[idx]) if idx is not None and idx < len(row) else ""

    term_raw = cell("term_raw")
    if not term_raw:
        _, inferred_label, _ = infer_term_from_path(path)
        term_raw = inferred_label
    term_code, term_label, term_sort = normalize_term(term_raw)
    chapter_raw = cell("chapter_raw")
    status_raw = cell("status_raw")
    grad_text = cell("graduation_text_raw") or status_raw
    explicit = contains_graduation_text(" ".join([status_raw, grad_text]))
    return {
        "source_category": category,
        "source_file": str(path),
        "source_sheet": sheet,
        "row_number": row_number,
        "student_id_raw": cell("student_id_raw"),
        "first_name": cell("first_name"),
        "last_name": cell("last_name"),
        "term_raw": term_raw,
        "term_code": term_code,
        "term_label": term_label,
        "term_sort": term_sort,
        "chapter_raw": chapter_raw,
        "chapter": normalize_chapter_name(chapter_raw),
        "council": cell("council"),
        "status_raw": status_raw,
        "status_bucket": "",
        "graduation_text_raw": grad_text,
        "explicit_graduation_evidence": explicit,
        "evidence_detail": grad_text,
    }


def load_tabular_required_fields(category: str, path: Path) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for sheet_name, frame in _read_tabular(path):
        if frame.empty:
            continue
        header_index, header_map = _find_header_row(frame)
        if "student_id_raw" not in header_map:
            continue
        data = frame.iloc[header_index + 1 :].copy()
        for row_offset, row in data.iterrows():
            record = _record_from_row(category, path, sheet_name, int(row_offset) + 1, row, header_map)
            if any(clean_text(record.get(column)) for column in ["student_id_raw", "first_name", "last_name", "status_raw"]):
                rows.append(record)
    return pd.DataFrame(rows) if rows else empty_required_frame()


def load_transcript_required_fields(path: Path) -> pd.DataFrame:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return empty_required_frame()
    banner_id = banner_id_from_text(path.name) or banner_id_from_text(text)
    explicit = contains_graduation_text(text)
    if not banner_id and not explicit:
        return empty_required_frame()
    term_code, term_label, term_sort = infer_term_from_path(path)
    return pd.DataFrame(
        [
            {
                "source_category": "transcript",
                "source_file": str(path),
                "source_sheet": "",
                "row_number": 1,
                "student_id_raw": banner_id,
                "first_name": "",
                "last_name": "",
                "term_raw": term_label,
                "term_code": term_code,
                "term_label": term_label,
                "term_sort": term_sort,
                "chapter_raw": "",
                "chapter": "",
                "council": "",
                "status_raw": "",
                "status_bucket": "",
                "graduation_text_raw": text[:1000],
                "explicit_graduation_evidence": explicit,
                "evidence_detail": "transcript graduation evidence" if explicit else "",
            }
        ]
    )


def load_required_fields(manifest: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for row in manifest.to_dict(orient="records"):
        path = Path(str(row["source_file"]))
        category = str(row["source_category"])
        if category == "transcript":
            frames.append(load_transcript_required_fields(path))
        else:
            frames.append(load_tabular_required_fields(category, path))
    frames = [frame for frame in frames if not frame.empty]
    return pd.concat(frames, ignore_index=True) if frames else empty_required_frame()

