from __future__ import annotations

from io import BytesIO
import re
from typing import Dict, List

import pandas as pd
import plotly.graph_objects as go


EXCEL_MAX_ROWS = 1_048_576
EXCEL_MAX_COLUMNS = 16_384
EXCEL_MAX_DATA_ROWS = EXCEL_MAX_ROWS - 1
EXCEL_SHEET_NAME_LIMIT = 31


def dataframe_to_csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False).encode("utf-8")


def _safe_excel_sheet_name(value: str) -> str:
    cleaned = re.sub(r"[\[\]\:\*\?\/\\]", "_", str(value)).strip()
    return cleaned or "Sheet"


def _unique_sheet_name(base_name: str, used_names: set[str]) -> str:
    safe_base = _safe_excel_sheet_name(base_name)
    candidate = safe_base[:EXCEL_SHEET_NAME_LIMIT]
    suffix = 1
    while candidate in used_names:
        suffix_text = f"_{suffix}"
        candidate = f"{safe_base[: EXCEL_SHEET_NAME_LIMIT - len(suffix_text)]}{suffix_text}"
        suffix += 1
    used_names.add(candidate)
    return candidate


def _excel_sheet_chunks(
    sheet_name: str,
    frame: pd.DataFrame,
    max_data_rows: int = EXCEL_MAX_DATA_ROWS,
    max_columns: int = EXCEL_MAX_COLUMNS,
) -> List[tuple[str, pd.DataFrame, int, int]]:
    if max_data_rows < 1:
        raise ValueError("max_data_rows must be at least 1")
    if max_columns < 1:
        raise ValueError("max_columns must be at least 1")

    row_ranges = [(0, 0)] if frame.empty else [(start, min(start + max_data_rows, len(frame))) for start in range(0, len(frame), max_data_rows)]
    column_ranges = (
        [(0, 0)]
        if len(frame.columns) == 0
        else [(start, min(start + max_columns, len(frame.columns))) for start in range(0, len(frame.columns), max_columns)]
    )
    multi_part = len(row_ranges) > 1 or len(column_ranges) > 1
    chunks: List[tuple[str, pd.DataFrame, int, int]] = []

    for row_number, (row_start, row_end) in enumerate(row_ranges, start=1):
        for column_number, (column_start, column_end) in enumerate(column_ranges, start=1):
            chunk_name = sheet_name
            if multi_part:
                chunk_name = f"{sheet_name} {row_number:03d}"
                if len(column_ranges) > 1:
                    chunk_name = f"{chunk_name} C{column_number:02d}"
            chunk = frame.iloc[row_start:row_end, column_start:column_end].copy()
            chunks.append((chunk_name, chunk, row_start + 1 if row_end else 0, row_end))
    return chunks


def frames_to_excel_bytes(
    frames: Dict[str, pd.DataFrame],
    max_data_rows_per_sheet: int = EXCEL_MAX_DATA_ROWS,
    max_columns_per_sheet: int = EXCEL_MAX_COLUMNS,
) -> bytes:
    buffer = BytesIO()
    used_sheet_names: set[str] = set()
    manifest_rows: List[dict] = []
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name, frame in frames.items():
            export_frame = frame if frame is not None else pd.DataFrame()
            for chunk_name, chunk, row_start, row_end in _excel_sheet_chunks(
                sheet_name,
                export_frame,
                max_data_rows=max_data_rows_per_sheet,
                max_columns=max_columns_per_sheet,
            ):
                safe_name = _unique_sheet_name(chunk_name, used_sheet_names)
                chunk.to_excel(writer, sheet_name=safe_name, index=False)
                manifest_rows.append(
                    {
                        "Original Table": sheet_name,
                        "Workbook Sheet": safe_name,
                        "Original Row Start": row_start,
                        "Original Row End": row_end,
                        "Rows Written": int(len(chunk)),
                        "Columns Written": int(len(chunk.columns)),
                        "Original Rows": int(len(export_frame)),
                        "Original Columns": int(len(export_frame.columns)),
                    }
                )
        manifest = pd.DataFrame(manifest_rows)
        manifest_name = _unique_sheet_name("Export Manifest", used_sheet_names)
        manifest.to_excel(writer, sheet_name=manifest_name, index=False)
    buffer.seek(0)
    return buffer.read()


def figure_to_png_bytes(figure: go.Figure) -> bytes:
    return figure.to_image(format="png", scale=2)


def figure_to_html_bytes(figure: go.Figure) -> bytes:
    return figure.to_html(include_plotlyjs="cdn").encode("utf-8")
