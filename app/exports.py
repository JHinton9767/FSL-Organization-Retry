from __future__ import annotations

from io import BytesIO
from typing import Dict

import pandas as pd
import plotly.graph_objects as go


def dataframe_to_csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False).encode("utf-8")


def frames_to_excel_bytes(frames: Dict[str, pd.DataFrame]) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name, frame in frames.items():
            safe_name = sheet_name[:31] or "Sheet1"
            frame.to_excel(writer, sheet_name=safe_name, index=False)
    buffer.seek(0)
    return buffer.read()


def figure_to_png_bytes(figure: go.Figure) -> bytes:
    return figure.to_image(format="png", scale=2)


def figure_to_html_bytes(figure: go.Figure) -> bytes:
    return figure.to_html(include_plotlyjs="cdn").encode("utf-8")

