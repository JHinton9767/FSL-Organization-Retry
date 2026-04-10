from __future__ import annotations

from typing import Iterable, List

import numpy as np
import pandas as pd

from app.io_utils import normalize_text
from app.models import MetricDefinition


def metric_available(definition: MetricDefinition, summary: pd.DataFrame, longitudinal: pd.DataFrame) -> bool:
    source = summary if definition.source_table == "summary" else longitudinal
    required = [value for value in [definition.value_field, definition.numerator_field, definition.denominator_field] if value]
    if not all(column in source.columns for column in required):
        return False

    if definition.kind == "count_unique":
        usable = _usable_series(source, definition.value_field).fillna("").astype(str).str.strip()
        return usable.replace("", pd.NA).dropna().shape[0] > 0

    if definition.kind == "mean":
        return pd.to_numeric(_usable_series(source, definition.value_field), errors="coerce").dropna().shape[0] > 0

    if definition.kind in {"sum_bool", "rate_bool"}:
        observed = _usable_series(source, definition.numerator_field)
        if observed.dtype == "object":
            return observed.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().shape[0] > 0
        return observed.notna().sum() > 0

    return True


def available_metrics(
    definitions: Iterable[MetricDefinition],
    summary: pd.DataFrame,
    longitudinal: pd.DataFrame,
) -> List[MetricDefinition]:
    return [definition for definition in definitions if metric_available(definition, summary, longitudinal)]


def _usable_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(index=frame.index, dtype="object")
    return frame[column]


def _bool_mask(series: pd.Series) -> pd.Series:
    if series.dtype == "bool":
        return series.fillna(False)
    lowered = series.fillna("").astype(str).str.strip().str.lower()
    return lowered.eq("true") | lowered.eq("yes") | lowered.eq("1")


def compute_metric(frame: pd.DataFrame, definition: MetricDefinition) -> dict[str, object]:
    if frame.empty:
        return {
            "value": np.nan,
            "numerator": 0,
            "denominator": 0,
            "students": 0,
            "format": definition.format,
        }

    students = int(frame["student_id"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()) if "student_id" in frame.columns else int(len(frame))

    if definition.kind == "count_unique":
        usable = _usable_series(frame, definition.value_field).fillna("").astype(str).str.strip().replace("", pd.NA).dropna()
        value = int(usable.nunique())
        return {"value": value, "numerator": value, "denominator": value, "students": students, "format": definition.format}

    if definition.kind == "sum_bool":
        numerator = int(_bool_mask(_usable_series(frame, definition.value_field)).sum())
        return {"value": numerator, "numerator": numerator, "denominator": students, "students": students, "format": definition.format}

    if definition.kind == "mean":
        values = pd.to_numeric(_usable_series(frame, definition.value_field), errors="coerce")
        usable = values.dropna()
        value = float(usable.mean()) if not usable.empty else np.nan
        return {
            "value": value,
            "numerator": np.nan,
            "denominator": int(usable.shape[0]),
            "students": students,
            "format": definition.format,
        }

    if definition.kind == "rate_bool":
        if definition.denominator_field:
            denominator_mask = _bool_mask(_usable_series(frame, definition.denominator_field))
        else:
            observed = _usable_series(frame, definition.numerator_field)
            denominator_mask = observed.notna()
            if observed.dtype == "object":
                denominator_mask &= observed.astype(str).str.strip().ne("")
        eligible = frame.loc[denominator_mask].copy()
        numerator = int(_bool_mask(_usable_series(eligible, definition.numerator_field)).sum())
        denominator = int(len(eligible))
        value = (numerator / denominator) if denominator else np.nan
        return {
            "value": value,
            "numerator": numerator,
            "denominator": denominator,
            "students": students,
            "format": definition.format,
        }

    raise ValueError(f"Unsupported metric kind: {definition.kind}")


def format_metric_value(value: object, format_code: str) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "Not available"
    if format_code == "percent":
        return f"{float(value):.1%}"
    if format_code == "integer":
        return f"{int(round(float(value))):,}"
    if format_code == "hours":
        return f"{float(value):,.1f}"
    return f"{float(value):,.2f}"


def metric_by_key(definitions: Iterable[MetricDefinition], metric_key: str) -> MetricDefinition:
    match = next((definition for definition in definitions if definition.key == metric_key), None)
    if match is None:
        raise KeyError(f"Metric not found: {metric_key}")
    return match


def metric_caption(definition: MetricDefinition) -> str:
    pieces = [definition.description]
    if normalize_text(definition.logic_source):
        pieces.append(f"Logic source: {definition.logic_source}")
    if normalize_text(definition.notes):
        pieces.append(definition.notes)
    if normalize_text(definition.limitations):
        pieces.append(f"Limitations: {definition.limitations}")
    return " | ".join(piece for piece in pieces if normalize_text(piece))
