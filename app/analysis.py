from __future__ import annotations

from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd

from app.metrics_engine import compute_metric
from app.models import MetricDefinition


DIMENSION_LABELS = {
    "chapter": "Chapter",
    "chapter_group": "Chapter Group",
    "custom_group": "Custom Group",
    "council": "Council",
    "org_type": "Fraternity / Sorority",
    "family": "Organization Family",
    "join_term": "Join Term",
    "join_year": "Join Year",
    "graduation_year": "Graduation Year",
    "status_group": "Latest Status",
    "major_group": "Major",
    "pell_group": "Pell Group",
    "transfer_group": "Transfer Group",
    "estimated_join_stage": "Estimated Join Stage",
    "high_hours_group": "Current Hours Group",
    "active_membership_group": "Membership Activity",
    "chapter_size_band": "Chapter Size Band",
    "snapshot_group": "Snapshot Match Status",
}


def _meets_min_n(result: dict[str, object], min_n: int) -> bool:
    denominator = result.get("denominator")
    comparison_n = result.get("students", 0) if denominator is None or denominator == "" or pd.isna(denominator) else int(denominator)
    return comparison_n >= min_n


def available_dimensions(summary: pd.DataFrame) -> dict[str, str]:
    return {
        key: label
        for key, label in DIMENSION_LABELS.items()
        if key in summary.columns and summary[key].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().shape[0] > 0
    }


def filter_options(summary: pd.DataFrame, column: str) -> list[str]:
    if column not in summary.columns:
        return []
    cleaned = summary[column].fillna("").astype(str).str.strip()
    return sorted(value for value in cleaned.unique().tolist() if value)


def apply_summary_filters(summary: pd.DataFrame, filters: Dict[str, object]) -> pd.DataFrame:
    frame = summary.copy()

    list_filters = {
        "chapters": "chapter",
        "chapter_groups": "chapter_group",
        "custom_groups": "custom_group",
        "councils": "council",
        "org_types": "org_type",
        "families": "family",
        "join_terms": "join_term",
        "statuses": "status_group",
        "majors": "major_group",
        "pell_groups": "pell_group",
        "transfer_groups": "transfer_group",
        "estimated_join_stages": "estimated_join_stage",
        "high_hours_groups": "high_hours_group",
        "active_groups": "active_membership_group",
        "chapter_size_bands": "chapter_size_band",
        "snapshot_groups": "snapshot_group",
    }

    for filter_key, column in list_filters.items():
        selected = filters.get(filter_key, [])
        if selected and column in frame.columns:
            frame = frame.loc[frame[column].isin(selected)].copy()

    population = filters.get("population", "FSL Only")
    if population == "FSL Only" and "is_fsl_member" in frame.columns:
        frame = frame.loc[frame["is_fsl_member"].fillna(False)].copy()
    elif population == "Campus Baseline Only" and "is_fsl_member" in frame.columns:
        frame = frame.loc[~frame["is_fsl_member"].fillna(False)].copy()

    if "join_year" in frame.columns:
        join_range = filters.get("join_year_range")
        if join_range and len(join_range) == 2:
            frame = frame.loc[frame["join_year"].between(join_range[0], join_range[1], inclusive="both") | frame["join_year"].isna()].copy()

    if "graduation_year" in frame.columns:
        grad_range = filters.get("graduation_year_range")
        if grad_range and len(grad_range) == 2:
            frame = frame.loc[
                frame["graduation_year"].between(grad_range[0], grad_range[1], inclusive="both") | frame["graduation_year"].isna()
            ].copy()

    return frame.reset_index(drop=True)


def apply_longitudinal_filters(
    longitudinal: pd.DataFrame,
    filtered_summary: pd.DataFrame,
    filters: Dict[str, object],
) -> pd.DataFrame:
    frame = longitudinal.copy()
    if frame.empty:
        return frame

    if "student_id" in frame.columns and "student_id" in filtered_summary.columns:
        student_ids = set(filtered_summary["student_id"].fillna("").astype(str).str.strip())
        frame = frame.loc[frame["student_id"].fillna("").astype(str).str.strip().isin(student_ids)].copy()

    observed_terms = filters.get("observed_terms", [])
    if observed_terms and "observed_term" in frame.columns:
        frame = frame.loc[frame["observed_term"].isin(observed_terms)].copy()

    observed_year_range = filters.get("observed_year_range")
    if observed_year_range and "observed_year" in frame.columns:
        frame = frame.loc[
            frame["observed_year"].between(observed_year_range[0], observed_year_range[1], inclusive="both") | frame["observed_year"].isna()
        ].copy()

    return frame.reset_index(drop=True)


def summarize_metric_by_group(
    summary: pd.DataFrame,
    metric: MetricDefinition,
    group_field: str,
    min_n: int,
) -> pd.DataFrame:
    if summary.empty or group_field not in summary.columns:
        return pd.DataFrame(columns=["Group", "Students", "Eligible N", "Numerator", "Metric Value"])

    rows = []
    for group_value, frame in summary.groupby(group_field, dropna=False):
        label = str(group_value).strip() if pd.notna(group_value) and str(group_value).strip() else "Unknown"
        result = compute_metric(frame, metric)
        denominator = result["denominator"]
        comparison_n = result["students"] if denominator is None or denominator == "" or pd.isna(denominator) else int(denominator)
        if comparison_n < min_n:
            continue
        rows.append(
            {
                "Group": label,
                "Students": result["students"],
                "Eligible N": result["denominator"],
                "Numerator": result["numerator"],
                "Metric Value": result["value"],
            }
        )
    ranked = pd.DataFrame(rows)
    if ranked.empty:
        return ranked
    return ranked.sort_values(["Metric Value", "Students", "Group"], ascending=[False, False, True]).reset_index(drop=True)


def build_comparison_table(
    summary: pd.DataFrame,
    metric: MetricDefinition,
    compare_field: str,
    selected_values: Iterable[str],
    min_n: int,
) -> pd.DataFrame:
    rows = []
    values = list(selected_values)
    for value in values:
        frame = summary.loc[summary[compare_field].fillna("").astype(str).str.strip().eq(value)].copy()
        if frame.empty:
            continue
        result = compute_metric(frame, metric)
        if not _meets_min_n(result, min_n):
            continue
        rows.append(
            {
                "Comparison Group": value,
                "Students": result["students"],
                "Eligible N": result["denominator"],
                "Numerator": result["numerator"],
                "Metric Value": result["value"],
            }
        )

    overall = summary.loc[summary["is_fsl_member"].fillna(True)] if "is_fsl_member" in summary.columns else summary
    overall_result = compute_metric(overall, metric)
    if overall_result["students"] > 0:
        rows.append(
            {
                "Comparison Group": "FSL-wide Average",
                "Students": overall_result["students"],
                "Eligible N": overall_result["denominator"],
                "Numerator": overall_result["numerator"],
                "Metric Value": overall_result["value"],
            }
        )

    if "is_fsl_member" in summary.columns and (~summary["is_fsl_member"].fillna(True)).any():
        campus = summary.loc[~summary["is_fsl_member"].fillna(True)].copy()
        campus_result = compute_metric(campus, metric)
        if campus_result["students"] > 0:
            rows.append(
                {
                    "Comparison Group": "Campus Baseline",
                    "Students": campus_result["students"],
                    "Eligible N": campus_result["denominator"],
                    "Numerator": campus_result["numerator"],
                    "Metric Value": campus_result["value"],
                }
            )

    return pd.DataFrame(rows)


def build_controlled_comparison(
    summary: pd.DataFrame,
    metric: MetricDefinition,
    compare_field: str,
    selected_values: Iterable[str],
    control_field: str,
    min_n: int,
) -> pd.DataFrame:
    if not control_field or control_field not in summary.columns:
        return pd.DataFrame()

    rows = []
    control_values = filter_options(summary, control_field)
    for compare_value in selected_values:
        selected = summary.loc[summary[compare_field].fillna("").astype(str).str.strip().eq(compare_value)].copy()
        for control_value in control_values:
            frame = selected.loc[selected[control_field].fillna("").astype(str).str.strip().eq(control_value)].copy()
            result = compute_metric(frame, metric)
            if not _meets_min_n(result, min_n):
                continue
            rows.append(
                {
                    "Comparison Group": compare_value,
                    "Control Group": control_value,
                    "Students": result["students"],
                    "Eligible N": result["denominator"],
                    "Metric Value": result["value"],
                }
            )
    return pd.DataFrame(rows)


def build_distribution_table(
    summary: pd.DataFrame,
    group_field: str,
    category_field: str,
    min_n: int,
) -> pd.DataFrame:
    if summary.empty or group_field not in summary.columns or category_field not in summary.columns:
        return pd.DataFrame()

    counts = (
        summary.groupby([group_field, category_field], dropna=False)["student_id"]
        .nunique()
        .reset_index(name="Count")
    )
    counts[group_field] = counts[group_field].fillna("").astype(str).str.strip().replace("", "Unknown")
    counts[category_field] = counts[category_field].fillna("").astype(str).str.strip().replace("", "Unknown")
    totals = counts.groupby(group_field)["Count"].transform("sum")
    counts = counts.loc[totals >= min_n].copy()
    counts["Share"] = counts["Count"] / counts.groupby(group_field)["Count"].transform("sum")
    return counts.rename(columns={group_field: "Group", category_field: "Category"}).sort_values(["Group", "Category"])


def build_summary_time_series(
    summary: pd.DataFrame,
    metric: MetricDefinition,
    time_field: str,
    segment_field: Optional[str],
    min_n: int,
) -> pd.DataFrame:
    if summary.empty or time_field not in summary.columns:
        return pd.DataFrame()

    rows = []
    group_fields = [time_field] + ([segment_field] if segment_field else [])
    for group_value, frame in summary.groupby(group_fields, dropna=False):
        if not isinstance(group_value, tuple):
            group_value = (group_value,)
        time_value = group_value[0]
        segment_value = group_value[1] if len(group_value) > 1 else "All Students"
        result = compute_metric(frame, metric)
        if not _meets_min_n(result, min_n):
            continue
        rows.append(
            {
                "Time": time_value,
                "Segment": segment_value,
                "Metric Value": result["value"],
                "Students": result["students"],
                "Eligible N": result["denominator"],
            }
        )
    return pd.DataFrame(rows).sort_values(["Time", "Segment"])


def build_observed_term_series(
    longitudinal: pd.DataFrame,
    measure: str,
    segment_field: Optional[str],
) -> pd.DataFrame:
    if longitudinal.empty or "observed_term" not in longitudinal.columns:
        return pd.DataFrame()

    frame = longitudinal.copy()
    if segment_field and segment_field not in frame.columns:
        segment_field = None
    group_fields = ["observed_term", "observed_term_sort"] + ([segment_field] if segment_field else [])
    rows = []
    for group_value, group in frame.groupby(group_fields, dropna=False):
        if not isinstance(group_value, tuple):
            group_value = (group_value,)
        term = group_value[0]
        segment = group_value[2] if len(group_value) > 2 else "All Students"
        if measure == "Headcount":
            value = int(group["student_id"].nunique())
        elif measure == "Average Term GPA":
            value = pd.to_numeric(group["term_gpa"], errors="coerce").dropna().mean()
        elif measure == "Average Cumulative GPA":
            value = pd.to_numeric(group["cumulative_gpa"], errors="coerce").dropna().mean()
        elif measure == "Average Passed Hours":
            value = pd.to_numeric(group["term_passed_hours"], errors="coerce").dropna().mean()
        else:
            value = pd.to_numeric(group["cumulative_hours"], errors="coerce").dropna().mean()
        rows.append(
            {
                "Observed Term": term,
                "Observed Term Sort": group_value[1] if len(group_value) > 1 else 999999,
                "Segment": segment,
                "Metric Value": value,
            }
        )
    return pd.DataFrame(rows).sort_values(["Observed Term Sort", "Segment"])


def build_scatter_frame(summary: pd.DataFrame, metric: MetricDefinition, group_field: str, min_n: int) -> pd.DataFrame:
    table = summarize_metric_by_group(summary, metric, group_field, min_n)
    if table.empty:
        return table
    size_lookup = (
        summary.groupby(group_field, dropna=False)["student_id"]
        .nunique()
        .reset_index(name="Chapter Size")
        .rename(columns={group_field: "Group"})
    )
    return table.merge(size_lookup, on="Group", how="left")


def stakeholder_summary(ranked_table: pd.DataFrame, metric: MetricDefinition) -> list[str]:
    if ranked_table.empty:
        return ["No groups met the current sample-size threshold for this metric."]

    highest = ranked_table.iloc[0]
    lowest = ranked_table.iloc[-1]
    high_value = "n/a" if pd.isna(highest["Metric Value"]) else f"{float(highest['Metric Value']):.3f}"
    low_value = "n/a" if pd.isna(lowest["Metric Value"]) else f"{float(lowest['Metric Value']):.3f}"
    notes = [
        f"Highest {metric.display_name.lower()}: {highest['Group']} ({high_value}).",
        f"Lowest {metric.display_name.lower()}: {lowest['Group']} ({low_value}).",
    ]
    return notes
