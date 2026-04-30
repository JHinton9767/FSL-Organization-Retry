from __future__ import annotations

import re
from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd

from app.io_utils import parse_term_label
from app.metrics_engine import (
    ALL_STUDENTS_LABEL,
    RESOLVED_OUTCOMES_ONLY_LABEL,
    compute_metric_views,
    metric_population_column,
    select_metric_view,
)
from app.models import MetricDefinition
from app.status_framework import resolved_outcomes_only_frame, student_count


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
    "outcome_resolution_group": "Outcome Resolution Group",
}

PERSISTENCE_COUNCIL_OPTIONS = ["ALL", "IFC", "PHC", "NPHC", "MGC", "FRA", "SOR"]
PERSISTENCE_TOTAL_RE = re.compile(r"^(?:fall\s+)?((?:19|20)\d{2})\s+total$", re.IGNORECASE)


def _meets_min_n(result: dict[str, object], min_n: int) -> bool:
    denominator = result.get("denominator")
    comparison_n = result.get("students", 0) if denominator is None or denominator == "" or pd.isna(denominator) else int(denominator)
    return comparison_n >= min_n


def _population_metric_columns(metric_views: dict[str, object], population_label: str) -> dict[str, object]:
    primary = select_metric_view(metric_views, population_label)
    all_result = metric_views["all"]
    resolved_result = metric_views["resolved_only"]
    return {
        "Population View": population_label,
        "Students": primary["students"],
        "Eligible N": primary["denominator"],
        "Numerator": primary["numerator"],
        "Metric Value": primary["value"],
        metric_population_column("Students", ALL_STUDENTS_LABEL): all_result["students"],
        metric_population_column("Eligible N", ALL_STUDENTS_LABEL): all_result["denominator"],
        metric_population_column("Numerator", ALL_STUDENTS_LABEL): all_result["numerator"],
        metric_population_column("Metric Value", ALL_STUDENTS_LABEL): all_result["value"],
        metric_population_column("Students", RESOLVED_OUTCOMES_ONLY_LABEL): resolved_result["students"],
        metric_population_column("Eligible N", RESOLVED_OUTCOMES_ONLY_LABEL): resolved_result["denominator"],
        metric_population_column("Numerator", RESOLVED_OUTCOMES_ONLY_LABEL): resolved_result["numerator"],
        metric_population_column("Metric Value", RESOLVED_OUTCOMES_ONLY_LABEL): resolved_result["value"],
        "Resolved Count": metric_views["resolved_n"],
        "Graduated Count": metric_views["graduated_n"],
        "Resolved Non-Graduate Exit Count": metric_views["resolved_non_graduate_exit_n"],
        "Still Active Count": metric_views["still_active_n"],
        "Truly Unknown Count": metric_views["truly_unknown_n"],
        "Other / Unmapped Count": metric_views["other_unmapped_n"],
        "Excluded Count": metric_views["excluded_n"],
        "Excluded Share": metric_views["excluded_share"],
    }


def _label_or_unknown(value: object) -> str:
    if pd.isna(value):
        return "Unknown"
    text = str(value).strip()
    return text or "Unknown"


def _metric_row(
    frame: pd.DataFrame,
    metric: MetricDefinition,
    population_label: str,
    min_n: int | None = None,
    **labels: object,
) -> dict[str, object] | None:
    metric_views = compute_metric_views(frame, metric)
    primary = select_metric_view(metric_views, population_label)
    if min_n is not None and not _meets_min_n(primary, min_n):
        return None
    row = {key: value for key, value in labels.items()}
    row.update(_population_metric_columns(metric_views, population_label))
    return row


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
        "resolved_outcome_groups": "outcome_resolution_group",
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
    population_label: str = ALL_STUDENTS_LABEL,
) -> pd.DataFrame:
    if summary.empty or group_field not in summary.columns:
        return pd.DataFrame(columns=["Group", "Students", "Eligible N", "Numerator", "Metric Value"])

    rows = []
    for group_value, frame in summary.groupby(group_field, dropna=False):
        row = _metric_row(frame, metric, population_label, min_n=min_n, Group=_label_or_unknown(group_value))
        if row is not None:
            rows.append(row)
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
    population_label: str = ALL_STUDENTS_LABEL,
) -> pd.DataFrame:
    rows = []
    values = list(selected_values)
    for value in values:
        frame = summary.loc[summary[compare_field].fillna("").astype(str).str.strip().eq(value)].copy()
        if frame.empty:
            continue
        row = _metric_row(frame, metric, population_label, min_n=min_n, **{"Comparison Group": value})
        if row is not None:
            rows.append(row)

    overall = summary.loc[summary["is_fsl_member"].fillna(True)] if "is_fsl_member" in summary.columns else summary
    overall_row = _metric_row(overall, metric, population_label, min_n=0, **{"Comparison Group": "FSL-wide Average"})
    if overall_row is not None and int(overall_row["Students"]) > 0:
        rows.append(overall_row)

    if "is_fsl_member" in summary.columns and (~summary["is_fsl_member"].fillna(True)).any():
        campus = summary.loc[~summary["is_fsl_member"].fillna(True)].copy()
        campus_row = _metric_row(campus, metric, population_label, min_n=0, **{"Comparison Group": "Campus Baseline"})
        if campus_row is not None and int(campus_row["Students"]) > 0:
            rows.append(campus_row)

    return pd.DataFrame(rows)


def build_controlled_comparison(
    summary: pd.DataFrame,
    metric: MetricDefinition,
    compare_field: str,
    selected_values: Iterable[str],
    control_field: str,
    min_n: int,
    population_label: str = ALL_STUDENTS_LABEL,
) -> pd.DataFrame:
    if not control_field or control_field not in summary.columns:
        return pd.DataFrame()

    rows = []
    control_values = filter_options(summary, control_field)
    for compare_value in selected_values:
        selected = summary.loc[summary[compare_field].fillna("").astype(str).str.strip().eq(compare_value)].copy()
        for control_value in control_values:
            frame = selected.loc[selected[control_field].fillna("").astype(str).str.strip().eq(control_value)].copy()
            row = _metric_row(
                frame,
                metric,
                population_label,
                min_n=min_n,
                **{
                    "Comparison Group": compare_value,
                    "Control Group": control_value,
                },
            )
            if row is not None:
                rows.append(row)
    return pd.DataFrame(rows)


def build_distribution_table(
    summary: pd.DataFrame,
    group_field: str,
    category_field: str,
    min_n: int,
    population_label: str = ALL_STUDENTS_LABEL,
) -> pd.DataFrame:
    if summary.empty or group_field not in summary.columns or category_field not in summary.columns:
        return pd.DataFrame()

    def _truthy_sum(series: pd.Series) -> int:
        lowered = series.fillna("").astype(str).str.strip().str.lower()
        return int((lowered.eq("true") | lowered.eq("yes") | lowered.eq("1")).sum())

    def _distribution_counts(frame: pd.DataFrame, count_column: str, share_column: str) -> pd.DataFrame:
        counts = (
            frame.groupby([group_field, category_field], dropna=False)["student_id"]
            .nunique()
            .reset_index(name=count_column)
        )
        if counts.empty:
            return counts
        counts[group_field] = counts[group_field].fillna("").astype(str).str.strip().replace("", "Unknown")
        counts[category_field] = counts[category_field].fillna("").astype(str).str.strip().replace("", "Unknown")
        totals = counts.groupby(group_field)[count_column].transform("sum")
        counts[share_column] = counts[count_column] / totals
        return counts

    all_counts = _distribution_counts(
        summary,
        metric_population_column("Count", ALL_STUDENTS_LABEL),
        metric_population_column("Share", ALL_STUDENTS_LABEL),
    )
    resolved_counts = _distribution_counts(
        resolved_outcomes_only_frame(summary),
        metric_population_column("Count", RESOLVED_OUTCOMES_ONLY_LABEL),
        metric_population_column("Share", RESOLVED_OUTCOMES_ONLY_LABEL),
    )
    counts = all_counts.merge(
        resolved_counts,
        on=[group_field, category_field],
        how="outer",
    ).fillna(0)
    if counts.empty:
        return counts

    selected_count_column = metric_population_column("Count", population_label)
    selected_share_column = metric_population_column("Share", population_label)
    totals = counts.groupby(group_field)[selected_count_column].transform("sum")
    counts = counts.loc[totals >= min_n].copy()
    counts["Count"] = counts[selected_count_column]
    counts["Share"] = counts[selected_share_column]
    group_population = (
        summary.groupby(group_field, dropna=False)
        .agg(
            **{
                "All Students Count": ("student_id", "nunique"),
                "Resolved Count": ("is_resolved_outcome", _truthy_sum),
                "Still Active Count": ("is_active_outcome", _truthy_sum),
                "Truly Unknown Count": ("is_unknown_outcome", _truthy_sum),
                "Graduated Count": ("is_graduated", _truthy_sum),
                "Resolved Non-Graduate Exit Count": ("is_known_non_graduate_exit", _truthy_sum),
            }
        )
        .reset_index()
    )
    group_population[group_field] = group_population[group_field].fillna("").astype(str).str.strip().replace("", "Unknown")
    group_population["Other / Unmapped Count"] = (
        group_population["All Students Count"]
        - group_population["Resolved Count"]
        - group_population["Still Active Count"]
        - group_population["Truly Unknown Count"]
    ).clip(lower=0)
    group_population["Excluded Count"] = group_population["Still Active Count"] + group_population["Truly Unknown Count"] + group_population["Other / Unmapped Count"]
    counts = counts.merge(group_population, on=group_field, how="left")
    return counts.rename(columns={group_field: "Group", category_field: "Category"}).sort_values(["Group", "Category"])


def build_summary_time_series(
    summary: pd.DataFrame,
    metric: MetricDefinition,
    time_field: str,
    segment_field: Optional[str],
    min_n: int,
    population_label: str = ALL_STUDENTS_LABEL,
) -> pd.DataFrame:
    if summary.empty or time_field not in summary.columns:
        return pd.DataFrame()

    rows = []
    group_fields = [time_field] + ([segment_field] if segment_field else [])
    for group_value, frame in summary.groupby(group_fields, dropna=False):
        if not isinstance(group_value, tuple):
            group_value = (group_value,)
        row = _metric_row(
            frame,
            metric,
            population_label,
            min_n=min_n,
            Time=group_value[0],
            Segment=group_value[1] if len(group_value) > 1 else "All Students",
        )
        if row is not None:
            rows.append(row)
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    return result.sort_values(["Time", "Segment"])


def build_observed_term_series(
    longitudinal: pd.DataFrame,
    measure: str,
    segment_field: Optional[str],
    summary: Optional[pd.DataFrame] = None,
    population_label: str = ALL_STUDENTS_LABEL,
) -> pd.DataFrame:
    if longitudinal.empty or "observed_term" not in longitudinal.columns:
        return pd.DataFrame()

    frame = longitudinal.copy()
    if segment_field and segment_field not in frame.columns:
        segment_field = None
    resolved_student_ids = set()
    if summary is not None and "student_id" in summary.columns:
        resolved_student_ids = set(
            resolved_outcomes_only_frame(summary)["student_id"].fillna("").astype(str).str.strip()
        )

    def _measure_value(group: pd.DataFrame) -> float | int:
        if measure == "Headcount":
            return int(group["student_id"].nunique())
        if measure == "Average Term GPA":
            return pd.to_numeric(group["term_gpa"], errors="coerce").dropna().mean()
        if measure == "Average Cumulative GPA":
            return pd.to_numeric(group["cumulative_gpa"], errors="coerce").dropna().mean()
        if measure == "Average Passed Hours":
            return pd.to_numeric(group["term_passed_hours"], errors="coerce").dropna().mean()
        return pd.to_numeric(group["cumulative_hours"], errors="coerce").dropna().mean()

    group_fields = ["observed_term", "observed_term_sort"] + ([segment_field] if segment_field else [])
    rows = []
    for group_value, group in frame.groupby(group_fields, dropna=False):
        if not isinstance(group_value, tuple):
            group_value = (group_value,)
        term = group_value[0]
        segment = group_value[2] if len(group_value) > 2 else "All Students"
        resolved_group = group.loc[group["student_id"].fillna("").astype(str).str.strip().isin(resolved_student_ids)].copy()
        full_value = _measure_value(group)
        resolved_value = _measure_value(resolved_group) if not resolved_group.empty else np.nan
        full_students = student_count(group)
        resolved_students = student_count(resolved_group)
        rows.append(
            {
                "Observed Term": term,
                "Observed Term Sort": group_value[1] if len(group_value) > 1 else 999999,
                "Segment": segment,
                "Population View": population_label,
                "Metric Value": resolved_value if population_label == RESOLVED_OUTCOMES_ONLY_LABEL else full_value,
                metric_population_column("Metric Value", ALL_STUDENTS_LABEL): full_value,
                metric_population_column("Metric Value", RESOLVED_OUTCOMES_ONLY_LABEL): resolved_value,
                "Students": resolved_students if population_label == RESOLVED_OUTCOMES_ONLY_LABEL else full_students,
                metric_population_column("Students", ALL_STUDENTS_LABEL): full_students,
                metric_population_column("Students", RESOLVED_OUTCOMES_ONLY_LABEL): resolved_students,
                "Excluded Count": max(full_students - resolved_students, 0),
            }
        )
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    return result.sort_values(["Observed Term Sort", "Segment"])


def build_scatter_frame(
    summary: pd.DataFrame,
    metric: MetricDefinition,
    group_field: str,
    min_n: int,
    population_label: str = ALL_STUDENTS_LABEL,
) -> pd.DataFrame:
    table = summarize_metric_by_group(summary, metric, group_field, min_n, population_label=population_label)
    if table.empty:
        return table
    table["Population Students"] = table["Students"]
    return table


def stakeholder_summary(ranked_table: pd.DataFrame, metric: MetricDefinition, population_label: str = ALL_STUDENTS_LABEL) -> list[str]:
    if ranked_table.empty:
        return ["No groups met the current sample-size threshold for this metric."]

    highest = ranked_table.iloc[0]
    lowest = ranked_table.iloc[-1]
    high_value = "n/a" if pd.isna(highest["Metric Value"]) else f"{float(highest['Metric Value']):.3f}"
    low_value = "n/a" if pd.isna(lowest["Metric Value"]) else f"{float(lowest['Metric Value']):.3f}"
    notes = [
        f"Highest {metric.display_name.lower()} ({population_label.lower()}): {highest['Group']} ({high_value}).",
        f"Lowest {metric.display_name.lower()} ({population_label.lower()}): {lowest['Group']} ({low_value}).",
    ]
    return notes


def _persistence_total_start_year(value: object) -> int | None:
    match = PERSISTENCE_TOTAL_RE.fullmatch(str(value).strip())
    return int(match.group(1)) if match else None


def _persistence_academic_year_start(term_label: object) -> int | None:
    parsed = parse_term_label(term_label)
    year = parsed["year"]
    season = str(parsed["season"]).lower()
    if year is None:
        return None
    if season == "fall":
        return int(year)
    if season == "spring":
        return int(year) - 1
    return None


def _persistence_academic_year_label(start_year: int) -> str:
    return f"Fall {int(start_year)} Total"


def persistence_cohort_sort_key(value: str) -> tuple[int, int, int, str]:
    total_year = _persistence_total_start_year(value)
    if total_year is not None:
        return (total_year, 2, total_year + 1, str(value).strip().lower())

    parsed = parse_term_label(value)
    year = parsed["year"]
    season = str(parsed["season"]).lower()
    if year is None:
        return (9999, 9, 9999, str(value).strip().lower())
    if season == "fall":
        return (int(year), 0, int(year), str(value).strip().lower())
    if season == "spring":
        return (int(year) - 1, 1, int(year), str(value).strip().lower())
    return (int(year), 3, int(year), str(value).strip().lower())


def persistence_checkpoint_sort_value(cohort_label: str, offset: int) -> int | None:
    total_year = _persistence_total_start_year(cohort_label)
    if total_year is not None:
        return int(parse_term_label(f"Spring {int(total_year) + int(offset) + 1}")["sort_value"])

    parsed = parse_term_label(cohort_label)
    year = parsed["year"]
    season = str(parsed["season"]).lower()
    season_codes = {"winter": "WI", "spring": "SP", "summer": "SU", "fall": "FA"}
    season_code = season_codes.get(season, "")
    if year is None or not season_code:
        return None
    return int(parse_term_label(f"{int(year) + int(offset)}{season_code}")["sort_value"])


def persistence_cohort_options(summary: pd.DataFrame) -> list[str]:
    if summary.empty or "join_term" not in summary.columns:
        return []
    values = (
        summary["join_term"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    sorted_terms = sorted(values, key=persistence_cohort_sort_key)
    academic_year_seasons: dict[int, set[str]] = {}
    for value in sorted_terms:
        parsed = parse_term_label(value)
        season = str(parsed["season"]).lower()
        start_year = _persistence_academic_year_start(value)
        if start_year is None or season not in {"fall", "spring"}:
            continue
        academic_year_seasons.setdefault(start_year, set()).add(season)

    options: list[str] = []
    emitted_totals: set[int] = set()
    for value in sorted_terms:
        options.append(value)
        parsed = parse_term_label(value)
        season = str(parsed["season"]).lower()
        start_year = _persistence_academic_year_start(value)
        if (
            season == "spring"
            and start_year is not None
            and {"fall", "spring"}.issubset(academic_year_seasons.get(start_year, set()))
            and start_year not in emitted_totals
        ):
            options.append(_persistence_academic_year_label(start_year))
            emitted_totals.add(start_year)
    return options


def _normalized_council_series(frame: pd.DataFrame) -> pd.Series:
    if "council" not in frame.columns:
        return pd.Series("Unknown", index=frame.index, dtype="object")
    return (
        frame["council"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
        .replace({"MCG": "MGC", "": "Unknown"})
    )


def _normalized_org_type_series(frame: pd.DataFrame) -> pd.Series:
    if "org_type" not in frame.columns:
        return pd.Series("Unknown", index=frame.index, dtype="object")
    lowered = frame["org_type"].fillna("").astype(str).str.strip().str.lower()
    result = pd.Series("Unknown", index=frame.index, dtype="object")
    result = result.where(~lowered.str.contains("fraternity", na=False), "FRA")
    result = result.where(~lowered.str.contains("sorority", na=False), "SOR")
    return result


def filter_persistence_population(summary: pd.DataFrame, cohort_term: str, distinction: str = "ALL") -> pd.DataFrame:
    if summary.empty or "join_term" not in summary.columns:
        return pd.DataFrame(columns=summary.columns)

    join_terms = summary["join_term"].fillna("").astype(str).str.strip()
    cohort_label = str(cohort_term).strip()
    total_start_year = _persistence_total_start_year(cohort_label)
    if total_start_year is not None:
        academic_year_start = join_terms.map(_persistence_academic_year_start)
        season_series = join_terms.map(lambda value: str(parse_term_label(value)["season"]).lower())
        frame = summary.loc[
            academic_year_start.eq(total_start_year)
            & season_series.isin({"fall", "spring"})
        ].copy()
    else:
        frame = summary.loc[join_terms.eq(cohort_label)].copy()
    if frame.empty:
        return frame

    distinction_clean = str(distinction or "ALL").strip().upper()
    council_series = _normalized_council_series(frame)
    org_type_series = _normalized_org_type_series(frame)
    frame["persistence_council_distinction"] = council_series
    frame["persistence_orgtype_distinction"] = org_type_series

    if distinction_clean == "ALL":
        return frame.reset_index(drop=True)
    if distinction_clean in {"FRA", "SOR"}:
        return frame.loc[org_type_series.eq(distinction_clean)].reset_index(drop=True)
    return frame.loc[council_series.eq(distinction_clean)].reset_index(drop=True)


def _milestone_label(base_label: str, offset: int) -> str:
    if offset == 0:
        return f"Cohort Year<br>{base_label}"
    if offset == 1:
        return f"1 Year<br>{base_label}"
    if offset in {4, 6}:
        return f"{offset} Year<br>{base_label}"
    return base_label.replace(" ", "<br>", 1)


def build_persistence_dashboard(
    summary: pd.DataFrame,
    longitudinal: pd.DataFrame,
    cohort_term: str,
    distinction: str = "ALL",
) -> dict[str, object]:
    cohort = filter_persistence_population(summary, cohort_term, distinction)
    empty = {
        "cohort": cohort,
        "chart_frame": pd.DataFrame(columns=["Milestone", "Milestone Sort", "Outcome", "Share", "Count", "Label"]),
        "table_frame": pd.DataFrame(columns=["Milestone", "Term", "Retained", "Retained Count", "Graduated", "Graduated Count", "Not Retained / Unresolved", "Not Retained / Unresolved Count"]),
        "meta": {
            "cohort_term": cohort_term,
            "distinction": distinction,
            "students": int(len(cohort)),
            "max_milestone": "",
            "note": "No students matched the current cohort and distinction.",
        },
    }
    if cohort.empty:
        return empty

    total_start_year = _persistence_total_start_year(cohort_term)
    cohort_term_parts = parse_term_label(cohort_term)
    base_year = cohort_term_parts["year"]
    base_season = str(cohort_term_parts["season"]).lower()
    season_codes = {"winter": "WI", "spring": "SP", "summer": "SU", "fall": "FA"}
    season_code = season_codes.get(base_season, "")
    is_total_cohort = total_start_year is not None
    if not is_total_cohort and (base_year is None or not season_code):
        empty["meta"]["note"] = "The selected cohort term could not be parsed into milestone checkpoints."
        return empty

    student_ids = (
        cohort["student_id"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .tolist()
    )
    if not student_ids:
        empty["meta"]["note"] = "The selected cohort does not contain usable student identifiers."
        return empty

    long_frame = longitudinal.copy()
    if long_frame.empty or "student_id" not in long_frame.columns:
        empty["meta"]["note"] = "Longitudinal academic data is not available for milestone calculations."
        return empty

    long_frame["student_id"] = long_frame["student_id"].fillna("").astype(str).str.strip()
    long_frame = long_frame.loc[long_frame["student_id"].isin(student_ids)].copy()
    if long_frame.empty:
        empty["meta"]["note"] = "No longitudinal rows matched the selected cohort."
        return empty

    if "observed_term_sort" not in long_frame.columns:
        if "observed_term" in long_frame.columns:
            long_frame["observed_term_sort"] = long_frame["observed_term"].map(lambda value: parse_term_label(value)["sort_value"])
        else:
            long_frame["observed_term_sort"] = 999999

    academic_mask = (
        long_frame.get("academic_present", pd.Series(False, index=long_frame.index))
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .isin({"yes", "true", "1"})
    )
    academic_rows = long_frame.loc[academic_mask].copy()
    if academic_rows.empty:
        empty["meta"]["note"] = "No academic-present longitudinal rows matched the selected cohort."
        return empty

    academic_presence_by_term = {
        int(term_sort): set(group["student_id"].tolist())
        for term_sort, group in academic_rows.groupby("observed_term_sort", dropna=False)
        if pd.notna(term_sort)
    }
    academic_rows["persistence_academic_year_start"] = academic_rows.get("observed_term", pd.Series("", index=academic_rows.index)).map(
        _persistence_academic_year_start
    )
    academic_presence_by_academic_year = {
        int(year_value): set(group["student_id"].tolist())
        for year_value, group in academic_rows.loc[academic_rows["persistence_academic_year_start"].notna()].groupby(
            "persistence_academic_year_start",
            dropna=False,
        )
        if pd.notna(year_value)
    }
    max_term_sort = int(pd.to_numeric(academic_rows["observed_term_sort"], errors="coerce").dropna().max()) if not academic_rows.empty else 0

    cohort_work = cohort.copy()
    cohort_work["student_id"] = cohort_work["student_id"].fillna("").astype(str).str.strip()
    cohort_work["graduation_sort"] = cohort_work.get("graduation_term", pd.Series("", index=cohort_work.index)).map(
        lambda value: parse_term_label(value)["sort_value"] if str(value).strip() else 999999
    )
    if "graduation_term_code" in cohort_work.columns:
        alt_sort = cohort_work["graduation_term_code"].map(lambda value: parse_term_label(value)["sort_value"] if str(value).strip() else 999999)
        cohort_work["graduation_sort"] = cohort_work["graduation_sort"].where(cohort_work["graduation_sort"].lt(999999), alt_sort)
    graduated_mask = cohort_work.get("is_graduated", pd.Series(False, index=cohort_work.index)).fillna(False).astype(bool)
    cohort_work["graduation_sort"] = cohort_work["graduation_sort"].where(graduated_mask, 999999)

    chart_rows: list[dict[str, object]] = []
    table_rows: list[dict[str, object]] = []
    student_count_total = int(cohort_work["student_id"].nunique())
    last_milestone_label = ""

    for offset in range(0, 7):
        if is_total_cohort:
            target_year = int(total_start_year) + offset
            target_label = _persistence_academic_year_label(target_year)
            target_sort = int(parse_term_label(f"Spring {target_year + 1}")["sort_value"])
            measurable = offset == 0 or (target_sort <= max_term_sort)
            display_label = target_label
        else:
            target_year = int(base_year) + offset
            target_code = f"{target_year}{season_code}"
            target_term = parse_term_label(target_code)
            target_sort = int(target_term["sort_value"])
            measurable = offset == 0 or (target_sort <= max_term_sort)
            display_label = str(target_term["label"])
        if not measurable:
            continue

        last_milestone_label = display_label
        milestone_label = _milestone_label(display_label, offset)
        if offset == 0:
            retained_count = student_count_total
            graduated_count = 0
            not_retained_count = 0
        else:
            graduated_students = set(
                cohort_work.loc[cohort_work["graduation_sort"].le(target_sort), "student_id"]
                .dropna()
                .astype(str)
                .str.strip()
                .tolist()
            )
            if is_total_cohort:
                retained_students = academic_presence_by_academic_year.get(target_year, set()) - graduated_students
            else:
                retained_students = academic_presence_by_term.get(target_sort, set()) - graduated_students
            graduated_count = len(graduated_students)
            retained_count = len(retained_students)
            not_retained_count = max(student_count_total - retained_count - graduated_count, 0)

        milestone_counts = {
            "Retained": retained_count,
            "Graduated": graduated_count,
            "Not Retained / Unresolved": not_retained_count,
        }
        table_rows.append(
            {
                "Milestone": f"{offset} Year" if offset else "Cohort Year",
                "Term": display_label,
                "Retained": (retained_count / student_count_total) if student_count_total else np.nan,
                "Retained Count": retained_count,
                "Graduated": (graduated_count / student_count_total) if student_count_total else np.nan,
                "Graduated Count": graduated_count,
                "Not Retained / Unresolved": (not_retained_count / student_count_total) if student_count_total else np.nan,
                "Not Retained / Unresolved Count": not_retained_count,
            }
        )
        for outcome, count in milestone_counts.items():
            share = (count / student_count_total) if student_count_total else np.nan
            label = ""
            if count > 0 and share >= 0.085:
                label = f"{outcome}<br>{share:.1%}<br>(n={count:,})"
            chart_rows.append(
                {
                    "Milestone": milestone_label,
                    "Milestone Sort": offset,
                    "Outcome": outcome,
                    "Share": share,
                    "Count": count,
                    "Label": label,
                }
            )

    chart_frame = pd.DataFrame(chart_rows)
    table_frame = pd.DataFrame(table_rows)
    return {
        "cohort": cohort_work.reset_index(drop=True),
        "chart_frame": chart_frame.sort_values(["Milestone Sort", "Outcome"]).reset_index(drop=True),
        "table_frame": table_frame.reset_index(drop=True),
        "meta": {
            "cohort_term": cohort_term,
            "distinction": distinction,
            "students": student_count_total,
            "max_milestone": last_milestone_label,
            "note": (
                "Retained counts show students observed academically in the checkpoint term. Graduated counts use explicit graduation evidence only. Students not observed in the checkpoint term remain in Not Retained / Unresolved."
                if not is_total_cohort
                else "Retained counts show students observed academically in either the fall or spring term of that academic year checkpoint. Graduated counts use explicit graduation evidence only through the end of the checkpoint spring term. Students not observed during that academic year remain in Not Retained / Unresolved."
            ),
        },
    }
