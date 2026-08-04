from __future__ import annotations

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
from app.status_framework import outcome_population_summary, resolved_outcomes_only_frame, student_count
from src.persistence_outcomes import CHAPTER_KICKED_OUTCOME, PERSISTENCE_OUTCOME_ORDER, checkpoint_outcome_counts


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
PERSISTENCE_ALL_TIME_LABEL = "All Time"
CHAPTER_HEALTH_OUTCOME_ORDER = [
    "Graduated",
    "Resolved Non-Graduate Exit",
    CHAPTER_KICKED_OUTCOME,
    "Still Active",
    "Roster Dissapeared/Unknown",
    "Other Unknown",
    "Other / Unmapped",
]
RISK_SEVERITY_ORDER = {"High": 0, "Medium": 1, "Monitor": 2}
ADVISOR_PRIORITY_ORDER = {"High": 0, "Medium": 1, "Monitor": 2}
ADVISOR_QUEUE_RENAME_MAP = {
    "priority": "Priority",
    "risk_score": "Risk Score",
    "student_name": "Student Name",
    "student_id": "Student ID",
    "current_chapter_label": "Current Chapter",
    "current_council_label": "Council",
    "join_term": "Join Term",
    "average_cumulative_gpa_num": "Average Cumulative GPA",
    "first_year_avg_term_gpa_num": "Average First-Year GPA",
    "data_completeness_rate_num": "Data Completeness Rate",
    "latest_outcome_bucket": "Latest Outcome",
    "risk_flags_text": "Risk Flags",
}
ADVISOR_ROLLUP_COLUMNS = [
    "Current Chapter",
    "Council",
    "Current Active Students",
    "Flagged Students",
    "High Priority",
    "Medium Priority",
    "Monitor",
    "Average Risk Score",
]
GRADUATION_DENOMINATOR_COLUMNS = [
    "Group",
    "Total Unique Students",
    "Explicit Graduates",
    "Resolved Outcomes",
    "Still Active",
    "Unknown / Unresolved",
    "Other / Unmapped",
    "Graduation Rate (Resolved Outcomes Only)",
    "Graduation Rate (Full Population)",
    "Unknown Share",
]
ROSTER_DISAPPEARANCE_STUDENT_COLUMNS = [
    "Student Name",
    "Student ID",
    "Chapter",
    "Council",
    "Join Term",
    "Last Observed Org Term",
    "Latest Outcome",
    "Outcome Resolution Group",
    "Outcome Evidence Source",
    "Data Completeness Rate",
]
RETENTION_DASHBOARD_COLUMNS = [
    "Group",
    "Students",
    "Organization Retention Denominator",
    "Retained In Organization Next Fall",
    "Organization Retention Rate",
    "Academic Continuation Denominator",
    "Academically Continued Next Fall",
    "Academic Continuation Rate",
    "Explicit Graduates",
    "Still Active",
    "Unknown / Unresolved",
]
GPA_TREND_COLUMNS = [
    "Observed Term",
    "Observed Term Sort",
    "Segment",
    "Roster Students",
    "Academic Students",
    "Students With Term GPA",
    "Term GPA Coverage",
    "Average Term GPA",
    "Average Cumulative GPA",
    "Average Passed Hours",
    "Average Cumulative Hours",
]


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


def _truthy_series(series: pd.Series | None, index: pd.Index) -> pd.Series:
    if series is None:
        return pd.Series(False, index=index, dtype="bool")
    lowered = series.fillna("").astype(str).str.strip().str.lower()
    return (lowered.eq("true") | lowered.eq("yes") | lowered.eq("1")).fillna(False)


def _text_series(series: pd.Series | None, index: pd.Index) -> pd.Series:
    if series is None:
        return pd.Series("", index=index, dtype="object")
    return series.fillna("").astype(str).str.strip()


def _frame_text_series(frame: pd.DataFrame, column: str, default: str = "") -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype="object")
    return _text_series(frame[column], frame.index)


def _lower_text_series(series: pd.Series | None, index: pd.Index) -> pd.Series:
    return _text_series(series, index).str.lower()


def _chapter_match_mask(frame: pd.DataFrame, column: str, chapter_key: str) -> pd.Series:
    return _lower_text_series(frame.get(column), frame.index).eq(chapter_key)


def _truthy_filter(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    return frame.loc[_truthy_series(frame.get(column), frame.index)].copy()


def _truthy_student_count(frame: pd.DataFrame, column: str) -> int:
    return student_count(_truthy_filter(frame, column))


def _selected_table(
    frame: pd.DataFrame,
    rename_map: dict[str, str],
    sort_by: list[str] | None = None,
) -> pd.DataFrame:
    present_columns = [column for column in rename_map if column in frame.columns]
    if not present_columns:
        return pd.DataFrame(columns=list(rename_map.values()))
    result = frame.loc[:, present_columns].rename(columns={column: rename_map[column] for column in present_columns})
    if sort_by:
        present_sort = [column for column in sort_by if column in result.columns]
        if present_sort:
            result = result.sort_values(present_sort, na_position="last")
    return result.reset_index(drop=True)


def _sort_join_term_table(frame: pd.DataFrame, join_term_column: str = "Join Term", name_column: str = "Student Name") -> pd.DataFrame:
    if frame.empty:
        return frame
    if join_term_column in frame.columns:
        frame = frame.copy()
        frame["_join_sort"] = frame[join_term_column].map(persistence_cohort_sort_key)
        sort_columns = ["_join_sort"] + ([name_column] if name_column in frame.columns else [])
        return frame.sort_values(sort_columns, na_position="last").drop(columns=["_join_sort"]).reset_index(drop=True)
    if name_column in frame.columns:
        return frame.sort_values([name_column], na_position="last").reset_index(drop=True)
    return frame.reset_index(drop=True)


def _first_non_blank(series: pd.Series | None) -> str:
    if series is None:
        return ""
    cleaned = series.fillna("").astype(str).str.strip()
    usable = cleaned.loc[cleaned.ne("")]
    return usable.iloc[0] if not usable.empty else ""


def _term_label_by_sort(values: pd.Series | None, *, latest: bool) -> str:
    if values is None:
        return ""
    cleaned = values.fillna("").astype(str).str.strip()
    cleaned = cleaned.loc[cleaned.ne("")]
    if cleaned.empty:
        return ""
    ranked = cleaned.to_frame(name="term")
    ranked["_sort"] = ranked["term"].map(lambda value: parse_term_label(value)["sort_value"])
    ranked = ranked.sort_values(["_sort", "term"], ascending=[True, True], na_position="last")
    return str(ranked.iloc[-1 if latest else 0]["term"])


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _share(numerator: float | int, denominator: float | int) -> float:
    denominator_value = float(denominator or 0)
    if denominator_value <= 0:
        return float("nan")
    return float(numerator) / denominator_value


def _unique_student_count(values: pd.Series) -> int:
    return int(
        values.fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .nunique()
    )


def _roster_disappeared_mask(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(False, index=frame.index, dtype="bool")
    masks: list[pd.Series] = []
    if "roster_disappeared_unknown_flag" in frame.columns:
        masks.append(_truthy_series(frame["roster_disappeared_unknown_flag"], frame.index))
    if "latest_outcome_bucket" in frame.columns:
        masks.append(
            _frame_text_series(frame, "latest_outcome_bucket").str.contains(
                r"roster\s+dis+ap+eared|roster\s+dis+appeared|roster\s+dissapeared|chapter\s+kicked",
                case=False,
                regex=True,
                na=False,
            )
        )
    if not masks:
        return pd.Series(False, index=frame.index, dtype="bool")
    combined = masks[0]
    for mask in masks[1:]:
        combined = combined | mask
    return combined.fillna(False)


def _chapter_label_for_tracker(frame: pd.DataFrame) -> pd.Series:
    label = _frame_text_series(frame, "initial_chapter")
    for column in ["latest_chapter", "chapter", "current_active_chapter"]:
        candidate = _frame_text_series(frame, column)
        label = label.where(label.ne(""), candidate)
    return label.replace("", "Unknown")


def _council_label_for_tracker(frame: pd.DataFrame) -> pd.Series:
    label = _frame_text_series(frame, "council")
    for column in ["current_active_council", "chapter_group"]:
        candidate = _frame_text_series(frame, column)
        label = label.where(label.ne(""), candidate)
    return label.replace("", "Unknown")


def build_graduation_denominator_comparison(summary: pd.DataFrame, group_field: str | None = None) -> pd.DataFrame:
    """Compare explicit-graduation rates under the two app-supported denominator definitions."""
    if summary.empty:
        return pd.DataFrame(columns=GRADUATION_DENOMINATOR_COLUMNS)

    def _row(label: object, frame: pd.DataFrame) -> dict[str, object]:
        population = outcome_population_summary(frame)
        total = int(population["all_students"])
        resolved = int(population["resolved_students"])
        graduates = int(population["graduated_students"])
        unknown = int(population["unknown_students"])
        return {
            "Group": _label_or_unknown(label),
            "Total Unique Students": total,
            "Explicit Graduates": graduates,
            "Resolved Outcomes": resolved,
            "Still Active": int(population["still_active_students"]),
            "Unknown / Unresolved": unknown,
            "Other / Unmapped": int(population["other_unmapped_students"]),
            "Graduation Rate (Resolved Outcomes Only)": (graduates / resolved) if resolved else np.nan,
            "Graduation Rate (Full Population)": (graduates / total) if total else np.nan,
            "Unknown Share": (unknown / total) if total else np.nan,
        }

    if not group_field or group_field == "Overall" or group_field not in summary.columns:
        return pd.DataFrame([_row("Overall", summary)], columns=GRADUATION_DENOMINATOR_COLUMNS)

    rows = [_row(group_value, group.copy()) for group_value, group in summary.groupby(group_field, dropna=False)]
    result = pd.DataFrame(rows, columns=GRADUATION_DENOMINATOR_COLUMNS)
    if result.empty:
        return result
    return result.sort_values(
        ["Total Unique Students", "Graduation Rate (Resolved Outcomes Only)", "Group"],
        ascending=[False, False, True],
        na_position="last",
    ).reset_index(drop=True)


def build_roster_disappearance_tracker(summary: pd.DataFrame) -> dict[str, object]:
    """Build app-facing tables for students left unknown because chapter roster coverage disappeared."""
    empty = {
        "meta": {
            "affected_students": 0,
            "affected_chapters": 0,
            "total_students": student_count(summary),
            "affected_share": 0.0,
        },
        "student_table": pd.DataFrame(columns=ROSTER_DISAPPEARANCE_STUDENT_COLUMNS),
        "chapter_rollup": pd.DataFrame(),
        "cohort_rollup": pd.DataFrame(),
        "last_observed_rollup": pd.DataFrame(),
    }
    if summary.empty:
        return empty

    disappeared = summary.loc[_roster_disappeared_mask(summary)].copy()
    if disappeared.empty:
        return empty

    disappeared["tracker_chapter"] = _chapter_label_for_tracker(disappeared)
    disappeared["tracker_council"] = _council_label_for_tracker(disappeared)
    disappeared["tracker_join_term"] = _frame_text_series(disappeared, "join_term").replace("", "Unknown")
    last_observed = _frame_text_series(disappeared, "last_observed_org_term")
    if last_observed.eq("").all():
        last_observed = _frame_text_series(disappeared, "last_observed_org_term_code")
    disappeared["tracker_last_observed_org_term"] = last_observed.replace("", "Unknown")
    disappeared["data_completeness_rate_num"] = _numeric_series(disappeared, "data_completeness_rate")
    if "current_active_flag" not in disappeared.columns:
        disappeared["current_active_flag"] = ""

    affected_students = student_count(disappeared)
    total_students = student_count(summary)
    affected_chapters = int(disappeared["tracker_chapter"].replace("Unknown", pd.NA).dropna().nunique())

    def _truthy_count(values: pd.Series) -> int:
        return int(_truthy_series(values, values.index).sum())

    chapter_rollup = (
        disappeared.groupby(["tracker_chapter", "tracker_council"], dropna=False)
        .agg(
            **{
                "Affected Students": ("student_id", _unique_student_count),
                "First Join Term": ("tracker_join_term", lambda values: _term_label_by_sort(values, latest=False)),
                "Latest Last Observed Org Term": ("tracker_last_observed_org_term", lambda values: _term_label_by_sort(values, latest=True)),
                "Current Active Conflicts": ("current_active_flag", _truthy_count),
                "Average Data Completeness": ("data_completeness_rate_num", "mean"),
            }
        )
        .reset_index()
        .rename(columns={"tracker_chapter": "Chapter", "tracker_council": "Council"})
        .sort_values(["Affected Students", "Chapter"], ascending=[False, True], na_position="last")
        .reset_index(drop=True)
    )

    cohort_rollup = (
        disappeared.groupby("tracker_join_term", dropna=False)
        .agg(**{"Affected Students": ("student_id", _unique_student_count)})
        .reset_index()
        .rename(columns={"tracker_join_term": "Join Term"})
    )
    if not cohort_rollup.empty:
        cohort_rollup["_sort"] = cohort_rollup["Join Term"].map(persistence_cohort_sort_key)
        cohort_rollup = cohort_rollup.sort_values(["_sort", "Join Term"]).drop(columns=["_sort"]).reset_index(drop=True)

    last_observed_rollup = (
        disappeared.groupby("tracker_last_observed_org_term", dropna=False)
        .agg(**{"Affected Students": ("student_id", _unique_student_count)})
        .reset_index()
        .rename(columns={"tracker_last_observed_org_term": "Last Observed Org Term"})
    )
    if not last_observed_rollup.empty:
        last_observed_rollup["_sort"] = last_observed_rollup["Last Observed Org Term"].map(lambda value: parse_term_label(value)["sort_value"])
        last_observed_rollup = last_observed_rollup.sort_values(["_sort", "Last Observed Org Term"]).drop(columns=["_sort"]).reset_index(drop=True)

    student_table = _selected_table(
        disappeared,
        {
            "student_name": "Student Name",
            "student_id": "Student ID",
            "tracker_chapter": "Chapter",
            "tracker_council": "Council",
            "tracker_join_term": "Join Term",
            "tracker_last_observed_org_term": "Last Observed Org Term",
            "latest_outcome_bucket": "Latest Outcome",
            "outcome_resolution_group": "Outcome Resolution Group",
            "outcome_evidence_source": "Outcome Evidence Source",
            "data_completeness_rate": "Data Completeness Rate",
        },
        sort_by=["Chapter", "Student Name", "Student ID"],
    )

    return {
        "meta": {
            "affected_students": affected_students,
            "affected_chapters": affected_chapters,
            "total_students": total_students,
            "affected_share": (affected_students / total_students) if total_students else 0.0,
        },
        "student_table": student_table,
        "chapter_rollup": chapter_rollup,
        "cohort_rollup": cohort_rollup,
        "last_observed_rollup": last_observed_rollup,
    }


def _sorted_risk_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["_severity_sort"] = frame["Severity"].map(RISK_SEVERITY_ORDER).fillna(99)
    return frame.sort_values(["_severity_sort", "Flag"]).drop(columns=["_severity_sort"]).reset_index(drop=True)


def _chapter_risk_flags(
    *,
    meta: dict[str, object],
    kpis: dict[str, object],
    yearly_trend: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    entry_total = int(kpis.get("students_entering_chapter", 0) or 0)
    resolved_total = int(kpis.get("resolved_students", 0) or 0)
    measurable_retention_total = int(kpis.get("measurable_next_fall_students", 0) or 0)
    first_year_gpa_students = int(kpis.get("first_year_gpa_students", 0) or 0)
    unknown_total = int(kpis.get("unknown_outcomes", 0) or 0)
    roster_disappeared_total = int(kpis.get("roster_disappeared_unknown", 0) or 0)
    chapter_kicked_total = int(kpis.get("chapter_kicked", 0) or 0)
    current_active_total = int(kpis.get("current_active_members", 0) or 0)

    unknown_share = _share(unknown_total, entry_total)
    roster_disappeared_share = _share(roster_disappeared_total, entry_total)
    resolved_grad_rate = kpis.get("resolved_graduation_rate")
    next_fall_rate = kpis.get("next_fall_retention_rate")
    average_first_year_gpa = kpis.get("average_first_year_gpa")
    average_data_completeness = kpis.get("average_data_completeness_rate")

    if not bool(meta.get("is_currently_active")):
        rows.append(
            {
                "Severity": "High",
                "Flag": "Chapter not currently active",
                "Details": "The latest roster does not show this chapter as active, so current student outcomes are more likely to need manual follow-up.",
            }
        )

    if entry_total >= 8 and pd.notna(unknown_share):
        if unknown_total >= 5 or unknown_share >= 0.25:
            rows.append(
                {
                    "Severity": "High",
                    "Flag": "High unresolved outcome share",
                    "Details": f"{unknown_total:,} of {entry_total:,} entry students ({unknown_share:.1%}) are still unknown or unresolved.",
                }
            )
        elif unknown_total >= 3 or unknown_share >= 0.15:
            rows.append(
                {
                    "Severity": "Medium",
                    "Flag": "Meaningful unresolved outcome share",
                    "Details": f"{unknown_total:,} of {entry_total:,} entry students ({unknown_share:.1%}) are still unknown or unresolved.",
                }
            )

    if entry_total >= 8 and pd.notna(roster_disappeared_share):
        if roster_disappeared_total >= 4 or roster_disappeared_share >= 0.15:
            rows.append(
                {
                    "Severity": "High",
                    "Flag": "Roster disappeared unknowns",
                    "Details": f"{roster_disappeared_total:,} students ({roster_disappeared_share:.1%} of entry students) remain unresolved after chapter roster coverage disappeared.",
                }
            )
        elif roster_disappeared_total >= 2 or roster_disappeared_share >= 0.08:
            rows.append(
                {
                    "Severity": "Medium",
                    "Flag": "Roster continuity concern",
                    "Details": f"{roster_disappeared_total:,} students are in the Roster Dissapeared/Unknown bucket for this chapter.",
                }
            )
    if chapter_kicked_total:
        rows.append(
            {
                "Severity": "Monitor",
                "Flag": "Chapter kicked outcome",
                "Details": f"{chapter_kicked_total:,} entry student(s) are classified as Chapter Kicked from roster coverage gaps.",
            }
        )

    if resolved_total >= 10 and pd.notna(resolved_grad_rate):
        if float(resolved_grad_rate) < 0.45:
            rows.append(
                {
                    "Severity": "High",
                    "Flag": "Low resolved graduation rate",
                    "Details": f"Resolved graduation rate is {float(resolved_grad_rate):.1%} across {resolved_total:,} resolved students.",
                }
            )
        elif float(resolved_grad_rate) < 0.60:
            rows.append(
                {
                    "Severity": "Medium",
                    "Flag": "Resolved graduation rate below target",
                    "Details": f"Resolved graduation rate is {float(resolved_grad_rate):.1%} across {resolved_total:,} resolved students.",
                }
            )

    if measurable_retention_total >= 10 and pd.notna(next_fall_rate):
        if float(next_fall_rate) < 0.55:
            rows.append(
                {
                    "Severity": "High",
                    "Flag": "Low next-fall retention",
                    "Details": f"Next-fall retention is {float(next_fall_rate):.1%} across {measurable_retention_total:,} measurable students.",
                }
            )
        elif float(next_fall_rate) < 0.70:
            rows.append(
                {
                    "Severity": "Medium",
                    "Flag": "Next-fall retention below target",
                    "Details": f"Next-fall retention is {float(next_fall_rate):.1%} across {measurable_retention_total:,} measurable students.",
                }
            )

    if first_year_gpa_students >= 8 and pd.notna(average_first_year_gpa):
        if float(average_first_year_gpa) < 2.5:
            rows.append(
                {
                    "Severity": "High",
                    "Flag": "Low first-year GPA",
                    "Details": f"Average first-year GPA is {float(average_first_year_gpa):.2f} across {first_year_gpa_students:,} students.",
                }
            )
        elif float(average_first_year_gpa) < 2.8:
            rows.append(
                {
                    "Severity": "Medium",
                    "Flag": "First-year GPA below target",
                    "Details": f"Average first-year GPA is {float(average_first_year_gpa):.2f} across {first_year_gpa_students:,} students.",
                }
            )

    if entry_total >= 10 and pd.notna(average_data_completeness):
        if float(average_data_completeness) < 0.60:
            rows.append(
                {
                    "Severity": "High",
                    "Flag": "Low data completeness",
                    "Details": f"Average data completeness is {float(average_data_completeness):.0%}, which makes the chapter history harder to interpret confidently.",
                }
            )
        elif float(average_data_completeness) < 0.80:
            rows.append(
                {
                    "Severity": "Monitor",
                    "Flag": "Moderate data completeness gap",
                    "Details": f"Average data completeness is {float(average_data_completeness):.0%}.",
                }
            )

    if not yearly_trend.empty and {"Year", "Distinct Students"}.issubset(yearly_trend.columns):
        trend_base = yearly_trend.loc[pd.to_numeric(yearly_trend["Distinct Students"], errors="coerce").notna()].copy()
        if len(trend_base) >= 2:
            trend_base["Distinct Students"] = pd.to_numeric(trend_base["Distinct Students"], errors="coerce")
            trend_base = trend_base.sort_values("Year")
            latest_students = float(trend_base.iloc[-1]["Distinct Students"])
            prior_peak = float(trend_base.iloc[:-1]["Distinct Students"].max())
            if prior_peak >= 15 and latest_students <= (prior_peak * 0.65):
                rows.append(
                    {
                        "Severity": "Monitor",
                        "Flag": "Observed headcount is well below prior peak",
                        "Details": f"Latest observed distinct-student count is {int(latest_students):,} versus a prior peak of {int(prior_peak):,}.",
                    }
                )

    return _sorted_risk_frame(rows)


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


def chapter_health_options(summary: pd.DataFrame, longitudinal: pd.DataFrame) -> list[str]:
    values: set[str] = set()
    for frame, columns in [
        (summary, ["chapter", "initial_chapter", "latest_chapter", "current_active_chapter"]),
        (longitudinal, ["chapter"]),
    ]:
        if frame.empty:
            continue
        for column in columns:
            if column not in frame.columns:
                continue
            cleaned = _frame_text_series(frame, column).replace("", pd.NA).dropna()
            values.update(value for value in cleaned.tolist() if str(value).strip().lower() != "unknown")
    return sorted(values, key=lambda value: value.lower())


def _is_persistence_all_time(value: object) -> bool:
    return str(value or "").strip().casefold() == PERSISTENCE_ALL_TIME_LABEL.casefold()


def persistence_cohort_sort_key(value: str) -> tuple[int, int, int, str]:
    if _is_persistence_all_time(value):
        return (-9999, -1, -9999, PERSISTENCE_ALL_TIME_LABEL.casefold())

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
    if _is_persistence_all_time(cohort_label):
        return None

    parsed = parse_term_label(cohort_label)
    year = parsed["year"]
    season = str(parsed["season"]).lower()
    season_codes = {"winter": "WI", "spring": "SP", "summer": "SU", "fall": "FA"}
    season_code = season_codes.get(season, "")
    if year is None or not season_code:
        return None
    return int(parse_term_label(f"{int(year) + int(offset)}{season_code}")["sort_value"])


def _personal_persistence_checkpoint_sort(row: pd.Series, offset: int) -> int | None:
    term_value = ""
    for column in ["join_term_code", "join_term"]:
        value = row.get(column, "")
        if str(value).strip():
            term_value = value
            break
    parsed = parse_term_label(term_value)
    year = parsed["year"]
    season = str(parsed["season"]).lower()
    season_codes = {"winter": "WI", "spring": "SP", "summer": "SU", "fall": "FA"}
    season_code = season_codes.get(season, "")
    if year is None or not season_code:
        return None
    return int(parse_term_label(f"{int(year) + int(offset)}{season_code}")["sort_value"])


def _persistence_cohort_series(summary: pd.DataFrame) -> pd.Series:
    if summary.empty:
        return pd.Series(dtype="object")
    if "join_term" in summary.columns:
        return _frame_text_series(summary, "join_term")
    return pd.Series("", index=summary.index, dtype="object")


def persistence_cohort_options(summary: pd.DataFrame) -> list[str]:
    if summary.empty:
        return []
    values = (
        _persistence_cohort_series(summary)
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    sorted_terms = sorted(values, key=persistence_cohort_sort_key)
    return [PERSISTENCE_ALL_TIME_LABEL, *sorted_terms] if sorted_terms else []


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
    if summary.empty:
        return pd.DataFrame(columns=summary.columns)

    cohort_terms = _persistence_cohort_series(summary)
    cohort_label = str(cohort_term).strip()
    if _is_persistence_all_time(cohort_label):
        frame = summary.loc[cohort_terms.fillna("").astype(str).str.strip().ne("")].copy()
    else:
        frame = summary.loc[cohort_terms.eq(cohort_label)].copy()
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
    if _is_persistence_all_time(base_label):
        milestone = "Cohort Year" if offset == 0 else "1 Year" if offset == 1 else f"{offset} Year"
        return f"{milestone}<br>{base_label}"
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
    chapter_status_events: pd.DataFrame | None = None,
) -> dict[str, object]:
    cohort = filter_persistence_population(summary, cohort_term, distinction)
    empty = {
        "cohort": cohort,
        "chart_frame": pd.DataFrame(columns=["Milestone", "Milestone Sort", "Outcome", "Share", "Count", "Label"]),
        "table_frame": pd.DataFrame(
            columns=[
                "Milestone",
                "Term",
                "Measured Students",
                *[
                    column
                    for outcome in PERSISTENCE_OUTCOME_ORDER
                    for column in (outcome, f"{outcome} Count")
                ],
            ]
        ),
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

    is_all_time_cohort = _is_persistence_all_time(cohort_term)
    cohort_term_parts = parse_term_label(cohort_term)
    base_year = cohort_term_parts["year"]
    base_season = str(cohort_term_parts["season"]).lower()
    season_codes = {"winter": "WI", "spring": "SP", "summer": "SU", "fall": "FA"}
    season_code = season_codes.get(base_season, "")
    if not is_all_time_cohort and (base_year is None or not season_code):
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
    coverage_frame = long_frame.copy()
    long_frame = long_frame.loc[long_frame["student_id"].isin(student_ids)].copy()
    if long_frame.empty:
        empty["meta"]["note"] = "No longitudinal rows matched the selected cohort."
        return empty

    for frame in [coverage_frame, long_frame]:
        if "observed_term_sort" not in frame.columns:
            if "observed_term" in frame.columns:
                frame["observed_term_sort"] = frame["observed_term"].map(lambda value: parse_term_label(value)["sort_value"])
            else:
                frame["observed_term_sort"] = 999999

    presence_mask = _truthy_series(coverage_frame.get("roster_present"), coverage_frame.index)
    presence_rows = coverage_frame.loc[presence_mask].copy()
    if presence_rows.empty:
        empty["meta"]["note"] = "No roster-present longitudinal rows were available for milestone calculations."
        return empty

    max_term_sort = int(pd.to_numeric(presence_rows["observed_term_sort"], errors="coerce").dropna().max()) if not presence_rows.empty else 0

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
    cohort_work["_graduation_sort"] = cohort_work["graduation_sort"]
    manual_status = cohort_work.get("manual_outcome_status", pd.Series("", index=cohort_work.index)).fillna("").astype(str).str.strip()
    manual_term = cohort_work.get("manual_outcome_term", pd.Series("", index=cohort_work.index)).fillna("").astype(str).str.strip()
    fallback_manual_term = cohort_work.get(
        "last_observed_org_term_code",
        cohort_work.get("last_observed_org_term", pd.Series("", index=cohort_work.index)),
    ).fillna("").astype(str).str.strip()
    effective_manual_term = manual_term.where(manual_term.ne(""), fallback_manual_term)
    cohort_work["_manual_outcome_sort"] = [
        parse_term_label(term_value)["sort_value"] if status_value and term_value else 999999
        for status_value, term_value in zip(manual_status, effective_manual_term)
    ]

    chart_rows: list[dict[str, object]] = []
    table_rows: list[dict[str, object]] = []
    student_count_total = int(cohort_work["student_id"].nunique())
    last_milestone_label = ""
    all_time_checkpoint_sorts_by_offset: dict[int, pd.Series] = {}
    all_time_fixed_measured_mask = pd.Series(False, index=cohort_work.index)
    all_time_max_offset = 0

    if is_all_time_cohort:
        for offset in range(0, 7):
            checkpoint_sorts = pd.to_numeric(
                cohort_work.apply(lambda row: _personal_persistence_checkpoint_sort(row, offset), axis=1),
                errors="coerce",
            )
            if max_term_sort:
                checkpoint_sorts = checkpoint_sorts.where(checkpoint_sorts.le(max_term_sort), max_term_sort)
            if checkpoint_sorts.notna().any():
                all_time_checkpoint_sorts_by_offset[offset] = checkpoint_sorts
                all_time_max_offset = offset
        if all_time_checkpoint_sorts_by_offset:
            all_time_fixed_measured_mask = all_time_checkpoint_sorts_by_offset[0].notna()

    loop_offsets = range(0, all_time_max_offset + 1) if is_all_time_cohort else range(0, 7)
    for offset in loop_offsets:
        if is_all_time_cohort:
            checkpoint_sorts = all_time_checkpoint_sorts_by_offset.get(offset, pd.Series(pd.NA, index=cohort_work.index))
            measurable_mask = all_time_fixed_measured_mask
            measurable_cohort = cohort_work.loc[measurable_mask].copy()
            if measurable_cohort.empty:
                continue

            display_label = PERSISTENCE_ALL_TIME_LABEL
            milestone_counts = {status: 0 for status in PERSISTENCE_OUTCOME_ORDER}
            measurable_cohort["_personal_checkpoint_sort"] = checkpoint_sorts.loc[measurable_mask].astype(int)
            for target_sort, checkpoint_group in measurable_cohort.groupby("_personal_checkpoint_sort", dropna=False):
                group_counts = checkpoint_outcome_counts(
                    checkpoint_group.drop(columns=["_personal_checkpoint_sort"]),
                    coverage_frame,
                    int(target_sort),
                    max_term_sort,
                    presence_start_sort=int(target_sort),
                    baseline=offset == 0,
                    chapter_status_events=chapter_status_events,
                )
                for outcome, count in group_counts.items():
                    milestone_counts[outcome] = int(milestone_counts.get(outcome, 0)) + int(count)
            milestone_student_count = int(measurable_cohort["student_id"].nunique())
        else:
            target_year = int(base_year) + offset
            target_code = f"{target_year}{season_code}"
            target_term = parse_term_label(target_code)
            target_sort = int(target_term["sort_value"])
            target_retention_sort = target_sort
            measurable = offset == 0 or (target_sort <= max_term_sort)
            display_label = str(target_term["label"])
            if not measurable:
                continue
            milestone_counts = checkpoint_outcome_counts(
                cohort_work,
                coverage_frame,
                target_sort,
                max_term_sort,
                presence_start_sort=target_retention_sort,
                baseline=offset == 0,
                chapter_status_events=chapter_status_events,
            )
            milestone_student_count = student_count_total

        milestone_name = f"{offset} Year" if offset else "Cohort Year"
        last_milestone_label = milestone_name if is_all_time_cohort else display_label
        milestone_label = _milestone_label(display_label, offset)
        table_row: dict[str, object] = {
            "Milestone": milestone_name,
            "Term": display_label,
            "Measured Students": milestone_student_count,
        }
        for outcome in PERSISTENCE_OUTCOME_ORDER:
            count = int(milestone_counts.get(outcome, 0))
            table_row[outcome] = (count / milestone_student_count) if milestone_student_count else np.nan
            table_row[f"{outcome} Count"] = count
        table_rows.append(table_row)
        for outcome, count in milestone_counts.items():
            share = (count / milestone_student_count) if milestone_student_count else np.nan
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
                    "Denominator": milestone_student_count,
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
                "All Time uses each member's organization join term as their own clock with one fixed denominator. Future personal checkpoints after the latest loaded roster are evaluated as of the latest loaded roster term."
                if is_all_time_cohort
                else "Checkpoint outcomes use roster status and later roster presence first. Explicit graduation evidence is required for Graduated, and dated manual corrections are applied last. Checkpoints after the latest roster are not measured."
            ),
        },
    }


def build_chapter_health_dashboard(
    summary: pd.DataFrame,
    longitudinal: pd.DataFrame,
    chapter_name: str,
) -> dict[str, object]:
    chapter_label = str(chapter_name).strip()
    empty = {
        "meta": {
            "chapter": chapter_label,
            "chapter_group": "",
            "council": "",
            "org_type": "",
            "family": "",
            "is_currently_active": False,
            "latest_current_roster_term": "",
            "last_observed_term": "",
            "notes": "No students or longitudinal records matched the selected chapter.",
        },
        "kpis": {},
        "yearly_trend": pd.DataFrame(),
        "yearly_gpa_trend": pd.DataFrame(),
        "outcome_breakdown": pd.DataFrame(),
        "cohort_table": pd.DataFrame(),
        "risk_flags": pd.DataFrame(),
        "current_active_students": pd.DataFrame(),
        "review_students": pd.DataFrame(),
        "entry_students": pd.DataFrame(),
        "chapter_rows": pd.DataFrame(),
    }
    if not chapter_label:
        return empty

    chapter_key = chapter_label.lower()
    summary_work = summary.copy()
    longitudinal_work = longitudinal.copy()

    entry_students = summary_work.loc[_chapter_match_mask(summary_work, "initial_chapter", chapter_key)].copy()

    related_masks = []
    for column in ["chapter", "initial_chapter", "latest_chapter", "current_active_chapter"]:
        if column in summary_work.columns:
            related_masks.append(_chapter_match_mask(summary_work, column, chapter_key))
    chapter_summary = summary_work.loc[pd.concat(related_masks, axis=1).any(axis=1)].copy() if related_masks else pd.DataFrame(columns=summary_work.columns)

    chapter_rows = longitudinal_work.loc[_chapter_match_mask(longitudinal_work, "chapter", chapter_key)].copy()

    if entry_students.empty and chapter_summary.empty and chapter_rows.empty:
        return empty

    current_active = chapter_summary.loc[
        _chapter_match_mask(chapter_summary, "current_active_chapter", chapter_key)
        & _truthy_series(chapter_summary.get("current_active_flag"), chapter_summary.index)
    ].copy()

    related_for_meta = current_active if not current_active.empty else chapter_summary
    chapter_group = _first_non_blank(related_for_meta.get("chapter_group"))
    council = _first_non_blank(related_for_meta.get("council"))
    org_type = _first_non_blank(related_for_meta.get("org_type"))
    family = _first_non_blank(related_for_meta.get("family"))
    latest_current_roster_term = _first_non_blank(current_active.get("current_active_roster_term"))
    last_observed_term = ""
    if not chapter_rows.empty:
        ordered_rows = chapter_rows.sort_values("observed_term_sort", na_position="last")
        last_observed_term = _first_non_blank(pd.Series([ordered_rows.iloc[-1].get("observed_term", "")]))

    entry_total = student_count(entry_students)
    current_active_total = student_count(current_active)
    ever_observed_total = student_count(chapter_rows if not chapter_rows.empty else chapter_summary)
    resolved_entry = resolved_outcomes_only_frame(entry_students)
    resolved_total = student_count(resolved_entry)
    graduated_total = _truthy_student_count(entry_students, "is_graduated")
    active_outcome_total = _truthy_student_count(entry_students, "is_active_outcome")
    unknown_total = _truthy_student_count(entry_students, "is_unknown_outcome")
    chapter_kicked_total = student_count(
        entry_students.loc[_frame_text_series(entry_students, "latest_outcome_bucket").eq(CHAPTER_KICKED_OUTCOME)]
    )
    roster_disappeared_total = max(
        _truthy_student_count(entry_students, "roster_disappeared_unknown_flag") - chapter_kicked_total,
        0,
    )
    resolved_non_grad_total = _truthy_student_count(entry_students, "is_known_non_graduate_exit")

    measurable_next_fall = _truthy_filter(entry_students, "retained_next_fall_measurable")
    measurable_next_fall_n = student_count(measurable_next_fall)
    retained_next_fall_n = _truthy_student_count(measurable_next_fall, "retained_next_fall")
    resolved_grad_rate = (graduated_total / resolved_total) if resolved_total else np.nan
    full_grad_rate = (graduated_total / entry_total) if entry_total else np.nan
    next_fall_rate = (retained_next_fall_n / measurable_next_fall_n) if measurable_next_fall_n else np.nan
    first_year_gpa_series = _numeric_series(entry_students, "first_year_avg_term_gpa")
    cumulative_gpa_series = _numeric_series(entry_students, "average_cumulative_gpa")
    completeness_series = _numeric_series(entry_students, "data_completeness_rate")

    kpis = {
        "current_active_members": current_active_total,
        "students_ever_observed": ever_observed_total,
        "students_entering_chapter": entry_total,
        "resolved_graduation_rate": resolved_grad_rate,
        "full_population_graduation_rate": full_grad_rate,
        "next_fall_retention_rate": next_fall_rate,
        "average_first_year_gpa": first_year_gpa_series.mean(),
        "average_cumulative_gpa": cumulative_gpa_series.mean(),
        "first_year_gpa_students": int(first_year_gpa_series.notna().sum()),
        "cumulative_gpa_students": int(cumulative_gpa_series.notna().sum()),
        "average_data_completeness_rate": completeness_series.mean(),
        "measurable_next_fall_students": measurable_next_fall_n,
        "resolved_students": resolved_total,
        "still_active_outcomes": active_outcome_total,
        "unknown_outcomes": unknown_total,
        "roster_disappeared_unknown": roster_disappeared_total,
        "chapter_kicked": chapter_kicked_total,
        "resolved_non_graduate_exits": resolved_non_grad_total,
    }

    yearly_trend = pd.DataFrame()
    yearly_gpa_trend = pd.DataFrame()
    if not chapter_rows.empty and "observed_year" in chapter_rows.columns:
        yearly_base = chapter_rows.loc[pd.to_numeric(chapter_rows["observed_year"], errors="coerce").notna()].copy()
        if not yearly_base.empty:
            yearly_base["observed_year"] = pd.to_numeric(yearly_base["observed_year"], errors="coerce").astype(int)
            yearly_trend = (
                yearly_base.groupby("observed_year", dropna=False)
                .agg(
                    distinct_students=("student_id", lambda values: values.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
                    roster_rows=("roster_present", lambda values: int(_truthy_series(values, values.index).sum())),
                    academic_rows=("academic_present", lambda values: int(_truthy_series(values, values.index).sum())),
                )
                .reset_index()
                .rename(
                    columns={
                        "observed_year": "Year",
                        "distinct_students": "Distinct Students",
                        "roster_rows": "Roster Rows",
                        "academic_rows": "Academic Rows",
                    }
                )
                .sort_values("Year")
            )
            gpa_frame = (
                yearly_base.groupby("observed_year", dropna=False)
                .agg(
                    average_term_gpa=("term_gpa", lambda values: pd.to_numeric(values, errors="coerce").mean()),
                    average_cumulative_gpa=("cumulative_gpa", lambda values: pd.to_numeric(values, errors="coerce").mean()),
                )
                .reset_index()
                .rename(columns={"observed_year": "Year"})
                .sort_values("Year")
            )
            yearly_gpa_trend = gpa_frame.melt(
                id_vars=["Year"],
                value_vars=["average_term_gpa", "average_cumulative_gpa"],
                var_name="Metric",
                value_name="Value",
            )
            yearly_gpa_trend["Metric"] = yearly_gpa_trend["Metric"].replace(
                {
                    "average_term_gpa": "Average Term GPA",
                    "average_cumulative_gpa": "Average Cumulative GPA",
                }
            )

    resolved_non_grad_without_kicked = max(resolved_non_grad_total - chapter_kicked_total, 0)
    other_unknown_total = max(unknown_total - roster_disappeared_total, 0)
    other_unmapped_total = max(
        entry_total
        - graduated_total
        - resolved_non_grad_without_kicked
        - chapter_kicked_total
        - active_outcome_total
        - roster_disappeared_total
        - other_unknown_total,
        0,
    )
    outcome_breakdown = pd.DataFrame(
        [
            {"Outcome": "Graduated", "Students": graduated_total},
            {"Outcome": "Resolved Non-Graduate Exit", "Students": resolved_non_grad_without_kicked},
            {"Outcome": CHAPTER_KICKED_OUTCOME, "Students": chapter_kicked_total},
            {"Outcome": "Still Active", "Students": active_outcome_total},
            {"Outcome": "Roster Dissapeared/Unknown", "Students": roster_disappeared_total},
            {"Outcome": "Other Unknown", "Students": other_unknown_total},
            {"Outcome": "Other / Unmapped", "Students": other_unmapped_total},
        ]
    )
    outcome_breakdown = outcome_breakdown.loc[outcome_breakdown["Students"].gt(0)].copy()
    if not outcome_breakdown.empty:
        outcome_breakdown["Share"] = outcome_breakdown["Students"] / entry_total if entry_total else np.nan
        outcome_breakdown["Order"] = outcome_breakdown["Outcome"].map(
            {label: idx for idx, label in enumerate(CHAPTER_HEALTH_OUTCOME_ORDER)}
        )
        outcome_breakdown = outcome_breakdown.sort_values(["Order", "Outcome"]).drop(columns=["Order"]).reset_index(drop=True)

    cohort_rows: list[dict[str, object]] = []
    if not entry_students.empty and "join_term" in entry_students.columns:
        for join_term, frame in entry_students.groupby("join_term", dropna=False):
            frame = frame.copy()
            cohort_students = student_count(frame)
            cohort_resolved = resolved_outcomes_only_frame(frame)
            cohort_resolved_n = student_count(cohort_resolved)
            cohort_graduated_n = _truthy_student_count(frame, "is_graduated")
            cohort_active_n = _truthy_student_count(frame, "is_active_outcome")
            cohort_unknown_n = _truthy_student_count(frame, "is_unknown_outcome")
            cohort_chapter_kicked_n = student_count(
                frame.loc[_frame_text_series(frame, "latest_outcome_bucket").eq(CHAPTER_KICKED_OUTCOME)]
            )
            cohort_roster_disappeared_n = max(
                _truthy_student_count(frame, "roster_disappeared_unknown_flag") - cohort_chapter_kicked_n,
                0,
            )
            cohort_measurable = _truthy_filter(frame, "retained_next_fall_measurable")
            cohort_measurable_n = student_count(cohort_measurable)
            cohort_retained_n = _truthy_student_count(cohort_measurable, "retained_next_fall")
            cohort_first_year_gpa = _numeric_series(frame, "first_year_avg_term_gpa")
            cohort_cumulative_gpa = _numeric_series(frame, "average_cumulative_gpa")
            cohort_rows.append(
                {
                    "Cohort": _label_or_unknown(join_term),
                    "Students": cohort_students,
                    "Resolved Students": cohort_resolved_n,
                    "Graduated Students": cohort_graduated_n,
                    "Still Active": cohort_active_n,
                    "Unknown": cohort_unknown_n,
                    CHAPTER_KICKED_OUTCOME: cohort_chapter_kicked_n,
                    "Roster Dissapeared/Unknown": cohort_roster_disappeared_n,
                    "Resolved Graduation Rate": (cohort_graduated_n / cohort_resolved_n) if cohort_resolved_n else np.nan,
                    "Full Population Graduation Rate": (cohort_graduated_n / cohort_students) if cohort_students else np.nan,
                    "Next Fall Retention": (cohort_retained_n / cohort_measurable_n) if cohort_measurable_n else np.nan,
                    "Average First-Year GPA": cohort_first_year_gpa.mean(),
                    "Average Cumulative GPA": cohort_cumulative_gpa.mean(),
                }
            )
    cohort_table = pd.DataFrame(cohort_rows)
    if not cohort_table.empty:
        cohort_table["_cohort_sort"] = cohort_table["Cohort"].map(persistence_cohort_sort_key)
        cohort_table = cohort_table.sort_values(["_cohort_sort", "Cohort"]).drop(columns=["_cohort_sort"]).reset_index(drop=True)

    current_active_students = pd.DataFrame()
    if not current_active.empty:
        current_active_students = _selected_table(
            current_active,
            {
                "student_name": "Student Name",
                "student_id": "Student ID",
                "join_term": "Join Term",
                "current_active_roster_term": "Current Active Roster Term",
                "latest_outcome_bucket": "Latest Outcome",
                "data_completeness_rate": "Data Completeness Rate",
            },
            sort_by=["Student Name", "Student ID"],
        )

    review_students = pd.DataFrame()
    if not entry_students.empty:
        chapter_kicked_mask = _frame_text_series(entry_students, "latest_outcome_bucket").eq(CHAPTER_KICKED_OUTCOME)
        review_mask = (
            _truthy_series(entry_students.get("is_unknown_outcome"), entry_students.index)
            | (_truthy_series(entry_students.get("roster_disappeared_unknown_flag"), entry_students.index) & ~chapter_kicked_mask)
        )
        review_students = _selected_table(
            entry_students.loc[review_mask].copy(),
            {
                "student_name": "Student Name",
                "student_id": "Student ID",
                "join_term": "Join Term",
                "latest_outcome_bucket": "Latest Outcome",
                "outcome_resolution_group": "Outcome Resolution Group",
                "outcome_evidence_source": "Outcome Evidence Source",
                "data_completeness_rate": "Data Completeness Rate",
            },
        )
        review_students = _sort_join_term_table(review_students)

    note_parts = []
    if current_active_total == 0:
        note_parts.append("This chapter is not currently active on the latest roster.")
    if chapter_kicked_total:
        note_parts.append(f"{chapter_kicked_total:,} entry student(s) are classified as Chapter Kicked.")
    if roster_disappeared_total:
        note_parts.append(f"{roster_disappeared_total:,} entry student(s) are currently classified as Roster Dissapeared/Unknown.")
    if not note_parts:
        note_parts.append("This chapter has current and historical health metrics available from the canonical bundle.")

    meta = {
        "chapter": chapter_label,
        "chapter_group": chapter_group,
        "council": council,
        "org_type": org_type,
        "family": family,
        "is_currently_active": current_active_total > 0,
        "latest_current_roster_term": latest_current_roster_term,
        "last_observed_term": last_observed_term,
        "notes": " ".join(note_parts),
    }
    risk_flags = _chapter_risk_flags(meta=meta, kpis=kpis, yearly_trend=yearly_trend)

    return {
        "meta": meta,
        "kpis": kpis,
        "yearly_trend": yearly_trend,
        "yearly_gpa_trend": yearly_gpa_trend,
        "outcome_breakdown": outcome_breakdown,
        "cohort_table": cohort_table,
        "risk_flags": risk_flags,
        "current_active_students": current_active_students,
        "review_students": review_students,
        "entry_students": entry_students,
        "chapter_rows": chapter_rows,
    }


def build_retention_dashboard(summary: pd.DataFrame, group_field: str | None = None, min_denominator: int = 1) -> pd.DataFrame:
    """Build next-fall organization retention and academic continuation rates with explicit denominators."""
    if summary.empty:
        return pd.DataFrame(columns=RETENTION_DASHBOARD_COLUMNS)

    if group_field and group_field in summary.columns:
        groups = [(_label_or_unknown(group_value), group.copy()) for group_value, group in summary.groupby(group_field, dropna=False)]
    else:
        groups = [("Overall", summary.copy())]

    rows: list[dict[str, object]] = []
    for label, frame in groups:
        org_measurable = _truthy_filter(frame, "retained_next_fall_measurable")
        org_denominator = student_count(org_measurable)
        academic_measurable = _truthy_filter(frame, "continued_next_fall_measurable")
        academic_denominator = student_count(academic_measurable)
        if max(org_denominator, academic_denominator) < int(min_denominator):
            continue
        org_retained = _truthy_student_count(org_measurable, "retained_next_fall")
        academic_continued = _truthy_student_count(academic_measurable, "continued_next_fall")
        rows.append(
            {
                "Group": label,
                "Students": student_count(frame),
                "Organization Retention Denominator": org_denominator,
                "Retained In Organization Next Fall": org_retained,
                "Organization Retention Rate": (org_retained / org_denominator) if org_denominator else np.nan,
                "Academic Continuation Denominator": academic_denominator,
                "Academically Continued Next Fall": academic_continued,
                "Academic Continuation Rate": (academic_continued / academic_denominator) if academic_denominator else np.nan,
                "Explicit Graduates": _truthy_student_count(frame, "is_graduated"),
                "Still Active": _truthy_student_count(frame, "is_active_outcome"),
                "Unknown / Unresolved": _truthy_student_count(frame, "is_unknown_outcome"),
            }
        )

    result = pd.DataFrame(rows, columns=RETENTION_DASHBOARD_COLUMNS)
    if result.empty:
        return result
    return result.sort_values(
        ["Organization Retention Denominator", "Organization Retention Rate", "Group"],
        ascending=[False, False, True],
        na_position="last",
    ).reset_index(drop=True)


def build_gpa_trend_with_coverage(longitudinal: pd.DataFrame, segment_field: str | None = None) -> pd.DataFrame:
    """Build term GPA trend rows with explicit roster-to-GPA coverage."""
    if longitudinal.empty or "observed_term" not in longitudinal.columns:
        return pd.DataFrame(columns=GPA_TREND_COLUMNS)

    frame = longitudinal.copy()
    if "observed_term_sort" not in frame.columns:
        frame["observed_term_sort"] = frame["observed_term"].map(lambda value: parse_term_label(value)["sort_value"])
    if segment_field and segment_field not in frame.columns:
        segment_field = None
    group_fields = ["observed_term", "observed_term_sort"] + ([segment_field] if segment_field else [])

    rows: list[dict[str, object]] = []
    for group_value, group in frame.groupby(group_fields, dropna=False):
        if not isinstance(group_value, tuple):
            group_value = (group_value,)
        observed_term = group_value[0]
        observed_sort = group_value[1] if len(group_value) > 1 else 999999
        segment = _label_or_unknown(group_value[2]) if len(group_value) > 2 else "All Students"

        term_gpa = pd.to_numeric(group.get("term_gpa", pd.Series(np.nan, index=group.index)), errors="coerce")
        cumulative_gpa = pd.to_numeric(group.get("cumulative_gpa", pd.Series(np.nan, index=group.index)), errors="coerce")
        metric_group = group.assign(_has_term_gpa=term_gpa.notna(), _term_gpa_num=term_gpa, _cumulative_gpa_num=cumulative_gpa)
        if "student_id" in metric_group.columns:
            metric_group = (
                metric_group.sort_values(["_has_term_gpa", "_term_gpa_num"], ascending=[False, False], na_position="last")
                .drop_duplicates(subset=["student_id"], keep="first")
                .copy()
            )

        roster_mask = _truthy_series(metric_group.get("roster_present"), metric_group.index) if "roster_present" in metric_group.columns else pd.Series(True, index=metric_group.index)
        academic_mask = (
            _truthy_series(metric_group.get("academic_present"), metric_group.index)
            if "academic_present" in metric_group.columns
            else pd.Series(False, index=metric_group.index)
        )
        academic_mask = academic_mask | metric_group["_has_term_gpa"] | metric_group["_cumulative_gpa_num"].notna()
        term_gpa_mask = metric_group["_has_term_gpa"]

        roster_students = student_count(metric_group.loc[roster_mask])
        academic_students = student_count(metric_group.loc[academic_mask])
        term_gpa_students = student_count(metric_group.loc[term_gpa_mask])
        denominator = roster_students or academic_students

        rows.append(
            {
                "Observed Term": observed_term,
                "Observed Term Sort": observed_sort,
                "Segment": segment,
                "Roster Students": roster_students,
                "Academic Students": academic_students,
                "Students With Term GPA": term_gpa_students,
                "Term GPA Coverage": (term_gpa_students / denominator) if denominator else np.nan,
                "Average Term GPA": metric_group.loc[term_gpa_mask, "_term_gpa_num"].mean(),
                "Average Cumulative GPA": metric_group["_cumulative_gpa_num"].dropna().mean(),
                "Average Passed Hours": pd.to_numeric(metric_group.get("term_passed_hours", pd.Series(np.nan, index=metric_group.index)), errors="coerce").dropna().mean(),
                "Average Cumulative Hours": pd.to_numeric(metric_group.get("cumulative_hours", pd.Series(np.nan, index=metric_group.index)), errors="coerce").dropna().mean(),
            }
        )

    result = pd.DataFrame(rows, columns=GPA_TREND_COLUMNS)
    if result.empty:
        return result
    return result.sort_values(["Observed Term Sort", "Segment"]).reset_index(drop=True)


def build_advisor_intervention_queue(summary: pd.DataFrame) -> dict[str, object]:
    empty_queue = pd.DataFrame(columns=list(ADVISOR_QUEUE_RENAME_MAP.values()))
    empty_rollup = pd.DataFrame(columns=ADVISOR_ROLLUP_COLUMNS)
    empty = {
        "queue": empty_queue,
        "chapter_rollup": empty_rollup,
        "meta": {
            "current_active_students": 0,
            "flagged_students": 0,
            "high_priority_students": 0,
            "medium_priority_students": 0,
            "monitor_students": 0,
        },
    }
    if summary.empty:
        return empty

    work = summary.copy()
    active_mask = _truthy_series(work.get("current_active_flag"), work.index)
    active = work.loc[active_mask].copy()
    if active.empty:
        return empty

    current_active_chapter = _frame_text_series(active, "current_active_chapter")
    latest_chapter = _frame_text_series(active, "latest_chapter", default="Unknown")
    current_active_council = _frame_text_series(active, "current_active_council")
    latest_council = _frame_text_series(active, "council", default="Unknown")
    active["current_chapter_label"] = current_active_chapter.where(current_active_chapter.ne(""), latest_chapter)
    active["current_council_label"] = current_active_council.where(current_active_council.ne(""), latest_council)
    active["average_cumulative_gpa_num"] = _numeric_series(active, "average_cumulative_gpa")
    active["first_year_avg_term_gpa_num"] = _numeric_series(active, "first_year_avg_term_gpa")
    active["data_completeness_rate_num"] = _numeric_series(active, "data_completeness_rate")

    active["flag_critical_cumulative_gpa"] = active["average_cumulative_gpa_num"].lt(2.0)
    active["flag_low_cumulative_gpa"] = active["average_cumulative_gpa_num"].between(2.0, 2.49, inclusive="both")
    active["flag_borderline_cumulative_gpa"] = active["average_cumulative_gpa_num"].between(2.5, 2.79, inclusive="both")
    active["flag_low_first_year_gpa"] = active["first_year_avg_term_gpa_num"].lt(2.3)
    active["flag_borderline_first_year_gpa"] = active["first_year_avg_term_gpa_num"].between(2.3, 2.69, inclusive="both")
    active["flag_low_data_completeness"] = active["data_completeness_rate_num"].lt(0.60)
    active["flag_borderline_data_completeness"] = active["data_completeness_rate_num"].between(0.60, 0.79, inclusive="both")
    active["flag_unknown_outcome_mismatch"] = _truthy_series(active.get("is_unknown_outcome"), active.index) | _frame_text_series(
        active, "latest_outcome_bucket"
    ).str.contains("unknown|unresolved", case=False, na=False)
    active["flag_missing_current_chapter"] = _frame_text_series(active, "current_chapter_label").eq("")
    active["flag_missing_council"] = _frame_text_series(active, "current_council_label").eq("")

    active["risk_score"] = (
        active["flag_critical_cumulative_gpa"].astype(int) * 5
        + active["flag_low_cumulative_gpa"].astype(int) * 4
        + active["flag_borderline_cumulative_gpa"].astype(int) * 2
        + active["flag_low_first_year_gpa"].astype(int) * 3
        + active["flag_borderline_first_year_gpa"].astype(int) * 2
        + active["flag_low_data_completeness"].astype(int) * 2
        + active["flag_borderline_data_completeness"].astype(int) * 1
        + active["flag_unknown_outcome_mismatch"].astype(int) * 2
        + active["flag_missing_current_chapter"].astype(int) * 2
        + active["flag_missing_council"].astype(int) * 1
    )
    active["risk_flag_count"] = (
        active[
            [
                "flag_critical_cumulative_gpa",
                "flag_low_cumulative_gpa",
                "flag_borderline_cumulative_gpa",
                "flag_low_first_year_gpa",
                "flag_borderline_first_year_gpa",
                "flag_low_data_completeness",
                "flag_borderline_data_completeness",
                "flag_unknown_outcome_mismatch",
                "flag_missing_current_chapter",
                "flag_missing_council",
            ]
        ]
        .astype(int)
        .sum(axis=1)
    )
    active["priority"] = np.select(
        [
            active["risk_score"].ge(6) | active["flag_critical_cumulative_gpa"],
            active["risk_score"].ge(3),
            active["risk_score"].gt(0),
        ],
        ["High", "Medium", "Monitor"],
        default="",
    )

    flagged = active.loc[active["risk_score"].gt(0)].copy()
    if flagged.empty:
        return {
            "queue": empty_queue,
            "chapter_rollup": empty_rollup,
            "meta": {
                "current_active_students": int(student_count(active)),
                "flagged_students": 0,
                "high_priority_students": 0,
                "medium_priority_students": 0,
                "monitor_students": 0,
            },
        }

    def _risk_reason_text(row: pd.Series) -> str:
        reasons: list[str] = []
        if bool(row["flag_critical_cumulative_gpa"]):
            reasons.append("critical cumulative GPA")
        elif bool(row["flag_low_cumulative_gpa"]):
            reasons.append("low cumulative GPA")
        elif bool(row["flag_borderline_cumulative_gpa"]):
            reasons.append("borderline cumulative GPA")

        if bool(row["flag_low_first_year_gpa"]):
            reasons.append("low first-year GPA")
        elif bool(row["flag_borderline_first_year_gpa"]):
            reasons.append("borderline first-year GPA")

        if bool(row["flag_low_data_completeness"]):
            reasons.append("low data completeness")
        elif bool(row["flag_borderline_data_completeness"]):
            reasons.append("moderate data completeness gap")

        if bool(row["flag_unknown_outcome_mismatch"]):
            reasons.append("active/unknown outcome mismatch")
        if bool(row["flag_missing_current_chapter"]):
            reasons.append("missing current chapter")
        if bool(row["flag_missing_council"]):
            reasons.append("missing council assignment")
        return ", ".join(reasons)

    flagged["risk_flags_text"] = flagged.apply(_risk_reason_text, axis=1)
    flagged["current_chapter_label"] = flagged["current_chapter_label"].map(_label_or_unknown)
    flagged["current_council_label"] = flagged["current_council_label"].map(_label_or_unknown)

    queue = _selected_table(flagged, ADVISOR_QUEUE_RENAME_MAP)
    queue["_priority_sort"] = queue["Priority"].map(ADVISOR_PRIORITY_ORDER).fillna(99)
    queue = queue.sort_values(
        ["_priority_sort", "Risk Score", "Average Cumulative GPA", "Student Name"],
        ascending=[True, False, True, True],
        na_position="last",
    ).drop(columns=["_priority_sort"]).reset_index(drop=True)

    active_counts = (
        active.assign(
            current_chapter_label=active["current_chapter_label"].map(_label_or_unknown),
            current_council_label=active["current_council_label"].map(_label_or_unknown),
        )
        .groupby(["current_chapter_label", "current_council_label"], dropna=False)
        .agg(**{"Current Active Students": ("student_id", _unique_student_count)})
        .reset_index()
    )
    flagged_rollup = (
        flagged.groupby(["current_chapter_label", "current_council_label"], dropna=False)
        .agg(
            **{
                "Flagged Students": ("student_id", _unique_student_count),
                "High Priority": ("priority", lambda values: int(pd.Series(values).eq("High").sum())),
                "Medium Priority": ("priority", lambda values: int(pd.Series(values).eq("Medium").sum())),
                "Monitor": ("priority", lambda values: int(pd.Series(values).eq("Monitor").sum())),
                "Average Risk Score": ("risk_score", "mean"),
            }
        )
        .reset_index()
    )
    chapter_rollup = active_counts.merge(
        flagged_rollup,
        how="left",
        on=["current_chapter_label", "current_council_label"],
    ).rename(columns={"current_chapter_label": "Current Chapter", "current_council_label": "Council"})
    for column in ["Flagged Students", "High Priority", "Medium Priority", "Monitor"]:
        chapter_rollup[column] = chapter_rollup[column].fillna(0).astype(int)
    chapter_rollup["Average Risk Score"] = pd.to_numeric(chapter_rollup["Average Risk Score"], errors="coerce")
    chapter_rollup = chapter_rollup.sort_values(
        ["High Priority", "Medium Priority", "Flagged Students", "Current Active Students", "Current Chapter"],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)

    return {
        "queue": queue,
        "chapter_rollup": chapter_rollup,
        "meta": {
            "current_active_students": int(student_count(active)),
            "flagged_students": int(student_count(flagged)),
            "high_priority_students": int(flagged["priority"].eq("High").sum()),
            "medium_priority_students": int(flagged["priority"].eq("Medium").sum()),
            "monitor_students": int(flagged["priority"].eq("Monitor").sum()),
        },
    }
