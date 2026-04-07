from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.chart import BarChart, LineChart, Reference
from openpyxl.chart.label import DataLabelList
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENHANCED_ROOT = ROOT / "output" / "enhanced_metrics"
DEFAULT_OUTPUT_ROOT = ROOT / "output" / "presentation_ready"
DEFAULT_SEGMENT_MIN_SIZE = 10
DEFAULT_CHAPTER_MIN_SIZE = 15
DEFAULT_TOP_CHAPTER_COUNT = 10

TITLE_FILL = "1F4E79"
ACCENT_FILL = "D9EAF7"
CARD_FILL = "F7FBFF"
HEADER_FILL = "DCE6F1"
TEXT_DARK = "1F1F1F"
BORDER_COLOR = "B7C9D6"

SHEET_COLUMNS = {
    "student_summary": "Student_Summary",
    "cohort_metrics": "Cohort_Metrics",
    "outcome_segments": "Outcome_Segments",
    "qa_checks": "QA_Checks",
    "metric_definitions": "Metric_Definitions",
    "status_mapping": "Status_Mapping",
    "master_longitudinal": "Master_Longitudinal",
    "change_log": "Change_Log",
}

CSV_FILES = {
    "student_summary": "student_summary.csv",
    "cohort_metrics": "cohort_metrics.csv",
    "outcome_segments": "outcome_segments.csv",
    "qa_checks": "qa_checks.csv",
    "metric_definitions": "metric_definitions.csv",
    "status_mapping": "status_mapping.csv",
    "master_longitudinal": "master_longitudinal.csv",
    "change_log": "change_log.csv",
}

UNRESOLVED_OUTCOME_BUCKETS = {
    "Active/Unknown",
    "No Further Observation",
}


@dataclass
class SourceBundle:
    enhanced_folder: Path
    enhanced_workbook: Path
    tables: Dict[str, pd.DataFrame]
    sources_used: List[str]
    sources_ignored: List[str]
    caveats: List[str]


@dataclass
class ReportBundle:
    source_bundle: SourceBundle
    kpis: List[Dict[str, object]]
    takeaways: List[str]
    frames: Dict[str, pd.DataFrame]
    chart_specs: List[Dict[str, object]]
    qa_notes: List[str]
    withheld_items: List[str]
    definitions: List[Tuple[str, str]]
    limitations: List[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build additive executive-facing reporting outputs.")
    parser.add_argument("--enhanced-root", default=str(DEFAULT_ENHANCED_ROOT))
    parser.add_argument("--enhanced-folder", default="")
    parser.add_argument("--enhanced-workbook", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--segment-min-size", type=int, default=DEFAULT_SEGMENT_MIN_SIZE)
    parser.add_argument("--chapter-min-size", type=int, default=DEFAULT_CHAPTER_MIN_SIZE)
    parser.add_argument("--top-chapters", type=int, default=DEFAULT_TOP_CHAPTER_COUNT)
    parser.add_argument("--skip-chart-export", action="store_true")
    return parser.parse_args()


def clean_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def slugify(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "_", clean_text(value).lower()).strip("_")
    return text or "item"


def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def yes_mask(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.lower().eq("yes")


def percent_text(value: object) -> str:
    if value in ("", None) or (isinstance(value, float) and math.isnan(value)):
        return "Not available"
    return f"{float(value):.1%}"


def decimal_text(value: object, digits: int = 2) -> str:
    if value in ("", None) or (isinstance(value, float) and math.isnan(value)):
        return "Not available"
    return f"{float(value):,.{digits}f}"


def count_text(value: object) -> str:
    if value in ("", None) or (isinstance(value, float) and math.isnan(value)):
        return "0"
    return f"{int(round(float(value))):,}"


def sort_term_label_key(value: str) -> Tuple[int, int]:
    match = re.search(r"(Spring|Summer|Fall|Winter)\s+(\d{4})", clean_text(value), re.IGNORECASE)
    if not match:
        return (9999, 99)
    season = match.group(1).lower()
    year = int(match.group(2))
    order = {"winter": 0, "spring": 1, "summer": 2, "fall": 3}.get(season, 9)
    return (year, order)


def load_latest_bundle(
    enhanced_root: Path,
    explicit_folder: Optional[Path],
    explicit_workbook: Optional[Path],
) -> SourceBundle:
    if explicit_workbook:
        workbook = explicit_workbook.expanduser().resolve()
        if not workbook.exists():
            raise FileNotFoundError(f"Enhanced analytics workbook not found: {workbook}")
        folder = workbook.parent
    elif explicit_folder:
        folder = explicit_folder.expanduser().resolve()
        if not folder.exists():
            raise FileNotFoundError(f"Enhanced analytics folder not found: {folder}")
        matches = sorted(folder.glob("organization_entry_analytics_enhanced_*.xlsx"))
        if not matches:
            raise FileNotFoundError(f"No enhanced analytics workbook found in {folder}")
        workbook = matches[-1]
    else:
        if not enhanced_root.exists():
            raise FileNotFoundError(
                f"Enhanced analytics root not found at {enhanced_root}. Run py run_enhanced_org_analytics.py first."
            )
        run_folders = sorted([path for path in enhanced_root.iterdir() if path.is_dir() and path.name.startswith("run_")])
        if not run_folders:
            raise FileNotFoundError(
                f"No enhanced analytics runs found in {enhanced_root}. Run py run_enhanced_org_analytics.py first."
            )
        folder = run_folders[-1]
        matches = sorted(folder.glob("organization_entry_analytics_enhanced_*.xlsx"))
        if not matches:
            raise FileNotFoundError(f"No enhanced analytics workbook found in {folder}")
        workbook = matches[-1]

    tables: Dict[str, pd.DataFrame] = {}
    sources_used: List[str] = []
    sources_ignored: List[str] = []
    caveats: List[str] = []

    for key, csv_name in CSV_FILES.items():
        csv_path = folder / csv_name
        if csv_path.exists():
            tables[key] = pd.read_csv(csv_path)
            sources_used.append(str(csv_path))

    if len(tables) < 5:
        wb = load_workbook(workbook, data_only=True, read_only=True)
        try:
            for key, sheet_name in SHEET_COLUMNS.items():
                if key in tables or sheet_name not in wb.sheetnames:
                    continue
                tables[key] = pd.read_excel(workbook, sheet_name=sheet_name)
                sources_used.append(f"{workbook}::{sheet_name}")
        finally:
            wb.close()

    required = ["student_summary", "cohort_metrics", "outcome_segments", "qa_checks", "metric_definitions"]
    missing = [name for name in required if name not in tables]
    if missing:
        raise FileNotFoundError(
            "Enhanced analytics bundle is incomplete. Missing required reporting tables: "
            + ", ".join(missing)
        )

    sources_ignored.extend(
        [
            "Master_FSL_Roster.xlsx not used directly because enhanced analytics provides cleaner reporting-ready aggregates.",
            "Master_Roster_Grades.xlsx not used directly because enhanced analytics is preferred as source of truth.",
            "Member_Tenure_Report.xlsx not used directly because the executive package favors the additive enhanced outputs.",
        ]
    )

    if "master_longitudinal" not in tables:
        caveats.append(
            "Master_Longitudinal was not available, so the GPA trend chart uses a student-summary fallback instead of full relative-term averages."
        )

    return SourceBundle(
        enhanced_folder=folder,
        enhanced_workbook=workbook,
        tables=tables,
        sources_used=sources_used,
        sources_ignored=sources_ignored,
        caveats=caveats,
    )


def get_metric_row(
    cohort_metrics: pd.DataFrame,
    metric_group: str,
    metric_label: str,
    cohort: str = "Overall",
    dimension: Optional[str] = None,
    value: Optional[str] = None,
) -> Optional[pd.Series]:
    frame = cohort_metrics.copy()
    for column in ["Metric Group", "Metric Label", "Cohort", "Dimension", "Value"]:
        if column in frame.columns:
            frame[column] = frame[column].fillna("").astype(str)
    mask = (
        frame["Metric Group"].eq(metric_group)
        & frame["Metric Label"].eq(metric_label)
        & frame["Cohort"].eq(cohort)
    )
    if dimension is not None:
        mask &= frame["Dimension"].eq(dimension)
    if value is not None:
        mask &= frame["Value"].eq(value)
    matches = frame.loc[mask]
    if matches.empty:
        return None
    return matches.iloc[0]


def resolved_outcome_mask(frame: pd.DataFrame) -> pd.Series:
    latest = frame["Latest Known Outcome Bucket"].fillna("").astype(str).str.strip()
    return ~latest.isin(UNRESOLVED_OUTCOME_BUCKETS)


def adjusted_graduation_rate(
    frame: pd.DataFrame,
    graduation_field: str,
    measurable_field: Optional[str] = None,
) -> Tuple[object, int]:
    eligible = frame.copy()
    if measurable_field and measurable_field in eligible.columns:
        eligible = eligible.loc[yes_mask(eligible[measurable_field])]
    eligible = eligible.loc[resolved_outcome_mask(eligible)]
    if eligible.empty:
        return "", 0
    graduates = yes_mask(eligible[graduation_field]).sum()
    return float(graduates) / float(len(eligible)), int(len(eligible))


def selected_cumulative_gpa(frame: pd.DataFrame) -> pd.Series:
    overall = coerce_numeric(frame["Latest Overall Cumulative GPA"]) if "Latest Overall Cumulative GPA" in frame.columns else pd.Series(index=frame.index, dtype=float)
    txstate = coerce_numeric(frame["Latest TxState Cumulative GPA"]) if "Latest TxState Cumulative GPA" in frame.columns else pd.Series(index=frame.index, dtype=float)
    return overall.where(overall.notna(), txstate)


def cumulative_gpa_band(value: object) -> str:
    if value in ("", None) or (isinstance(value, float) and math.isnan(value)):
        return "Unknown"
    gpa = float(value)
    if gpa < 2.0:
        return "Below 2.0"
    if gpa < 2.5:
        return "2.0 to 2.49"
    if gpa < 3.0:
        return "2.5 to 2.99"
    if gpa < 3.5:
        return "3.0 to 3.49"
    return "3.5 to 4.00"


def build_kpis(source_bundle: SourceBundle) -> Tuple[List[Dict[str, object]], List[str]]:
    summary = source_bundle.tables["student_summary"].copy()
    metrics = source_bundle.tables["cohort_metrics"].copy()

    total_students = int(summary["Student ID"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique())
    cohorts = (
        summary["Organization Entry Cohort"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    cohort_count = len(cohorts)

    kpis: List[Dict[str, object]] = [
        {
            "Label": "Students tracked",
            "Value": total_students,
            "Display": count_text(total_students),
            "Format": "count",
            "Explanation": "The number of distinct students included in the reporting package.",
        },
        {
            "Label": "Organization-entry cohorts covered",
            "Value": cohort_count,
            "Display": count_text(cohort_count),
            "Format": "count",
            "Explanation": "The number of join-term cohorts represented in the analysis.",
        },
    ]

    adjusted_eventual_grad, adjusted_grad_denominator = adjusted_graduation_rate(
        summary,
        "Eventual Observed Graduation From Org Entry",
    )
    unresolved_share = ""
    if len(summary):
        unresolved_share = 1.0 - (adjusted_grad_denominator / float(len(summary)))

    metric_specs = [
        (
            "Observed graduation rate after joining, excluding unresolved outcomes",
            adjusted_eventual_grad,
            "Share of students observed as graduated after first observed organization entry, excluding students whose latest outcome is still unresolved.",
            "percent",
        ),
        (
            "Returned the next term after joining",
            "Organization Retention",
            "Next observed term after first observed organization term",
            "Retained Flag",
            "Yes",
            "Rate",
            "Share of students still seen in the organization in the next observed term after joining.",
        ),
        (
            "Returned the following fall after joining",
            "Organization Retention",
            "Next fall after first observed organization term",
            "Retained Flag",
            "Yes",
            "Rate",
            "Share of students still seen in the organization the following fall.",
        ),
        (
            "Earned 15+ passed hours in the first term after joining",
            "Credit Momentum",
            "Passed 15+ hours in first academic term after organization entry",
            "Flag",
            "Yes",
            "Rate",
            "Share of students who passed at least 15 hours in the first academic term after joining.",
        ),
        (
            "Earned 30+ passed hours in the first year after joining",
            "Credit Momentum",
            "Passed 30+ hours in first academic year after organization entry",
            "Flag",
            "Yes",
            "Rate",
            "Share of students who passed at least 30 hours in the first academic year after joining.",
        ),
        (
            "Average first-term GPA after joining",
            "GPA and Academic Progress",
            "Average first-term GPA after organization entry",
            "Average",
            "First Post-Entry Term GPA",
            "Average Value",
            "Average term GPA in the first observed academic term after organization entry.",
        ),
        (
            "In good academic standing in the first term after joining",
            "Academic Standing",
            "Good standing in first observed academic term after organization entry",
            "Flag",
            "Yes",
            "Rate",
            "Share of students in good academic standing in the first observed academic term after joining.",
        ),
    ]

    first_item = metric_specs[0]
    kpis.append(
        {
            "Label": first_item[0],
            "Value": first_item[1],
            "Display": percent_text(first_item[1]),
            "Format": first_item[3],
            "Explanation": f"{first_item[2]} Based on {count_text(adjusted_grad_denominator)} students with a more resolved latest outcome.",
        }
    )

    for label, metric_group, metric_label, dimension, value, field_name, explanation in metric_specs[1:]:
        row = get_metric_row(metrics, metric_group, metric_label, "Overall", dimension, value)
        metric_value = row.get(field_name, "") if row is not None else ""
        is_rate = field_name == "Rate"
        kpis.append(
            {
                "Label": label,
                "Value": metric_value,
                "Display": percent_text(metric_value) if is_rate else decimal_text(metric_value, 2),
                "Format": "percent" if is_rate else "decimal",
                "Explanation": explanation,
            }
        )

    kpis.append(
        {
            "Label": "Outcomes still unresolved",
            "Value": unresolved_share,
            "Display": percent_text(unresolved_share),
            "Format": "percent",
            "Explanation": "Students whose latest outcome is still active/unknown or otherwise unresolved and who are excluded from the adjusted graduation rate.",
        }
    )

    outcome_rows = metrics[
        (metrics["Metric Group"].fillna("").astype(str) == "Cohort Counts")
        & (metrics["Metric Label"].fillna("").astype(str) == "Latest known outcome bucket")
        & (metrics["Cohort"].fillna("").astype(str) == "Overall")
        & (metrics["Dimension"].fillna("").astype(str) == "Latest Outcome Bucket")
    ].copy()
    outcome_rows["Rate"] = coerce_numeric(outcome_rows["Rate"])
    for bucket, label in [
        ("Dropped/Resigned/Revoked/Inactive", "Latest observed dropped / inactive / resigned / revoked"),
        ("Suspended", "Latest observed suspended"),
        ("Transfer", "Latest observed transfer"),
        ("No Further Observation", "No further records after last observation"),
    ]:
        match = outcome_rows.loc[outcome_rows["Value"].fillna("").astype(str) == bucket]
        value = match.iloc[0]["Rate"] if not match.empty else ""
        kpis.append(
            {
                "Label": label,
                "Value": value,
                "Display": percent_text(value),
                "Format": "percent",
                "Explanation": "Share of students in this latest observed outcome category.",
            }
        )

    return kpis, cohorts


def build_overall_outcome_table(source_bundle: SourceBundle) -> pd.DataFrame:
    metrics = source_bundle.tables["cohort_metrics"].copy()
    rows = metrics[
        (metrics["Metric Group"].fillna("").astype(str) == "Cohort Counts")
        & (metrics["Metric Label"].fillna("").astype(str) == "Latest known outcome bucket")
        & (metrics["Cohort"].fillna("").astype(str) == "Overall")
        & (metrics["Dimension"].fillna("").astype(str) == "Latest Outcome Bucket")
    ].copy()
    rows["Student Count"] = coerce_numeric(rows["Student Count"])
    rows["Rate"] = coerce_numeric(rows["Rate"])
    rows = rows.rename(
        columns={
            "Value": "Latest observed outcome",
            "Student Count": "Students",
            "Rate": "Percent of students",
        }
    )
    return rows[["Latest observed outcome", "Students", "Percent of students"]].sort_values(
        by="Students", ascending=False
    )


def build_frames(
    source_bundle: SourceBundle,
    segment_min_size: int,
    chapter_min_size: int,
    top_chapters: int,
) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    summary = source_bundle.tables["student_summary"].copy()
    metrics = source_bundle.tables["cohort_metrics"].copy()
    segments = source_bundle.tables["outcome_segments"].copy()
    frames: Dict[str, pd.DataFrame] = {}
    withheld: List[str] = []

    summary["Organization Entry Cohort"] = summary["Organization Entry Cohort"].fillna("").astype(str)
    summary["Initial Chapter"] = summary["Initial Chapter"].fillna("").astype(str)
    metrics["Rate"] = coerce_numeric(metrics["Rate"])
    metrics["Average Value"] = coerce_numeric(metrics["Average Value"])
    metrics["Eligible Students"] = coerce_numeric(metrics["Eligible Students"])
    metrics["Student Count"] = coerce_numeric(metrics["Student Count"])
    if not segments.empty and "Group Size" in segments.columns:
        segments["Group Size"] = coerce_numeric(segments["Group Size"])

    cohort_overview = (
        summary.loc[summary["Organization Entry Cohort"].str.strip() != ""]
        .groupby("Organization Entry Cohort", as_index=False)["Student ID"]
        .nunique()
        .rename(columns={"Organization Entry Cohort": "Cohort", "Student ID": "Students"})
        .sort_values(by="Cohort", key=lambda col: col.map(sort_term_label_key))
    )
    frames["Cohort Overview"] = cohort_overview

    retention_rows = []
    for cohort in sorted([value for value in summary["Organization Entry Cohort"].unique().tolist() if clean_text(value)], key=sort_term_label_key):
        next_term = get_metric_row(
            metrics,
            "Organization Retention",
            "Next observed term after first observed organization term",
            cohort,
            "Retained Flag",
            "Yes",
        )
        next_fall = get_metric_row(
            metrics,
            "Organization Retention",
            "Next fall after first observed organization term",
            cohort,
            "Retained Flag",
            "Yes",
        )
        retention_rows.append(
            {
                "Cohort": cohort,
                "Returned the next term": next_term.get("Rate", "") if next_term is not None else "",
                "Returned the following fall": next_fall.get("Rate", "") if next_fall is not None else "",
            }
        )
    frames["Retention by Cohort"] = pd.DataFrame(retention_rows)

    continuation_rows = []
    for cohort in sorted([value for value in summary["Organization Entry Cohort"].unique().tolist() if clean_text(value)], key=sort_term_label_key):
        next_term = get_metric_row(
            metrics,
            "Institutional Continuation",
            "Next observed academic term after first observed organization term",
            cohort,
            "Continuation Flag",
            "Yes",
        )
        next_fall = get_metric_row(
            metrics,
            "Institutional Continuation",
            "Next fall after first observed organization term",
            cohort,
            "Continuation Flag",
            "Yes",
        )
        continuation_rows.append(
            {
                "Cohort": cohort,
                "Still enrolled next term": next_term.get("Rate", "") if next_term is not None else "",
                "Still enrolled the following fall": next_fall.get("Rate", "") if next_fall is not None else "",
            }
        )
    frames["School Continuation by Cohort"] = pd.DataFrame(continuation_rows)

    graduation_rows = []
    for cohort in sorted([value for value in summary["Organization Entry Cohort"].unique().tolist() if clean_text(value)], key=sort_term_label_key):
        cohort_rows = summary.loc[summary["Organization Entry Cohort"] == cohort].copy()
        eventual, eventual_n = adjusted_graduation_rate(
            cohort_rows,
            "Eventual Observed Graduation From Org Entry",
        )
        grad4, grad4_n = adjusted_graduation_rate(
            cohort_rows,
            "Observed Graduation Within 4 Years Of Org Entry",
            "Observed Graduation Within 4 Years Of Org Entry Measurable",
        )
        grad6, grad6_n = adjusted_graduation_rate(
            cohort_rows,
            "Observed Graduation Within 6 Years Of Org Entry",
            "Observed Graduation Within 6 Years Of Org Entry Measurable",
        )
        graduation_rows.append(
            {
                "Cohort": cohort,
                "Observed graduation, excluding unresolved outcomes": eventual,
                "Graduated within 4 years, excluding unresolved outcomes": grad4,
                "Graduated within 6 years, excluding unresolved outcomes": grad6,
                "Students with resolved outcomes": eventual_n,
                "Resolved 4-year denominator": grad4_n,
                "Resolved 6-year denominator": grad6_n,
            }
        )
    frames["Graduation by Cohort"] = pd.DataFrame(graduation_rows)

    credit_rows = []
    for label in [
        "Passed 12+ hours in first academic term after organization entry",
        "Passed 15+ hours in first academic term after organization entry",
        "Passed 24+ hours in first academic year after organization entry",
        "Passed 30+ hours in first academic year after organization entry",
    ]:
        row = get_metric_row(metrics, "Credit Momentum", label, "Overall", "Flag", "Yes")
        credit_rows.append({"Measure": label.replace("organization entry", "joining"), "Rate": row.get("Rate", "") if row is not None else ""})
    frames["Credit Momentum Overview"] = pd.DataFrame(credit_rows)

    if "master_longitudinal" in source_bundle.tables:
        longitudinal = source_bundle.tables["master_longitudinal"].copy()
        longitudinal["Academic Present"] = longitudinal["Academic Present"].fillna("").astype(str)
        longitudinal["Term GPA"] = coerce_numeric(longitudinal["Term GPA"])
        longitudinal["Relative Term Index From Org Entry"] = coerce_numeric(
            longitudinal["Relative Term Index From Org Entry"]
        )
        gpa_points = longitudinal[
            (longitudinal["Academic Present"].str.strip().str.lower() == "yes")
            & longitudinal["Term GPA"].notna()
            & longitudinal["Relative Term Index From Org Entry"].notna()
            & (longitudinal["Relative Term Index From Org Entry"] >= 0)
            & (longitudinal["Relative Term Index From Org Entry"] <= 7)
        ].copy()
        if not gpa_points.empty:
            gpa_trend = (
                gpa_points.groupby("Relative Term Index From Org Entry", as_index=False)["Term GPA"]
                .mean()
                .rename(
                    columns={
                        "Relative Term Index From Org Entry": "Relative term after joining",
                        "Term GPA": "Average term GPA",
                    }
                )
            )
            frames["GPA by Relative Term"] = gpa_trend
        else:
            withheld.append("GPA by Relative Term chart withheld because no usable relative-term GPA observations were found.")
    if "GPA by Relative Term" not in frames:
        fallback_rows = []
        for label, field in [
            ("First academic term after joining", "First Post-Entry Term GPA"),
            ("Second academic term after joining", "Second Post-Entry Term GPA"),
            ("Average across the first year after joining", "First-Year Average Term GPA After Org Entry"),
        ]:
            values = coerce_numeric(summary[field]) if field in summary.columns else pd.Series(dtype=float)
            values = values.dropna()
            fallback_rows.append({"Period": label, "Average GPA": values.mean() if not values.empty else ""})
        frames["GPA by Relative Term"] = pd.DataFrame(fallback_rows)
        withheld.append(
            "GPA by Relative Term uses summary-level fallback values because Master_Longitudinal was not available or usable."
        )

    gpa_summary = summary.copy()
    gpa_summary["Selected Cumulative GPA"] = selected_cumulative_gpa(gpa_summary)
    gpa_summary["Selected Cumulative GPA Band"] = gpa_summary["Selected Cumulative GPA"].apply(cumulative_gpa_band)
    resolved_gpa_rows = gpa_summary.loc[
        resolved_outcome_mask(gpa_summary) & gpa_summary["Selected Cumulative GPA"].notna()
    ].copy()

    if not resolved_gpa_rows.empty:
        outcome_groups = []
        for label, mask in [
            ("Graduated", resolved_gpa_rows["Latest Known Outcome Bucket"].fillna("").astype(str) == "Graduated"),
            (
                "Dropped / Suspended / Inactive-type outcomes",
                resolved_gpa_rows["Latest Known Outcome Bucket"].fillna("").astype(str).isin(
                    ["Dropped/Resigned/Revoked/Inactive", "Suspended"]
                ),
            ),
            ("Transfer", resolved_gpa_rows["Latest Known Outcome Bucket"].fillna("").astype(str) == "Transfer"),
        ]:
            group = resolved_gpa_rows.loc[mask].copy()
            if group.empty:
                continue
            outcome_groups.append(
                {
                    "Outcome group": label,
                    "Students": len(group),
                    "Average latest cumulative GPA": group["Selected Cumulative GPA"].mean(),
                }
            )
        if outcome_groups:
            frames["Average GPA by Outcome Group"] = pd.DataFrame(outcome_groups)
        else:
            withheld.append("Average GPA by Outcome Group withheld because no resolved GPA outcome groups were available.")

        gpa_band_rows = []
        band_order = ["Below 2.0", "2.0 to 2.49", "2.5 to 2.99", "3.0 to 3.49", "3.5 to 4.00"]
        for band in band_order:
            band_group = resolved_gpa_rows.loc[resolved_gpa_rows["Selected Cumulative GPA Band"] == band].copy()
            if band_group.empty:
                continue
            gpa_band_rows.append(
                {
                    "Latest cumulative GPA band": band,
                    "Students with GPA and resolved outcome": len(band_group),
                    "Observed graduation rate": yes_mask(band_group["Eventual Observed Graduation From Org Entry"]).sum() / len(band_group),
                    "Average latest cumulative GPA": band_group["Selected Cumulative GPA"].mean(),
                }
            )
        if gpa_band_rows:
            frames["Graduation Rate by GPA Band"] = pd.DataFrame(gpa_band_rows)
        else:
            withheld.append("Graduation Rate by GPA Band withheld because no resolved cumulative GPA bands were available.")
    else:
        withheld.append("GPA-based outcome views were withheld because no resolved students had a usable latest cumulative GPA.")

    standing_rows = []
    standing_series = (
        summary["First Academic Standing After Org Entry"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
    )
    if not standing_series.empty:
        standing_counts = standing_series.value_counts(dropna=False)
        standing_total = standing_counts.sum()
        for bucket, count in standing_counts.items():
            standing_rows.append(
                {
                    "Standing group": bucket,
                    "Students": int(count),
                    "Rate": float(count) / float(standing_total) if standing_total else "",
                }
            )
    frames["Academic Standing Overview"] = pd.DataFrame(standing_rows)

    frames["Outcome Mix"] = build_overall_outcome_table(source_bundle)

    if not segments.empty:
        join_hours = segments[
            (segments["Cohort"].fillna("").astype(str) == "Overall")
            & (segments["Dimension"].fillna("").astype(str) == "Entry Cumulative Hours Bucket")
            & (segments["Group Size"] >= segment_min_size)
        ].copy()
        if not join_hours.empty:
            join_rows = []
            for bucket in sorted(join_hours["Value"].dropna().astype(str).unique().tolist()):
                bucket_rows = summary.loc[summary["Entry Cumulative Hours Bucket"].fillna("").astype(str) == bucket].copy()
                grad_rate, resolved_n = adjusted_graduation_rate(
                    bucket_rows,
                    "Eventual Observed Graduation From Org Entry",
                )
                retained_rows = bucket_rows.loc[yes_mask(bucket_rows["Organization Next Fall Measurable"])]
                acad_rows = bucket_rows.loc[yes_mask(bucket_rows["Academic Next Fall Measurable"])]
                join_rows.append(
                    {
                        "Entry cumulative hours bucket": bucket,
                        "Group Size": len(bucket_rows),
                        "Observed graduation, excluding unresolved outcomes": grad_rate,
                        "Students with resolved outcomes": resolved_n,
                        "Retained In Organization To Next Fall": (
                            yes_mask(retained_rows["Retained In Organization To Next Fall"]).sum() / len(retained_rows)
                            if len(retained_rows)
                            else ""
                        ),
                        "Continued Academically To Next Fall": (
                            yes_mask(acad_rows["Continued Academically To Next Fall"]).sum() / len(acad_rows)
                            if len(acad_rows)
                            else ""
                        ),
                    }
                )
            frames["Join Hours Comparison"] = pd.DataFrame(join_rows)
        else:
            withheld.append("Join Hours Comparison withheld because no entry-hours groups met the minimum sample size.")

    chapter_grouped = (
        summary.loc[summary["Initial Chapter"].str.strip() != ""]
        .groupby("Initial Chapter", as_index=False)
        .agg(
            Students=("Student ID", "nunique"),
            Graduated=("Eventual Observed Graduation From Org Entry", lambda s: yes_mask(s).sum()),
            NextFallEligible=("Organization Next Fall Measurable", lambda s: yes_mask(s).sum()),
            NextFallRetained=("Retained In Organization To Next Fall", lambda s: yes_mask(s).sum()),
        )
    )
    chapter_grouped = chapter_grouped[chapter_grouped["Students"] >= chapter_min_size].copy()
    if not chapter_grouped.empty:
        adjusted_chapter_rows = []
        for _, row in chapter_grouped.iterrows():
            chapter = row["Initial Chapter"]
            chapter_rows = summary.loc[summary["Initial Chapter"] == chapter].copy()
            grad_rate, resolved_n = adjusted_graduation_rate(
                chapter_rows,
                "Eventual Observed Graduation From Org Entry",
            )
            adjusted_chapter_rows.append((chapter, grad_rate, resolved_n))
        adjusted_map = {chapter: (rate, resolved_n) for chapter, rate, resolved_n in adjusted_chapter_rows}
        chapter_grouped["Observed graduation, excluding unresolved outcomes"] = chapter_grouped["Initial Chapter"].map(
            lambda value: adjusted_map.get(value, ("", ""))[0]
        )
        chapter_grouped["Students with resolved outcomes"] = chapter_grouped["Initial Chapter"].map(
            lambda value: adjusted_map.get(value, ("", ""))[1]
        )
        chapter_grouped["Returned the following fall"] = chapter_grouped.apply(
            lambda row: row["NextFallRetained"] / row["NextFallEligible"] if row["NextFallEligible"] else math.nan,
            axis=1,
        )
        chapter_grouped = chapter_grouped.sort_values(
            by=["Students", "Observed graduation, excluding unresolved outcomes"], ascending=[False, False]
        ).head(top_chapters)
        frames["Chapter Comparison"] = chapter_grouped[
            ["Initial Chapter", "Students", "Students with resolved outcomes", "Observed graduation, excluding unresolved outcomes", "Returned the following fall"]
        ].rename(columns={"Initial Chapter": "Chapter"})
    else:
        withheld.append("Chapter Comparison withheld because no chapters met the minimum sample size.")

    return frames, withheld


def build_takeaways(kpis: Sequence[Dict[str, object]], frames: Dict[str, pd.DataFrame]) -> List[str]:
    by_label = {item["Label"]: item for item in kpis}
    takeaways: List[str] = []

    grad = by_label.get("Observed graduation rate after joining, excluding unresolved outcomes", {}).get("Display", "Not available")
    next_fall = by_label.get("Returned the following fall after joining", {}).get("Display", "Not available")
    good_standing = by_label.get("In good academic standing in the first term after joining", {}).get("Display", "Not available")
    takeaways.append(
        f"After removing students whose latest outcomes are still unresolved, {grad} of students with more resolved outcomes eventually graduated after first observed organization entry, and {next_fall} were still in the organization the following fall."
    )
    takeaways.append(
        f"Early academic footing looks strongest where students enter and remain in good standing; {good_standing} were in good standing in their first observed academic term after joining."
    )

    outcome_mix = frames.get("Outcome Mix", pd.DataFrame())
    if not outcome_mix.empty:
        top_row = outcome_mix.iloc[0]
        takeaways.append(
            f"The largest latest observed outcome group is '{top_row['Latest observed outcome']}', representing {percent_text(top_row['Percent of students'])} of tracked students."
        )

    join_hours = frames.get("Join Hours Comparison", pd.DataFrame())
    if not join_hours.empty:
        valid = join_hours.dropna(subset=["Observed graduation, excluding unresolved outcomes"]).copy()
        if not valid.empty:
            best_row = valid.sort_values(
                by="Observed graduation, excluding unresolved outcomes", ascending=False
            ).iloc[0]
            takeaways.append(
                f"Students who joined with {best_row['Entry cumulative hours bucket']} completed hours had the strongest observed graduation results among the join-hours groups shown."
            )

    gpa_band = frames.get("Graduation Rate by GPA Band", pd.DataFrame())
    if not gpa_band.empty:
        best_band = gpa_band.sort_values(by="Observed graduation rate", ascending=False).iloc[0]
        takeaways.append(
            f"Students in the {best_band['Latest cumulative GPA band']} latest cumulative GPA range had the strongest observed graduation rate among the GPA bands shown."
        )

    takeaways.append(
        "Recent cohorts should be interpreted cautiously because long-term outcomes are still in progress and not every student has had enough observed time to reach 4-year or 6-year milestones."
    )
    return takeaways


def build_definitions_and_limitations(source_bundle: SourceBundle) -> Tuple[List[Tuple[str, str]], List[str]]:
    definitions = [
        ("Organization-entry cohort", "A group of students based on the first observed term when they appear in the organization data."),
        ("Retention after joining", "Whether a student is still observed in the organization at a later follow-up point such as the next term or the next fall."),
        ("Observed graduation", "A graduation outcome that appears in the available records after a student first appears in the organization data."),
        ("Earned credit momentum", "How quickly students passed hours after joining, such as passing 15+ hours in the first term or 30+ hours in the first year."),
        ("Observed", "The result is based on records present in the available dataset, not on a complete history from a student’s first day at the university."),
        ("Why some recent cohorts are excluded from long-window metrics", "Recent cohorts have not yet had enough observed time to reach 4-year or 6-year graduation windows, so those rates are shown only where the timeline makes sense."),
        ("Latest cumulative GPA used in the executive report", "The GPA comparison views use the latest available overall cumulative GPA when present, and otherwise fall back to the latest Texas State cumulative GPA."),
    ]

    limitations = [
        "Roster tracking begins at observed organization participation, not necessarily at true school entry.",
        "Academic records are term-level and only reflect the terms present in the available files.",
        "Some recent cohorts are still in progress, so long-term outcomes are incomplete.",
        "Some exits are explicit in the data, while others are only visible because no further records appear.",
        "Some joins may rely on fallback matching when Student ID is missing.",
    ]
    limitations.extend(source_bundle.caveats)
    return definitions, limitations


def build_qa_notes(source_bundle: SourceBundle, frames: Dict[str, pd.DataFrame], withheld: Sequence[str]) -> List[str]:
    notes: List[str] = []
    qa = source_bundle.tables["qa_checks"].copy()
    notes.append(
        "Executive graduation views exclude students whose latest outcome is still 'Active/Unknown' or 'No Further Observation' so recent in-progress cohorts do not artificially lower the displayed graduation rate."
    )
    if not qa.empty:
        flagged = qa[qa["Status"].fillna("").astype(str) == "Flag"]
        notes.append(f"QA checks flagged in source bundle: {len(flagged)}.")
        for _, row in flagged.head(8).iterrows():
            notes.append(f"Flagged QA check: {row.get('Check', 'Unknown check')} ({row.get('Value', '')}).")

    summary = source_bundle.tables["student_summary"].copy()
    unique_students = summary["Student ID"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()
    overall_row = get_metric_row(
        source_bundle.tables["cohort_metrics"],
        "Cohort Counts",
        "Cohort size",
        "Overall",
        "All",
        "Cohort size",
    )
    if overall_row is not None:
        overall_count = overall_row.get("Student Count", "")
        try:
            overall_count_value = int(round(float(overall_count)))
            if overall_count_value != unique_students:
                notes.append(
                    f"Student count mismatch detected: Student_Summary has {unique_students:,} students while Cohort_Metrics overall cohort size is {overall_count_value:,}."
                )
            else:
                notes.append("Student count check passed: Student_Summary matches Cohort_Metrics overall cohort size.")
        except (TypeError, ValueError):
            notes.append("Could not reconcile Cohort_Metrics overall cohort size because the value was not numeric.")

    notes.extend(withheld)
    if not withheld:
        notes.append("No charts or sections were withheld for sample-size or completeness reasons.")
    return notes


def table_description(sheet_name: str) -> Tuple[str, str]:
    descriptions = {
        "Cohort Overview": (
            "How to read this",
            "Each row shows how many students first appeared in the organization during that cohort term.",
        ),
        "Retention": (
            "Why this matters",
            "This section shows how often students stayed connected to the organization after joining.",
        ),
        "Graduation Outcomes": (
            "What this tells us",
            "These figures show observed graduation outcomes after first observed organization entry, not true first-time-in-college graduation rates.",
        ),
        "Credit Momentum": (
            "What this tells us",
            "This section shows how quickly students earned passed hours after joining.",
        ),
        "GPA and Academic Progress": (
            "How to read this",
            "These GPA measures describe early academic performance after joining, using only terms present in the available data.",
        ),
        "Academic Standing": (
            "Why this matters",
            "Academic standing helps show whether students appear to be on solid academic footing after joining.",
        ),
        "Outcome Breakdown": (
            "How to read this",
            "Comparisons are shown only where enough students were present to make the result readable and less misleading.",
        ),
    }
    return descriptions.get(sheet_name, ("What this tells us", "This section summarizes one part of the current analytics package."))


def style_sheet_title(ws, title: str, subtitle: str) -> None:
    ws.merge_cells("A1:H1")
    ws["A1"] = title
    ws["A1"].font = Font(bold=True, size=18, color="FFFFFF")
    ws["A1"].fill = PatternFill("solid", fgColor=TITLE_FILL)
    ws["A1"].alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[1].height = 26
    ws.merge_cells("A2:H2")
    ws["A2"] = subtitle
    ws["A2"].font = Font(italic=True, size=10, color=TEXT_DARK)
    ws["A2"].fill = PatternFill("solid", fgColor=ACCENT_FILL)
    ws["A2"].alignment = Alignment(wrap_text=True)
    ws.row_dimensions[2].height = 30


def style_header_row(ws, row_idx: int, end_col: int) -> None:
    thin = Side(style="thin", color=BORDER_COLOR)
    for col_idx in range(1, end_col + 1):
        cell = ws.cell(row=row_idx, column=col_idx)
        cell.font = Font(bold=True, color=TEXT_DARK)
        cell.fill = PatternFill("solid", fgColor=HEADER_FILL)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)


def autosize_columns(ws, max_width: int = 40) -> None:
    widths: Dict[int, int] = {}
    for row in ws.iter_rows():
        for cell in row:
            value = clean_text(cell.value)
            if not value:
                continue
            widths[cell.column] = min(max(widths.get(cell.column, 0), len(value) + 2), max_width)
    for column, width in widths.items():
        ws.column_dimensions[get_column_letter(column)].width = max(12, width)


def write_dataframe(
    ws,
    df: pd.DataFrame,
    start_row: int,
    start_col: int = 1,
    percent_columns: Optional[Iterable[str]] = None,
    decimal_columns: Optional[Iterable[str]] = None,
) -> Tuple[int, int]:
    percent_columns = set(percent_columns or [])
    decimal_columns = set(decimal_columns or [])
    columns = list(df.columns)
    for idx, column in enumerate(columns, start=start_col):
        ws.cell(row=start_row, column=idx, value=column)
    style_header_row(ws, start_row, start_col + len(columns) - 1)

    thin = Side(style="thin", color=BORDER_COLOR)
    for row_offset, values in enumerate(df.itertuples(index=False), start=1):
        for col_offset, value in enumerate(values, start=0):
            cell = ws.cell(row=start_row + row_offset, column=start_col + col_offset, value=value)
            cell.border = Border(left=thin, right=thin, top=thin, bottom=thin)
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            column_name = columns[col_offset]
            if column_name in percent_columns and value not in ("", None):
                cell.number_format = "0.0%"
            elif column_name in decimal_columns and value not in ("", None):
                cell.number_format = "0.00"
            elif isinstance(value, (int, float)) and column_name.lower().endswith("students"):
                cell.number_format = "#,##0"
    return start_row, start_row + len(df)


def add_bar_chart(
    ws,
    table_start_row: int,
    table_end_row: int,
    title: str,
    x_title: str,
    y_title: str,
    chart_anchor: str,
    stacked: bool = False,
    percent_axis: bool = False,
) -> None:
    if table_end_row <= table_start_row + 1:
        return
    chart = BarChart()
    chart.type = "bar"
    chart.style = 10
    chart.title = title
    chart.y_axis.title = x_title
    chart.x_axis.title = y_title
    chart.height = 8
    chart.width = 13
    if stacked:
        chart.grouping = "stacked"
        chart.overlap = 100
    data = Reference(ws, min_col=2, max_col=ws.max_column, min_row=table_start_row, max_row=table_end_row)
    categories = Reference(ws, min_col=1, min_row=table_start_row + 1, max_row=table_end_row)
    chart.add_data(data, titles_from_data=True)
    chart.set_categories(categories)
    chart.legend.position = "r"
    chart.dLbls = DataLabelList()
    chart.dLbls.showVal = True
    if percent_axis:
        chart.x_axis.numFmt = "0%"
    ws.add_chart(chart, chart_anchor)


def add_line_chart(
    ws,
    table_start_row: int,
    table_end_row: int,
    title: str,
    x_title: str,
    y_title: str,
    chart_anchor: str,
    percent_axis: bool = False,
) -> None:
    if table_end_row <= table_start_row + 1:
        return
    chart = LineChart()
    chart.style = 10
    chart.title = title
    chart.y_axis.title = y_title
    chart.x_axis.title = x_title
    chart.height = 8
    chart.width = 13
    data = Reference(ws, min_col=2, max_col=ws.max_column, min_row=table_start_row, max_row=table_end_row)
    categories = Reference(ws, min_col=1, min_row=table_start_row + 1, max_row=table_end_row)
    chart.add_data(data, titles_from_data=True)
    chart.set_categories(categories)
    chart.legend.position = "r"
    chart.dLbls = DataLabelList()
    chart.dLbls.showVal = False
    if percent_axis:
        chart.y_axis.numFmt = "0%"
    ws.add_chart(chart, chart_anchor)


def make_output_folder(output_root: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder = output_root / f"run_{timestamp}"
    folder.mkdir(parents=True, exist_ok=False)
    return folder


def build_chart_specs(frames: Dict[str, pd.DataFrame]) -> List[Dict[str, object]]:
    specs: List[Dict[str, object]] = []
    mapping = [
        ("cohort_overview", "Cohort Overview", "Students in each organization-entry cohort", "Counts of students first observed in the organization by cohort term", "bar", "Cohort", ["Students"], "count", "This shows where the largest observed entry cohorts appear in the timeline."),
        ("retention_by_cohort", "Retention by Cohort", "Returned after joining", "Compares next-term and next-fall organization retention by cohort", "line", "Cohort", ["Returned the next term", "Returned the following fall"], "percent", "This shows how consistently students remained involved after joining."),
        ("school_continuation_by_cohort", "School Continuation by Cohort", "Still enrolled after joining", "Compares next-term and next-fall academic continuation by cohort", "line", "Cohort", ["Still enrolled next term", "Still enrolled the following fall"], "percent", "This shows how often students are still present in the academic records after joining."),
        ("graduation_by_cohort", "Graduation Outcomes by Cohort", "Observed graduation after joining", "Compares eventual, 4-year, and 6-year observed graduation rates after removing unresolved outcomes from the denominator", "line", "Cohort", ["Observed graduation, excluding unresolved outcomes", "Graduated within 4 years, excluding unresolved outcomes", "Graduated within 6 years, excluding unresolved outcomes"], "percent", "Long-window graduation rates should be read carefully because recent cohorts are still in progress."),
        ("outcome_mix", "Latest Observed Outcome Mix", "Where students most recently appear to land", "Shows the mix of latest observed outcome categories across all tracked students", "bar", "Latest observed outcome", ["Percent of students"], "percent", "This helps show whether the most common latest outcomes are graduation, transfer, suspension, or uncertain exits."),
        ("credit_momentum", "Credit Momentum After Joining", "Passed hours in the first term and first year", "Shows the share of students meeting common passed-hours milestones after joining", "bar", "Measure", ["Rate"], "percent", "Higher values suggest stronger early academic credit progress."),
        ("gpa_relative_term", "Average GPA Over Time After Joining", "Early academic performance after organization entry", "Shows GPA patterns after joining using either relative-term averages or summary-level fallback values", "line", "Relative term after joining", ["Average term GPA"], "decimal", "This helps show whether academic performance looks stable, improving, or weaker after entry."),
        ("gpa_outcome_comparison", "Average GPA by Outcome Group", "Average cumulative GPA by outcome group", "Compares the latest available cumulative GPA for graduates versus key non-graduate outcome groups", "bar", "Outcome group", ["Average latest cumulative GPA"], "decimal", "This helps show whether graduates and higher-risk outcome groups appear academically different by latest cumulative GPA."),
        ("gpa_band_grad_rate", "Graduation Rate by GPA Band", "Graduation outcomes by cumulative GPA range", "Shows the share of students who graduate within each latest cumulative GPA range, excluding unresolved outcomes", "line", "Latest cumulative GPA band", ["Observed graduation rate"], "percent", "This gives a simple view of how graduation rates change across GPA ranges."),
        ("standing_overview", "Academic Standing Overview", "First observed academic standing after joining", "Shows how students first appear academically after organization entry", "bar", "Standing group", ["Rate"], "percent", "This helps show whether most students start in good standing or in higher-risk academic situations."),
        ("join_hours_comparison", "Join Hours Comparison", "Outcomes by cumulative hours at joining", "Compares outcomes for students who joined with different numbers of cumulative hours already completed", "line", "Entry cumulative hours bucket", ["Observed graduation, excluding unresolved outcomes", "Retained In Organization To Next Fall", "Continued Academically To Next Fall"], "percent", "This suggests whether students entering at different academic stages had different observed outcomes."),
        ("chapter_comparison", "Chapter Comparison", "Observed outcomes by chapter", "Compares the largest chapters only, using minimum-size rules to reduce misleading comparisons", "bar", "Chapter", ["Observed graduation, excluding unresolved outcomes", "Returned the following fall"], "percent", "These comparisons are best used as a conversation starter, not as a final judgment of a chapter."),
    ]
    frame_lookup = {
        "Cohort Overview": "Cohort Overview",
        "Retention by Cohort": "Retention by Cohort",
        "School Continuation by Cohort": "School Continuation by Cohort",
        "Graduation Outcomes by Cohort": "Graduation by Cohort",
        "Latest Observed Outcome Mix": "Outcome Mix",
        "Credit Momentum After Joining": "Credit Momentum Overview",
        "Average GPA Over Time After Joining": "GPA by Relative Term",
        "Average GPA by Outcome Group": "Average GPA by Outcome Group",
        "Graduation Rate by GPA Band": "Graduation Rate by GPA Band",
        "Academic Standing Overview": "Academic Standing Overview",
        "Join Hours Comparison": "Join Hours Comparison",
        "Chapter Comparison": "Chapter Comparison",
    }
    for slug, title, subtitle, detail, chart_type, x_field, series_fields, y_format, takeaway in mapping:
        frame_key = frame_lookup[title]
        if frame_key not in frames or frames[frame_key].empty:
            continue
        frame = frames[frame_key]
        if frame_key == "GPA by Relative Term":
            actual_columns = list(frame.columns)
            x_field = actual_columns[0]
            series_fields = [actual_columns[1]] if len(actual_columns) > 1 else []
        specs.append(
            {
                "slug": slug,
                "frame_key": frame_key,
                "title": title,
                "subtitle": subtitle,
                "detail": detail,
                "chart_type": chart_type,
                "x_field": x_field,
                "series_fields": series_fields,
                "y_format": y_format,
                "takeaway": takeaway,
            }
        )
    return specs


def write_chart_data(output_folder: Path, frames: Dict[str, pd.DataFrame], chart_specs: Sequence[Dict[str, object]]) -> Path:
    charts_dir = output_folder / "charts"
    data_dir = charts_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    manifest: List[Dict[str, object]] = []
    for spec in chart_specs:
        frame = frames.get(spec["frame_key"])
        if frame is None or frame.empty:
            continue
        csv_path = data_dir / f"{spec['slug']}.csv"
        frame.to_csv(csv_path, index=False)
        manifest.append(
            {
                "slug": spec["slug"],
                "title": spec["title"],
                "subtitle": spec["subtitle"],
                "detail": spec["detail"],
                "chart_type": spec["chart_type"],
                "x_field": spec["x_field"],
                "series_fields": spec["series_fields"],
                "y_format": spec["y_format"],
                "takeaway": spec["takeaway"],
                "csv_path": str(csv_path),
                "png_path": str(charts_dir / f"{spec['slug']}.png"),
            }
        )

    manifest_path = charts_dir / "chart_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path


def write_markdown_summary(output_folder: Path, report: ReportBundle) -> Path:
    path = output_folder / "Executive_Summary.md"
    lines = ["# Executive Summary", "", "## Top-line metrics", ""]
    for kpi in report.kpis[:10]:
        lines.append(f"- **{kpi['Label']}**: {kpi['Display']}. {kpi['Explanation']}")
    lines.extend(["", "## Key takeaways", ""])
    for takeaway in report.takeaways:
        lines.append(f"- {takeaway}")
    lines.extend(["", "## Important cautions", ""])
    for limitation in report.limitations:
        lines.append(f"- {limitation}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_reporting_readme(output_folder: Path, report: ReportBundle, workbook_path: Path, slides_path: Path) -> Path:
    path = output_folder / "Reporting_README.md"
    lines = [
        "# Reporting Package",
        "",
        "This folder contains a presentation-ready reporting package built additively from the enhanced analytics output bundle.",
        "",
        "## Files",
        "",
        f"- `{workbook_path.name}`: executive-facing workbook with summary pages, charts, supporting tables, definitions, QA, and appendix material.",
        f"- `{slides_path.name}`: chart-ready and slide-ready data tables.",
        "- `Executive_Summary.md`: one-page plain-English summary.",
        "- `charts/`: exported PNG charts and chart source data.",
        "",
        "## Source bundle used",
        "",
    ]
    for item in report.source_bundle.sources_used:
        lines.append(f"- `{item}`")
    lines.extend(["", "## Sources intentionally not used directly", ""])
    for item in report.source_bundle.sources_ignored:
        lines.append(f"- {item}")
    lines.extend(["", "## Notes", "", "- Metrics are labeled from first observed organization participation or first observed academic terms.", "- Recent cohorts are still in progress and should be interpreted cautiously in long-window outcome views."])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_slides_data_workbook(output_folder: Path, report: ReportBundle) -> Path:
    workbook_path = output_folder / "Executive_Report_Slides_Data.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "Overview"
    style_sheet_title(ws, "Slide-Ready Data", "Use these sheets as the clean source tables for slides, one-pagers, and paste-ready chart data.")
    ws["A4"] = "Section"
    ws["B4"] = "Description"
    style_header_row(ws, 4, 2)
    row = 5
    for name in report.frames:
        ws.cell(row=row, column=1, value=name)
        ws.cell(row=row, column=2, value=f"Slide-ready table for {name}.")
        row += 1
    autosize_columns(ws)

    for name, frame in report.frames.items():
        safe_title = name[:31]
        sheet = wb.create_sheet(title=safe_title)
        style_sheet_title(sheet, safe_title, "Clean export table for presentation use.")
        percent_columns = [col for col in frame.columns if "rate" in col.lower() or "percent" in col.lower()]
        decimal_columns = [col for col in frame.columns if "gpa" in col.lower() or "average" in col.lower()]
        write_dataframe(sheet, frame, start_row=4, percent_columns=percent_columns, decimal_columns=decimal_columns)
        sheet.freeze_panes = "A5"
        autosize_columns(sheet)

    wb.save(workbook_path)
    return workbook_path


def write_executive_workbook(output_folder: Path, report: ReportBundle) -> Path:
    workbook_path = output_folder / "Executive_Report.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "Executive Summary"
    style_sheet_title(ws, "Executive Summary", "A presentation-ready overview of student outcomes after first observed organization entry. Labels are intentionally cautious and do not claim true first-time-in-college rates.")
    row = 4
    cards_per_row = 2
    for index, kpi in enumerate(report.kpis[:12]):
        start_col = 1 if index % cards_per_row == 0 else 5
        if index and index % cards_per_row == 0:
            row += 5
        ws.merge_cells(start_row=row, start_column=start_col, end_row=row, end_column=start_col + 2)
        ws.cell(row=row, column=start_col, value=kpi["Label"])
        ws.cell(row=row, column=start_col).font = Font(bold=True, size=11, color=TEXT_DARK)
        ws.cell(row=row, column=start_col).fill = PatternFill("solid", fgColor=ACCENT_FILL)
        ws.merge_cells(start_row=row + 1, start_column=start_col, end_row=row + 2, end_column=start_col + 2)
        ws.cell(row=row + 1, column=start_col, value=kpi["Display"])
        ws.cell(row=row + 1, column=start_col).font = Font(bold=True, size=18, color=TITLE_FILL)
        ws.cell(row=row + 1, column=start_col).fill = PatternFill("solid", fgColor=CARD_FILL)
        ws.cell(row=row + 1, column=start_col).alignment = Alignment(vertical="center", horizontal="center")
        ws.merge_cells(start_row=row + 3, start_column=start_col, end_row=row + 3, end_column=start_col + 2)
        ws.cell(row=row + 3, column=start_col, value=kpi["Explanation"])
        ws.cell(row=row + 3, column=start_col).alignment = Alignment(wrap_text=True)
        for r in range(row, row + 4):
            for c in range(start_col, start_col + 3):
                ws.cell(row=r, column=c).border = Border(left=Side(style="thin", color=BORDER_COLOR), right=Side(style="thin", color=BORDER_COLOR), top=Side(style="thin", color=BORDER_COLOR), bottom=Side(style="thin", color=BORDER_COLOR))

    takeaway_row = row + 6
    ws[f"A{takeaway_row}"] = "Key takeaways"
    ws[f"A{takeaway_row}"].font = Font(bold=True, size=13, color=TITLE_FILL)
    for idx, takeaway in enumerate(report.takeaways, start=1):
        ws[f"A{takeaway_row + idx}"] = f"- {takeaway}"
        ws[f"A{takeaway_row + idx}"].alignment = Alignment(wrap_text=True)
    ws.freeze_panes = "A4"
    autosize_columns(ws)

    takeaways_ws = wb.create_sheet(title="Key Takeaways")
    style_sheet_title(takeaways_ws, "Key Takeaways", "Plain-English summary of the biggest patterns currently visible in the data.")
    for idx, takeaway in enumerate(report.takeaways, start=4):
        takeaways_ws[f"A{idx}"] = f"- {takeaway}"
        takeaways_ws[f"A{idx}"].alignment = Alignment(wrap_text=True)
    autosize_columns(takeaways_ws)

    sheet_plan = [
        ("Cohort Overview", "Cohort Overview", "A11", "bar"),
        ("Retention", "Retention by Cohort", "F4", "line"),
        ("Graduation Outcomes", "Graduation by Cohort", "F4", "line"),
        ("Credit Momentum", "Credit Momentum Overview", "F4", "bar"),
        ("GPA and Academic Progress", "GPA by Relative Term", "F4", "line"),
        ("Academic Standing", "Academic Standing Overview", "F4", "bar"),
        ("Outcome Breakdown", "Chapter Comparison", "F4", "bar"),
    ]

    for sheet_name, frame_key, chart_anchor, chart_type in sheet_plan:
        frame = report.frames.get(frame_key, pd.DataFrame())
        sheet = wb.create_sheet(title=sheet_name[:31])
        heading, note = table_description(sheet_name)
        style_sheet_title(sheet, sheet_name, note)
        sheet["A4"] = heading
        sheet["A4"].font = Font(bold=True, color=TITLE_FILL)
        sheet["A5"] = note
        sheet["A5"].alignment = Alignment(wrap_text=True)
        if frame.empty:
            sheet["A7"] = "No reliable data was available for this section."
            continue
        percent_columns = [col for col in frame.columns if "rate" in col.lower() or "percent" in col.lower()]
        decimal_columns = [col for col in frame.columns if "gpa" in col.lower() or "average" in col.lower()]
        table_start, table_end = write_dataframe(sheet, frame, start_row=7, percent_columns=percent_columns, decimal_columns=decimal_columns)
        sheet.freeze_panes = "A8"
        autosize_columns(sheet)
        if frame.shape[1] >= 2:
            chart_frame = frame.copy()
            if frame_key == "Graduation by Cohort":
                chart_frame = frame[
                    [
                        "Cohort",
                        "Observed graduation, excluding unresolved outcomes",
                        "Graduated within 4 years, excluding unresolved outcomes",
                        "Graduated within 6 years, excluding unresolved outcomes",
                    ]
                ]
            elif frame_key == "Chapter Comparison":
                chart_frame = frame[
                    [
                        "Chapter",
                        "Observed graduation, excluding unresolved outcomes",
                        "Returned the following fall",
                    ]
                ]
            elif frame_key == "Join Hours Comparison":
                chart_frame = frame[
                    [
                        "Entry cumulative hours bucket",
                        "Observed graduation, excluding unresolved outcomes",
                        "Retained In Organization To Next Fall",
                        "Continued Academically To Next Fall",
                    ]
                ]
            chart_row_start = max(table_end + 3, 7)
            chart_percent_columns = [col for col in chart_frame.columns if "rate" in col.lower() or "percent" in col.lower()]
            chart_decimal_columns = [col for col in chart_frame.columns if "gpa" in col.lower() or "average" in col.lower()]
            chart_table_start, chart_table_end = write_dataframe(
                sheet,
                chart_frame,
                start_row=chart_row_start,
                percent_columns=chart_percent_columns,
                decimal_columns=chart_decimal_columns,
            )
            chart_title = "Average GPA Over Time After Joining" if frame_key == "GPA by Relative Term" else sheet_name
            if chart_type == "line":
                add_line_chart(sheet, chart_table_start, chart_table_end, chart_title, chart_frame.columns[0], "Rate" if chart_percent_columns else "Value", chart_anchor, bool(chart_percent_columns))
            else:
                add_bar_chart(sheet, chart_table_start, chart_table_end, chart_title, chart_frame.columns[0], "Rate" if chart_percent_columns else "Value", chart_anchor, False, bool(chart_percent_columns))

    outcome_ws = wb["Outcome Breakdown"]
    if "Join Hours Comparison" in report.frames and not report.frames["Join Hours Comparison"].empty:
        outcome_ws["A24"] = "Additional segment view"
        outcome_ws["A24"].font = Font(bold=True, color=TITLE_FILL)
        outcome_ws["A25"] = "This comparison looks at students based on cumulative hours already completed when they first joined."
        write_dataframe(
            outcome_ws,
            report.frames["Join Hours Comparison"],
            start_row=27,
            percent_columns=[
                "Observed graduation, excluding unresolved outcomes",
                "Retained In Organization To Next Fall",
                "Continued Academically To Next Fall",
            ],
        )
        autosize_columns(outcome_ws)

    retention_ws = wb["Retention"]
    continuation_frame = report.frames.get("School Continuation by Cohort", pd.DataFrame())
    if not continuation_frame.empty:
        retention_ws["A24"] = "School continuation"
        retention_ws["A24"].font = Font(bold=True, color=TITLE_FILL)
        retention_ws["A25"] = "This table shows how often students were still present in academic records after joining."
        write_dataframe(
            retention_ws,
            continuation_frame,
            start_row=27,
            percent_columns=["Still enrolled next term", "Still enrolled the following fall"],
        )
        autosize_columns(retention_ws)

    gpa_ws = wb["GPA and Academic Progress"]
    outcome_gpa_frame = report.frames.get("Average GPA by Outcome Group", pd.DataFrame())
    gpa_band_frame = report.frames.get("Graduation Rate by GPA Band", pd.DataFrame())
    gpa_section_row = 24
    if not outcome_gpa_frame.empty:
        gpa_ws[f"A{gpa_section_row}"] = "Average cumulative GPA by outcome group"
        gpa_ws[f"A{gpa_section_row}"].font = Font(bold=True, color=TITLE_FILL)
        gpa_ws[f"A{gpa_section_row + 1}"] = "This compares the latest available cumulative GPA for graduates versus the key non-graduate outcome groups."
        gpa_ws[f"A{gpa_section_row + 1}"].alignment = Alignment(wrap_text=True)
        _, outcome_end = write_dataframe(
            gpa_ws,
            outcome_gpa_frame,
            start_row=gpa_section_row + 3,
            decimal_columns=["Average latest cumulative GPA"],
        )
        outcome_chart_frame = outcome_gpa_frame[["Outcome group", "Average latest cumulative GPA"]].copy()
        outcome_chart_start, outcome_chart_end = write_dataframe(
            gpa_ws,
            outcome_chart_frame,
            start_row=gpa_section_row + 3,
            start_col=8,
            decimal_columns=["Average latest cumulative GPA"],
        )
        add_bar_chart(
            gpa_ws,
            outcome_chart_start,
            outcome_chart_end,
            "Average GPA by Outcome Group",
            "Outcome group",
            "Average cumulative GPA",
            "F24",
            False,
            False,
        )
        gpa_section_row = outcome_end + 4

    if not gpa_band_frame.empty:
        gpa_ws[f"A{gpa_section_row}"] = "Graduation rate by cumulative GPA band"
        gpa_ws[f"A{gpa_section_row}"].font = Font(bold=True, color=TITLE_FILL)
        gpa_ws[f"A{gpa_section_row + 1}"] = "This shows the percent of students who graduate within each latest cumulative GPA range, excluding unresolved outcomes."
        gpa_ws[f"A{gpa_section_row + 1}"].alignment = Alignment(wrap_text=True)
        _, band_end = write_dataframe(
            gpa_ws,
            gpa_band_frame,
            start_row=gpa_section_row + 3,
            percent_columns=["Observed graduation rate"],
            decimal_columns=["Average latest cumulative GPA"],
        )
        gpa_band_chart_frame = gpa_band_frame[["Latest cumulative GPA band", "Observed graduation rate"]].copy()
        band_chart_start, band_chart_end = write_dataframe(
            gpa_ws,
            gpa_band_chart_frame,
            start_row=gpa_section_row + 3,
            start_col=8,
            percent_columns=["Observed graduation rate"],
        )
        add_line_chart(
            gpa_ws,
            band_chart_start,
            band_chart_end,
            "Graduation Rate by GPA Band",
            "Latest cumulative GPA band",
            "Graduation rate",
            f"F{gpa_section_row}",
            True,
        )
        autosize_columns(gpa_ws)

    definitions_ws = wb.create_sheet(title="Definitions and Notes")
    style_sheet_title(definitions_ws, "Definitions and Notes", "Short plain-language explanations of how to read the measures in this package.")
    definitions_ws["A4"] = "Term"
    definitions_ws["B4"] = "Meaning"
    style_header_row(definitions_ws, 4, 2)
    for idx, (term, meaning) in enumerate(report.definitions, start=5):
        definitions_ws[f"A{idx}"] = term
        definitions_ws[f"B{idx}"] = meaning
        definitions_ws[f"B{idx}"].alignment = Alignment(wrap_text=True)
    autosize_columns(definitions_ws)

    limitations_ws = wb.create_sheet(title="Data Limitations")
    style_sheet_title(limitations_ws, "Data Limitations", "These cautions help keep the results honest and prevent overstatement.")
    for idx, limitation in enumerate(report.limitations, start=4):
        limitations_ws[f"A{idx}"] = f"- {limitation}"
        limitations_ws[f"A{idx}"].alignment = Alignment(wrap_text=True)
    autosize_columns(limitations_ws)

    qa_ws = wb.create_sheet(title="QA Summary")
    style_sheet_title(qa_ws, "QA Summary", "Reporting source checks, caveats, and any items intentionally withheld.")
    qa_ws["A4"] = "Sources used"
    qa_ws["A4"].font = Font(bold=True, color=TITLE_FILL)
    for idx, item in enumerate(report.source_bundle.sources_used, start=5):
        qa_ws[f"A{idx}"] = item
    row = 6 + len(report.source_bundle.sources_used)
    qa_ws[f"A{row}"] = "Sources ignored"
    qa_ws[f"A{row}"].font = Font(bold=True, color=TITLE_FILL)
    for idx, item in enumerate(report.source_bundle.sources_ignored, start=row + 1):
        qa_ws[f"A{idx}"] = item
        qa_ws[f"A{idx}"].alignment = Alignment(wrap_text=True)
    row = row + 2 + len(report.source_bundle.sources_ignored)
    qa_ws[f"A{row}"] = "QA notes"
    qa_ws[f"A{row}"].font = Font(bold=True, color=TITLE_FILL)
    for idx, item in enumerate(report.qa_notes, start=row + 1):
        qa_ws[f"A{idx}"] = f"- {item}"
        qa_ws[f"A{idx}"].alignment = Alignment(wrap_text=True)
    autosize_columns(qa_ws)

    appendix_ws = wb.create_sheet(title="Appendix")
    style_sheet_title(appendix_ws, "Appendix / Technical Detail", "Friendly label map, selected technical field names, and additional detail for advanced readers.")
    appendix_rows = [
        ("Student ID", "Banner ID in the underlying source data."),
        ("Organization Retention", "Whether a student is still observed in the organization at a follow-up point."),
        ("School Continuation", "Whether a student is still observed in academic records at a follow-up point."),
        ("Earned Credit Momentum", "How quickly students passed hours after joining."),
    ]
    appendix_ws["A4"] = "Friendly label"
    appendix_ws["B4"] = "Meaning"
    style_header_row(appendix_ws, 4, 2)
    for idx, (label, meaning) in enumerate(appendix_rows, start=5):
        appendix_ws[f"A{idx}"] = label
        appendix_ws[f"B{idx}"] = meaning
        appendix_ws[f"B{idx}"].alignment = Alignment(wrap_text=True)
    autosize_columns(appendix_ws)

    wb.save(workbook_path)
    return workbook_path


def export_png_charts(output_folder: Path, skip: bool) -> Tuple[bool, str]:
    if skip:
        return False, "Chart export skipped by flag."
    script_path = ROOT / "scripts" / "export_executive_charts.ps1"
    if not script_path.exists():
        return False, f"Chart export helper not found at {script_path}."
    command = ["powershell.exe", "-ExecutionPolicy", "Bypass", "-File", str(script_path), "-InputDir", str(output_folder / "charts"), "-OutputDir", str(output_folder / "charts")]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as exc:
        return False, f"Could not start PowerShell chart export helper: {exc}"
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "Unknown PowerShell chart export error."
        return False, message
    return True, result.stdout.strip() or "PNG charts exported."


def build_report_bundle(source_bundle: SourceBundle, segment_min_size: int, chapter_min_size: int, top_chapters: int) -> ReportBundle:
    kpis, _ = build_kpis(source_bundle)
    frames, withheld = build_frames(source_bundle, segment_min_size, chapter_min_size, top_chapters)
    takeaways = build_takeaways(kpis, frames)
    definitions, limitations = build_definitions_and_limitations(source_bundle)
    qa_notes = build_qa_notes(source_bundle, frames, withheld)
    chart_specs = build_chart_specs(frames)
    return ReportBundle(source_bundle=source_bundle, kpis=kpis, takeaways=takeaways, frames=frames, chart_specs=chart_specs, qa_notes=qa_notes, withheld_items=list(withheld), definitions=definitions, limitations=limitations)


def build_executive_report(
    enhanced_root: Path,
    output_root: Path,
    explicit_folder: Optional[Path],
    explicit_workbook: Optional[Path],
    segment_min_size: int,
    chapter_min_size: int,
    top_chapters: int,
    skip_chart_export: bool,
) -> Dict[str, object]:
    source_bundle = load_latest_bundle(enhanced_root, explicit_folder, explicit_workbook)
    report = build_report_bundle(source_bundle, segment_min_size, chapter_min_size, top_chapters)
    output_folder = make_output_folder(output_root)
    chart_manifest = write_chart_data(output_folder, report.frames, report.chart_specs)
    slides_path = write_slides_data_workbook(output_folder, report)
    workbook_path = write_executive_workbook(output_folder, report)
    summary_path = write_markdown_summary(output_folder, report)
    readme_path = write_reporting_readme(output_folder, report, workbook_path, slides_path)
    exported, export_message = export_png_charts(output_folder, skip_chart_export)
    return {
        "output_folder": output_folder,
        "executive_workbook": workbook_path,
        "slides_workbook": slides_path,
        "executive_summary": summary_path,
        "readme": readme_path,
        "chart_manifest": chart_manifest,
        "chart_exported": exported,
        "chart_export_message": export_message,
        "sources_used": source_bundle.sources_used,
        "sources_ignored": source_bundle.sources_ignored,
        "withheld_items": report.withheld_items,
    }


def main() -> None:
    args = parse_args()
    explicit_folder = Path(args.enhanced_folder).expanduser().resolve() if args.enhanced_folder else None
    explicit_workbook = Path(args.enhanced_workbook).expanduser().resolve() if args.enhanced_workbook else None
    result = build_executive_report(
        enhanced_root=Path(args.enhanced_root).expanduser().resolve(),
        output_root=Path(args.output_root).expanduser().resolve(),
        explicit_folder=explicit_folder,
        explicit_workbook=explicit_workbook,
        segment_min_size=args.segment_min_size,
        chapter_min_size=args.chapter_min_size,
        top_chapters=args.top_chapters,
        skip_chart_export=args.skip_chart_export,
    )
    print(f"Executive reporting package created: {result['output_folder']}")
    print(f"Executive workbook: {result['executive_workbook']}")
    print(f"Slides workbook: {result['slides_workbook']}")
    print(f"Executive summary: {result['executive_summary']}")
    print(f"README: {result['readme']}")
    print(f"Chart export: {result['chart_export_message']}")
    if result["withheld_items"]:
        print("Withheld items:")
        for item in result["withheld_items"]:
            print(f"- {item}")


if __name__ == "__main__":
    main()
