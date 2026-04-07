from __future__ import annotations

import argparse
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill

from src.build_executive_report import (
    DEFAULT_ENHANCED_ROOT,
    clean_text,
    coerce_numeric,
    load_latest_bundle,
)


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_ROOT = ROOT / "output" / "record_priority"

TITLE_FILL = "1F4E79"
HEADER_FILL = "DCE6F1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank students whose full pre-organization academic records would have the highest analytic impact.")
    parser.add_argument("--enhanced-root", default=str(DEFAULT_ENHANCED_ROOT))
    parser.add_argument("--enhanced-folder", default="")
    parser.add_argument("--enhanced-workbook", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--top", type=int, default=250, help="Top students to include on the priority-only sheet.")
    return parser.parse_args()


def extract_year(term_label: object) -> int | None:
    match = re.search(r"(19\d{2}|20\d{2})", clean_text(term_label))
    return int(match.group(1)) if match else None


def estimate_missing_pre_org_terms(entry_hours: object) -> int:
    if entry_hours in ("", None) or (isinstance(entry_hours, float) and math.isnan(entry_hours)):
        return 0
    return max(int(math.floor(float(entry_hours) / 15.0)), 0)


def latest_cumulative_gpa(row: pd.Series) -> object:
    overall = row.get("Latest Overall Cumulative GPA", "")
    txstate = row.get("Latest TxState Cumulative GPA", "")
    if overall not in ("", None) and not (isinstance(overall, float) and math.isnan(overall)):
        return overall
    return txstate


def identity_score(identity_basis: str) -> Tuple[int, str]:
    basis = clean_text(identity_basis).lower()
    if not basis:
        return 8, "Identity basis is missing, which increases matching uncertainty."
    if "name" in basis and "email" in basis:
        return 10, "Identity uses name and email matching rather than a clean student ID."
    if "name" in basis:
        return 9, "Identity appears to rely on name matching."
    if "email" in basis:
        return 6, "Identity appears to rely on email matching."
    if "|" in basis:
        return 6, "Identity uses multiple resolution methods."
    return 0, ""


def outcome_score(outcome_bucket: str) -> Tuple[int, str]:
    bucket = clean_text(outcome_bucket)
    mapping = {
        "Graduated": (8, "Student has a terminal graduation outcome, so missing early records can materially change time-to-degree calculations."),
        "Dropped/Resigned/Revoked/Inactive": (8, "Student has a terminal non-graduation outcome, so missing early records can change persistence and GPA interpretation."),
        "Suspended": (8, "Student has a suspension outcome, which makes full academic history especially useful for interpretation."),
        "Transfer": (6, "Transfer-related history is more complex and benefits from full academic context."),
        "No Further Observation": (5, "Outcome is unresolved because the student disappears from the observed data."),
        "Active/Unknown": (3, "Outcome is still active or unresolved, so earlier context may still matter."),
    }
    return mapping.get(bucket, (2, "Outcome is not clearly classified in the current data."))


def standing_score(first_standing: str) -> Tuple[int, str]:
    standing = clean_text(first_standing)
    if standing == "Suspended":
        return 5, "First observed academic standing is suspended."
    if standing == "Probation/Warning":
        return 4, "First observed academic standing is probation or warning."
    return 0, ""


def build_priority_list(summary: pd.DataFrame) -> pd.DataFrame:
    summary = summary.copy()
    numeric_fields = [
        "Entry Cumulative Hours",
        "Roster Terms Observed",
        "Academic Terms Observed",
        "Total Terms Observed Overall",
        "Latest TxState Cumulative GPA",
        "Latest Overall Cumulative GPA",
    ]
    for field in numeric_fields:
        if field in summary.columns:
            summary[field] = coerce_numeric(summary[field])

    latest_year = max((extract_year(value) or 0) for value in summary["Organization Entry Cohort"].tolist())
    if latest_year == 0:
        latest_year = datetime.now().year

    rows: List[Dict[str, object]] = []
    for _, row in summary.iterrows():
        student_id = clean_text(row.get("Student ID"))
        first_name = clean_text(row.get("Preferred First Name"))
        last_name = clean_text(row.get("Preferred Last Name"))
        if not student_id and not (first_name or last_name):
            continue

        entry_hours = row.get("Entry Cumulative Hours", "")
        missing_terms = estimate_missing_pre_org_terms(entry_hours)
        first_org_term = clean_text(row.get("First Observed Organization Term"))
        first_academic_term = clean_text(row.get("First Observed Academic Term"))
        org_year = extract_year(first_org_term)
        cohort_age = max(latest_year - org_year, 0) if org_year is not None else 0
        latest_outcome = clean_text(row.get("Latest Known Outcome Bucket")) or "Unknown"
        identity_basis = clean_text(row.get("Identity Resolution Basis Used"))
        ever_transfer = clean_text(row.get("Ever Transfer Flag")) == "Yes"
        total_terms = row.get("Total Terms Observed Overall", 0)
        latest_gpa = latest_cumulative_gpa(row)
        same_term_high_hours = (
            first_org_term
            and first_academic_term
            and first_org_term == first_academic_term
            and entry_hours not in ("", None)
            and not (isinstance(entry_hours, float) and math.isnan(entry_hours))
            and float(entry_hours) >= 30
        )

        score = 0
        reasons: List[Tuple[int, str]] = []

        if not first_academic_term and first_org_term:
            score += 35
            reasons.append((35, "No academic term is currently observed, so the student’s full school history is almost entirely missing."))

        missing_term_score = min(missing_terms * 4, 32)
        if missing_term_score:
            score += missing_term_score
            reasons.append(
                (
                    missing_term_score,
                    f"Student appears to join with about {missing_terms} prior term(s) already completed, suggesting substantial pre-organization academic history is missing.",
                )
            )

        if same_term_high_hours:
            score += 12
            reasons.append((12, "Academic history begins in the same term as organization entry despite high cumulative hours, which strongly suggests missing earlier records."))

        age_score = min(cohort_age * 2, 16)
        if age_score:
            score += age_score
            reasons.append((age_score, f"Older cohort ({first_org_term}) affects more long-window metrics and trend calculations."))

        outcome_points, outcome_reason = outcome_score(latest_outcome)
        score += outcome_points
        if outcome_reason:
            reasons.append((outcome_points, outcome_reason))

        identity_points, identity_reason = identity_score(identity_basis)
        score += identity_points
        if identity_reason:
            reasons.append((identity_points, identity_reason))

        if ever_transfer:
            score += 10
            reasons.append((10, "Transfer status appears in the current data, which makes school-start timing more complex."))

        if clean_text(row.get("Observed Graduation Within 4 Years Of Org Entry Measurable")) == "Yes":
            score += 4
            reasons.append((4, "Student is already contributing to measurable 4-year graduation calculations."))
        if clean_text(row.get("Observed Graduation Within 6 Years Of Org Entry Measurable")) == "Yes":
            score += 4
            reasons.append((4, "Student is already contributing to measurable 6-year graduation calculations."))

        standing_points, standing_reason = standing_score(clean_text(row.get("First Academic Standing After Org Entry")))
        score += standing_points
        if standing_reason:
            reasons.append((standing_points, standing_reason))

        if total_terms not in ("", None) and not (isinstance(total_terms, float) and math.isnan(total_terms)):
            span_score = min(int(total_terms // 2), 6)
            score += span_score
            if span_score:
                reasons.append((span_score, "Student has a longer observed timeline, so updated starting records would influence more downstream metrics."))

        priority_tier = "Lower"
        if score >= 60:
            priority_tier = "Very High"
        elif score >= 45:
            priority_tier = "High"
        elif score >= 30:
            priority_tier = "Medium"

        top_reasons = " | ".join(reason for _, reason in sorted(reasons, key=lambda item: (-item[0], item[1]))[:4])

        rows.append(
            {
                "Priority Score": score,
                "Priority Tier": priority_tier,
                "Student ID": student_id,
                "Last Name": last_name,
                "First Name": first_name,
                "Chapter": clean_text(row.get("Initial Chapter")),
                "Organization Entry Cohort": first_org_term,
                "First Observed Academic Term": first_academic_term,
                "Entry Cumulative Hours": entry_hours,
                "Estimated Missing Pre-Org Terms": missing_terms,
                "Latest Known Outcome": latest_outcome,
                "Ever Transfer": "Yes" if ever_transfer else "No",
                "Identity Resolution Basis": identity_basis,
                "Latest Cumulative GPA": latest_gpa,
                "Academic Terms Observed": row.get("Academic Terms Observed", ""),
                "Total Terms Observed": total_terms,
                "Why Pull Full Record Early": top_reasons,
            }
        )

    priority = pd.DataFrame(rows)
    if priority.empty:
        return priority

    priority["_cohort_year_sort"] = priority["Organization Entry Cohort"].map(lambda value: extract_year(value) or 9999)
    priority = priority.sort_values(
        by=[
            "Priority Score",
            "Estimated Missing Pre-Org Terms",
            "_cohort_year_sort",
            "Organization Entry Cohort",
            "Last Name",
            "First Name",
        ],
        ascending=[False, False, True, True, True],
    ).reset_index(drop=True)
    priority = priority.drop(columns=["_cohort_year_sort"])
    priority.insert(0, "Priority Rank", range(1, len(priority) + 1))
    return priority


def write_sheet(ws, title: str, subtitle: str, frame: pd.DataFrame) -> None:
    ws.merge_cells("A1:H1")
    ws["A1"] = title
    ws["A1"].font = Font(bold=True, size=16, color="FFFFFF")
    ws["A1"].fill = PatternFill("solid", fgColor=TITLE_FILL)
    ws["A2"] = subtitle
    ws["A2"].alignment = Alignment(wrap_text=True)
    ws["A2"].font = Font(italic=True)
    ws.row_dimensions[2].height = 36

    headers = list(frame.columns)
    for idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=4, column=idx, value=header)
        cell.font = Font(bold=True)
        cell.fill = PatternFill("solid", fgColor=HEADER_FILL)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for row_idx, values in enumerate(frame.itertuples(index=False), start=5):
        for col_idx, value in enumerate(values, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            header = headers[col_idx - 1]
            if "score" in header.lower() or "terms" in header.lower() or "hours" in header.lower():
                if value not in ("", None):
                    cell.number_format = "0.00" if "GPA" in header else "#,##0.0"

    ws.freeze_panes = "A5"
    for col_idx in range(1, len(headers) + 1):
        column_letter = ws.cell(row=4, column=col_idx).column_letter
        width = 14
        for row in range(1, ws.max_row + 1):
            width = min(max(width, len(clean_text(ws[f"{column_letter}{row}"].value)) + 2), 45)
        ws.column_dimensions[column_letter].width = width


def write_method_sheet(ws) -> None:
    ws["A1"] = "Method"
    ws["A1"].font = Font(bold=True, size=16)
    lines = [
        "This ranking is meant to help decide which students should have their full academic records pulled first.",
        "Higher ranks go to students whose missing pre-organization academic history is most likely to change graduation timing, GPA interpretation, or cohort calculations.",
        "The strongest signals are high cumulative hours at organization entry, missing academic history, older cohorts, terminal outcomes, transfer status, and weaker identity resolution.",
        "This is a prioritization aid, not a claim that lower-ranked students do not matter.",
    ]
    for idx, line in enumerate(lines, start=3):
        ws[f"A{idx}"] = f"- {line}"
        ws[f"A{idx}"].alignment = Alignment(wrap_text=True)
    ws.column_dimensions["A"].width = 120


def build_full_record_priority_list(
    enhanced_root: Path,
    output_root: Path,
    explicit_folder: Path | None,
    explicit_workbook: Path | None,
    top_n: int,
) -> Dict[str, object]:
    bundle = load_latest_bundle(enhanced_root, explicit_folder, explicit_workbook)
    summary = bundle.tables["student_summary"].copy()
    priority = build_priority_list(summary)
    if priority.empty:
        raise ValueError("No students were available to rank from Student_Summary.")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_folder = output_root / f"run_{timestamp}"
    output_folder.mkdir(parents=True, exist_ok=False)

    csv_path = output_folder / "full_academic_record_priority_list.csv"
    workbook_path = output_folder / "Full_Academic_Record_Priority_List.xlsx"
    readme_path = output_folder / "README.md"

    priority.to_csv(csv_path, index=False)

    top_frame = priority.head(top_n).copy()
    reason_counts = (
        priority["Priority Tier"]
        .value_counts()
        .rename_axis("Priority Tier")
        .reset_index(name="Students")
    )

    wb = Workbook()
    ws = wb.active
    ws.title = "Priority List"
    write_sheet(
        ws,
        "Full Academic Record Priority List",
        "Students ranked from highest to lowest priority for pulling full academic history from school start rather than organization entry.",
        priority,
    )

    top_ws = wb.create_sheet(title="Top Priority")
    write_sheet(
        top_ws,
        f"Top {min(top_n, len(priority))} Students",
        "Start with these students first if you are pulling records one by one.",
        top_frame,
    )

    summary_ws = wb.create_sheet(title="Summary")
    write_sheet(
        summary_ws,
        "Priority Tier Summary",
        "How many students fall into each priority tier.",
        reason_counts,
    )

    method_ws = wb.create_sheet(title="Method")
    write_method_sheet(method_ws)

    wb.save(workbook_path)

    readme = [
        "# Full Academic Record Priority List",
        "",
        "This folder ranks students from highest to lowest priority for pulling full academic records from school start.",
        "",
        "## Files",
        "",
        f"- `{workbook_path.name}`: Excel version with full list, top-priority subset, tier summary, and method notes.",
        f"- `{csv_path.name}`: Flat ranked list for quick sorting/filtering.",
        "",
        "## Source used",
        "",
        f"- Enhanced analytics bundle: `{bundle.enhanced_folder}`",
        "",
        "## Ranking idea",
        "",
        "- Higher priority means the student is more likely to change important calculations if earlier academic history is added.",
        "- The list emphasizes high entry cumulative hours, missing early academic history, older cohorts, terminal outcomes, transfer complexity, and weaker identity resolution.",
    ]
    readme_path.write_text("\n".join(readme) + "\n", encoding="utf-8")

    return {
        "output_folder": output_folder,
        "workbook_path": workbook_path,
        "csv_path": csv_path,
        "readme_path": readme_path,
        "top_students": top_frame[["Priority Rank", "Last Name", "First Name", "Student ID", "Priority Score", "Why Pull Full Record Early"]].head(25),
    }


def main() -> None:
    args = parse_args()
    explicit_folder = Path(args.enhanced_folder).expanduser().resolve() if args.enhanced_folder else None
    explicit_workbook = Path(args.enhanced_workbook).expanduser().resolve() if args.enhanced_workbook else None
    result = build_full_record_priority_list(
        enhanced_root=Path(args.enhanced_root).expanduser().resolve(),
        output_root=Path(args.output_root).expanduser().resolve(),
        explicit_folder=explicit_folder,
        explicit_workbook=explicit_workbook,
        top_n=args.top,
    )
    print(f"Priority list created: {result['workbook_path']}")
    print(f"CSV: {result['csv_path']}")
    print("Top students:")
    print(result["top_students"].to_string(index=False))


if __name__ == "__main__":
    main()
