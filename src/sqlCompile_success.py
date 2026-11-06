"""Verified student outcomes and an aggregate-only external impact report."""
from __future__ import annotations

from datetime import datetime, timezone
from html import escape

import pandas as pd

from src.sqlCompile_cohort import _semester_sort
from src.sqlCompile_records import checkpoint_records, index_record_history, RECORD_COLUMNS
from src.sqlCompile_success_records import (
    ALL_CHAPTERS, GRADUATION_COLUMNS, ACADEMIC_COLUMNS, IMPACT_COLUMNS, IMPACT_UNITS,
    normalize_graduation_records, normalize_academic_records, normalize_impact_records,
)


SUCCESS_VERSION = "success-2026-11-06.1"
DEFAULT_MINIMUM_GROUP = 5
STUDENT_COLUMNS = ["Student ID", "Student Name", "Cohort Semester", "Cohort Chapter"]
METRIC_COLUMNS = ["Year", "Eligible students", "Future students", "Verified graduates", "No degree confirmed",
                  "Degree unknown", "Graduation rate", "Degree coverage", "Enrolled or graduated",
                  "Other verified outcome", "Continuation unknown", "Continuation rate", "Continuation coverage"]
DEFINITIONS = [
    "Population: unique students in the selected original new-member cohorts, including former chapter members. Chapter exits do not remove students from academic denominators.",
    "Verified graduates: a reviewed award with an award date, semester, degree level, title, institution, source, reviewer, and review date; matched to an FSL cohort, awarded in or after the join term, and free of unresolved degree or identity conflicts. Within-term order is not established by semester-only join dates.",
    "Graduation rate: verified graduates by a completed checkpoint divided by every eligible cohort student. Missing evidence remains in the denominator. External rates require complete outcome verification.",
    "University continuation: enrolled at the checkpoint or already awarded the selected degree level, divided by every eligible cohort student. This is not a graduation rate. Enrollment requires university evidence in that exact semester.",
    "Year N is the Spring checkpoint in the join calendar year plus N; Summer joins use Summer plus N. These are semester checkpoints since joining FSL, not college-entry anniversaries or IPEDS graduation rates.",
    "Academic GPA is total quality points divided by total GPA credits on a 4.0 scale, not an average of chapter averages. Public GPA requires verified academic summaries for every university-confirmed enrolled student in the selected cohort population and complete enrollment evidence for that population.",
    "Community impact totals cover the stated report semesters and chapters, independently of join-cohort filters. They are documented totals from reviewed records and may not capture every activity. Money is reported in USD.",
    "Documented counts are not estimates of unrecorded outcomes. Draft, excluded, future, unmatched, and conflicted records do not contribute. Small student groups are withheld from external headline metrics.",
]


def _frame(tables, name: str, columns: list[str]) -> pd.DataFrame:
    value = getattr(tables, name, None)
    return value.copy() if value is not None and len(value.columns) else pd.DataFrame(columns=columns)


def _cohort_students(outcomes: pd.DataFrame) -> pd.DataFrame:
    columns = [*STUDENT_COLUMNS, "Last Known Outcome Bucket"]
    result = outcomes.reindex(columns=columns, fill_value="").fillna("").copy()
    result["_join"] = result["Cohort Semester"].map(_semester_sort)
    # Normally already consolidated by the dashboard. Keep an earliest join and
    # flag duplicate/invalid identities instead of dropping their denominator.
    duplicate_ids = set(result.loc[result["Student ID"].duplicated(False), "Student ID"])
    result = result.sort_values("_join", kind="stable").drop_duplicates("Student ID").reset_index(drop=True)
    result["Identity review"] = (~result["Student ID"].str.fullmatch(r"A0\d{7}")
                                  | result["Student ID"].isin(duplicate_ids)
                                  | result["Last Known Outcome Bucket"].str.contains("name", case=False, regex=False))
    return result


def _semester_label(sort: int) -> str:
    return f"{ {1: 'Spring', 2: 'Summer', 3: 'Fall'}[sort % 10]} {sort // 10}"


def _share(count: int, total: int):
    return count / total if total else None


def _degree_at(index: dict, student: dict, target: int) -> dict:
    if student["Identity review"]:
        return {"Degree Outcome": "Unknown", "Enrollment Outcome": "Unknown", "Evidence Conflict": "Identity requires review."}
    return checkpoint_records(index, student["Student ID"], target, student["_join"])


def _award_review(graduations: pd.DataFrame, students: pd.DataFrame, index: dict, cutoff: int, level: str) -> pd.DataFrame:
    lookup = students.set_index("Student ID").to_dict("index")
    rows = []
    for award in graduations.to_dict("records"):
        person = lookup.get(award["Student ID"])
        reason = "Included"
        if award["Verification"] != "Verified":
            reason = award["Verification"]
        elif _semester_sort(award["Award Semester"]) > cutoff:
            reason = "After reporting cutoff"
        elif person is None:
            reason = "Not in selected FSL joining cohorts"
        elif person["Identity review"]:
            reason = "Identity requires review"
        elif _semester_sort(award["Award Semester"]) < person["_join"]:
            reason = "Award predates FSL join semester"
        elif level != "All degree levels" and award["Degree Level"] != level:
            reason = "Different degree level"
        else:
            state = _degree_at(index, {**person, "Student ID": award["Student ID"]}, cutoff)
            if "Degree:" in state["Evidence Conflict"] or state["Degree Outcome"] != "Graduated":
                reason = "Degree evidence requires reconciliation"
        rows.append({**award, "Cohort Semester": person["Cohort Semester"] if person else "",
                     "Cohort Chapter": person["Cohort Chapter"] if person else "", "Use in metrics": reason})
    return pd.DataFrame(rows, columns=[*GRADUATION_COLUMNS, "Cohort Semester", "Cohort Chapter", "Use in metrics"])


def _academic_summary(academic: pd.DataFrame, students: pd.DataFrame, index: dict, start: int, cutoff: int) -> pd.DataFrame:
    rows = []
    identities = set(students.loc[~students["Identity review"], "Student ID"])
    known = academic.loc[academic["Verification"].eq("Verified") & academic["Student ID"].isin(identities)].copy()
    known = known.loc[known["Semester"].map(_semester_sort).between(start, cutoff)]
    for semester, group in known.groupby("Semester", sort=False):
        target = _semester_sort(semester)
        population = students.loc[students["_join"].le(target)]
        enrolled = set()
        missing = 0
        for person in population.to_dict("records"):
            state = _degree_at(index, person, target)
            if state["Enrollment Outcome"] == "Enrolled":
                enrolled.add(person["Student ID"])
            elif state["Enrollment Outcome"] != "Not enrolled":
                missing += 1
        group = group.loc[group["Student ID"].isin(population["Student ID"])].copy()
        if group.empty:
            continue
        credits = pd.to_numeric(group["GPA Credits"].replace("", float("nan")))
        points = pd.to_numeric(group["Quality Points"].replace("", float("nan")))
        gpa_rows = group.loc[credits.gt(0)]
        summaries = set(group.loc[credits.notna(), "Student ID"])
        # Public GPA uses the exact enrolled population; observed-only figures
        # remain useful internally without claiming full-cohort coverage.
        enrolled_rows = group.loc[group["Student ID"].isin(enrolled)]
        enrolled_credits = pd.to_numeric(enrolled_rows["GPA Credits"].replace("", "0")).sum()
        enrolled_points = pd.to_numeric(enrolled_rows["Quality Points"].replace("", "0")).sum()
        public_ready = bool(enrolled and missing == 0 and enrolled.issubset(summaries) and enrolled_credits > 0)
        honors = group["Honors"].isin(["Dean's list", "President's list", "Other honors"])
        rows.append({"Semester": semester, "Reviewed students": len(group), "GPA students": len(gpa_rows),
                     "Weighted GPA": float(points.sum() / credits.sum()) if credits.sum() > 0 else None,
                     "Verified honors students": int(honors.sum()),
                     "Documented earned credits": float(pd.to_numeric(group["Earned Credits"].replace("", "0")).sum()),
                     "University-confirmed enrolled": len(enrolled), "Enrollment unknown": missing,
                     "Public GPA ready": public_ready, "Public GPA students": len(enrolled_rows.loc[pd.to_numeric(enrolled_rows["GPA Credits"].replace("", "0")).gt(0)]),
                     "Public GPA": float(enrolled_points / enrolled_credits) if public_ready else None})
    columns = ["Semester", "Reviewed students", "GPA students", "Weighted GPA", "Verified honors students", "Documented earned credits",
               "University-confirmed enrolled", "Enrollment unknown", "Public GPA ready", "Public GPA students", "Public GPA"]
    return pd.DataFrame(rows, columns=columns).sort_values("Semester", key=lambda series: series.map(_semester_sort))


def build_student_success(tables, *, semesters=None, chapters=None, degree_level="Bachelor's", report_start=None,
                          report_end=None, minimum_group=DEFAULT_MINIMUM_GROUP) -> dict:
    """One calculation feeds owner details, external copy, CSV, and offline HTML."""
    from src.sqlCompile_dashboard import _milestone_target_sort
    from src.sqlCompile_success_records import graduation_university_rows

    if not isinstance(minimum_group, int) or isinstance(minimum_group, bool) or minimum_group < 5:
        raise ValueError("External student metrics require a minimum group size of at least five.")
    from src.sqlCompile_success_records import DEGREE_LEVELS
    if degree_level not in ["All degree levels", *DEGREE_LEVELS]:
        raise ValueError("Choose a supported degree level.")
    all_students = _cohort_students(tables.outcomes)
    cutoff_label = getattr(tables, "reporting_cutoff", None)
    if not cutoff_label:
        cutoff_label = max(getattr(tables, "timeline", pd.DataFrame()).get("Semester", pd.Series(dtype=str)), key=_semester_sort, default="Spring 2026")
    from src.sqlCompile_reporting import normalize_reporting_cutoff
    cutoff_label = normalize_reporting_cutoff(cutoff_label)
    report_end = normalize_reporting_cutoff(report_end or cutoff_label)
    report_start = normalize_reporting_cutoff(report_start or min(all_students["Cohort Semester"], key=_semester_sort, default=report_end))
    start, end = _semester_sort(report_start), _semester_sort(report_end)
    if start > end or end > _semester_sort(cutoff_label):
        raise ValueError("Report semesters must be ordered and cannot exceed the reporting cutoff.")
    students = all_students.loc[all_students["_join"].le(end)].copy()
    if semesters is not None:
        students = students.loc[students["Cohort Semester"].isin(semesters)]
    if chapters is not None:
        students = students.loc[students["Cohort Chapter"].isin(chapters)]
    graduates = normalize_graduation_records(_frame(tables, "graduation_records", GRADUATION_COLUMNS))
    academic = normalize_academic_records(_frame(tables, "academic_records", ACADEMIC_COLUMNS))
    impact = normalize_impact_records(_frame(tables, "impact_records", IMPACT_COLUMNS))
    history = _frame(tables, "record_history", RECORD_COLUMNS)
    history = history.loc[history["Domain"].isin(["degree", "enrollment"])].copy()
    # This also supports callers building a report directly from loaded ledgers.
    award_evidence = [{"Student ID": r["Student ID"], "Semester": r["Semester"], "Chapter": "", "Domain": "degree",
                       "Value": "Graduated", "Source": "verified award", "Evidence": r["Source"], "Priority": 1, "Resolution": ""}
                      for r in graduation_university_rows(graduates).to_dict("records")]
    history = pd.concat([history, pd.DataFrame(award_evidence, columns=RECORD_COLUMNS)], ignore_index=True).drop_duplicates()
    index = index_record_history(history)
    awards = _award_review(graduates, students, index, end, degree_level)
    accepted = awards.loc[awards["Use in metrics"].eq("Included")].copy()
    first_awards = accepted.assign(_award=accepted["Award Semester"].map(_semester_sort)).groupby("Student ID")["_award"].min().to_dict()
    details = []
    current = []
    for student in students.to_dict("records"):
        student_id = student["Student ID"]
        latest = _degree_at(index, student, end)
        current.append({**{key: student[key] for key in STUDENT_COLUMNS}, "Identity review": student["Identity review"],
                        "Verified graduate": student_id in first_awards, "Enrollment": latest["Enrollment Outcome"],
                        "Degree evidence": latest["Degree Outcome"], "Review issue": latest["Evidence Conflict"]})
        for year in range(1, 7):
            target = _milestone_target_sort(student["Cohort Semester"], year)
            future = target > end
            state = _degree_at(index, student, target) if not future else {}
            graduated = not future and first_awards.get(student_id, 999999) <= target and state["Degree Outcome"] == "Graduated" and "Degree:" not in state["Evidence Conflict"]
            no_degree = not future and state["Degree Outcome"] == "No degree confirmed"
            enrolled = not future and state["Enrollment Outcome"] == "Enrolled"
            other = not future and not graduated and no_degree and state["Enrollment Outcome"] == "Not enrolled"
            details.append({**{key: student[key] for key in STUDENT_COLUMNS}, "Year": year,
                            "Checkpoint": _semester_label(target), "Future": future, "Verified graduate": bool(graduated),
                            "No degree confirmed": bool(no_degree), "Enrolled or graduated": bool(graduated or enrolled),
                            "Other verified outcome": bool(other), "Review issue": state.get("Evidence Conflict", "")})
    detail_columns = [*STUDENT_COLUMNS, "Year", "Checkpoint", "Future", "Verified graduate", "No degree confirmed", "Enrolled or graduated", "Other verified outcome", "Review issue"]
    detail = pd.DataFrame(details, columns=detail_columns)
    metrics = []
    for year in range(1, 7):
        group = detail.loc[detail["Year"].eq(year)]
        eligible = group.loc[~group["Future"].astype(bool)]
        n = len(eligible)
        g, negative, positive, other = [int(eligible[column].sum()) for column in ["Verified graduate", "No degree confirmed", "Enrolled or graduated", "Other verified outcome"]]
        metrics.append({"Year": year, "Eligible students": n, "Future students": len(group) - n,
                        "Verified graduates": g, "No degree confirmed": negative, "Degree unknown": n - g - negative,
                        "Graduation rate": _share(g, n), "Degree coverage": _share(g + negative, n),
                        "Enrolled or graduated": positive, "Other verified outcome": other, "Continuation unknown": n - positive - other,
                        "Continuation rate": _share(positive, n), "Continuation coverage": _share(positive + other, n)})
    metrics = pd.DataFrame(metrics, columns=METRIC_COLUMNS)
    current = pd.DataFrame(current, columns=[*STUDENT_COLUMNS, "Identity review", "Verified graduate", "Enrollment", "Degree evidence", "Review issue"])
    academic_metrics = _academic_summary(academic, students, index, start, end)
    impact_selected = impact.loc[impact["Verification"].eq("Verified") & impact["Semester"].map(_semester_sort).between(start, end)].copy()
    if chapters is not None:
        impact_selected = impact_selected.loc[impact_selected["Chapter"].isin(chapters)]
    impact_rows = []
    for metric, group in impact_selected.groupby("Metric", sort=False):
        impact_rows.append({"Metric": metric, "Documented total": float(pd.to_numeric(group["Value"]).sum()),
                            "Unit": IMPACT_UNITS[metric], "Reviewed totals": len(group)})
    impact_metrics = pd.DataFrame(impact_rows, columns=["Metric", "Documented total", "Unit", "Reviewed totals"])
    period_awards = accepted.loc[accepted["Award Semester"].map(_semester_sort).between(start, end)]
    result = {"students": current, "awards": awards, "checkpoint_details": detail, "metrics": metrics,
              "academic_metrics": academic_metrics, "impact_metrics": impact_metrics,
              "period_graduates": int(period_awards["Student ID"].nunique()), "period_awards": len(period_awards),
              "report_start": report_start, "report_end": report_end, "degree_level": degree_level,
              "minimum_group": minimum_group, "join_evidence_complete": bool(getattr(tables, "new_member_evidence_complete", False)),
              "selected_semesters": sorted(students["Cohort Semester"].unique().tolist(), key=_semester_sort),
              "selected_chapters": sorted(students["Cohort Chapter"].unique().tolist()),
              "community_scope": "All FSL chapters" if chapters is None else ", ".join(chapters) or "No chapters selected"}
    result["public"] = public_summary(result)
    return result


def public_summary(report: dict) -> dict:
    """Explicit allowlist: no row evidence, names, student IDs, notes, or source paths."""
    minimum = report["minimum_group"]
    students = report["students"]
    cohort_ready = report["join_evidence_complete"] and not bool(students["Identity review"].any())
    cards = []

    def add(key, title, value, ready, detail, unit="students", waiting="Awaiting verification"):
        cards.append({"key": key, "title": title, "value": value if ready else None,
                      "unit": unit, "status": "Ready" if ready else waiting, "detail": detail})

    level = report["degree_level"]
    degree_description = "all degree levels" if level == "All degree levels" else level.lower() + " awards"
    n = report["period_graduates"]
    add("graduates", "Documented graduates", n, report["join_evidence_complete"] and n >= minimum,
        f"{degree_description.capitalize()}; unique students with a verified award in {report['report_start']} through {report['report_end']}, within the selected joining cohorts. A documented count; additional outcomes may remain unverified.",
        waiting="Below display minimum" if 0 < n < minimum else "Awaiting verified awards")
    add("cohort", "Documented new-member cohort", len(students), cohort_ready and len(students) >= minimum,
        "Unique students with original joining evidence in the selected cohorts, including people who later left their chapter.",
        waiting="Cohort review required" if not cohort_ready else "Below display minimum")
    for row in report["metrics"].to_dict("records"):
        if row["Year"] not in {1, 4, 6}:
            continue
        n = row["Eligible students"]
        if row["Year"] in {4, 6}:
            add(f"graduation_{row['Year']}", f"Year {row['Year']} verified graduation", row["Graduation rate"],
                cohort_ready and n >= minimum and row["Degree unknown"] == 0,
                f"Selected degree level: {level}. {n:,} eligible students; {row['Degree unknown']:,} degree outcomes not verified. Semester checkpoints since joining FSL; every eligible student stays in the denominator.", "rate",
                "No eligible cohort" if n == 0 else "Below display minimum" if n < minimum else "Outcome verification incomplete")
        else:
            add("continuation_1", "Year 1 university continuation", row["Continuation rate"],
                cohort_ready and n >= minimum and row["Continuation unknown"] == 0,
                f"Enrolled at the checkpoint or earned the selected degree level ({level}). {n:,} eligible students; {row['Continuation unknown']:,} outcomes not verified. Checkpoint since joining FSL; not an IPEDS retention measure.", "rate",
                "No eligible cohort" if n == 0 else "Below display minimum" if n < minimum else "Outcome verification incomplete")
    academics = report["academic_metrics"]
    latest = academics.loc[academics["Semester"].eq(report["report_end"])]
    academic_row = latest.iloc[0] if len(latest) else None
    gpa_ready = academic_row is not None and bool(academic_row["Public GPA ready"]) and academic_row["Public GPA students"] >= minimum and cohort_ready
    add("gpa", "Term GPA", float(academic_row["Public GPA"]) if gpa_ready else None, gpa_ready,
        f"{report['report_end']}; quality points / GPA credits on a 4.0 scale for the university-confirmed enrolled students in the selected joining cohorts. Zero-GPA-credit students contribute no GPA weight.", "gpa")
    honors = int(academic_row["Verified honors students"]) if academic_row is not None else 0
    add("honors", "Documented academic honors", honors, report["join_evidence_complete"] and honors >= minimum,
        f"Unique students with verified academic honors in {report['report_end']} in the selected joining cohorts. A documented count, not a complete honors rate.",
        waiting="Below display minimum" if 0 < honors < minimum else "Awaiting academic records")
    community = report["impact_metrics"].set_index("Metric").to_dict("index")
    for metric, unit in IMPACT_UNITS.items():
        item = community.get(metric)
        add("impact_" + metric, metric, item["Documented total"] if item else None, item is not None,
            f"Documented total for {report['report_start']} through {report['report_end']}; {report['community_scope']}. Independent of joining-cohort filters; additional activity may remain unrecorded.", unit,
            "Awaiting community records")
    return {"title": "Student Success & Impact", "version": SUCCESS_VERSION,
            "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "period": f"{report['report_start']} through {report['report_end']}",
            "degreeLevel": level, "joinCohorts": report["selected_semesters"], "chapters": report["selected_chapters"],
            "communityScope": report["community_scope"], "minimumGroup": minimum, "cards": cards, "definitions": DEFINITIONS}


def display_public_value(card: dict) -> str:
    value = card["value"]
    if value is None:
        return "—"
    if card["unit"] == "rate":
        return f"{value:.1%}"
    if card["unit"] == "gpa":
        return f"{value:.2f}"
    if card["unit"] == "USD":
        return f"${value:,.2f}"
    return f"{value:,.1f}".removesuffix(".0")


def public_summary_text(summary: dict, *, ready_only=True) -> str:
    lines = [summary["title"], f"Report period: {summary['period']}",
             "Joining cohorts: " + (", ".join(summary["joinCohorts"]) or "None selected"),
             "Joining chapters: " + (", ".join(summary["chapters"]) or "None selected"), ""]
    cards = [card for card in summary["cards"] if not ready_only or card["status"] == "Ready"]
    for card in cards:
        lines.extend([f"{card['title']}: {display_public_value(card) if card['status'] == 'Ready' else card['status']}", card["detail"], ""])
    if not cards:
        lines.append("No verified headline metrics are ready for this selection. Records are being prepared; no outcome estimates are published.")
    lines.extend(["Methodology", *summary["definitions"],
                  f"External student headline metrics require groups of at least {summary['minimumGroup']}. Prepared {summary['generated']}. Rules: {summary['version']}."])
    return "\n".join(lines) + "\n"


def public_metrics_csv(summary: dict) -> bytes:
    rows = [{"Metric": c["title"], "Value": c["value"], "Unit": c["unit"], "Period": summary["period"],
             "Joining cohorts": "; ".join(summary["joinCohorts"]), "Joining chapters": "; ".join(summary["chapters"]),
             "Definition": c["detail"], "Rules": summary["version"]} for c in summary["cards"] if c["status"] == "Ready"]
    return pd.DataFrame(rows, columns=["Metric", "Value", "Unit", "Period", "Joining cohorts", "Joining chapters", "Definition", "Rules"]).to_csv(index=False).encode("utf-8-sig")


def render_public_summary(summary: dict, *, standalone=True) -> str:
    """Static, offline, print-friendly HTML; no private data or runtime libraries."""
    cards = []
    for card in summary["cards"]:
        if card["status"] != "Ready":
            continue
        cards.append(f'<article class="success-card"><h3>{escape(card["title"])}</h3><strong>{escape(display_public_value(card))}</strong><p>{escape(card["detail"])}</p></article>')
    if not cards:
        cards.append('<p class="success-empty">Verified results are being prepared. No headline metrics are ready for this selection.</p>')
    methods = "".join(f"<li>{escape(line)}</li>" for line in summary["definitions"])
    body = f'''<section class="success-report" aria-label="Student Success and Impact">
      <p class="success-eyebrow">FRATERNITY AND SORORITY LIFE</p><h1>Student Success &amp; Impact</h1>
      <p>{escape(summary['period'])} · Reviewed evidence</p>
      <p class="success-scope"><b>Joining cohorts:</b> {escape(', '.join(summary['joinCohorts']) or 'None selected')}<br>
      <b>Joining chapters:</b> {escape(', '.join(summary['chapters']) or 'None selected')}</p>
      <div class="success-cards">{''.join(cards)}</div>
      <details class="success-methods"><summary>Sources and methodology</summary><p>Verified university awards and enrollment records; reviewed academic and community summaries. Individual records remain in the private dashboard.</p><ul>{methods}</ul>
      <p>Student headline minimum: {summary['minimumGroup']}. Prepared {escape(summary['generated'])}. Rules: {escape(summary['version'])}.</p></details></section>'''
    css = '''.success-report{max-width:1120px;margin:24px auto;padding:32px;color:#182b39;font:16px/1.5 system-ui,sans-serif;background:#fff}.success-report h1{font-size:36px;color:#621b2b;margin:8px 0}.success-eyebrow{letter-spacing:2px;font-size:12px;color:#8c6a20}.success-scope{font-size:13px;color:#586775;border-bottom:4px solid #c49a35;padding-bottom:20px}.success-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,260px),1fr));gap:18px;margin:28px 0}.success-card{border:1px solid #dce4e5;border-radius:12px;padding:22px;background:#f8fbfa;break-inside:avoid}.success-card h3{font-size:16px;margin:0 0 12px}.success-card strong{display:block;font-size:36px;color:#25604b}.success-card p,.success-methods{font-size:13px;color:#465c68}.success-methods summary{cursor:pointer;font-weight:600}.success-empty{padding:24px;background:#f4f6f8}.success-methods li{margin:8px 0}@media print{.success-report{padding:0;margin:0}.success-cards{grid-template-columns:1fr 1fr}.success-methods{display:block}.success-methods>*{display:block}}'''
    if not standalone:
        return f"<style>{css}</style>{body}"
    return '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="Content-Security-Policy" content="default-src \'none\'; style-src \'unsafe-inline\'; base-uri \'none\'; form-action \'none\'"><title>Student Success &amp; Impact</title><style>' + css + "</style></head><body>" + body + "</body></html>"
