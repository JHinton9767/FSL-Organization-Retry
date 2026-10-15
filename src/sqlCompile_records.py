"""Independent membership, enrollment, and degree evidence for P&G checkpoints."""
from __future__ import annotations

from bisect import bisect_right
from pathlib import Path

import pandas as pd

from src.path_config import ROOT
from src.sqlCompile import OUTPUT_COLUMNS, resolve_semester_statuses
from src.sqlCompile_cohort import (_semester_sort, normalize_status_code,
                                   _prepared_manual_rows, _manual_rows_for_cohort)
from src.persistence_outcomes import persistence_outcome_from_status
from src.sqlCompile_storage import read_review_csv, write_review_csv


METRIC_VERSION = "pg-2026-10-15.1"
ROSTER_VIEW = "Roster outcomes (legacy)"
MEMBERSHIP_VIEW = "FSL membership"
DEGREE_VIEW = "Graduation after joining FSL"
ENROLLMENT_VIEW = "University enrollment"
OUTCOME_VIEWS = [DEGREE_VIEW, MEMBERSHIP_VIEW, ENROLLMENT_VIEW, ROSTER_VIEW]
DOMAIN_COLUMNS = {MEMBERSHIP_VIEW: "Membership Outcome", DEGREE_VIEW: "Degree Outcome", ENROLLMENT_VIEW: "Enrollment Outcome"}
POPULATION_OPTIONS = ["Eligible at each year", "Same students across selected years"]
UNIVERSITY_COLUMNS = ["Student ID", "Semester", "Enrollment Status", "Degree Status", "Source", "Reviewed By", "Resolution", "Notes"]
DEFAULT_UNIVERSITY_PATH = ROOT / "config" / "sqlCompile_university_records.csv"
RECORD_COLUMNS = ["Student ID", "Semester", "Chapter", "Domain", "Value", "Source", "Evidence", "Priority", "Resolution"]


def normalize_university_records(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.reindex(columns=UNIVERSITY_COLUMNS, fill_value="").fillna("").astype(str)
    result = result.apply(lambda column: column.str.strip())
    result = result.loc[result.ne("").any(axis=1)].copy()
    result["Student ID"] = result["Student ID"].str.upper()
    if result.empty:
        return result
    if not result["Student ID"].str.fullmatch(r"A0\d{7}").all():
        raise ValueError("University records require a valid A0 followed by seven digits for every student.")
    if result["Semester"].map(_semester_sort).ge(999999).any():
        raise ValueError("University records require a recognized semester on every row.")
    allowed = {"Enrollment Status": {"", "Unknown", "Enrolled", "Not enrolled"},
               "Degree Status": {"", "Unknown", "Graduated", "No degree"},
               "Resolution": {"", "Use university record"}}
    for column, values in allowed.items():
        if not result[column].isin(values).all():
            raise ValueError(f"Invalid {column}; use the choices in the university record template.")
    if result["Source"].eq("").any() or result["Reviewed By"].eq("").any():
        raise ValueError("Every university record needs its source and reviewer.")
    if (result["Resolution"].ne("") & result["Notes"].eq("")).any():
        raise ValueError("A conflict resolution requires a note explaining the verified decision.")
    return result.drop_duplicates().reset_index(drop=True)


def read_university_records(path: Path = DEFAULT_UNIVERSITY_PATH) -> pd.DataFrame:
    return normalize_university_records(read_review_csv(path, UNIVERSITY_COLUMNS, create=False))


def save_university_records(frame: pd.DataFrame, path: Path, *, expected_rows=None) -> Path:
    return write_review_csv(frame, path, normalize_university_records, expected_rows=expected_rows)


def build_record_history(raw: pd.DataFrame, compiled: pd.DataFrame, timeline: pd.DataFrame,
                         outcomes: pd.DataFrame, manual: pd.DataFrame, joins: pd.DataFrame | None,
                         university: pd.DataFrame) -> pd.DataFrame:
    records = []
    def add(row, domain, value, source, evidence, priority=0, resolution=""):
        records.append({"Student ID": row["Student ID"], "Semester": row["Semester"],
                        "Chapter": row.get("Chapter", ""), "Domain": domain, "Value": value,
                        "Source": source, "Evidence": evidence, "Priority": priority, "Resolution": resolution})

    roster = raw if not raw.empty else compiled
    if not roster.empty:
        roster = roster.copy()
        roster["_code"] = roster["Status"].map(normalize_status_code)
        # Graduation cannot win the membership field and erase a same-term exit.
        membership = roster.loc[~roster["_code"].eq("G")].copy()
        if not raw.empty and not membership.empty:
            winners = resolve_semester_statuses(membership)
            membership = membership.merge(winners, on=OUTPUT_COLUMNS, how="inner")
        for row in membership.to_dict("records"):
            reference = f"{row.get('_source_path', row.get('_source_file', 'Compiled roster'))} | {row.get('_source_sheet', '')} | row {row.get('_source_row_index', '')}"
            add(row, "membership", persistence_outcome_from_status(row["Status"]), "roster", reference)
        for row in roster.loc[roster["_code"].eq("G")].to_dict("records"):
            reference = f"{row.get('_source_path', row.get('_source_file', 'Compiled roster'))} | {row.get('_source_sheet', '')} | row {row.get('_source_row_index', '')}"
            add(row, "degree", "Graduated", "roster", reference)

    if not timeline.empty:
        for row in timeline.loc[timeline["Source"].eq("chapter_disappearance")].to_dict("records"):
            add(row, "membership", "Unknown", "inferred chapter gap", row.get("Notes", "") + " Confirm chapter status; a missing roster is not proof of removal.", 1)
    # Reuse cohort-scope matching, including corrections scoped to later duplicate joins.
    memberships = joins.copy() if joins is not None else compiled.loc[compiled["Status"].map(normalize_status_code).eq("N")].copy()
    memberships["_semester_normalized"] = memberships["Semester"]
    applicable = _manual_rows_for_cohort(_prepared_manual_rows(manual), outcomes, memberships)
    roster_degrees = set(zip(roster.loc[roster["_code"].eq("G"), "Student ID"], roster.loc[roster["_code"].eq("G"), "Semester"])) if not roster.empty else set()
    for row in applicable.to_dict("records"):
        code = normalize_status_code(row["Status"])
        add(row, "degree" if code == "G" else "membership",
            "Graduated" if code == "G" else persistence_outcome_from_status(code),
            "manual correction", row.get("Notes", ""), 2 + row.get("_manual_sequence", 0))
        if code != "G" and (row["Student ID"], row["Semester"]) in roster_degrees:
            add(row, "degree", "Review required", "legacy manual conflict",
                "A saved non-G correction overlaps roster G evidence. Verify the degree separately; the old correction is preserved.", 2)
    for row in university.to_dict("records"):
        for column, domain in [("Enrollment Status", "enrollment"), ("Degree Status", "degree")]:
            if row[column] not in {"", "Unknown"}:
                add(row, domain, row[column], "university", row["Source"], 1, row["Resolution"])
    result = pd.DataFrame(records, columns=RECORD_COLUMNS).drop_duplicates()
    return result.reset_index(drop=True)


def index_record_history(records: pd.DataFrame) -> dict:
    result = {}
    if records.empty:
        return result
    work = records.copy()
    work["_sort"] = work["Semester"].map(_semester_sort)
    work = work.sort_values(["_sort", "Priority"], kind="stable")
    for (student, domain), group in work.groupby(["Student ID", "Domain"], sort=False):
        result[(student, domain)] = (group["_sort"].tolist(), group.to_dict("records"))
    return result


def checkpoint_records(index: dict, student: str, target: int, join_sort: int) -> dict:
    result = {"Membership Outcome": "Unknown", "Degree Outcome": "Unknown", "Enrollment Outcome": "Unknown",
              "Membership Evidence": "", "Degree Evidence": "", "Enrollment Evidence": "", "Evidence Conflict": ""}
    for domain, label in [("membership", "Membership"), ("degree", "Degree"), ("enrollment", "Enrollment")]:
        sorts, rows = index.get((student, domain), ([], []))
        past = rows[:bisect_right(sorts, target)]
        if domain in {"membership", "degree"}:
            past = [row for row in past if row["_sort"] >= join_sort]
        if not past:
            continue
        latest = past[-1]
        conflict = False
        evidence = latest["Evidence"]
        if domain == "membership":
            outcome = latest["Value"]
            # An old active observation is not proof of membership at this checkpoint.
            if outcome == "Active" and latest["_sort"] != target:
                outcome = "Unknown"
        elif domain == "enrollment":
            exact = [row for row in past if row["_sort"] == target]
            resolved = [row for row in exact if row["Resolution"] == "Use university record"]
            chosen = resolved or exact
            values = {row["Value"] for row in chosen}
            conflict = len(values) > 1
            outcome = next(iter(values)) if len(values) == 1 else "Unknown"
            evidence = " | ".join(dict.fromkeys(row["Evidence"] for row in chosen))
        else:
            resolutions = [row for row in past if row["Resolution"] == "Use university record"]
            if resolutions:
                boundary = max(row["_sort"] for row in resolutions)
                past = [row for row in past if row["_sort"] > boundary or (row["_sort"] == boundary and row["Resolution"] == "Use university record")]
            positive = [row for row in past if row["Value"] == "Graduated"]
            negative = [row for row in past if row["Value"] == "No degree"]
            conflict = any(row["Value"] == "Review required" for row in past) or bool(positive and negative and max(row["_sort"] for row in negative) >= min(row["_sort"] for row in positive))
            outcome = "Unknown" if conflict else "Graduated" if positive else "No degree confirmed" if any(row["_sort"] == target for row in negative) else "Unknown"
            evidence = " | ".join(dict.fromkeys(row["Evidence"] for row in past if row["Value"] == "Graduated" or row["_sort"] == target or conflict))
        result[f"{label} Outcome"] = outcome
        result[f"{label} Evidence"] = evidence
        if conflict:
            result["Evidence Conflict"] += f"{label}: contradictory evidence; review required. "
    return result
