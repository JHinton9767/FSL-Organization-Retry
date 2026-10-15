import json
from pathlib import Path

import pandas as pd
import pytest
from openpyxl import Workbook

from src.sqlCompile import OUTPUT_COLUMNS, sqlCompile, write_sqlite
from src.sqlCompile_audit import read_change_history, change_contents, review_context, audit_warning
from src.sqlCompile_cohort import append_manual_status_rows, read_manual_status_rows
from src.sqlCompile_dashboard import load_dashboard_tables, build_sql_compile_milestone_dashboard
from src.sqlCompile_host import HostConfig
from src.sqlCompile_quality import build_coverage, coverage_findings, reconcile_milestones, save_coverage
from src.sqlCompile_records import (DEGREE_VIEW, MEMBERSHIP_VIEW, ENROLLMENT_VIEW, POPULATION_OPTIONS,
                                   RECORD_COLUMNS, UNIVERSITY_COLUMNS, checkpoint_records, index_record_history,
                                   normalize_university_records, save_university_records)
from src.sqlCompile_sources import SOURCE_VERSIONS, SOURCE_OBSERVATIONS, SOURCE_MANIFEST, read_optional_table
from src.sqlCompile_storage import atomic_database_update
from src.sqlCompile_viewer import publish_viewer, build_viewer_payload
from src.sqlCompile_reporting import save_reporting_cutoff
from verify_sql_compile import backup_baseline

STUDENT = "A01234567"


def make_roster(path, status="N", extra=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    book = Workbook()
    sheet = book.active
    sheet.append(["Alpha Sigma Phi Fall 2020 Roster"])
    sheet.append(["Student ID", "Student Name", "Status"])
    sheet.append([STUDENT, "Test Student", status])
    if extra:
        sheet.append(extra)
    book.save(path)


def load_case(tmp_path, history, university=None):
    database = tmp_path / "data.sqlite"
    rows = pd.DataFrame(history, columns=OUTPUT_COLUMNS)
    write_sqlite(rows, database, new_member_observations=rows.loc[rows.Status.eq("N"), OUTPUT_COLUMNS[:3]])
    university_path = tmp_path / "university.csv"
    if university is not None:
        save_university_records(university, university_path)
    tables = load_dashboard_tables(database, tmp_path / "manual.csv", tmp_path / "names.csv", tmp_path / "rechecks.csv",
                                  zero_member_periods_file=tmp_path / "zero.csv", university_records_file=university_path)
    return tables


def dashboard(tables, view, years=(1, 2, 3, 4, 5, 6), cutoff="Spring 2026", population=POPULATION_OPTIONS[0]):
    return build_sql_compile_milestone_dashboard(tables.timeline, tables.outcomes, chart_milestone_offsets=years,
        reporting_cutoff=cutoff, record_history=tables.record_history, outcome_view=view, population=population)


def test_resignation_and_graduation_remain_independent(tmp_path):
    tables = load_case(tmp_path, [["Fall 2020", "Alpha", STUDENT, "N"], ["Spring 2022", "Alpha", STUDENT, "RS"],
                                  ["Spring 2024", "Alpha", STUDENT, "G"]])
    membership = dashboard(tables, MEMBERSHIP_VIEW, [4])["detail_frame"].iloc[0]
    assert membership["P&G Outcome Bucket"] == "Dropped/Resigned"
    assert membership["Degree Outcome"] == "Graduated"
    assert membership["Enrollment Outcome"] == "Unknown"
    assert dashboard(tables, DEGREE_VIEW, [4])["chart_frame"].iloc[0]["Share"] == 1
    assert not reconcile_milestones(dashboard(tables, DEGREE_VIEW))


def test_early_alumni_is_not_a_degree_and_active_is_not_enrollment(tmp_path):
    tables = load_case(tmp_path, [["Fall 2020", "Alpha", STUDENT, "N"], ["Spring 2021", "Alpha", STUDENT, "AL"]])
    assert dashboard(tables, MEMBERSHIP_VIEW, [1])["detail_frame"].iloc[0]["P&G Outcome Bucket"] == "Early Alumni"
    assert dashboard(tables, DEGREE_VIEW, [1])["detail_frame"].iloc[0]["P&G Outcome Bucket"] == "Unknown"
    assert dashboard(tables, ENROLLMENT_VIEW, [1])["detail_frame"].iloc[0]["P&G Outcome Bucket"] == "Unknown"


def test_return_can_restore_membership_without_changing_entry_chapter(tmp_path):
    tables = load_case(tmp_path, [["Fall 2020", "Alpha", STUDENT, "N"], ["Spring 2021", "Alpha", STUDENT, "CK"],
                                  ["Spring 2022", "Beta", STUDENT, "A"]])
    row = dashboard(tables, MEMBERSHIP_VIEW, [2])["detail_frame"].iloc[0]
    assert row["Cohort Chapter"] == "Alpha"
    assert row["P&G Outcome Bucket"] == "Active"


def test_later_activity_does_not_fill_an_unobserved_checkpoint(tmp_path):
    tables = load_case(tmp_path, [["Fall 2020", "Alpha", STUDENT, "N"], ["Spring 2022", "Alpha", STUDENT, "A"]])
    assert dashboard(tables, MEMBERSHIP_VIEW, [1])["detail_frame"].iloc[0]["P&G Outcome Bucket"] == "Unknown"


def test_degree_before_join_is_retained_but_not_counted(tmp_path):
    university = pd.DataFrame([academic_row(Semester="Spring 2019", **{"Degree Status": "Graduated"})])
    tables = load_case(tmp_path, [["Fall 2020", "Alpha", STUDENT, "N"]], university)
    assert "Graduated" in set(tables.record_history.Value)
    assert dashboard(tables, DEGREE_VIEW, [1])["detail_frame"].iloc[0]["P&G Outcome Bucket"] == "Unknown"


def test_saved_non_graduation_correction_is_not_silently_undone(tmp_path):
    rows = [["Fall 2020", "Alpha", STUDENT, "N"], ["Spring 2024", "Alpha", STUDENT, "G"]]
    load_case(tmp_path, rows)
    append_manual_status_rows(pd.DataFrame([{"Student ID": STUDENT, "Semester": "Spring 2024", "Status": "RS", "Notes": "Corrected old G"}]), tmp_path / "manual.csv")
    tables = load_case(tmp_path, rows)
    result = dashboard(tables, DEGREE_VIEW, [4])["detail_frame"].iloc[0]
    assert result["P&G Outcome Bucket"] == "Unknown" and result["Evidence Conflict"]
    assert result["Membership Outcome"] == "Dropped/Resigned"


def test_unfinished_first_year_is_future_and_matched_population_is_fixed(tmp_path):
    tables = load_case(tmp_path, [["Fall 2020", "Alpha", STUDENT, "N"], ["Fall 2025", "Alpha", "A01234568", "N"]])
    future = dashboard(tables, DEGREE_VIEW, [1], cutoff="Fall 2025")["detail_frame"]
    assert future.loc[future["Student ID"].eq("A01234568"), "P&G Outcome Bucket"].tolist() == ["Future"]
    fixed = dashboard(tables, DEGREE_VIEW, [1, 6], population=POPULATION_OPTIONS[1])
    assert fixed["detail_frame"]["Student ID"].tolist() == [STUDENT, STUDENT]
    assert fixed["meta"]["excluded_students"] == 1


def academic_row(**changes):
    return {"Student ID": STUDENT, "Semester": "Spring 2024", "Enrollment Status": "", "Degree Status": "No degree",
            "Source": "University term history", "Reviewed By": "Reviewer", "Resolution": "", "Notes": "", **changes}


def test_conflicts_stay_unknown_until_reviewed_and_new_evidence_reopens(tmp_path):
    rows = [["Fall 2020", "Alpha", STUDENT, "N"], ["Spring 2024", "Alpha", STUDENT, "G"]]
    tables = load_case(tmp_path, rows, pd.DataFrame([academic_row()]))
    conflict = dashboard(tables, DEGREE_VIEW, [4])["detail_frame"].iloc[0]
    assert conflict["P&G Outcome Bucket"] == "Unknown" and conflict["Evidence Conflict"]
    corrected = pd.DataFrame([academic_row(Resolution="Use university record", Notes="Original G was incorrect")])
    tables = load_case(tmp_path, rows, corrected)
    assert dashboard(tables, DEGREE_VIEW, [4])["detail_frame"].iloc[0]["P&G Outcome Bucket"] == "No degree confirmed"
    later = pd.DataFrame([academic_row(Semester="Spring 2025", **{"Degree Status": "Graduated"}), academic_row(Semester="Spring 2026")])
    tables = load_case(tmp_path, rows, pd.concat([corrected, later], ignore_index=True))
    assert dashboard(tables, DEGREE_VIEW, [6])["detail_frame"].iloc[0]["Evidence Conflict"]


@pytest.mark.parametrize("change", [{"Student ID": "bad"}, {"Semester": "garbage"}, {"Source": ""}, {"Reviewed By": ""}, {"Degree Status": "Not Graduated"}, {"Resolution": "Use university record"}])
def test_university_import_validates_evidence_instead_of_guessing(change):
    with pytest.raises(ValueError):
        normalize_university_records(pd.DataFrame([academic_row(**change)]))


def test_source_cache_preserves_old_versions_and_manual_work(tmp_path, monkeypatch):
    import importlib
    compiler = importlib.import_module("src.sqlCompile")
    root, database = tmp_path / "rosters", tmp_path / "data.sqlite"
    path = root / "Fall 2020" / "Alpha Sigma Phi.xlsx"
    make_roster(path)
    sqlCompile([root], database)
    manual = tmp_path / "manual.csv"
    append_manual_status_rows(pd.DataFrame([{"Student ID": STUDENT, "Semester": "Spring 2024", "Status": "G", "Notes": "Verified degree"}]), manual)
    before = manual.read_bytes()
    with monkeypatch.context() as scoped:
        scoped.setattr(compiler, "load_workbook", lambda *a, **k: pytest.fail("Unchanged workbook was reparsed"))
        sqlCompile([root], database)
    assert read_optional_table(database, SOURCE_MANIFEST)["Reused"].tolist() == ["Yes"]
    make_roster(path, "RS")
    sqlCompile([root], database)
    versions = read_optional_table(database, SOURCE_VERSIONS)
    assert len(versions) == 2 and '"Status": "N"' in versions.iloc[0].rows_json
    assert read_optional_table(database, SOURCE_OBSERVATIONS)["Status"].tolist() == ["RS"]
    assert manual.read_bytes() == before
    path.unlink()
    with pytest.raises(ValueError, match="preserved"):
        sqlCompile([root], database)
    assert read_optional_table(database, SOURCE_OBSERVATIONS)["Status"].tolist() == ["RS"]


def test_invalid_ids_are_audited_and_not_counted(tmp_path):
    root = tmp_path / "rosters"
    make_roster(root / "roster.xlsx", extra=["INVALID", "Another Student", "N"])
    result = sqlCompile([root], tmp_path / "data.sqlite")
    assert result.row_count == 1
    assert "sql_compile_invalid_id" in set(result.issues.exception_type)


def test_permanent_history_survives_rotating_backups_and_records_reviewer(tmp_path):
    path = tmp_path / "manual.csv"
    for number in range(15):
        with review_context("Owner", f"Review {number}", "Source form"):
            append_manual_status_rows(pd.DataFrame([{"Student ID": STUDENT, "Semester": "Spring 2024", "Status": "G" if number % 2 else "RS"}]), path)
    history = read_change_history(path)
    assert len(history) == 15 and set(history.state) == {"saved"}
    assert history.iloc[0].reviewer == "Owner" and history.iloc[0].reason == "Review 14"
    before, after = change_contents(path, history.iloc[0].id)
    assert b",G," in before and b",RS," in after
    assert not audit_warning(path)
    path.write_text(path.read_text().replace(",RS,", ",AL,"), encoding="utf-8")
    assert "outside" in audit_warning(path)


def test_coverage_detects_false_completeness_and_zero_member_conflict():
    from src.sqlCompile import ROSTER_INVENTORY_COLUMNS
    inventory = pd.DataFrame([["Fall 2020", "Alpha", "Final", 3, "", 0, "a.xlsx", "Roster", 10]], columns=ROSTER_INVENTORY_COLUMNS)
    decisions = pd.DataFrame([{"Chapter": "Alpha", "Semester": "Spring 2021", "Coverage": "Complete", "Source": "Review", "Reviewed By": "Owner", "Notes": ""}])
    coverage = build_coverage(inventory, "Spring 2021", decisions, pd.DataFrame())
    errors, warnings = coverage_findings(coverage)
    assert any("no compiled" in message for message in errors) and warnings
    coverage.loc[coverage.Semester.eq("Fall 2020"), "Coverage"] = "Zero members"
    assert any("conflicts" in message for message in coverage_findings(coverage)[0])


def test_reconciliation_rejects_corrupted_publication_counts(tmp_path):
    tables = load_case(tmp_path, [["Fall 2020", "Alpha", STUDENT, "N"]])
    result = dashboard(tables, DEGREE_VIEW)
    assert reconcile_milestones(result, [["Fall 2020", "Alpha", 1, "Graduated", 999]])


def test_baseline_backup_includes_permanent_history_and_checksums(tmp_path):
    database = tmp_path / "data.sqlite"
    write_sqlite(pd.DataFrame([["Fall 2020", "Alpha", STUDENT, "N"]], columns=OUTPUT_COLUMNS), database)
    config = HostConfig(database=database, manual_status=tmp_path / "manual.csv", name_choices=tmp_path / "names.csv",
        name_rechecks=tmp_path / "rechecks.csv", zero_member_periods=tmp_path / "zero.csv", reporting_settings=tmp_path / "reporting.json",
        university_records=tmp_path / "university.csv", coverage_reviews=tmp_path / "coverage.csv")
    append_manual_status_rows(pd.DataFrame([{"Student ID": STUDENT, "Semester": "Spring 2024", "Status": "G"}]), config.manual_status)
    folder = backup_baseline(config)
    manifest = json.loads((folder / "manifest.json").read_text())
    assert any(row.get("backup", "").endswith(".history.sqlite") for row in manifest["files"])
    assert any(row.get("missing") for row in manifest["files"])
    from src.sqlCompile_sources import file_digest
    for row in manifest["files"]:
        if "backup" in row:
            assert file_digest(folder / row["backup"]) == row["sha256"]


def test_owner_can_open_new_review_screens(tmp_path, monkeypatch):
    from streamlit.testing.v1 import AppTest
    from src.path_config import ROOT
    load_case(tmp_path, [["Fall 2020", "Alpha", STUDENT, "N"], ["Spring 2026", "Alpha", STUDENT, "G"]])
    settings = {"database": str(tmp_path / "data.sqlite"), "manual_status": str(tmp_path / "manual.csv"),
                "name_choices": str(tmp_path / "names.csv"), "name_rechecks": str(tmp_path / "rechecks.csv"),
                "zero_member_periods": str(tmp_path / "zero.csv"), "university_records": str(tmp_path / "university.csv"),
                "coverage_reviews": str(tmp_path / "coverage.csv"), "reporting_settings": str(tmp_path / "reporting.json")}
    save_reporting_cutoff("Spring 2026", tmp_path / "reporting.json")
    path = tmp_path / "host.json"
    path.write_text(json.dumps(settings), encoding="utf-8")
    monkeypatch.setenv("FSL_DASHBOARD_HOST_CONFIG", str(path))
    monkeypatch.delenv("FSL_DASHBOARD_SHARED", raising=False)
    app = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=30).run()
    app.radio(key="sql_compile_dashboard_section").set_value("Record accuracy").run()
    for area in ["Source coverage", "Evidence conflicts", "University records", "Student history", "Correction history"]:
        next(widget for widget in app.selectbox if widget.label == "Review area").set_value(area).run()
        assert not app.exception and not app.error, area
    app.radio(key="sql_compile_dashboard_section").set_value("Manual Checker").run()
    assert not app.exception and not app.error
    assert app.selectbox(key="pg_measure").value == DEGREE_VIEW
    assert app.dataframe[0].value["Student ID"].tolist() == [STUDENT]
    assert app.dataframe[0].value["P&G Outcome Bucket"].tolist() == ["Graduated"]
    app.selectbox(key="pg_measure").set_value(ENROLLMENT_VIEW).run()
    assert not app.exception and not app.error
    assert app.dataframe[0].value["P&G Outcome Bucket"].tolist() == ["Unknown"]


def test_small_source_changes_are_still_flagged_for_review():
    from src.sqlCompile_publication import publication_changes
    before = {"dataThrough": "Spring 2026", "sourceRevision": "old", "units": [["Fall 2020", "Alpha", 1, "Unknown", 100]]}
    after = {**before, "sourceRevision": "changed"}
    assert any("source files" in item for item in publication_changes(after, before))
