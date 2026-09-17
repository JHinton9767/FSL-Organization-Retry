import json

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from src.path_config import ROOT
from src.sqlCompile import COMPILE_AUDIT_TABLE, COMPILE_ISSUES_TABLE, OUTPUT_COLUMNS, write_sqlite
from src.sqlCompile_cohort import append_manual_status_rows, read_manual_status_rows, read_sql_compile_table
from src.sqlCompile_dashboard import build_sql_compile_milestone_dashboard, load_dashboard_tables
from src.sqlCompile_host import load_host_config
from src.sqlCompile_publication import build_publication_check, publication_changes, read_previous_publication
from src.sqlCompile_reporting import REPORTING_EXAMPLE, normalize_reporting_cutoff, read_reporting_cutoff, save_reporting_cutoff
from src.sqlCompile_storage import read_database
from src.sqlCompile_viewer import build_viewer_payload, main, publish_viewer


@pytest.fixture
def reporting_host(tmp_path, monkeypatch):
    path = tmp_path / "host.json"
    path.write_text(json.dumps({
        "database": str(tmp_path / "data.sqlite"), "manual_status": str(tmp_path / "manual.csv"),
        "name_choices": str(tmp_path / "names.csv"), "name_rechecks": str(tmp_path / "rechecks.csv"),
        "zero_member_periods": str(tmp_path / "zero.csv"), "reporting_settings": str(tmp_path / "reporting.json"),
    }), encoding="utf-8")
    config = load_host_config(path)
    # Full regular-term inventory, plus the owner's initial-only Fall 2026 roster.
    terms = [f"{season} {year}" for year in range(2020, 2027) for season in ("Spring", "Fall")
             if (year, season) != (2020, "Spring")]
    rows = [[term, "Alpha", "PRIVATE-1", "N" if term == "Fall 2020" else "A"] for term in terms]
    rows.extend([["Fall 2025", "Alpha", "PRIVATE-2", "N"], ["Spring 2026", "Alpha", "PRIVATE-2", "A"],
                 ["Fall 2026", "Alpha", "PRIVATE-3", "N"]])
    inventory = pd.DataFrame([{"Semester": term, "Chapter": "Alpha", "Source File": f"PRIVATE-{term}.xlsx",
                               "Roster Pass": "initial" if term == "Fall 2026" else "final", "Student Rows": 1}
                              for term in terms])
    write_sqlite(pd.DataFrame(rows, columns=OUTPUT_COLUMNS), config.database, roster_inventory=inventory,
                 compile_issues=pd.DataFrame(), source_file_count=len(terms))
    read_manual_status_rows(config.manual_status)
    config.zero_member_periods.write_text("Chapter,Start Semester,End Semester,Notes\n", encoding="utf-8")
    save_reporting_cutoff("Spring 2026", config.reporting_settings)
    monkeypatch.setenv("FSL_DASHBOARD_HOST_CONFIG", str(path))
    return config, path


def load_cutoff_tables(config, cutoff="Spring 2026"):
    return load_dashboard_tables(database_path=config.database, manual_status_file=config.manual_status,
                                 duplicate_name_resolution_file=config.name_choices, duplicate_name_recheck_file=config.name_rechecks,
                                 zero_member_periods_file=config.zero_member_periods, reporting_cutoff=cutoff,
                                 create_review_files=False)


def test_confirmed_initial_cutoff():
    assert read_reporting_cutoff(REPORTING_EXAMPLE) == "Spring 2026"


@pytest.mark.parametrize("value", [None, "", 2026, "Unknown", "Spring 26", "2026", "Spring 0000", "Fall 2026<script>"])
def test_invalid_cutoff_is_not_silently_inferred(value):
    with pytest.raises(ValueError, match="Reporting cutoff"):
        normalize_reporting_cutoff(value)


def test_settings_are_explicit_and_custom_missing_files_fail(tmp_path):
    path = tmp_path / "settings.json"
    with pytest.raises(FileNotFoundError):
        read_reporting_cutoff(path)
    save_reporting_cutoff("Fall 2026", path)
    assert read_reporting_cutoff(path) == "Fall 2026"
    path.write_text('{"wrong_field":"Spring 2026"}', encoding="utf-8")
    with pytest.raises(ValueError):
        read_reporting_cutoff(path)


def test_cutoff_excludes_initial_rosters_and_future_corrections_without_deleting_them(reporting_host):
    config, _ = reporting_host
    append_manual_status_rows(pd.DataFrame([
        {"Student ID": "PRIVATE-1", "Semester": "Fall 2026", "Chapter": "Alpha", "Status": "G"},
        {"Student ID": "PRIVATE-2", "Semester": "Spring 2032", "Chapter": "Alpha", "Status": "RS"},
    ]), config.manual_status)
    original = config.database.read_bytes(), config.manual_status.read_bytes()
    tables = load_cutoff_tables(config)
    payload = build_viewer_payload(tables)
    assert payload["dataThrough"] == "Spring 2026"
    assert "Fall 2026" not in payload["semesters"]
    assert not tables.timeline["Semester"].isin(["Fall 2026", "Spring 2032"]).any()
    assert len(tables.manual_rows) == 2  # Future corrections remain available to the owner's editor.
    assert ["Fall 2020", "Alpha", 6, "Active", 1] in payload["units"]
    assert ["Fall 2025", "Alpha", 2, "Future", 1] in payload["units"]
    assert ["Fall 2025", "Alpha", 6, "Future", 1] in payload["units"]
    assert not any(row[3] in {"Graduated", "Dropped/Resigned"} for row in payload["units"])
    assert original == (config.database.read_bytes(), config.manual_status.read_bytes())
    advanced = build_viewer_payload(load_cutoff_tables(config, "Fall 2026"))
    assert "Fall 2026" in advanced["semesters"]
    assert sum(row[4] for row in advanced["units"] if row[2] == 1) == 3


def test_milestone_builder_uses_cutoff_even_with_unfiltered_future_timeline(reporting_host):
    config, _ = reporting_host
    tables = load_cutoff_tables(config)
    timeline = pd.concat([tables.timeline, pd.DataFrame([{
        "Cohort Semester": "Fall 2025", "Student ID": "PRIVATE-2", "Semester": "Spring 2032",
        "Status Code": "G", "Source": "manual_status", "Included In Outcome": "Yes",
    }])], ignore_index=True)
    result = build_sql_compile_milestone_dashboard(timeline, tables.outcomes, ["Fall 2025"], reporting_cutoff="Spring 2026")
    assert result["detail_frame"]["P&G Outcome Bucket"].tolist() == ["Active"] + ["Future"] * 5
    assert "eligible students" in result["chart_frame"].iloc[0]["Milestone"]


def test_new_cutoff_must_have_roster_evidence_before_it_is_saved(reporting_host, tmp_path):
    config, host_path = reporting_host
    with pytest.raises(SystemExit) as exc:
        main(["--host-config", str(host_path), "--cutoff", "Spring 2027", "--output", str(tmp_path / "viewer.html")])
    assert exc.value.code == 1
    assert read_reporting_cutoff(config.reporting_settings) == "Spring 2026"
    assert not (tmp_path / "viewer.html").exists()


def test_clean_publication_reports_deferred_records_and_no_identifiers(reporting_host, tmp_path):
    config, _ = reporting_host
    output = tmp_path / "shared" / "dashboard.html"
    published, students = publish_viewer(config, output)
    assert students == 2
    assert "PRIVATE-" not in published.read_text(encoding="utf-8")
    payload = read_previous_publication(output)
    assert payload["dataThrough"] == "Spring 2026"
    report = json.loads((config.database.parent / "publication_checks" / "dashboard.json").read_text())
    assert report["warnings"] == []
    assert report["skipped_sheet_file_issues"] == 0
    assert report["deferred_roster_rows"] == 2
    assert not list(output.parent.glob("*.json"))  # Owner's checks never enter the shared viewer folder.


def test_quality_warnings_require_acknowledgment_and_check_only_does_not_publish(reporting_host, tmp_path):
    config, _ = reporting_host
    output = tmp_path / "viewer.html"
    publish_viewer(config, output)
    previous = output.read_bytes()
    append_manual_status_rows(pd.DataFrame([{
        "Student ID": "PRIVATE-1", "Semester": "Spring 2024", "Chapter": "Alpha", "Status": "G",
    }]), config.manual_status)
    with pytest.raises(ValueError, match="paused for review"):
        publish_viewer(config, output)
    assert output.read_bytes() == previous
    publish_viewer(config, output, check_only=True)
    assert output.read_bytes() == previous
    seen = []
    publish_viewer(config, output, review_check=lambda report: seen.append(report) or True)
    assert seen[0]["large_changes"]
    assert output.read_bytes() != previous
    assert ["Fall 2020", "Alpha", 4, "Graduated", 1] in read_previous_publication(output)["units"]


def test_changes_during_review_do_not_overwrite_viewer(reporting_host, tmp_path):
    config, _ = reporting_host
    output = tmp_path / "viewer.html"
    publish_viewer(config, output)
    original = output.read_bytes()
    def review(report):
        save_reporting_cutoff("Fall 2026", config.reporting_settings)
        return True
    with pytest.raises(RuntimeError, match="changed"):
        publish_viewer(config, output, review_check=review)
    assert output.read_bytes() == original


def test_compiler_audit_records_and_clears_issues(reporting_host):
    config, _ = reporting_host
    frame = read_sql_compile_table(config.database)
    issues = pd.DataFrame([{"exception_type": "open_error", "source_file": "PRIVATE-file.xlsx", "source_sheet": "", "details": "Cannot open"}])
    write_sqlite(frame, config.database, compile_issues=issues, source_file_count=17)
    tables = load_cutoff_tables(config)
    report = build_publication_check(config, tables, build_viewer_payload(tables), None)
    assert report["skipped_sheet_file_issues"] == 1
    assert report["source_files"] == 17
    assert any("skipped" in warning for warning in report["warnings"])
    with read_database(config.database) as connection:
        assert connection.execute(f'SELECT COUNT(*) FROM "{COMPILE_ISSUES_TABLE}"').fetchone()[0] == 1
    write_sqlite(frame, config.database, compile_issues=pd.DataFrame(), source_file_count=17)
    with read_database(config.database) as connection:
        assert connection.execute(f'SELECT "Issue Count" FROM "{COMPILE_AUDIT_TABLE}"').fetchone()[0] == 0
        assert connection.execute(f'SELECT COUNT(*) FROM "{COMPILE_ISSUES_TABLE}"').fetchone()[0] == 0


def test_legacy_audit_missing_is_unknown_not_zero(reporting_host):
    config, _ = reporting_host
    from src.sqlCompile_storage import atomic_database_update
    with atomic_database_update(config.database) as connection:
        connection.execute(f'DROP TABLE "{COMPILE_AUDIT_TABLE}"')
    tables = load_cutoff_tables(config)
    report = build_publication_check(config, tables, build_viewer_payload(tables), None)
    assert report["skipped_sheet_file_issues"] is None
    assert any("unavailable" in warning for warning in report["warnings"])


def test_coverage_exceptions_and_unresolved_names_are_reported(reporting_host):
    config, _ = reporting_host
    rows = read_sql_compile_table(config.database)
    inventory = pd.DataFrame([
        {"Semester": term, "Chapter": chapter, "Student Rows": 1}
        for chapter in ["Alpha", "Beta"] for term in ["Fall 2025", "Spring 2026"]
        if (chapter, term) != ("Beta", "Spring 2026")
    ])
    names = pd.DataFrame([{"Student ID": "PRIVATE-1", "Student Name": name, "Observation Count": 1}
                          for name in ["PRIVATE-NAME-1", "PRIVATE-NAME-2"]])
    write_sqlite(rows, config.database, roster_inventory=inventory, student_name_observations=names,
                 compile_issues=pd.DataFrame(), source_file_count=3)
    tables = load_cutoff_tables(config)
    report = build_publication_check(config, tables, build_viewer_payload(tables), None)
    assert report["unresolved_duplicate_ids"] == 1
    assert report["chapter_roster_gaps"] == [{"Chapter": "Beta", "Semester": "Spring 2026"}]
    config.zero_member_periods.write_text("Chapter,Start Semester,End Semester,Notes\nBeta,Spring 2026,Spring 2026,Recognized zero members\n", encoding="utf-8")
    report = build_publication_check(config, tables, build_viewer_payload(tables), None)
    assert not report["chapter_roster_gaps"]


def test_large_change_thresholds():
    previous = {"dataThrough": "Spring 2026", "units": [["Fall 2020", "Alpha", 1, "Active", 100]]}
    small = {"dataThrough": "Spring 2026", "units": [["Fall 2020", "Alpha", 1, "Active", 96], ["Fall 2020", "Alpha", 1, "Graduated", 4]]}
    assert publication_changes(small, previous) == []
    large = {"dataThrough": "Spring 2026", "units": [["Fall 2020", "Alpha", 1, "Active", 95], ["Fall 2020", "Alpha", 1, "Graduated", 5]]}
    assert len(publication_changes(large, previous)) == 2


def test_shared_dashboard_uses_same_cutoff_and_exposes_no_cutoff_editor(reporting_host, monkeypatch):
    monkeypatch.setenv("FSL_DASHBOARD_SHARED", "1")
    app = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=30).run()
    assert not app.exception and not app.error
    tables = app.session_state["sql_compile_shared_snapshot"][2]
    assert tables.reporting_cutoff == "Spring 2026"
    assert "Fall 2026" not in tables.selected_semesters
    assert not any(widget.label == "Latest complete roster semester" for widget in app.selectbox)
    assert any("Data complete through Spring 2026" in widget.value for widget in app.caption)


def test_owner_must_confirm_cutoff_change_and_dashboard_cache_refreshes(reporting_host, monkeypatch):
    config, _ = reporting_host
    monkeypatch.delenv("FSL_DASHBOARD_SHARED", raising=False)
    app = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=30).run()
    assert not app.exception and not app.error
    assert app.metric[0].value == "2"
    next(widget for widget in app.selectbox if widget.label == "Latest complete roster semester").set_value("Fall 2026")
    next(button for button in app.button if button.label == "Save cutoff").click().run()
    assert any("Confirm roster completeness" in notice.value for notice in app.error)
    assert read_reporting_cutoff(config.reporting_settings) == "Spring 2026"
    next(widget for widget in app.checkbox if widget.label == "I confirm the roster set is complete through this semester").check()
    next(button for button in app.button if button.label == "Save cutoff").click().run()
    assert not app.exception and not app.error
    assert read_reporting_cutoff(config.reporting_settings) == "Fall 2026"
    assert app.metric[0].value == "3"
