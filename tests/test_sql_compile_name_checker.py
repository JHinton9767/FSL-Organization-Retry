import json

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from src.path_config import ROOT
from src.sqlCompile import OUTPUT_COLUMNS, write_sqlite
from src.sqlCompile_audit import read_change_history
from src.sqlCompile_dashboard import append_duplicate_name_resolution_rows, read_duplicate_name_resolution_rows
from src.sqlCompile_reporting import save_reporting_cutoff


@pytest.fixture
def name_case(tmp_path, monkeypatch):
    database = tmp_path / "data.sqlite"
    rows = pd.DataFrame([
        ["Fall 2020", "Alpha", "A01234567", "N"],
        ["Spring 2026", "Alpha", "A01234567", "G"],
        ["Fall 2021", "Beta", "A01234568", "N"],
        ["Spring 2026", "Beta", "A01234568", "A"],
    ], columns=OUTPUT_COLUMNS)
    observations = pd.DataFrame([
        ["A01234567", "Avery One", 1], ["A01234567", "Avery Two", 1],
        ["A01234568", "Morgan One", 1], ["A01234568", "Morgan Two", 1],
    ], columns=["Student ID", "Student Name", "Observation Count"])
    write_sqlite(rows, database, student_names=observations.drop_duplicates("Student ID"),
                 student_name_observations=observations)
    settings = {"database": str(database), **{
        key: str(tmp_path / filename) for key, filename in {
            "manual_status": "manual.csv", "name_choices": "names.csv", "name_rechecks": "rechecks.csv",
            "zero_member_periods": "zero.csv", "reporting_settings": "reporting.json",
            "university_records": "university.csv", "coverage_reviews": "coverage.csv",
        }.items()
    }}
    save_reporting_cutoff("Spring 2026", tmp_path / "reporting.json")
    config = tmp_path / "host.json"
    config.write_text(json.dumps(settings), encoding="utf-8")
    monkeypatch.setenv("FSL_DASHBOARD_HOST_CONFIG", str(config))
    return tmp_path


def open_checker():
    app = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=30).run()
    # The default degree measure previously bypassed the buried name resolver.
    app.radio(key="sql_compile_dashboard_section").set_value("Duplicate Name Checker").run()
    assert not app.exception and not app.error
    return app


def name_picker(app, student_id):
    return next(widget for widget in app.selectbox if widget.key and widget.key.startswith(f"sql_compile_dup_name_{student_id}_"))


def click_button(app, label):
    next(button for button in app.button if button.label == label).click().run()
    assert not app.exception and not app.error


@pytest.mark.parametrize("shared", [False, True])
def test_name_checker_saves_only_selected_names_and_allows_later_edit(name_case, monkeypatch, shared):
    monkeypatch.setenv("FSL_DASHBOARD_SHARED", "1" if shared else "0")
    app = open_checker()
    assert name_picker(app, "A01234567").value is None
    assert name_picker(app, "A01234568").value is None
    click_button(app, "Save Name Choices")
    assert read_duplicate_name_resolution_rows(name_case / "names.csv").empty

    name_picker(app, "A01234567").select("Avery Two")
    click_button(app, "Save Name Choices")
    saved = read_duplicate_name_resolution_rows(name_case / "names.csv")
    assert saved["Student ID"].tolist() == ["A01234567"]
    assert saved["Student Name"].tolist() == ["Avery Two"]
    assert name_picker(app, "A01234568").value is None
    assert not any(widget.key and widget.key.startswith("sql_compile_dup_name_A01234567_") for widget in app.selectbox)

    editor = next(widget for widget in app.dataframe if widget.key and widget.key.startswith("sql_compile_saved_name_editor_"))
    app.session_state[editor.key] = {
        "edited_rows": {0: {"Student Name": "Avery Verified", "Notes": "Verified against university record"}},
        "added_rows": [], "deleted_rows": [],
    }
    click_button(app, "Save Edited Names")
    assert read_duplicate_name_resolution_rows(name_case / "names.csv")["Student Name"].tolist() == ["Avery Verified"]
    assert len(read_change_history(name_case / "names.csv")) >= 3

    app.text_input(key="sql_compile_dup_search").set_value("Morgan").run()
    assert name_picker(app, "A01234568").value is None
    app.text_input(key="sql_compile_dup_search").set_value("no matching name").run()
    assert any("match your search" in message.value for message in app.info)
    app.radio(key="sql_compile_dashboard_section").set_value("Manual Checker").run()
    assert not app.exception and not app.error
    assert not any(button.label == "Save Name Choices" for button in app.button)


def test_name_checker_keeps_another_reviewers_newer_name(name_case, monkeypatch):
    monkeypatch.setenv("FSL_DASHBOARD_SHARED", "1")
    app = open_checker()
    name_picker(app, "A01234567").select("Avery One")
    append_duplicate_name_resolution_rows(pd.DataFrame([{
        "Student ID": "A01234567", "Student Name": "Avery Two", "Notes": "Reviewed elsewhere",
    }]), name_case / "names.csv")
    click_button(app, "Save Name Choices")
    assert any("No rows were saved" in message.value for message in app.warning)
    assert read_duplicate_name_resolution_rows(name_case / "names.csv")["Student Name"].tolist() == ["Avery Two"]
