import json
import sys

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from src.path_config import ROOT
from src.sqlCompile import OUTPUT_COLUMNS, write_sqlite
from src.sqlCompile_cohort import append_manual_status_rows, read_manual_status_rows
from src.sqlCompile_dashboard import read_duplicate_name_recheck_rows, read_duplicate_name_resolution_rows
from src.sqlCompile_host import data_revision, load_host_config


@pytest.fixture
def shared_host(tmp_path, monkeypatch):
    database = tmp_path / "data.sqlite"
    write_sqlite(pd.DataFrame([
        ["Fall 2020", "Alpha", "TEST01", "N"],
        ["Spring 2021", "Alpha", "TEST01", "A"],
        ["Spring 2026", "Alpha", "TEST02", "N"],
    ], columns=OUTPUT_COLUMNS), database)
    payload = {
        "database": str(database), "manual_status": str(tmp_path / "manual.csv"),
        "name_choices": str(tmp_path / "names.csv"), "name_rechecks": str(tmp_path / "rechecks.csv"),
        "zero_member_periods": str(tmp_path / "zero.csv"), "address": "127.0.0.1",
    }
    path = tmp_path / "host.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    config = load_host_config(path)
    read_manual_status_rows(config.manual_status)
    read_duplicate_name_resolution_rows(config.name_choices)
    read_duplicate_name_recheck_rows(config.name_rechecks)
    config.zero_member_periods.write_text("Chapter,Start Semester,End Semester,Notes\n", encoding="utf-8")
    monkeypatch.setenv("FSL_DASHBOARD_SHARED", "1")
    monkeypatch.setenv("FSL_DASHBOARD_HOST_CONFIG", str(path))
    return config, path


@pytest.mark.parametrize("settings", [
    {"port": 80}, {"port": "8502"}, {"port": True}, {"port": 65536},
    {"address": ""}, {"address": None}, {"manual_status": ""},
    {"manual_status": "same.csv", "name_choices": "same.csv"}, {"unexpected": 1},
])
def test_host_config_rejects_bad_settings(tmp_path, settings):
    path = tmp_path / "host.json"
    path.write_text(json.dumps(settings), encoding="utf-8")
    with pytest.raises(ValueError):
        load_host_config(path)


def test_host_config_paths_are_relative_to_project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "host.json"
    path.write_text('{"database":"output/test.sqlite","port":8510}', encoding="utf-8")
    config = load_host_config(path)
    assert config.database == ROOT / "output" / "test.sqlite"
    assert config.port == 8510
    with pytest.raises(FileNotFoundError):
        load_host_config(tmp_path / "missing.json")


def test_shared_launcher_keeps_protections(shared_host, monkeypatch):
    import run_sql_compile_dashboard as launcher

    _, path = shared_host
    monkeypatch.setattr(sys, "argv", ["run_sql_compile_dashboard.py", "--shared", "--host-config", str(path)])
    captured = []
    monkeypatch.setattr(launcher.stcli, "main", lambda: captured.extend(sys.argv))
    with pytest.raises(SystemExit):
        launcher.main()
    assert captured[captured.index("--server.address") + 1] == "127.0.0.1"
    assert captured[captured.index("--server.enableCORS") + 1] == "true"
    assert captured[captured.index("--server.enableXsrfProtection") + 1] == "true"
    assert captured[captured.index("--theme.base") + 1] == "light"
    assert captured[captured.index("--client.toolbarMode") + 1] == "viewer"


def test_local_launcher_preserves_existing_arguments(monkeypatch):
    import run_sql_compile_dashboard as launcher

    monkeypatch.setattr(sys, "argv", ["run_sql_compile_dashboard.py", "--server.port", "8503"])
    captured = []
    monkeypatch.setattr(launcher.stcli, "main", lambda: captured.extend(sys.argv))
    with pytest.raises(SystemExit):
        launcher.main()
    assert captured[-2:] == ["--server.port", "8503"]
    assert "--server.address" not in captured


def test_shared_ui_keeps_snapshot_until_refresh(shared_host):
    config, _ = shared_host
    app = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=30).run()
    assert not app.exception and not app.error
    assert not app.sidebar.text_input
    assert "Run sqlCompile" not in [button.label for button in app.button]
    assert "Write Report Files" not in [button.label for button in app.button]
    original_revision = app.session_state["sql_compile_shared_snapshot"][1]
    append_manual_status_rows(pd.DataFrame([{
        "Student ID": "TEST01", "Semester": "Spring 2024", "Status": "G", "Chapter": "Alpha",
    }]), config.manual_status)
    assert data_revision(config.data_paths) != original_revision
    app.radio(key="sql_compile_dashboard_section").set_value("Manual Checker").run()
    assert not app.exception and not app.error
    assert any("Saved records changed" in notice.value for notice in app.info)
    assert app.session_state["sql_compile_shared_snapshot"][1] == original_revision
    assert not any(expander.label == "Reuse Legacy Manual Decisions" for expander in app.expander)
    assert app.session_state["sql_compile_shared_snapshot"][2].manual_rows.empty
    for button in app.sidebar.button:
        if button.label == "Refresh Dashboard Data":
            button.click().run()
            break
    assert not app.exception and not app.error
    assert app.session_state["sql_compile_shared_snapshot"][2].manual_rows["Status"].tolist() == ["G"]
    for section in ["Manual Rows", "Outcome Mix", "Persistence & Graduation"]:
        app.radio(key="sql_compile_dashboard_section").set_value(section).run()
        assert not app.exception and not app.error


def test_shared_checker_rejects_other_reviewers_newer_decision(shared_host):
    config, _ = shared_host
    app = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=30).run()
    app.radio(key="sql_compile_dashboard_section").set_value("Manual Checker").run()
    queue_key = "sql_compile_manual_checker_rows"
    draft = app.session_state[queue_key].copy()
    selected = draft["Student ID"].eq("TEST01")
    draft.loc[selected, "Semester"] = "Spring 2024"
    draft.loc[selected, "Chapter"] = "Alpha"
    draft.loc[selected, "Status"] = "RS"
    app.session_state[queue_key] = draft
    app.run()
    append_manual_status_rows(pd.DataFrame([{
        "Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha",
        "Student ID": "TEST01", "Semester": "Spring 2024", "Status": "G", "Chapter": "Alpha",
    }]), config.manual_status)
    save = next(button for button in app.button if button.label == "Save All Ready")
    assert not save.disabled
    save.click().run()
    assert not app.exception and not app.error
    assert any("No rows were saved" in notice.value for notice in app.warning)
    assert read_manual_status_rows(config.manual_status)["Status"].tolist() == ["G"]
    assert app.session_state[queue_key].loc[selected, "Status"].tolist() == ["RS"]


def test_shared_checker_saves_without_reloading_chart(shared_host):
    config, _ = shared_host
    app = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=30).run()
    app.radio(key="sql_compile_dashboard_section").set_value("Manual Checker").run()
    original_revision = app.session_state["sql_compile_shared_snapshot"][1]
    queue_key = "sql_compile_manual_checker_rows"
    draft = app.session_state[queue_key].copy()
    selected = draft["Student ID"].eq("TEST01")
    draft.loc[selected, "Semester"] = "Spring 2024"
    draft.loc[selected, "Chapter"] = "Alpha"
    draft.loc[selected, "Status"] = "G"
    app.session_state[queue_key] = draft
    app.run()
    next(button for button in app.button if button.label == "Save All Ready").click().run()
    assert not app.exception and not app.error
    assert read_manual_status_rows(config.manual_status)["Status"].tolist() == ["G"]
    assert app.session_state["sql_compile_shared_snapshot"][1] == original_revision
    second = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=30).run()
    assert not second.exception and not second.error
    assert second.session_state["sql_compile_shared_snapshot"][2].manual_rows["Status"].tolist() == ["G"]
