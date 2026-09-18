import json
import shutil
import subprocess

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from src.build_master_roster import normalize_chapter_name
from src.path_config import ROOT
from src.sqlCompile import OUTPUT_COLUMNS, write_sqlite
from src.sqlCompile_cohort import MANUAL_STATUS_COLUMNS, ZERO_MEMBER_PERIOD_COLUMNS, read_manual_status_rows
from src.sqlCompile_councils import (
    COUNCIL_ORGANIZATIONS, COUNCILS, CHAPTER_COUNCILS, UNMAPPED_COUNCIL,
    build_unmapped_organization_review, chapters_for_councils, council_for_chapter,
)
from src.sqlCompile_dashboard import build_sql_compile_milestone_dashboard, load_dashboard_tables
from src.sqlCompile_host import load_host_config
from src.sqlCompile_reporting import save_reporting_cutoff
from src.sqlCompile_viewer import ASSETS, build_viewer_payload, render_viewer


NEW_ASSIGNMENTS = [
    ("Alpha Epsilon Pi", "IFC"), ("Alpha Psi Lambda", "MGC"),
    ("Alpha Tau Omega", "IFC"), ("Delta Xi Nu", "MGC"),
    ("Kappa Alpha", "IFC"), ("Order of Omega", "Other"),
    ("Phi Iota Alpha", "MGC"), ("Phi Kappa Psi", "IFC"),
    ("Sigma Iota Alpha", "MGC"), ("Sigma Tau Gamma", "IFC"),
    ("Beta Upsilon Chi", "IFC"), ("Omega Phi Gamma", "MGC"),
    ("Phi Delta Delta", "Other"),
]


def test_council_assignments_match_all_55_owner_supplied_names():
    assert {council: len(chapters) for council, chapters in COUNCIL_ORGANIZATIONS.items()} == {"IFC": 24, "MGC": 12, "NPHC": 9, "PHC": 8, "Other": 2}
    assert len(CHAPTER_COUNCILS) == 55
    for council, names in COUNCIL_ORGANIZATIONS.items():
        for name in names:
            assert council_for_chapter(name) == council
            assert council_for_chapter(normalize_chapter_name(name)) == council
            assert council_for_chapter(f"  {name.upper()}  ") == council


@pytest.mark.parametrize("name,council", [
    ("Kappa Delta Chi Sorority Inc", "MGC"), ("Alpha Kappa Alpha (Sigma Epsilon)", "NPHC"),
    ("Phi Kappa Tau-Gamma Psi", "IFC"), ("Kappa Alpha Order", "IFC"),
    ("Kappa Alpha Psi Fraternity, Inc.", "NPHC"), ("Iota Phi Theta", "NPHC"),
    ("Alpha Sigma Rho", "MGC"), ("Alpha Sigma Phi", "IFC"),
    ("Alpha Gamma Delta", "PHC"), ("Sigma Iota Alpha (Sigma Iota Alpha)", "MGC"),
    ("Theta Xi", UNMAPPED_COUNCIL), ("Unknown", UNMAPPED_COUNCIL), (None, UNMAPPED_COUNCIL),
] + NEW_ASSIGNMENTS)
def test_council_normalization_does_not_guess_unlisted_organizations(name, council):
    assert council_for_chapter(name) == council


def test_all_councils_includes_unmapped_but_specific_groups_do_not():
    chapters = ["Theta Xi", "Sigma Iota Alpha", "Alpha Sigma Phi", "Zeta Tau Alpha", "Iota Phi Theta", "Order of Omega", "Phi Delta Delta"]
    assert chapters_for_councils(chapters, None) == chapters
    assert chapters_for_councils(chapters, ["IFC", "PHC"]) == ["Alpha Sigma Phi", "Zeta Tau Alpha"]
    assert chapters_for_councils(chapters, []) == []
    assert "Theta Xi" not in chapters_for_councils(chapters, COUNCILS)
    assert chapters_for_councils(chapters, ["Other"]) == ["Order of Omega", "Phi Delta Delta"]
    assert chapters_for_councils(chapters, ["MGC", "Other"]) == ["Sigma Iota Alpha", "Order of Omega", "Phi Delta Delta"]


@pytest.fixture
def council_host(tmp_path, monkeypatch):
    path = tmp_path / "host.json"
    path.write_text(json.dumps({
        "database": str(tmp_path / "data.sqlite"), "manual_status": str(tmp_path / "manual.csv"),
        "name_choices": str(tmp_path / "names.csv"), "name_rechecks": str(tmp_path / "rechecks.csv"),
        "zero_member_periods": str(tmp_path / "zero.csv"), "reporting_settings": str(tmp_path / "reporting.json"),
    }), encoding="utf-8")
    config = load_host_config(path)
    rows = []
    for chapter, student, semester, status in [
        ("Alpha Sigma Phi", "PRIVATE-IFC-1", "Fall 2020", "G"),
        ("Phi Gamma Delta", "PRIVATE-IFC-2", "Fall 2021", "A"),
        ("Zeta Tau Alpha", "PRIVATE-PHC", "Fall 2025", "A"),
        ("Sigma Lambda Gamma Sorority, Inc.", "PRIVATE-MGC", "Fall 2025", "A"),
        ("Iota Phi Theta Fraternity, Inc.", "PRIVATE-NPHC", "Fall 2020", "RS"),
        ("Sigma Iota Alpha", "PRIVATE-MGC-2", "Fall 2020", "CK"),
        ("Order of Omega", "PRIVATE-OTHER-1", "Fall 2020", "G"),
        ("Phi Delta Delta", "PRIVATE-OTHER-2", "Fall 2025", "A"),
        ("Delta Beta", "PRIVATE-UNMAPPED", "Fall 2020", "CK"),
    ]:
        rows.extend([[semester, chapter, student, "N"], ["Spring 2026", chapter, student, status]])
    rows.append(["Fall 2015", "Theta Xi", "PRIVATE-NO-COHORT", "A"])
    rows.append(["Fall 2026", "Alpha Kappa Lambda", "PRIVATE-FUTURE", "N"])
    compiled = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    inventory = compiled[["Semester", "Chapter"]].drop_duplicates().assign(**{"Student Rows": 1, "Roster Pass Priority": 3})
    write_sqlite(compiled, config.database, roster_inventory=inventory, compile_issues=pd.DataFrame(), source_file_count=len(inventory))
    read_manual_status_rows(config.manual_status)
    config.zero_member_periods.write_text("Chapter,Start Semester,End Semester,Notes\n", encoding="utf-8")
    save_reporting_cutoff("Spring 2026", config.reporting_settings)
    monkeypatch.setenv("FSL_DASHBOARD_HOST_CONFIG", str(path))
    monkeypatch.delenv("FSL_DASHBOARD_SHARED", raising=False)
    tables = load_dashboard_tables(database_path=config.database, manual_status_file=config.manual_status,
                                   duplicate_name_resolution_file=config.name_choices, duplicate_name_recheck_file=config.name_rechecks,
                                   zero_member_periods_file=config.zero_member_periods, reporting_cutoff="Spring 2026", create_review_files=False)
    return config, tables


def test_browser_council_filters_match_python_for_all_milestones_and_breakdowns(council_host):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required for viewer-model tests")
    _, tables = council_host
    payload = build_viewer_payload(tables)
    selections = [
        {"councils": councils, "semesters": semesters, "chapters": chapters, "breakdown": breakdown, "years": years}
        for councils in [None, [], ["IFC"], ["MGC"], ["NPHC"], ["PHC"], ["Other"], ["IFC", "PHC"], ["MGC", "Other"], list(COUNCILS)]
        for semesters in [payload["semesters"], ["Fall 2025"]]
        for chapters in [payload["chapters"], ["Phi Gamma Delta"], ["Sigma Iota Alpha"], ["Order of Omega"], ["Delta Beta"]]
        for breakdown, years in [("Overall", [1, 2, 3, 4, 5, 6]), ("Semester joined", [6]), ("Chapter joined", [1])]
    ]
    script = """
      const {aggregateViewer} = require(process.argv[1]);
      const input = JSON.parse(require('fs').readFileSync(0, 'utf8'));
      process.stdout.write(JSON.stringify(input.selections.map(s => aggregateViewer(
        input.payload, s.semesters, s.chapters, s.years, s.breakdown, s.councils))));
    """
    result = subprocess.run([node, "-e", script, str(ASSETS / "model.js")],
                            input=json.dumps({"payload": payload, "selections": selections}), text=True,
                            capture_output=True, check=True)
    for selection, actual in zip(selections, json.loads(result.stdout)):
        expected = build_sql_compile_milestone_dashboard(
            tables.timeline, tables.outcomes, selection["semesters"],
            selected_chapters=chapters_for_councils(selection["chapters"], selection["councils"]),
            reporting_cutoff="Spring 2026", chart_milestone_offsets=selection["years"], chart_breakdown=selection["breakdown"],
        )
        expected_rows = {(row["Chart Group"], row["Milestone Name"], row["Outcome"]):
                         (row["Count"], row["Share"], row["Eligible Students"], row["Future Students"])
                         for row in expected["chart_frame"].to_dict("records")}
        actual_rows = {(row["group"], f"{row['year']} Year", row["outcome"]):
                       (row["count"], row["share"], row["eligible"], row["future"]) for row in actual["rows"]}
        assert actual_rows == expected_rows, selection
        assert actual["students"] == expected["meta"]["students"], selection


def test_community_payload_contains_mapping_but_not_owner_review(council_host):
    _, tables = council_host
    payload = build_viewer_payload(tables)
    assert payload["councils"] == list(COUNCILS)
    assert payload["chapterCouncils"]["Iota Phi Theta Fraternity, Inc."] == "NPHC"
    assert payload["chapterCouncils"]["Sigma Iota Alpha"] == "MGC"
    assert payload["chapterCouncils"]["Order of Omega"] == "Other"
    assert payload["chapterCouncils"]["Phi Delta Delta"] == "Other"
    assert payload["chapterCouncils"]["Delta Beta"] == UNMAPPED_COUNCIL
    html = render_viewer(payload)
    assert 'id="council-mode"' in html
    assert "Organization Review" not in html
    assert "Evidence Notes" not in html
    assert "PRIVATE-" not in html
    assert "Theta Xi" not in html  # No new-member cohort, so owner inventory only.
    assert "Alpha Kappa Lambda" not in html  # Later initial roster, outside cutoff.


def test_unmapped_review_distinguishes_recorded_inferred_and_no_removal_evidence():
    compiled = pd.DataFrame([
        ["Fall 2015", "Theta Xi", "PRIVATE-1", "A"],
        ["Fall 2025", "Phi Alpha Theta", "PRIVATE-2", "CK"],
        ["Spring 2026", "Phi Alpha Theta", "PRIVATE-2", "A"],
        ["Spring 2026", "Alpha Kappa Lambda", "PRIVATE-3", "A"],
        ["Fall 2026", "Delta Beta", "PRIVATE-4", "N"],
        ["Spring 2026", "Alpha Sigma Phi", "PRIVATE-KNOWN", "A"],
    ], columns=OUTPUT_COLUMNS)
    inventory = compiled[["Semester", "Chapter"]].assign(**{"Student Rows": 1, "Roster Pass Priority": 3})
    manual = pd.DataFrame([
        {"Semester": "Fall 2026", "Chapter": "Alpha Kappa Lambda", "Student ID": "PRIVATE-3", "Status": "CK"},
        {"Semester": "Spring 2026", "Chapter": "", "Student ID": "PRIVATE-KNOWN", "Status": "G"},
    ], columns=MANUAL_STATUS_COLUMNS).fillna("")
    zero = pd.DataFrame(columns=ZERO_MEMBER_PERIOD_COLUMNS)
    original = compiled.copy(deep=True)
    review = build_unmapped_organization_review(compiled, inventory, manual, zero, "Spring 2026").set_index("Organization")
    assert set(review.index) == {"Theta Xi", "Phi Alpha Theta", "Alpha Kappa Lambda", "Delta Beta"}
    assert review.loc["Theta Xi", "Chapter Kicked Evidence"] == "Inferred roster disappearance"
    assert review.loc["Phi Alpha Theta", "Chapter Kicked Evidence"] == "Recorded CK status"
    assert review.loc["Alpha Kappa Lambda", "Chapter Kicked Evidence"] == "None through cutoff"
    assert review.loc["Delta Beta", "Records After Cutoff"] == "Yes"
    assert review.loc["Delta Beta", "Chapter Kicked Evidence"] == "None through cutoff"
    assert "PRIVATE-" not in review.to_csv()
    pd.testing.assert_frame_equal(compiled, original)


def test_new_assignments_leave_owner_review_without_changing_roster_outcomes():
    compiled = pd.DataFrame([
        ["Spring 2026", chapter, f"PRIVATE-{index}", "CK"]
        for index, (chapter, _) in enumerate(NEW_ASSIGNMENTS)
    ] + [["Spring 2026", "Theta Xi", "PRIVATE-UNMAPPED", "CK"]], columns=OUTPUT_COLUMNS)
    inventory = compiled[["Semester", "Chapter"]].assign(**{"Student Rows": 1})
    original = compiled.copy(deep=True)
    review = build_unmapped_organization_review(
        compiled, inventory, pd.DataFrame(columns=MANUAL_STATUS_COLUMNS),
        pd.DataFrame(columns=ZERO_MEMBER_PERIOD_COLUMNS), "Spring 2026",
    )
    assert review["Organization"].tolist() == ["Theta Xi"]
    pd.testing.assert_frame_equal(compiled, original)


def test_zero_member_exception_is_not_inferred_removal_and_empty_review_is_valid():
    rows = pd.DataFrame([
        ["Fall 2025", "Theta Xi", "PRIVATE-1", "A"],
        ["Spring 2026", "Alpha Kappa Alpha", "PRIVATE-2", "A"],
    ], columns=OUTPUT_COLUMNS)
    inventory = rows[["Semester", "Chapter"]].assign(**{"Student Rows": 1})
    zero = pd.DataFrame([["Theta Xi", "Spring 2026", "Spring 2026", "Recognized but zero members"]], columns=ZERO_MEMBER_PERIOD_COLUMNS)
    manual = pd.DataFrame(columns=MANUAL_STATUS_COLUMNS)
    review = build_unmapped_organization_review(rows, inventory, manual, zero, "Spring 2026")
    assert review["Organization"].tolist() == ["Theta Xi"]
    assert review["Chapter Kicked Evidence"].tolist() == ["None through cutoff"]
    assert build_unmapped_organization_review(rows.iloc[0:0], inventory.iloc[0:0], manual, zero, "Spring 2026").empty


def test_owner_filters_chart_and_checker_and_can_review_all_unmapped_organizations(council_host):
    _, _ = council_host
    app = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=30).run()
    assert not app.exception and not app.error
    assert app.metric[0].value == "9"
    next(widget for widget in app.selectbox if widget.label == "Council").set_value("IFC").run()
    assert not app.exception and not app.error
    assert app.metric[0].value == "2"
    assert app.metric[2].value == "IFC"
    app.radio(key="sql_compile_dashboard_section").set_value("Manual Checker").run()
    assert not app.exception and not app.error
    queue = app.session_state["sql_compile_manual_checker_rows"]
    assert set(queue["Cohort Chapter"]) == {"Alpha Sigma Phi", "Phi Gamma Delta"}
    app.radio(key="sql_compile_dashboard_section").set_value("Outcome Mix").run()
    assert not app.exception and not app.error
    assert app.metric[0].value == "2"
    app.radio(key="sql_compile_dashboard_section").set_value("Organization Review").run()
    assert not app.exception and not app.error
    assert set(app.dataframe[0].value["Organization"]) == {"Delta Beta", "Theta Xi", "Alpha Kappa Lambda"}
    assert not any(widget.label == "Council" for widget in app.selectbox)


def test_switching_council_resets_incompatible_chapter_filter_and_empty_group_stays_empty(council_host):
    app = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=30).run()
    next(widget for widget in app.selectbox if widget.label == "Council").set_value("IFC").run()
    next(widget for widget in app.radio if widget.label == "P&G chapter selection").set_value("Chapter group").run()
    next(widget for widget in app.multiselect if widget.label == "P&G chapters").set_value(["Phi Gamma Delta"]).run()
    assert app.metric[0].value == "1"
    next(widget for widget in app.selectbox if widget.label == "Council").set_value("PHC").run()
    assert not app.exception and not app.error
    assert app.metric[0].value == "1"
    assert next(widget for widget in app.multiselect if widget.label == "P&G chapters").value == ["Zeta Tau Alpha"]
    next(widget for widget in app.selectbox if widget.label == "Council").set_value("Council group").run()
    next(widget for widget in app.multiselect if widget.label == "Councils").set_value([]).run()
    assert not app.exception and not app.error
    assert app.metric[0].value == "0"


def test_shared_dashboard_has_council_filter_but_no_owner_review(council_host, monkeypatch):
    monkeypatch.setenv("FSL_DASHBOARD_SHARED", "1")
    app = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=30).run()
    assert not app.exception and not app.error
    assert "Organization Review" not in app.radio(key="sql_compile_dashboard_section").options
    next(widget for widget in app.selectbox if widget.label == "Council").set_value("NPHC").run()
    assert not app.exception and not app.error
    assert app.metric[0].value == "1"
    assert app.metric[2].value == "NPHC"


def test_other_group_and_combined_councils_filter_the_dashboard(council_host):
    app = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=30).run()
    next(widget for widget in app.selectbox if widget.label == "Council").set_value("Other").run()
    assert not app.exception and not app.error
    assert app.metric[0].value == "2"
    assert app.metric[2].value == "Other"
    app.radio(key="sql_compile_dashboard_section").set_value("Manual Checker").run()
    assert not app.exception and not app.error
    assert set(app.session_state["sql_compile_manual_checker_rows"]["Cohort Chapter"]) == {"Order of Omega", "Phi Delta Delta"}
    next(widget for widget in app.selectbox if widget.label == "Council").set_value("Council group").run()
    next(widget for widget in app.multiselect if widget.label == "Councils").set_value(["MGC", "Other"]).run()
    assert not app.exception and not app.error
    assert app.metric[0].value == "4"
