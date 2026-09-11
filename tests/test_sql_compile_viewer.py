import json
import shutil
import subprocess
from html.parser import HTMLParser
from types import SimpleNamespace

import pandas as pd
import pytest

from src import sqlCompile_viewer as viewer
from src.sqlCompile import OUTPUT_COLUMNS, write_sqlite
from src.sqlCompile_cohort import append_manual_status_rows
from src.sqlCompile_dashboard import build_sql_compile_milestone_dashboard
from src.sqlCompile_host import load_host_config


@pytest.fixture
def cohort_tables():
    cohorts = ["Fall 2017", "Fall 2020", "Spring 2021", "Fall 2021", "Fall 2025"]
    students, history = [], []
    for cohort in cohorts:
        start = int(cohort.split()[1])
        for index, status in enumerate(["G", "RS", "I", "S", "I/S", "RV", "T", "CK", "EA", "A"]):
            student = f"PRIVATE-ID-{start}-{cohort.split()[0]}-{index}"
            students.append({"Cohort Semester": cohort, "Cohort Chapter": "Alpha" if index % 2 else "Beta",
                             "Student ID": student, "Student Name": "PRIVATE-NAME"})
            for semester, code, source in [(cohort, "N", "sqlCompile"),
                                           (f"Spring {start + 1}", status, "manual_status")]:
                history.append({"Cohort Semester": cohort, "Student ID": student, "Semester": semester,
                                "Status Code": code, "Source": source, "Included In Outcome": "Yes"})
    history.append({"Cohort Semester": "Fall 2025", "Student ID": "PRIVATE-HORIZON", "Semester": "Spring 2026",
                    "Status Code": "A", "Source": "sqlCompile", "Included In Outcome": "Yes"})
    return SimpleNamespace(timeline=pd.DataFrame(history), outcomes=pd.DataFrame(students), selected_semesters=cohorts)


@pytest.fixture
def publisher_config(tmp_path):
    database = tmp_path / "source.sqlite"
    write_sqlite(pd.DataFrame([
        ["Fall 2020", "Alpha", "PRIVATE-ID-1", "N"],
        ["Spring 2021", "Alpha", "PRIVATE-ID-1", "A"],
        ["Fall 2025", "Alpha", "PRIVATE-ID-2", "N"],
        ["Spring 2026", "Alpha", "PRIVATE-ID-2", "A"],
    ], columns=OUTPUT_COLUMNS), database)
    path = tmp_path / "host.json"
    path.write_text(json.dumps({"database": str(database), "manual_status": str(tmp_path / "manual.csv"),
                               "name_choices": str(tmp_path / "names.csv"), "name_rechecks": str(tmp_path / "rechecks.csv"),
                               "zero_member_periods": str(tmp_path / "zero.csv")}), encoding="utf-8")
    config = load_host_config(path)
    append_manual_status_rows(pd.DataFrame([{
        "Student ID": "PRIVATE-ID-1", "Semester": "Spring 2024", "Chapter": "Alpha", "Status": "G",
        "Notes": "PRIVATE-NOTE",
    }]), config.manual_status)
    config.zero_member_periods.write_text("Chapter,Start Semester,End Semester,Notes\n", encoding="utf-8")
    return config, path


def javascript_results(payload, selections):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for browser-model parity tests")
    code = """
        const {aggregateViewer} = require(process.argv[1]);
        const input = JSON.parse(require('fs').readFileSync(0, 'utf8'));
        process.stdout.write(JSON.stringify(input.selections.map(s =>
            aggregateViewer(input.payload, s.semesters, s.chapters, s.years, s.breakdown))));
    """
    result = subprocess.run([node, "-e", code, str(viewer.ASSETS / "model.js")],
                            input=json.dumps({"payload": payload, "selections": selections}),
                            text=True, capture_output=True, check=True)
    return json.loads(result.stdout)


def test_viewer_matches_python_dashboard_for_all_filters(cohort_tables):
    payload = viewer.build_viewer_payload(cohort_tables)
    selections = [
        {"semesters": semesters, "chapters": chapters, "years": years, "breakdown": breakdown}
        for semesters in [payload["semesters"], ["Fall 2021"], ["Fall 2017"], ["Fall 2025"],
                          ["Fall 2020", "Fall 2021", "Spring 2021"], []]
        for chapters in [payload["chapters"], ["Alpha"], []]
        for breakdown, years in [("Overall", [1, 2, 3, 4, 5, 6]), ("Overall", [4, 5, 6]),
                                 ("Semester joined", [6]), ("Chapter joined", [1]), ("Chapter joined", [6])]
    ]
    for selected, actual in zip(selections, javascript_results(payload, selections)):
        dashboard = build_sql_compile_milestone_dashboard(
            cohort_tables.timeline, cohort_tables.outcomes, selected["semesters"],
            selected_chapters=selected["chapters"], chart_breakdown=selected["breakdown"],
            chart_milestone_offsets=selected["years"],
        )
        expected = [{"group": row["Chart Group"], "year": int(row["Milestone Name"].split()[0]),
                     "outcome": row["Outcome"], "count": row["Count"], "share": row["Share"],
                     "eligible": row["Eligible Students"], "future": row["Future Students"],
                     "total": row["Cohort Students"], "status": row["Milestone Status"]}
                    for row in dashboard["chart_frame"].to_dict("records")]
        key = lambda row: (row["group"], row["year"], row["outcome"])
        assert sorted(actual["rows"], key=key) == sorted(expected, key=key), selected
        assert actual["students"] == dashboard["meta"]["students"], selected
        for group in actual["groups"]:
            assert sum(row["share"] for row in actual["rows"] if row["group"] == group["label"]) == pytest.approx(1)


def test_empty_milestone_selection_does_not_show_stale_chart(cohort_tables):
    payload = viewer.build_viewer_payload(cohort_tables)
    actual = javascript_results(payload, [{"semesters": payload["semesters"], "chapters": payload["chapters"],
                                           "years": [], "breakdown": "Overall"}])[0]
    assert actual == {"students": 50, "groups": [], "rows": []}


def test_payload_contains_only_aggregate_data(cohort_tables):
    payload = viewer.build_viewer_payload(cohort_tables)
    assert set(payload) == {"schema", "published", "dataThrough", "semesters", "chapters", "outcomes", "colors", "units"}
    assert "PRIVATE-" not in json.dumps(payload)
    assert payload["dataThrough"] == "Spring 2026"
    assert len(payload["units"]) < len(cohort_tables.outcomes) * 6
    for semester in payload["semesters"]:
        for year in range(1, 7):
            assert sum(row[4] for row in payload["units"] if row[0] == semester and row[2] == year) == 10


class ScriptInspector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.scripts = []
        self.current = None
        self.csp = ""
        self.external = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "script":
            self.current = {"attrs": attrs, "text": ""}
            self.scripts.append(self.current)
        if tag in {"script", "img", "iframe", "link"} and ("src" in attrs or "href" in attrs):
            self.external.append(attrs)
        if tag == "meta" and attrs.get("http-equiv", "").lower() == "content-security-policy":
            self.csp = attrs["content"]

    def handle_endtag(self, tag):
        if tag == "script":
            self.current = None

    def handle_data(self, data):
        if self.current is not None:
            self.current["text"] += data


def test_html_is_offline_and_safely_embeds_labels(cohort_tables):
    payload = viewer.build_viewer_payload(cohort_tables)
    payload["chapters"].append('</script><script>alert("test")</script>&/*VIEWER_MODEL*/')
    html = viewer.render_viewer(payload)
    parser = ScriptInspector()
    parser.feed(html)
    assert len(parser.scripts) == 4
    assert not parser.external
    assert "connect-src 'none'" in parser.csp
    assert "plotly.js" in parser.scripts[0]["text"]
    assert len(parser.scripts[0]["text"]) > 1_000_000
    embedded = next(script["text"] for script in parser.scripts if script["attrs"].get("id") == "viewer-data")
    assert json.loads(embedded) == payload
    assert "PRIVATE-" not in html


def test_publisher_keeps_sources_and_applies_saved_corrections(publisher_config, tmp_path):
    config, _ = publisher_config
    before = {path: path.read_bytes() for path in config.data_paths if path.exists()}
    output, students = viewer.publish_viewer(config, tmp_path / "published.html")
    assert students == 2
    parser = ScriptInspector()
    parser.feed(output.read_text(encoding="utf-8"))
    payload = json.loads(next(script["text"] for script in parser.scripts if script["attrs"].get("id") == "viewer-data"))
    assert ["Fall 2020", "Alpha", 4, "Graduated", 1] in payload["units"]
    assert ["Fall 2025", "Alpha", 6, "Future", 1] in payload["units"]
    assert all(path.read_bytes() == content for path, content in before.items())
    assert not config.name_choices.exists() and not config.name_rechecks.exists()
    assert not list(output.parent.glob("*.tmp"))


@pytest.mark.parametrize("field", ["database", "manual_status", "zero_member_periods"])
def test_publisher_refuses_missing_authoritative_sources(publisher_config, tmp_path, field):
    config, _ = publisher_config
    getattr(config, field).unlink()
    output = tmp_path / "published.html"
    output.write_text("previous publication", encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="Required source file"):
        viewer.publish_viewer(config, output)
    assert output.read_text(encoding="utf-8") == "previous publication"


@pytest.mark.parametrize("failure", ["changed-source", "locked-output"])
def test_failed_publish_preserves_previous_viewer(publisher_config, tmp_path, monkeypatch, failure):
    config, _ = publisher_config
    output = tmp_path / "published.html"
    output.write_text("previous publication", encoding="utf-8")
    if failure == "changed-source":
        revisions = iter([((1, 1),), ((2, 2),)])
        monkeypatch.setattr(viewer, "data_revision", lambda paths: next(revisions))
    else:
        def locked(*args):
            raise PermissionError("File is locked")
        monkeypatch.setattr(viewer.os, "replace", locked)
    with pytest.raises((RuntimeError, PermissionError)):
        viewer.publish_viewer(config, output)
    assert output.read_text(encoding="utf-8") == "previous publication"
    assert not list(tmp_path.glob("*.tmp"))


def test_viewer_cli_and_destination_validation(publisher_config, tmp_path, capsys):
    config, path = publisher_config
    with pytest.raises(ValueError, match="end in .html"):
        viewer.publish_viewer(config, config.database)
    assert viewer.main(["--host-config", str(path), "--output", str(tmp_path / "cli.html")]) == 0
    assert "Cohort students: 2" in capsys.readouterr().out


def test_empty_cohorts_fail_with_actionable_message(cohort_tables):
    cohort_tables.outcomes = cohort_tables.outcomes.iloc[:0]
    with pytest.raises(ValueError, match="No new-member cohorts"):
        viewer.build_viewer_payload(cohort_tables)
