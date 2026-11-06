import json
from types import SimpleNamespace

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from src.path_config import ROOT
from src.sqlCompile import OUTPUT_COLUMNS, write_sqlite
from src.sqlCompile_audit import read_change_history
from src.sqlCompile_dashboard import load_dashboard_tables, build_sql_compile_milestone_dashboard
from src.sqlCompile_host import HostConfig
from src.sqlCompile_records import RECORD_COLUMNS, DEGREE_VIEW
from src.sqlCompile_reporting import save_reporting_cutoff
from src.sqlCompile_storage import ReviewConflictError
from src.sqlCompile_success import (build_student_success, public_metrics_csv, public_summary_text, render_public_summary)
from src.sqlCompile_success_records import (
    GRADUATION_COLUMNS, ACADEMIC_COLUMNS, IMPACT_COLUMNS, normalize_graduation_records,
    normalize_academic_records, normalize_impact_records, read_success_records, save_success_records,
)
from publish_student_success import publish_student_success


def student(number):
    return f"A0{number:07d}"


def award(number=1, **changes):
    return {"Student ID": student(number), "Award Date": "2024-05-15", "Award Semester": "Spring 2024",
            "Degree Level": "Bachelor's", "Degree Title": "BA", "Institution": "Test University",
            "Verification": "Verified", "Source": "PRIVATE-SOURCE", "Reviewed By": "PRIVATE-REVIEWER",
            "Reviewed On": "2026-05-20", "Notes": "PRIVATE-NOTES", **changes}


def evidence(number, semester, domain, value, resolution=""):
    return {"Student ID": student(number), "Semester": semester, "Domain": domain, "Value": value,
            "Source": "university", "Evidence": "PRIVATE-EVIDENCE", "Priority": 1, "Resolution": resolution, "Chapter": ""}


def case(count=6, awards=(), records=(), academics=(), impact=()):
    return SimpleNamespace(outcomes=pd.DataFrame([
        {"Student ID": student(n), "Student Name": "PRIVATE-NAME", "Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha",
         "Last Known Outcome Bucket": "Dropped/Resigned"} for n in range(1, count + 1)]),
        timeline=pd.DataFrame(), reporting_cutoff="Spring 2026", new_member_evidence_complete=True,
        record_history=pd.DataFrame(records, columns=RECORD_COLUMNS),
        graduation_records=pd.DataFrame(awards, columns=GRADUATION_COLUMNS),
        academic_records=pd.DataFrame(academics, columns=ACADEMIC_COLUMNS),
        impact_records=pd.DataFrame(impact, columns=IMPACT_COLUMNS))


def card(report, key):
    return next(row for row in report["public"]["cards"] if row["key"] == key)


def year(report, n):
    return report["metrics"].set_index("Year").loc[n]


def test_missing_data_is_unknown_and_no_placeholder_rates_are_exported():
    report = build_student_success(case())
    assert year(report, 6)["Degree unknown"] == 6
    assert year(report, 6)["Verified graduates"] == 0
    assert card(report, "graduation_6")["value"] is None
    assert card(report, "graduates")["value"] is None
    assert "Year 6 verified graduation" not in render_public_summary(report["public"])
    empty = build_student_success(SimpleNamespace(outcomes=pd.DataFrame(), reporting_cutoff="Spring 2026"))
    assert empty["metrics"]["Eligible students"].sum() == 0
    assert all(c["value"] is None for c in empty["public"]["cards"])
    assert "No verified headline metrics" in public_summary_text(empty["public"])


def test_roster_g_and_term_only_graduation_are_leads_not_fully_verified_awards():
    report = build_student_success(case(records=[evidence(n, "Spring 2024", "degree", "Graduated") for n in range(1, 7)]))
    assert report["students"]["Degree evidence"].eq("Graduated").all()
    assert report["period_graduates"] == 0
    assert year(report, 4)["Degree unknown"] == 6


def test_exact_count_unique_people_full_denominators_and_membership_exit_preserved():
    awards = [award(n) for n in range(1, 6)]
    awards.append(award(1, **{"Degree Title": "BS"}))
    report = build_student_success(case(awards=awards, records=[evidence(6, "Spring 2024", "degree", "No degree")]))
    assert report["period_graduates"] == 5
    assert report["period_awards"] == 6
    assert year(report, 4)["Graduation rate"] == pytest.approx(5 / 6)
    assert year(report, 4)["Degree unknown"] == 0
    assert card(report, "graduation_4")["value"] == pytest.approx(5 / 6)
    assert card(report, "graduation_6")["value"] is None  # 2024 No degree does not carry to 2026.
    assert card(report, "graduates")["value"] == 5


def test_unknowns_and_small_groups_never_disappear_from_rate_denominator():
    report = build_student_success(case(awards=[award(n) for n in range(1, 6)]))
    assert year(report, 4)["Eligible students"] == 6
    assert year(report, 4)["Graduation rate"] == pytest.approx(5 / 6)
    assert card(report, "graduation_4")["value"] is None
    small = build_student_success(case(4, awards=[award(n) for n in range(1, 5)]))
    assert card(small, "graduates")["value"] is None
    assert card(small, "graduation_4")["value"] is None


def test_verified_zero_is_publishable_only_with_complete_negative_evidence():
    report = build_student_success(case(records=[evidence(n, "Spring 2026", "degree", "No degree") for n in range(1, 7)]))
    assert card(report, "graduation_6")["value"] == 0
    assert card(report, "graduation_6")["status"] == "Ready"


@pytest.mark.parametrize("change", [
    {"Verification": "Draft"}, {"Verification": "Excluded"},
    {"Award Date": "2020-05-15", "Award Semester": "Spring 2020"},
    {"Award Date": "2026-08-15", "Award Semester": "Summer 2026", "Reviewed On": "2026-09-01"},
    {"Student ID": student(99)}, {"Degree Level": "Master's"},
])
def test_unusable_awards_are_retained_but_do_not_count(change):
    report = build_student_success(case(awards=[award(**change)]))
    assert len(report["awards"]) == 1
    assert report["awards"].iloc[0]["Use in metrics"] != "Included"
    assert report["period_graduates"] == 0


def test_identity_and_later_degree_conflicts_quarantine_awards():
    tables = case(awards=[award(1), award(2)], records=[evidence(2, "Spring 2026", "degree", "No degree")])
    tables.outcomes.loc[0, "Last Known Outcome Bucket"] = "Duplicate ID / Name Mismatch"
    report = build_student_success(tables)
    assert report["period_graduates"] == 0
    assert year(report, 4)["Eligible students"] == 6
    assert set(report["awards"]["Use in metrics"]) == {"Identity requires review", "Degree evidence requires reconciliation"}


def test_researched_negative_resolution_retracts_award_from_metrics():
    report = build_student_success(case(awards=[award(1)], records=[evidence(1, "Spring 2026", "degree", "No degree", "Use university record")]))
    assert report["period_graduates"] == 0
    assert report["awards"].iloc[0]["Use in metrics"] == "Degree evidence requires reconciliation"


def test_period_and_cohort_filters_have_distinct_effects_and_future_is_excluded():
    tables = case(awards=[award(1)])
    tables.outcomes.loc[1, "Cohort Semester"] = "Fall 2025"
    report = build_student_success(tables, report_start="Fall 2025")
    assert report["period_graduates"] == 0
    assert year(report, 4)["Verified graduates"] == 1
    assert year(report, 4)["Eligible students"] == 5
    assert year(report, 4)["Future students"] == 1
    assert year(report, 1)["Eligible students"] == 6
    empty = build_student_success(tables, semesters=[])
    assert empty["students"].empty
    assert empty["period_graduates"] == 0
    with pytest.raises(ValueError):
        build_student_success(tables, report_end="Fall 2026")
    with pytest.raises(ValueError):
        build_student_success(tables, report_start="Fall 2025", report_end="Spring 2024")


def test_continuation_requires_exact_enrollment_but_never_implies_graduation():
    records = [evidence(n, "Spring 2021", "enrollment", "Enrolled") for n in range(1, 7)]
    report = build_student_success(case(records=records))
    assert card(report, "continuation_1")["value"] == 1
    assert year(report, 1)["Verified graduates"] == 0
    assert year(report, 2)["Continuation unknown"] == 6
    conflict = build_student_success(case(records=records + [evidence(1, "Spring 2021", "enrollment", "Not enrolled")]))
    assert card(conflict, "continuation_1")["value"] is None


@pytest.mark.parametrize("change", [
    {"Source": ""}, {"Reviewed By": ""}, {"Reviewed On": ""}, {"Award Date": ""}, {"Institution": ""},
    {"Award Semester": "bad"}, {"Award Date": "2023-05-15"}, {"Award Date": "2024-02-30"},
    {"Reviewed On": "2023-05-15"}, {"Student ID": "invalid"}, {"Degree Level": "Alumni"}, {"Award Date": "2099-01-01"},
])
def test_verified_award_validation(change):
    with pytest.raises(ValueError):
        normalize_graduation_records(pd.DataFrame([award(**change)]))


def test_drafts_allow_partial_records_but_duplicate_awards_cannot_be_verified():
    draft = normalize_graduation_records(pd.DataFrame([{"Student ID": student(1)}]))
    assert draft.iloc[0]["Verification"] == "Draft"
    assert draft.iloc[0]["Record ID"].startswith("degree-")
    with pytest.raises(ValueError, match="same award"):
        normalize_graduation_records(pd.DataFrame([award(), award(**{"Record ID": "second", "Source": "other source"})]))
    with pytest.raises(ValueError, match="Unexpected columns"):
        normalize_graduation_records(pd.DataFrame([award(**{"Degree date": "2024-05-15"})]))


def academic(number=1, **changes):
    return {"Student ID": student(number), "Semester": "Spring 2026", "GPA Credits": "10", "Quality Points": "30",
            "Earned Credits": "10", "Honors": "Dean's list", "Verification": "Verified", "Source": "PRIVATE-SOURCE",
            "Reviewed By": "PRIVATE-REVIEWER", "Reviewed On": "2026-06-01", **changes}


def test_gpa_is_weighted_and_public_gpa_requires_complete_enrollment_and_academic_data():
    academics = [academic(n) for n in range(1, 7)]
    academics[0] = academic(1, **{"GPA Credits": "30", "Quality Points": "120"})
    records = [evidence(n, "Spring 2026", "enrollment", "Enrolled") for n in range(1, 7)]
    partial = build_student_success(case(academics=academics))
    assert partial["academic_metrics"].iloc[0]["Weighted GPA"] == pytest.approx(270 / 80)
    assert card(partial, "gpa")["value"] is None
    complete = build_student_success(case(academics=academics, records=records))
    assert card(complete, "gpa")["value"] == pytest.approx(270 / 80)
    assert card(complete, "honors")["value"] == 6
    incomplete = build_student_success(case(academics=academics[:5], records=records))
    assert card(incomplete, "gpa")["value"] is None


@pytest.mark.parametrize("change", [{"Quality Points": "41"}, {"GPA Credits": "-1"}, {"Quality Points": "nan"},
                                         {"GPA Credits": "inf"}, {"Quality Points": ""}, {"Honors": "Yes"}])
def test_bad_academic_values_rejected(change):
    with pytest.raises(ValueError):
        normalize_academic_records(pd.DataFrame([academic(**change)]))


def impact(**changes):
    return {"Semester": "Spring 2026", "Chapter": "Alpha", "Metric": "Service hours", "Value": "100",
            "Verification": "Verified", "Source": "PRIVATE-IMPACT-SOURCE", "Reviewed By": "PRIVATE-REVIEWER",
            "Reviewed On": "2026-06-01", **changes}


def test_impact_does_not_double_count_totals_and_subtotals():
    for second in [impact(**{"Source": "second"}), impact(**{"Chapter": "All FSL"})]:
        with pytest.raises(ValueError, match="overlap"):
            normalize_impact_records(pd.DataFrame([impact(), second]))
    tables = case(impact=[impact(), impact(**{"Chapter": "Beta", "Value": "200"})])
    report = build_student_success(tables, semesters=[])
    assert card(report, "impact_Service hours")["value"] == 300  # Community totals are not join-cohort metrics.
    alpha = build_student_success(tables, chapters=["Alpha"])
    assert card(alpha, "impact_Service hours")["value"] == 100
    none = build_student_success(tables, chapters=[])
    assert card(none, "impact_Service hours")["value"] is None


def test_verified_records_use_audited_saves_and_reject_stale_overwrites(tmp_path):
    path = tmp_path / "awards.csv"
    baseline = read_success_records(path, "graduation")
    save_success_records(pd.DataFrame([award()]), path, "graduation", expected_rows=baseline)
    saved = read_success_records(path, "graduation")
    assert len(read_change_history(path)) == 1
    with pytest.raises(ReviewConflictError):
        save_success_records(pd.DataFrame([award(2)]), path, "graduation", expected_rows=baseline)
    revised = saved.copy()
    revised.loc[0, "Verification"] = "Excluded"
    save_success_records(revised, path, "graduation", expected_rows=saved)
    assert len(read_change_history(path)) == 2
    assert read_success_records(path, "graduation").iloc[0]["Verification"] == "Excluded"


def test_bulk_history_index_preserves_term_priority_order_and_ignores_missing_keys():
    from src.sqlCompile_records import index_record_history
    records = [evidence(1, "Spring 2026", "degree", "No degree"),
               {**evidence(1, "Spring 2024", "degree", "Review required"), "Priority": 3},
               evidence(2, "Spring 2026", "enrollment", "Enrolled"),
               evidence(1, "Spring 2024", "degree", "Graduated"),
               {**evidence(3, "Spring 2026", "degree", "Graduated"), "Student ID": None},
               {**evidence(4, "Spring 2026", "degree", "Graduated"), "Domain": None}]
    indexed = index_record_history(pd.DataFrame(records))
    assert set(indexed) == {(student(1), "degree"), (student(2), "enrollment")}
    sorts, rows = indexed[(student(1), "degree")]
    assert sorts == [20241, 20241, 20261]
    assert [row["Value"] for row in rows] == ["Graduated", "Review required", "No degree"]


def test_public_exports_are_aggregate_only_and_html_escapes_labels():
    tables = case(awards=[award(n) for n in range(1, 7)], impact=[impact()])
    tables.outcomes["Cohort Chapter"] = '</script><img src=x onerror="alert(1)">'
    summary = build_student_success(tables)["public"]
    for content in [json.dumps(summary), public_metrics_csv(summary).decode("utf-8-sig"), public_summary_text(summary), render_public_summary(summary)]:
        for secret in ["PRIVATE-", student(1), "Award Date", "Reviewed By"]:
            assert secret not in content
    html = render_public_summary(summary)
    assert "<img src=x" not in html
    assert "&lt;img" in html
    assert "<script" not in html
    assert "http://" not in html


@pytest.fixture
def configured_case(tmp_path):
    config = HostConfig(database=tmp_path / "data.sqlite", manual_status=tmp_path / "manual.csv", name_choices=tmp_path / "names.csv",
        name_rechecks=tmp_path / "rechecks.csv", zero_member_periods=tmp_path / "zero.csv", reporting_settings=tmp_path / "reporting.json",
        university_records=tmp_path / "university.csv", coverage_reviews=tmp_path / "coverage.csv",
        graduation_records=tmp_path / "awards.csv", academic_records=tmp_path / "academics.csv", impact_records=tmp_path / "impact.csv")
    rows = pd.DataFrame([[term, "Alpha", student(n), status] for n in range(1, 7) for term, status in [("Fall 2020", "N"), ("Spring 2026", "RS")]], columns=OUTPUT_COLUMNS)
    write_sqlite(rows, config.database, new_member_observations=rows.loc[rows.Status.eq("N"), OUTPUT_COLUMNS[:3]])
    save_reporting_cutoff("Spring 2026", config.reporting_settings)
    save_success_records(pd.DataFrame([award(n) for n in range(1, 7)]), config.graduation_records, "graduation")
    return config


def load_configured(config):
    return load_dashboard_tables(database_path=config.database, manual_status_file=config.manual_status,
        duplicate_name_resolution_file=config.name_choices, duplicate_name_recheck_file=config.name_rechecks,
        zero_member_periods_file=config.zero_member_periods, reporting_cutoff="Spring 2026", university_records_file=config.university_records,
        graduation_records_file=config.graduation_records, academic_records_file=config.academic_records, impact_records_file=config.impact_records)


def test_awards_flow_to_existing_degree_view_without_overwriting_membership(configured_case):
    tables = load_configured(configured_case)
    dashboard = build_sql_compile_milestone_dashboard(tables.timeline, tables.outcomes, chart_milestone_offsets=[6], reporting_cutoff=tables.reporting_cutoff,
        record_history=tables.record_history, outcome_view=DEGREE_VIEW)
    assert dashboard["detail_frame"]["P&G Outcome Bucket"].eq("Graduated").all()
    assert tables.university_records.empty
    assert tables.timeline["Status Code"].eq("RS").any()


def test_external_publisher_archives_previous_report_and_detects_concurrent_change(configured_case, tmp_path, monkeypatch):
    import publish_student_success as publisher
    path = tmp_path / "external.html"
    path.write_text("previous", encoding="utf-8")
    output, summary = publish_student_success(configured_case, path)
    assert output == path
    assert next(c for c in summary["cards"] if c["key"] == "graduates")["value"] == 6
    assert next((tmp_path / "success_publications").glob("*/previous.html")).read_text(encoding="utf-8") == "previous"
    before = path.read_bytes()
    build = publisher.build_student_success
    def changed(*args, **kwargs):
        result = build(*args, **kwargs)
        configured_case.academic_records.write_text(",".join(ACADEMIC_COLUMNS) + "\n", encoding="utf-8")
        return result
    monkeypatch.setattr(publisher, "build_student_success", changed)
    with pytest.raises(RuntimeError, match="changed"):
        publish_student_success(configured_case, path)
    assert path.read_bytes() == before


def test_owner_can_open_all_success_areas_and_draft_import(configured_case, tmp_path, monkeypatch):
    from dataclasses import asdict
    settings = {key: str(value) if hasattr(value, "as_uri") else value for key, value in asdict(configured_case).items()}
    host = tmp_path / "host.json"
    host.write_text(json.dumps(settings), encoding="utf-8")
    monkeypatch.setenv("FSL_DASHBOARD_HOST_CONFIG", str(host))
    monkeypatch.delenv("FSL_DASHBOARD_SHARED", raising=False)
    app = AppTest.from_file(str(ROOT / "app" / "sql_compile_dashboard.py"), default_timeout=40).run()
    app.radio(key="sql_compile_dashboard_section").set_value("Student Success & Impact").run()
    assert not app.exception
    for area in ["Verified graduation records", "Academic & community records", "External summary", "Definitions & data readiness"]:
        app.radio(key="success_area").set_value(area).run()
        assert not app.exception
    app.radio(key="success_area").set_value("Verified graduation records").run()
    next(r for r in app.radio if r.label == "Graduation workspace").set_value("Enter or import awards").run()
    assert not app.exception
    assert any(b.label == "Save graduation records" for b in app.button)
    key = next(key for key in app.session_state.filtered_state if str(key).startswith("success_editor_graduation_"))
    app.session_state[key] = {"edited_rows": {0: {"Verification": "Excluded", "Notes": "Reviewed test correction"}}, "added_rows": [], "deleted_rows": []}
    next(b for b in app.button if b.label == "Save graduation records").click().run()
    assert not app.exception
    assert read_success_records(configured_case.graduation_records, "graduation").iloc[0]["Verification"] == "Excluded"
    assert len(read_change_history(configured_case.graduation_records)) == 2
