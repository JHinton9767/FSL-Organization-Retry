import pandas as pd
import pytest
from openpyxl import Workbook

from src.sqlCompile import OUTPUT_COLUMNS, build_sql_compile_frame, build_new_member_observations, load_sql_compile_rows, sqlCompile, write_sqlite
from src.sqlCompile_cohort import MANUAL_STATUS_COLUMNS, ZERO_MEMBER_PERIOD_COLUMNS, build_new_member_cohort_tables
from src.sqlCompile_dashboard import build_sql_compile_milestone_dashboard, load_dashboard_tables, consolidate_duplicate_student_outcomes, DUPLICATE_NAME_MISMATCH_OUTCOME
from src.sqlCompile_cohort import read_new_member_observations, build_new_member_cohort_report
from src.sqlCompile_legacy_manual import _convert_outcome_overrides, legacy_status_to_sql_status
from src.sqlCompile_viewer import build_viewer_payload


def write_roster(path, semester, statuses):
    path.parent.mkdir(parents=True, exist_ok=True)
    book = Workbook()
    sheet = book.active
    sheet.append([f"Alpha Sigma Phi {semester} Roster"])
    sheet.append(["Last Name", "First Name", "Banner ID", "Position", "Status"])
    for index, status in enumerate(statuses, start=1):
        sheet.append(["Synthetic", f"Student {index}", f"A{index:08d}", "Member", status])
    book.save(path)


def cohort_tables(rows, manual=None, **kwargs):
    return build_new_member_cohort_tables(
        pd.DataFrame(rows, columns=OUTPUT_COLUMNS),
        pd.DataFrame(manual or [], columns=MANUAL_STATUS_COLUMNS).fillna(""),
        zero_member_periods=pd.DataFrame(columns=ZERO_MEMBER_PERIOD_COLUMNS),
        all_cohorts=True, **kwargs,
    )


def chart(timeline, outcomes):
    return build_sql_compile_milestone_dashboard(timeline, outcomes, reporting_cutoff="Spring 2026")


@pytest.mark.parametrize("inactive", ["I", "S", "I/S", "Inactive/Suspended"])
@pytest.mark.parametrize("reactivated", ["A", "N"])
def test_inactive_students_can_return_to_active(inactive, reactivated):
    timeline = pd.DataFrame([
        {"Cohort Semester": "Fall 2020", "Student ID": "A00000001", "Semester": semester,
         "Status Code": status, "Source": "sqlCompile", "Included In Outcome": "Yes"}
        for semester, status in [("Fall 2020", "N"), ("Spring 2021", inactive),
                                 ("Spring 2022", reactivated), ("Spring 2026", "A")]
    ])
    outcomes = pd.DataFrame([{"Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha Sigma Phi", "Student ID": "A00000001"}])
    assert chart(timeline, outcomes)["detail_frame"]["P&G Outcome Bucket"].tolist() == ["Inactive/Suspended"] + ["Active"] * 5


@pytest.mark.parametrize("last_status,expected", [("S", "Inactive/Suspended"), ("G", "Graduated"), ("RS", "Dropped/Resigned")])
def test_last_inactive_or_terminal_outcome_still_carries_forward(last_status, expected):
    rows = [["Fall 2020", "Alpha Sigma Phi", "A00000001", "N"],
            ["Spring 2021", "Alpha Sigma Phi", "A00000001", last_status]]
    if last_status != "S":
        rows += [["Spring 2022", "Alpha Sigma Phi", "A00000001", "S"],
                 ["Spring 2026", "Alpha Sigma Phi", "A00000001", "A"]]
    timeline, outcomes, *_ = cohort_tables(rows)
    assert chart(timeline, outcomes)["detail_frame"]["P&G Outcome Bucket"].tolist() == [expected] * 6


@pytest.mark.parametrize("initial,final,expected", [
    ("I", "A", "A"), ("S", "A", "A"), ("I/S", "N", "N"),
    ("A", "S", "S"), ("S", "S", "S"), ("D", "A", "D"), ("N", "A", "N"),
])
def test_same_semester_reactivation_requires_later_roster(tmp_path, initial, final, expected):
    for phase, status in [("Initial", initial), ("Final", final)]:
        write_roster(tmp_path / "Spring 2021" / phase / "Alpha Sigma Phi.xlsx", "Spring 2021", [status])
    compiled, issues, _ = build_sql_compile_frame([tmp_path])
    assert issues.empty
    assert compiled["Status"].tolist() == [expected]


def test_roster_pass_and_month_ignore_unrelated_ancestors_and_use_matching_root(tmp_path):
    root = tmp_path / "Final December backup" / "rosters"
    write_roster(root / "Spring 2021" / "Initial January" / "Alpha Sigma Phi.xlsx", "Spring 2021", ["S"])
    write_roster(root / "Spring 2021" / "Updated March" / "Alpha Sigma Phi.xlsx", "Spring 2021", ["A"])
    rows, issues, _ = load_sql_compile_rows([tmp_path / "other_rosters", root])
    assert issues.empty
    rows = rows.set_index("Status")
    assert rows.loc["S", "_roster_file_version"] == "Initial"
    assert rows.loc["A", "_roster_file_version"] == "Updated"
    assert rows.loc["S", "_source_month_priority"] == 1
    assert rows.loc["A", "_source_month_priority"] == 3
    assert "Updated March" in rows.loc["A", "_source_file"]
    compiled, _, _ = build_sql_compile_frame([tmp_path / "other_rosters", root])
    assert compiled["Status"].tolist() == ["A"]


@pytest.mark.parametrize("phase", ["Initial", "Updated", "Final"])
def test_roster_root_can_itself_supply_the_roster_pass(tmp_path, phase):
    root = tmp_path / "Final backup" / phase
    write_roster(root / "Alpha Sigma Phi.xlsx", "Spring 2021", ["A"])
    rows, issues, _ = load_sql_compile_rows([root])
    assert issues.empty
    assert rows["_roster_file_version"].tolist() == [phase]


def test_original_new_members_survive_same_semester_exit_in_database_and_reports(tmp_path):
    source = tmp_path / "rosters"
    write_roster(source / "Fall 2020" / "Initial" / "Alpha Sigma Phi.xlsx", "Fall 2020", ["N", "N"])
    write_roster(source / "Fall 2020" / "Final" / "Alpha Sigma Phi.xlsx", "Fall 2020", ["D", "N"])
    write_roster(source / "Spring 2024" / "Alpha Sigma Phi.xlsx", "Spring 2024", ["D", "G"])
    write_roster(source / "Spring 2026" / "Alpha Sigma Phi.xlsx", "Spring 2026", ["D", "G"])
    database = tmp_path / "data.sqlite"
    sqlCompile([source], database)
    joins = read_new_member_observations(database)
    assert len(joins) == 2
    assert joins["Semester"].tolist() == ["Fall 2020", "Fall 2020"]
    tables = load_dashboard_tables(
        database, tmp_path / "manual.csv", tmp_path / "names.csv", tmp_path / "rechecks.csv",
        zero_member_periods_file=tmp_path / "zero.csv", reporting_cutoff="Spring 2026",
    )
    assert tables.new_member_evidence_complete
    assert len(tables.outcomes) == 2
    year4 = chart(tables.timeline, tables.outcomes)["chart_frame"].query('`Milestone Name` == "4 Year"')
    assert year4.set_index("Outcome")["Share"].to_dict() == {"Dropped/Resigned": 0.5, "Graduated": 0.5}
    payload = build_viewer_payload(tables)
    year4_units = [row for row in payload["units"] if row[2] == 4]
    assert sum(row[4] for row in year4_units) == 2
    assert sum(row[4] for row in year4_units if row[3] == "Graduated") == 1
    report = build_new_member_cohort_report(database, all_cohorts=True, manual_status_file=tmp_path / "manual.csv", output_dir=tmp_path / "reports")
    assert report.outcome_rows == 2


def test_old_database_is_readable_but_not_marked_as_complete_join_evidence(tmp_path):
    database = tmp_path / "old.sqlite"
    write_sqlite(pd.DataFrame([["Fall 2020", "Alpha Sigma Phi", "A00000001", "N"]], columns=OUTPUT_COLUMNS), database)
    assert read_new_member_observations(database) is None
    tables = load_dashboard_tables(database, tmp_path / "manual.csv", tmp_path / "names.csv", tmp_path / "rechecks.csv", zero_member_periods_file=tmp_path / "zero.csv")
    assert len(tables.outcomes) == 1
    assert not tables.new_member_evidence_complete


@pytest.mark.parametrize("explicit_removal,expected", [(False, "Active"), (True, "Chapter Kicked")])
def test_unequal_latest_roster_passes_need_explicit_midsemester_removal(explicit_removal, expected):
    inventory = pd.DataFrame([
        {"Semester": "Fall 2025", "Chapter": "Alpha Sigma Phi", "Roster Pass Priority": 1, "Student Rows": 1},
        {"Semester": "Spring 2026", "Chapter": "Alpha Sigma Phi", "Roster Pass Priority": 1, "Student Rows": 1},
        {"Semester": "Spring 2026", "Chapter": "Zeta Tau Alpha", "Roster Pass Priority": 3, "Student Rows": 1},
    ])
    manual = [{"Semester": "Spring 2026", "Chapter": "Alpha Sigma Phi", "Student ID": "A00000001", "Status": "CK"}] if explicit_removal else []
    timeline, outcomes, *_ = cohort_tables([
        ["Fall 2025", "Alpha Sigma Phi", "A00000001", "N"],
        ["Spring 2026", "Alpha Sigma Phi", "A00000001", "A"],
    ], manual, roster_inventory=inventory)
    assert chart(timeline, outcomes)["detail_frame"]["P&G Outcome Bucket"].iloc[0] == expected


def test_earliest_cohort_retains_later_cohort_scoped_manual_graduation():
    timeline, outcomes, _, _, selected = cohort_tables([
        ["Fall 2020", "Alpha Sigma Phi", "A00000001", "N"],
        ["Spring 2021", "Alpha Sigma Phi", "A00000001", "A"],
        ["Fall 2022", "Zeta Tau Alpha", "A00000001", "N"],
        ["Spring 2023", "Zeta Tau Alpha", "A00000001", "A"],
    ], [{"Cohort Semester": "Fall 2022", "Cohort Chapter": "Zeta Tau Alpha",
         "Semester": "Spring 2024", "Student ID": "A00000001", "Status": "G"}])
    assert selected == ["Fall 2020"]
    assert len(outcomes) == 1
    assert outcomes.iloc[0]["Cohort Chapter"] == "Alpha Sigma Phi"
    assert outcomes.iloc[0]["Last Known Status Code"] == "G"
    assert chart(timeline, outcomes)["detail_frame"]["P&G Outcome Bucket"].iloc[3] == "Graduated"


def test_original_join_chapter_and_same_semester_alternate_scope_are_preserved(tmp_path):
    raw = pd.DataFrame([
        ["Fall 2020", "Alpha Sigma Phi", "A00000001", "N", 1],
        ["Fall 2020", "Zeta Tau Alpha", "A00000001", "N", 3],
    ], columns=[*OUTPUT_COLUMNS, "_source_version_priority"])
    observations = build_new_member_observations(raw)
    assert observations["Chapter"].tolist() == ["Alpha Sigma Phi", "Zeta Tau Alpha"]
    database = tmp_path / "data.sqlite"
    write_sqlite(raw.loc[[1], OUTPUT_COLUMNS], database, new_member_observations=observations)
    timeline, outcomes, *_ = cohort_tables(raw.loc[[1], OUTPUT_COLUMNS].to_numpy().tolist(), [
        {"Cohort Semester": "Fall 2020", "Cohort Chapter": "Zeta Tau Alpha",
         "Semester": "Spring 2024", "Student ID": "A00000001", "Status": "G"},
    ], new_member_observations=read_new_member_observations(database))
    assert len(outcomes) == 1
    assert outcomes.iloc[0]["Cohort Chapter"] == "Alpha Sigma Phi"
    assert chart(timeline, outcomes)["detail_frame"]["P&G Outcome Bucket"].iloc[3] == "Graduated"


@pytest.mark.parametrize("reverse", [False, True])
def test_latest_applicable_manual_decision_wins_over_overlapping_scope(reverse):
    manual = [
        {"Semester": "Spring 2024", "Student ID": "A00000001", "Status": "D"},
        {"Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha Sigma Phi",
         "Semester": "Spring 2024", "Chapter": "Alpha Sigma Phi", "Student ID": "A00000001", "Status": "G"},
    ]
    if reverse:
        manual.reverse()
    timeline, outcomes, *_ = cohort_tables([
        ["Fall 2020", "Alpha Sigma Phi", "A00000001", "N"],
        ["Spring 2024", "Alpha Sigma Phi", "A00000001", "A"],
    ], manual)
    assert outcomes.iloc[0]["Last Known Status Code"] == manual[-1]["Status"]
    assert chart(timeline, outcomes)["detail_frame"]["P&G Outcome Bucket"].iloc[3] == ("Dropped/Resigned" if reverse else "Graduated")


def test_unrelated_manual_cohort_scope_is_not_applied_to_same_id():
    timeline, outcomes, *_ = cohort_tables([
        ["Fall 2020", "Alpha Sigma Phi", "A00000001", "N"],
        ["Spring 2024", "Alpha Sigma Phi", "A00000001", "A"],
    ], [{"Cohort Semester": "Fall 2022", "Cohort Chapter": "Zeta Tau Alpha",
         "Semester": "Spring 2024", "Student ID": "A00000001", "Status": "G"}])
    assert not timeline["Source"].eq("manual_status").any()
    assert outcomes.iloc[0]["Last Known Status Code"] == "A"


@pytest.mark.parametrize("chosen_name", ["", "Synthetic One", "Synthetic Two"])
def test_single_cohort_name_conflicts_are_reviewed_until_a_name_is_selected(chosen_name):
    timeline, outcomes, *_ = cohort_tables([
        ["Fall 2020", "Alpha Sigma Phi", "A00000001", "N"],
        ["Spring 2024", "Alpha Sigma Phi", "A00000001", "G"],
    ])
    observations = pd.DataFrame([
        ["A00000001", "Synthetic One", 1], ["A00000001", "Synthetic Two", 1],
    ], columns=["Student ID", "Student Name", "Observation Count"])
    decisions = pd.DataFrame([{"Student ID": "A00000001", "Student Name": chosen_name}])
    result = consolidate_duplicate_student_outcomes(outcomes, observations, decisions)
    if not chosen_name:
        assert result.iloc[0]["Last Known Outcome Bucket"] == DUPLICATE_NAME_MISMATCH_OUTCOME
        assert chart(timeline, result)["detail_frame"]["P&G Outcome Bucket"].iloc[3] == "Unknown"
    else:
        assert result.iloc[0]["Student Name"] == chosen_name
        assert chart(timeline, result)["detail_frame"]["P&G Outcome Bucket"].iloc[3] == "Graduated"


@pytest.mark.parametrize("manual_chapter", ["Alpha Sigma Phi Fraternity, Inc.", "ALPHA SIGMA PHI", "Alpha Sigma Phi"])
def test_imported_legal_chapter_names_match_compiled_short_names(manual_chapter):
    imported = _convert_outcome_overrides(pd.DataFrame([{
        "student_id": "A00000001", "organization_join_term": "Fall 2020",
        "organization_name": manual_chapter, "final_status_term": "Spring 2024", "final_status": "G",
    }]), "synthetic")
    timeline, outcomes, *_ = cohort_tables([
        ["Fall 2020", "Alpha Sigma Phi", "A00000001", "N"],
        ["Spring 2021", "Alpha Sigma Phi", "A00000001", "A"],
    ], imported.to_dict("records"))
    assert outcomes.iloc[0]["Last Known Status Code"] == "G"
    assert chart(timeline, outcomes)["detail_frame"]["P&G Outcome Bucket"].iloc[3] == "Graduated"


@pytest.mark.parametrize("label", ["Non-Graduate Exit", "Not Graduated", "No degree", "Graduation Unconfirmed", "Expected Graduation", "Graduated?", "No confirmed graduation"])
def test_legacy_import_requires_positive_graduation_evidence(label):
    assert legacy_status_to_sql_status(label) == ""


@pytest.mark.parametrize("label", ["G", "Graduated", "Graduated Confirmed", "Graduated (Confirmed)", "Degree Awarded"])
def test_positive_legacy_graduation_labels_remain_importable(label):
    assert legacy_status_to_sql_status(label) == "G"
