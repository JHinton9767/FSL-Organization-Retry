"""Owner workflow for verified records and aggregate external reporting."""
from __future__ import annotations

import hashlib
import io

import pandas as pd
import plotly.express as px
import streamlit as st

from src.sqlCompile_cohort import _semester_sort
from src.sqlCompile_success import (build_student_success, display_public_value, public_summary_text,
                                    public_metrics_csv, render_public_summary, DEFINITIONS)
from src.sqlCompile_success_records import (RECORD_TYPES, VERIFICATION_STATES, DEGREE_LEVELS, HONORS,
                                            IMPACT_UNITS, ALL_CHAPTERS, save_success_records)


AREAS = ["Overview", "Verified graduation records", "Academic & community records", "External summary", "Definitions & data readiness"]


@st.cache_data(show_spinner="Checking verified student outcomes...", max_entries=8)
def _report(tables, semesters, chapters, level, start, end, minimum):
    return build_student_success(tables, semesters=semesters, chapters=chapters, degree_level=level,
                                 report_start=start, report_end=end, minimum_group=minimum)


def _download_private(label: str, frame: pd.DataFrame, name: str):
    st.download_button(label, frame.to_csv(index=False).encode("utf-8-sig"), name, "text/csv")


def _record_editor(kind, saved, path, refresh, *, chapters=()):
    columns, normalize = RECORD_TYPES[kind]
    st.download_button(f"Download {kind} import template", pd.DataFrame(columns=columns).to_csv(index=False),
                       f"{kind}_records_template.csv", "text/csv", key=f"success_template_{kind}")
    st.caption("New records start as Draft. Verified records require a supporting source, reviewer, and review date (YYYY-MM-DD). Saves retain a permanent before-and-after history.")
    uploaded = st.file_uploader(f"Preview a {kind} CSV", type=["csv"], key=f"success_upload_{kind}")
    working = saved.reindex(columns=columns, fill_value="")
    upload_key = "none"
    if uploaded is not None:
        content = uploaded.getvalue()
        upload_key = hashlib.sha256(content).hexdigest()
        try:
            incoming = normalize(pd.read_csv(io.BytesIO(content), dtype=str).fillna(""))
            working = normalize(pd.concat([working, incoming], ignore_index=True).drop_duplicates())
            st.info(f"Previewing {len(incoming):,} incoming records. Nothing is saved until you select Save.")
        except (ValueError, UnicodeError) as exc:
            st.error(str(exc))
            return
    column_config = {
        "Record ID": None,
        "Verification": st.column_config.SelectboxColumn(options=VERIFICATION_STATES, required=True),
        "Reviewed On": st.column_config.TextColumn(help="Date the source was checked, YYYY-MM-DD."),
        "Source": st.column_config.TextColumn(help="Registrar file, report, or document reference. Kept private."),
    }
    if kind == "graduation":
        column_config["Degree Level"] = st.column_config.SelectboxColumn(options=DEGREE_LEVELS)
        st.caption("Award Date is the date the degree was awarded, not the ceremony or review date. Use the university's Award Semester. A roster G is a review lead; it is not automatically a fully verified award.")
    if kind == "academic":
        column_config["Honors"] = st.column_config.SelectboxColumn(options=HONORS)
        st.caption("One verified summary per student and semester. Enter GPA credits and quality points from the university report on a 4.0 scale. Leave unknown values blank; use 0 only when the source confirms zero. Honors and earned credits may be recorded without GPA.")
    if kind == "impact":
        column_config["Metric"] = st.column_config.SelectboxColumn(options=list(IMPACT_UNITS))
        st.caption(f"Enter one semester total per metric and chapter, or use '{ALL_CHAPTERS}' for a university-wide FSL total. Do not combine an All FSL total with its chapter subtotals. Dollars are USD; amounts raised are not estimates or pledges.")
    edited = st.data_editor(working, num_rows="dynamic", hide_index=True, use_container_width=True,
                            disabled=["Record ID"], column_config=column_config,
                            column_order=["Verification", *[column for column in columns if column not in {"Verification", "Record ID"}]],
                            key=f"success_editor_{kind}_{upload_key}_{st.session_state.get('sql_compile_dashboard_data_refresh_token', 0)}")
    st.caption("Record IDs are assigned when saved. Edit an existing record to correct it; mark Excluded to retain a record that should not count.")
    if st.button(f"Save {kind} records", key=f"success_save_{kind}", type="primary"):
        try:
            save_success_records(edited, path, kind, expected_rows=saved)
            refresh()
            st.rerun()
        except (OSError, ValueError) as exc:
            st.error(str(exc))


def _overview(report):
    students, metrics = report["students"], report["metrics"]
    cards = st.columns(4)
    cards[0].metric("Cohort students", f"{len(students):,}")
    cards[1].metric("Verified graduates", f"{report['period_graduates']:,}")
    cards[2].metric("Verified awards", f"{report['period_awards']:,}")
    enrolled = int(students["Enrollment"].eq("Enrolled").sum())
    cards[3].metric("Confirmed enrollment", f"{enrolled:,}")
    st.caption(f"Award counts: {report['report_start']} through {report['report_end']}. Enrollment: {report['report_end']}. Selected degree level: {report['degree_level']}.")
    st.caption("Graduates are unique people; awards can include more than one degree per person. Blank evidence is unknown, not zero. Enrollment and degree outcomes are independent of chapter membership.")
    if report["period_awards"] == 0:
        st.info("No fully verified awards match this selection yet. Add university award records in Verified graduation records. Existing roster G entries remain available as review leads.")
    st.subheader("Graduation and university continuation")
    st.caption("The report end semester determines which checkpoints are complete. Each year can contain different eligible cohorts. The report start only limits award and activity counts; it does not remove older members from checkpoint denominators.")
    measured = metrics.loc[metrics["Eligible students"].gt(0)].copy()
    if not measured.empty:
        chart = measured.melt(id_vars=["Year", "Eligible students"], value_vars=["Graduation rate", "Continuation rate"], var_name="Measure", value_name="Share")
        chart["Measure"] = chart["Measure"].replace({"Graduation rate": "Documented graduation share", "Continuation rate": "Confirmed enrolled or graduated share"})
        figure = px.line(chart, x="Year", y="Share", color="Measure", markers=True, hover_data=["Eligible students"],
                         color_discrete_sequence=["#28774e", "#19769a"])
        figure.update_layout(yaxis_tickformat=".0%", yaxis_range=[0, 1], xaxis_dtick=1,
                             xaxis_title="Year checkpoint since joining FSL", legend_title_text="", margin=dict(l=0, r=0, t=20, b=0))
        st.plotly_chart(figure, use_container_width=True)
        st.caption("These internal shares include every eligible student, even when evidence is incomplete. External rates remain unavailable until all relevant outcomes are verified.")
    else:
        st.info("No selected cohorts have reached a completed checkpoint.")
    display = metrics.copy()
    for column in ["Graduation rate", "Degree coverage", "Continuation rate", "Continuation coverage"]:
        display[column] = display[column].map(lambda value: f"{value:.1%}" if pd.notna(value) else "—")
    st.dataframe(display, hide_index=True, use_container_width=True)
    _download_private("Download checkpoint metrics", metrics, "student_success_checkpoints.csv")
    with st.expander("Students behind the metrics · private"):
        search = st.text_input("Find a student", key="success_student_search").strip()
        visible = students
        if search:
            visible = visible.loc[visible["Student ID"].str.contains(search, case=False, regex=False) | visible["Student Name"].str.contains(search, case=False, regex=False)]
        st.dataframe(visible, hide_index=True, use_container_width=True)
        _download_private("Download student success review list", visible, "student_success_review_private.csv")
    with st.expander("Compare joining cohorts"):
        year = st.selectbox("Completed checkpoint", list(range(1, 7)), index=5, format_func=lambda y: f"Year {y}")
        details = report["checkpoint_details"]
        details = details.loc[details["Year"].eq(year) & ~details["Future"].astype(bool)]
        if details.empty:
            st.info("No eligible students at this checkpoint.")
        else:
            groups = details.groupby(["Cohort Semester", "Cohort Chapter"]).agg(
                Students=("Student ID", "size"), Verified_graduates=("Verified graduate", "sum"),
                No_degree_confirmed=("No degree confirmed", "sum"), Enrolled_or_graduated=("Enrolled or graduated", "sum")).reset_index()
            groups["Degree unknown"] = groups["Students"] - groups["Verified_graduates"] - groups["No_degree_confirmed"]
            st.dataframe(groups, hide_index=True, use_container_width=True)
            _download_private("Download cohort comparison", groups, "student_success_cohorts.csv")


def _graduation_records(report, tables, config, refresh):
    st.subheader("Fully verified graduation records")
    st.caption("A verified award needs an exact award date, degree details, institution, and documented review. Matching and conflict checks determine whether it can contribute to the selected metrics.")
    subarea = st.radio("Graduation workspace", ["Review queue", "Award registry", "Enter or import awards"], horizontal=True)
    if subarea == "Enter or import awards":
        _record_editor("graduation", tables.graduation_records, config.graduation_records, refresh)
        return
    if subarea == "Award registry":
        awards = report["awards"]
        states = ["All records", *sorted(awards["Use in metrics"].unique())]
        state = st.selectbox("Record disposition", states)
        if state != "All records":
            awards = awards.loc[awards["Use in metrics"].eq(state)]
        st.dataframe(awards, hide_index=True, use_container_width=True)
        _download_private("Download reviewed award registry", awards, "graduation_registry_private.csv")
        st.caption("The registry includes unmatched records so they cannot disappear from review. Metric eligibility reflects the current filters and report end; the period start is applied to the headline count.")
        return
    queue = report["students"].loc[~report["students"]["Verified graduate"].astype(bool)].copy()
    mode = st.selectbox("Review priority", ["Recorded graduation awaiting full verification", "All students without a verified award", "Identity or evidence conflict"])
    if mode == "Recorded graduation awaiting full verification":
        queue = queue.loc[queue["Degree evidence"].eq("Graduated")]
    elif mode == "Identity or evidence conflict":
        queue = queue.loc[queue["Review issue"].ne("") | queue["Identity review"].astype(bool)]
    search = st.text_input("Search graduation queue by ID or name", key="success_award_search").strip()
    if search:
        queue = queue.loc[queue["Student ID"].str.contains(search, case=False, regex=False) | queue["Student Name"].str.contains(search, case=False, regex=False)]
    st.metric("Students awaiting this review", f"{len(queue):,}")
    st.dataframe(queue, hide_index=True, use_container_width=True)
    _download_private("Download graduation review queue", queue, "graduation_review_private.csv")
    if not queue.empty:
        columns = RECORD_TYPES["graduation"][0]
        template = pd.DataFrame("", index=range(len(queue)), columns=columns)
        template["Student ID"] = queue["Student ID"].to_numpy()
        template["Verification"] = "Draft"
        _download_private("Download award template for these students", template, "graduation_candidates_template.csv")
    st.info("Use Record accuracy → University records to enter term-level enrollment or degree evidence and resolve contradictions. Full award verification is a separate step. A student's resignation never removes them from this review queue.")


def _external(report):
    summary = report["public"]
    st.subheader("External summary")
    st.caption("This section exports aggregate results only. Student names, IDs, reviewer names, source references, and private notes are never included.")
    ready = [card for card in summary["cards"] if card["status"] == "Ready"]
    if ready:
        for offset in range(0, len(ready), 3):
            columns = st.columns(3)
            for column, card in zip(columns, ready[offset:offset + 3]):
                with column:
                    st.metric(card["title"], display_public_value(card))
                    st.caption(card["detail"])
    else:
        st.info("No verified headline metrics are ready for this selection. The export will show that results are being prepared; it will not display invented numbers or imply a 0% success rate.")
    st.download_button("Download external impact report (HTML)", render_public_summary(summary).encode("utf-8"), "FSL_Student_Success_Impact.html", "text/html")
    st.download_button("Download verified external metrics (CSV)", public_metrics_csv(summary), "FSL_External_Metrics.csv", "text/csv")
    copy = public_summary_text(summary)
    st.download_button("Download external summary text", copy.encode("utf-8"), "FSL_Impact_Summary.txt", "text/plain")
    with st.expander("Ready-to-use summary copy"):
        st.code(copy, language=None)
    waiting = [{"Metric": card["title"], "Readiness": card["status"], "Requirement": card["detail"]} for card in summary["cards"] if card["status"] != "Ready"]
    with st.expander("Metrics awaiting information", expanded=not bool(ready)):
        st.dataframe(pd.DataFrame(waiting), hide_index=True, use_container_width=True)
    st.caption("The external HTML is a separate shareable report. The existing P&G dashboard remains the internal/coworker analysis view. Review the selected period and population before sharing an export.")


def render_student_success(config, tables, refresh):
    st.subheader("Student Success & Impact")
    st.caption("Build a verified picture of academic outcomes and community contribution, with a separate summary for external use.")
    area = st.radio("Success & impact area", AREAS, horizontal=True, key="success_area")
    cohort_options = sorted(tables.outcomes.get("Cohort Semester", pd.Series(dtype=str)).unique(), key=_semester_sort)
    chapter_options = sorted(tables.outcomes.get("Cohort Chapter", pd.Series(dtype=str)).unique())
    with st.sidebar.expander("Student success filters", expanded=True):
        all_cohorts = st.checkbox("All joining cohorts", value=True, key="success_all_cohorts")
        cohorts = None if all_cohorts else st.multiselect("Joining semesters", cohort_options, key="success_cohorts")
        all_chapters = st.checkbox("All joining chapters", value=True, key="success_all_chapters")
        chapters = None if all_chapters else st.multiselect("Joining chapters", chapter_options, key="success_chapters")
        degree_level = st.selectbox("Degree level", ["Bachelor's", "All degree levels", *[level for level in DEGREE_LEVELS if level != "Bachelor's"]])
        cutoff = tables.reporting_cutoff
        recorded_terms = [*cohort_options, cutoff]
        for frame, column in [(tables.graduation_records, "Award Semester"), (tables.academic_records, "Semester"), (tables.impact_records, "Semester")]:
            recorded_terms.extend(term for term in frame.get(column, pd.Series(dtype=str)) if _semester_sort(term) < 999999)
        first_year = min(int(term.split()[1]) for term in recorded_terms)
        terms = [f"{season} {year}" for year in range(first_year, int(cutoff.split()[1]) + 1) for season in ["Spring", "Summer", "Fall"] if _semester_sort(f"{season} {year}") <= _semester_sort(cutoff)]
        start = st.selectbox("Award / activity period start", terms, index=terms.index(cohort_options[0] if cohort_options else cutoff), key="success_start")
        end = st.selectbox("Report end semester", terms, index=len(terms) - 1, key="success_end")
        minimum = int(st.number_input("Minimum external student group", min_value=5, value=5, step=1))
    try:
        report = _report(tables, tuple(cohorts) if cohorts is not None else None, tuple(chapters) if chapters is not None else None,
                         degree_level, start, end, minimum)
    except ValueError as exc:
        st.error(str(exc))
        return
    if area == "Overview":
        _overview(report)
    elif area == "Verified graduation records":
        _graduation_records(report, tables, config, refresh)
    elif area == "Academic & community records":
        kind = st.radio("Records workspace", ["Academic outcomes", "Community impact"], horizontal=True)
        if kind == "Academic outcomes":
            st.dataframe(report["academic_metrics"], hide_index=True, use_container_width=True)
            _record_editor("academic", tables.academic_records, config.academic_records, refresh)
        else:
            st.dataframe(report["impact_metrics"], hide_index=True, use_container_width=True)
            st.caption("Community totals use the report period and selected chapters; joining-cohort filters do not restrict these totals.")
            _record_editor("impact", tables.impact_records, config.impact_records, refresh, chapters=chapter_options)
    elif area == "External summary":
        _external(report)
    else:
        for definition in DEFINITIONS:
            st.markdown(f"- {definition}")
        readiness = [{"Dataset": "Original joining cohorts", "Ready records": len(report["students"]), "Next step": "Confirm roster completeness and resolve duplicate identities."},
                     {"Dataset": "University awards", "Ready records": len(report["awards"].loc[report["awards"]["Use in metrics"].eq("Included")]), "Next step": "Import dated university degree awards and verify each source."},
                     {"Dataset": "University term evidence", "Ready records": len(tables.university_records), "Next step": "Load enrollment and degree/non-degree evidence at the required checkpoints. A missing row is Unknown."},
                     {"Dataset": "Academic summaries", "Ready records": int(tables.academic_records.get("Verification", pd.Series(dtype=str)).eq("Verified").sum()), "Next step": "Import term-level quality points, GPA credits, earned credits, and honors."},
                     {"Dataset": "Community summaries", "Ready records": int(tables.impact_records.get("Verification", pd.Series(dtype=str)).eq("Verified").sum()), "Next step": "Enter documented semester totals and avoid overlapping subtotals."}]
        st.dataframe(pd.DataFrame(readiness), hide_index=True, use_container_width=True)
        st.info("College-entry four/six-year graduation, campus comparison groups, and transfer persistence need additional institutional entry and comparison records. They are not calculated from FSL joining dates. This section is ready for FSL-cohort evidence now; those institutional comparisons remain future work.")
