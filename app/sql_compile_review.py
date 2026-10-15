from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from src.sqlCompile import COMPILE_ISSUES_TABLE
from src.sqlCompile_audit import read_change_history, change_contents
from src.sqlCompile_cohort import read_roster_inventory_table, read_zero_member_periods
from src.sqlCompile_quality import COVERAGE_STATES, COVERAGE_COLUMNS, build_coverage, read_coverage, save_coverage, coverage_findings
from src.sqlCompile_records import UNIVERSITY_COLUMNS, DEGREE_VIEW, ENROLLMENT_VIEW, read_university_records, save_university_records, normalize_university_records
from src.sqlCompile_audit import review_context
from src.sqlCompile_sources import SOURCE_MANIFEST, SOURCE_OBSERVATIONS, read_optional_table


def render_university_checker(config, tables, dashboard, year, view, refresh) -> None:
    st.subheader("University evidence checker")
    details = dashboard["detail_frame"]
    queue = details.loc[details["Milestone Sort"].eq(year) & details["Milestone Status"].ne("Future")].copy()
    st.caption(f"This queue uses the same students, {year} Year checkpoint, measure, and comparison population as the chart. Future checkpoints are excluded.")
    if queue.empty:
        st.info("No eligible students match this checkpoint.")
        return
    options = sorted(queue["P&G Outcome Bucket"].unique())
    bucket = st.selectbox("Outcome to review", ["All", *options], index=options.index("Unknown") + 1 if "Unknown" in options else 0)
    if bucket != "All":
        queue = queue.loc[queue["P&G Outcome Bucket"].eq(bucket)]
    search = st.text_input("Find student by ID or name", key="university_checker_search").strip()
    if search:
        queue = queue.loc[queue["Student ID"].str.contains(search, case=False, regex=False) | queue["Student Name"].fillna("").str.contains(search, case=False, regex=False)]
    visible = queue[["Student ID", "Student Name", "Cohort Semester", "Cohort Chapter", "P&G Outcome Bucket", "Degree Evidence", "Enrollment Evidence", "Evidence Conflict"]].copy()
    visible.insert(0, "Select", False)
    edited = st.data_editor(visible, hide_index=True, use_container_width=True, height=400,
                            disabled=list(visible.columns[1:]), key=f"university_checker_{view}_{year}_{bucket}_{search}_{st.session_state.get('sql_compile_dashboard_data_refresh_token', 0)}")
    selected = edited.loc[edited["Select"].fillna(False), "Student ID"].tolist()
    st.download_button("Download this review queue", queue.to_csv(index=False).encode("utf-8"), "university_review_queue.csv", "text/csv")
    status = st.selectbox("Verified university outcome", ["Graduated", "No degree"] if view == DEGREE_VIEW else ["Enrolled", "Not enrolled"])
    semester = st.text_input("Effective semester of the evidence", placeholder="Spring 2024")
    evidence = st.text_input("University source or reference", key="university_checker_source")
    notes = st.text_input("Review notes", key="university_checker_notes")
    st.caption("Enter the actual effective semester from the source. Saving an outcome at the displayed checkpoint does not backdate it to an earlier graduation or enrollment term.")
    if st.button("Save university evidence for selected students", disabled=not selected):
        reviewer = st.session_state.get("record_reviewer", "").strip()
        incoming = pd.DataFrame([{"Student ID": student, "Semester": semester,
                                 "Degree Status": status if view == DEGREE_VIEW else "",
                                 "Enrollment Status": status if view == ENROLLMENT_VIEW else "",
                                 "Source": evidence, "Reviewed By": reviewer, "Resolution": "", "Notes": notes}
                                for student in selected], columns=UNIVERSITY_COLUMNS)
        try:
            with review_context(reviewer, notes or "Verified university outcome", evidence):
                save_university_records(pd.concat([tables.university_records, incoming], ignore_index=True),
                                        config.university_records, expected_rows=tables.university_records)
            refresh()
            st.rerun()
        except (OSError, ValueError) as exc:
            st.error(str(exc))


def render_record_review(config, tables, refresh) -> None:
    st.subheader("Record accuracy")
    st.caption("Review source coverage and university evidence here. Saved decisions keep a permanent before-and-after history on this computer.")
    tab = st.selectbox("Review area", ["Source coverage", "Evidence conflicts", "University records", "Student history", "Correction history"])
    if tab == "Source coverage":
        inventory = read_roster_inventory_table(config.database)
        saved = read_coverage(config.coverage_reviews)
        coverage = build_coverage(inventory, tables.reporting_cutoff, saved, read_zero_member_periods(config.zero_member_periods))
        errors, warnings = coverage_findings(coverage)
        for issue in errors:
            st.error(issue)
        for issue in warnings:
            st.warning(issue)
        edited = st.data_editor(coverage, hide_index=True, num_rows="dynamic", use_container_width=True,
                                disabled=["Observed Rows", "Roster Versions"],
                                column_config={"Coverage": st.column_config.SelectboxColumn(options=COVERAGE_STATES)},
                                key=f"coverage_editor_{st.session_state.get('sql_compile_dashboard_data_refresh_token', 0)}")
        st.caption("Mark a term Complete only after checking every expected roster. Use Not expected for a verified chapter absence; use Zero members for a verified empty chapter.")
        if st.button("Save coverage decisions"):
            try:
                # Keep future decisions even though this view stops at the reporting cutoff.
                from src.sqlCompile_cohort import _semester_sort
                future = saved.loc[saved["Semester"].map(_semester_sort).gt(_semester_sort(tables.reporting_cutoff))]
                save_coverage(pd.concat([edited.reindex(columns=COVERAGE_COLUMNS), future], ignore_index=True), config.coverage_reviews, expected_rows=saved)
                refresh()
                st.rerun()
            except (OSError, ValueError) as exc:
                st.error(str(exc))
        with st.expander("Imported files and excluded records"):
            st.dataframe(read_optional_table(config.database, SOURCE_MANIFEST), hide_index=True, use_container_width=True)
            issues = read_optional_table(config.database, COMPILE_ISSUES_TABLE)
            st.dataframe(issues, hide_index=True, use_container_width=True)
            st.download_button("Download source issues", issues.to_csv(index=False).encode("utf-8"), "source_issues.csv", "text/csv")
    elif tab == "Evidence conflicts":
        from src.sqlCompile_dashboard import build_sql_compile_milestone_dashboard
        dashboard = build_sql_compile_milestone_dashboard(tables.timeline, tables.outcomes,
            reporting_cutoff=tables.reporting_cutoff, record_history=tables.record_history, outcome_view=DEGREE_VIEW)
        details = dashboard["detail_frame"]
        conflicts = details.loc[details.get("Evidence Conflict", pd.Series("", index=details.index)).fillna("").ne("")]
        st.caption("Only contradictory evidence and identity conflicts appear here. Missing evidence stays Unknown without being treated as an established exit.")
        if conflicts.empty:
            st.success("No conflicting evidence was found at the reported checkpoints.")
        else:
            st.dataframe(conflicts, hide_index=True, use_container_width=True)
            st.download_button("Download evidence conflicts", conflicts.to_csv(index=False).encode("utf-8"), "evidence_conflicts.csv", "text/csv")
    elif tab == "University records":
        saved = read_university_records(config.university_records)
        st.caption("Use explicit university enrollment and degree evidence. Chapter resignation, alumni status, GPA, or disappearance do not establish these facts. Semester is the effective semester of the evidence.")
        st.download_button("Download university record template", pd.DataFrame(columns=UNIVERSITY_COLUMNS).to_csv(index=False), "university_records_template.csv", "text/csv")
        uploaded = st.file_uploader("Optional university CSV in the template format", type=["csv"])
        working = saved
        if uploaded is not None:
            try:
                incoming = normalize_university_records(pd.read_csv(uploaded, dtype=str).fillna(""))
                working = pd.concat([saved, incoming], ignore_index=True).drop_duplicates()
                st.info(f"Previewing {len(incoming):,} incoming rows. Save to add them to the dashboard.")
            except (ValueError, pd.errors.ParserError) as exc:
                st.error(str(exc))
                return
        edited = st.data_editor(working, num_rows="dynamic", hide_index=True, use_container_width=True,
            column_config={
                "Enrollment Status": st.column_config.SelectboxColumn(options=["", "Unknown", "Enrolled", "Not enrolled"]),
                "Degree Status": st.column_config.SelectboxColumn(options=["", "Unknown", "Graduated", "No degree"]),
                "Resolution": st.column_config.SelectboxColumn(options=["", "Use university record"]),
            }, key=f"university_editor_{st.session_state.get('sql_compile_dashboard_data_refresh_token', 0)}")
        st.caption("Leave Resolution blank normally. Use university record overrides earlier degree evidence from its effective semester, only after investigating a conflict and recording the reason in Notes. Conflicting university rows still require review.")
        if st.button("Save university records"):
            try:
                save_university_records(edited, config.university_records, expected_rows=saved)
                refresh()
                st.rerun()
            except (OSError, ValueError) as exc:
                st.error(str(exc))
    elif tab == "Student history":
        student = st.text_input("Student ID").strip().upper()
        if student:
            records = tables.record_history
            st.dataframe(records.loc[records["Student ID"].eq(student)], hide_index=True, use_container_width=True)
            with st.expander("Original roster observations"):
                raw = read_optional_table(config.database, SOURCE_OBSERVATIONS)
                if "Student ID" in raw:
                    st.dataframe(raw.loc[raw["Student ID"].eq(student)], hide_index=True, use_container_width=True)
    else:
        paths = [config.manual_status, config.name_choices, config.name_rechecks, config.coverage_reviews, config.university_records]
        path = st.selectbox("Decision file", paths, format_func=lambda p: p.name)
        history = read_change_history(path)
        st.dataframe(history.drop(columns=["before_hash", "after_hash"], errors="ignore"), hide_index=True, use_container_width=True)
        if not history.empty:
            chosen = st.selectbox("Inspect a saved change", history["id"], format_func=lambda key: str(history.loc[history["id"].eq(key), "recorded_at"].iloc[0]))
            before, after = change_contents(path, chosen)
            for label, content in [("Before", before), ("After", after)]:
                with st.expander(label):
                    st.dataframe(pd.read_csv(io.BytesIO(content), dtype=str).fillna("") if content else pd.DataFrame(), hide_index=True, use_container_width=True)
                    st.download_button(f"Download {label.lower()} records", content, f"{label.lower()}_{path.name}", "text/csv")
