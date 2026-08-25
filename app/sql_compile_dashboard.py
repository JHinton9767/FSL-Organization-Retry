from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from app.charts import COLOR_SEQUENCE, PLOTLY_TEMPLATE
from app.exports import dataframe_to_csv_bytes
from src.sqlCompile import DEFAULT_OUTPUT_PATH, sqlCompile
from src.sqlCompile_cohort import (
    DEFAULT_COHORT_OUTPUT_DIR,
    DEFAULT_MANUAL_STATUS_PATH,
    MANUAL_STATUS_COLUMNS,
    append_manual_status_rows,
    build_new_member_cohort_report,
    write_manual_status_rows,
)
from src.sqlCompile_dashboard import (
    ODD_RECORD_COLUMNS,
    build_dashboard_rate_table,
    load_dashboard_tables,
    odd_record_editor_to_manual_rows,
)


st.set_page_config(
    page_title="FSL sqlCompile Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _format_display_frame(
    frame: pd.DataFrame,
    *,
    percent_cols: list[str] | tuple[str, ...] = (),
    integer_cols: list[str] | tuple[str, ...] = (),
) -> pd.DataFrame:
    display = frame.copy()
    for column in percent_cols:
        if column in display.columns:
            display[column] = display[column].map(lambda value: "" if pd.isna(value) else f"{value:.1%}")
    for column in integer_cols:
        if column in display.columns:
            display[column] = display[column].map(lambda value: "" if pd.isna(value) else f"{int(value):,}")
    return display


def _persistence_header() -> None:
    st.markdown(
        """
        <style>
        .txst-persistence-wrap {margin-bottom: 1rem;}
        .txst-persistence-title {color: #5C1418; font-size: 2.25rem; font-weight: 700; line-height: 1.1; margin-bottom: 0.15rem;}
        .txst-persistence-subtitle {color: #5C1418; font-size: 1.35rem; font-style: italic; font-weight: 600; margin-bottom: 1rem;}
        .txst-persistence-rule {display: grid; grid-template-columns: 24% 5% 12% 16% 14% 29%; height: 8px; overflow: hidden; border-radius: 999px; margin-bottom: 1.2rem;}
        .txst-persistence-rule > span:nth-child(1) {background: #E3A617;}
        .txst-persistence-rule > span:nth-child(2) {background: #F0D8DA;}
        .txst-persistence-rule > span:nth-child(3) {background: #E53C5B;}
        .txst-persistence-rule > span:nth-child(4) {background: #39A56A;}
        .txst-persistence-rule > span:nth-child(5) {background: #8ED0E5;}
        .txst-persistence-rule > span:nth-child(6) {background: #0B6C94;}
        .txst-note {color: #4A4A4A; font-size: 0.96rem;}
        </style>
        <div class="txst-persistence-wrap">
          <div class="txst-persistence-title">Persistence and Graduation</div>
          <div class="txst-persistence-subtitle">sqlCompile New-Member Cohorts</div>
          <div class="txst-persistence-rule">
            <span></span><span></span><span></span><span></span><span></span><span></span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _path_text(value: str | Path) -> str:
    return str(Path(value))


def _format_percent(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value):.1%}"


def _selected_cohorts(rate_table: pd.DataFrame) -> list[str]:
    options = rate_table["Cohort Semester"].dropna().astype(str).tolist() if "Cohort Semester" in rate_table.columns else []
    options = [option for option in options if option]
    return options


def _cohort_filter(label: str, options: list[str]) -> list[str]:
    if not options:
        return []
    selected = st.sidebar.multiselect(label, options=options, default=options)
    return selected or options


def _render_rate_charts(rate_table: pd.DataFrame) -> None:
    if rate_table.empty:
        st.warning("No new-member cohorts were available. Run `python sqlCompile.py --all-semesters` after your roster path is configured.")
        return

    chart_frame = rate_table.melt(
        id_vars=["Cohort Semester"],
        value_vars=["Persistence Rate", "Graduation Rate"],
        var_name="Rate",
        value_name="Value",
    ).dropna(subset=["Value"])
    fig = px.line(
        chart_frame,
        x="Cohort Semester",
        y="Value",
        color="Rate",
        markers=True,
        title="Persistence and Graduation Rates by New-Member Cohort",
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_yaxes(tickformat=".0%", range=[0, 1])
    fig.update_layout(xaxis_title="", yaxis_title="Resolved-student rate", legend_title="")
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        _format_display_frame(
            rate_table,
            percent_cols=["Manual Review Share", "Persistence Rate", "Graduation Rate", "Known Exit Rate"],
            integer_cols=[
                "Cohort Students",
                "Resolved Students",
                "Needs Manual Review",
                "Persisted / Active",
                "Graduated",
                "Known Non-Graduate Exits",
                "Other / Unresolved",
            ],
        ),
        use_container_width=True,
        hide_index=True,
    )


def _render_outcome_distribution(distribution: pd.DataFrame) -> None:
    if distribution.empty:
        return
    fig = px.bar(
        distribution,
        x="Cohort Semester",
        y="Share of Cohort",
        color="Final Outcome Bucket",
        title="Final Outcome Mix by New-Member Cohort",
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_layout(barmode="stack", xaxis_title="", yaxis_title="Share of cohort", legend_title="")
    fig.update_yaxes(tickformat=".0%", range=[0, 1])
    st.plotly_chart(fig, use_container_width=True)


def _render_manual_checker(review_template: pd.DataFrame, manual_status_file: Path) -> None:
    st.subheader("Manual Checker")
    st.caption("These are students whose latest compiled status is `A`. Fill in the verified form result, save it, then refresh the dashboard.")

    if review_template.empty:
        st.success("No odd records are currently waiting for manual form review.")
    else:
        edited = st.data_editor(
            review_template,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={
                "Status": st.column_config.SelectboxColumn(
                    "Status",
                    options=["", "D", "G", "RS", "RV", "S", "T", "AL", "H", "A", "N"],
                ),
                "Semester": st.column_config.TextColumn("Correct Semester"),
                "Notes": st.column_config.TextColumn("Notes"),
            },
            disabled=["Cohort Semester", "Cohort Chapter", "Student ID", "Last Known Semester", "Last Known Chapter", "Last Known Status"],
            key="sql_compile_odd_record_editor",
        )
        save_cols = st.columns([1, 2])
        with save_cols[0]:
            if st.button("Save Completed Manual Rows", use_container_width=True):
                manual_rows = odd_record_editor_to_manual_rows(edited)
                try:
                    path, saved = append_manual_status_rows(manual_rows, manual_status_file)
                    if saved:
                        st.success(f"Saved {saved:,} completed manual row(s) to {path}.")
                        st.rerun()
                    else:
                        st.warning("Fill in at least Student ID, Semester, and Status before saving.")
                except OSError as exc:
                    st.error(f"Could not save manual rows. Close the CSV if it is open, then try again. Details: {exc}")
        with save_cols[1]:
            st.download_button(
                "Download Odd Records CSV",
                data=dataframe_to_csv_bytes(review_template),
                file_name="new_member_form_review.csv",
                mime="text/csv",
                use_container_width=True,
            )


def _render_manual_rows_editor(manual_rows: pd.DataFrame, manual_status_file: Path) -> None:
    st.subheader("Saved Manual Rows")
    editor_source = manual_rows if not manual_rows.empty else pd.DataFrame(columns=MANUAL_STATUS_COLUMNS)
    edited_manual = st.data_editor(
        editor_source,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        column_config={
            "Status": st.column_config.SelectboxColumn(
                "Status",
                options=["", "D", "G", "RS", "RV", "S", "T", "AL", "H", "A", "N"],
            ),
        },
        key="sql_compile_manual_rows_editor",
    )
    if st.button("Save Manual CSV", use_container_width=True):
        try:
            path = write_manual_status_rows(edited_manual, manual_status_file)
            st.success(f"Saved manual rows to {path}.")
            st.rerun()
        except OSError as exc:
            st.error(f"Could not save manual rows. Close the CSV if it is open, then try again. Details: {exc}")


def main() -> None:
    _persistence_header()
    st.caption("New baseline dashboard powered by the sqlCompile roster database and manual status review file.")

    default_database = DEFAULT_OUTPUT_PATH
    default_manual = DEFAULT_MANUAL_STATUS_PATH
    database_path = Path(st.sidebar.text_input("SQLite database", value=_path_text(default_database)))
    manual_status_file = Path(st.sidebar.text_input("Manual status CSV", value=_path_text(default_manual)))

    st.sidebar.subheader("Refresh")
    if st.sidebar.button("Run sqlCompile", use_container_width=True):
        try:
            result = sqlCompile(output_path=database_path)
            st.sidebar.success(f"Compiled {result.row_count:,} rows from {result.source_file_count:,} Excel file(s).")
            st.rerun()
        except Exception as exc:
            st.sidebar.error(f"Compile failed: {exc}")

    if not database_path.exists():
        st.error("No sqlCompile SQLite file was found. Run `python sqlCompile.py --all-semesters` first, or use the sidebar button after your roster paths are configured.")
        return

    try:
        all_tables = load_dashboard_tables(database_path=database_path, manual_status_file=manual_status_file, all_cohorts=True)
    except Exception as exc:
        st.error(f"Could not load sqlCompile dashboard data: {exc}")
        return

    cohort_options = _selected_cohorts(all_tables.rate_table)
    selected_cohorts = _cohort_filter("New-member cohorts", cohort_options)
    rate_table = all_tables.rate_table.loc[all_tables.rate_table["Cohort Semester"].isin(selected_cohorts)].copy() if selected_cohorts else all_tables.rate_table
    outcomes = all_tables.outcomes.loc[all_tables.outcomes["Cohort Semester"].isin(selected_cohorts)].copy() if selected_cohorts and not all_tables.outcomes.empty else all_tables.outcomes
    review_template = all_tables.manual_entry_template.loc[all_tables.manual_entry_template["Cohort Semester"].isin(selected_cohorts)].copy() if selected_cohorts and not all_tables.manual_entry_template.empty else all_tables.manual_entry_template
    distribution = all_tables.outcome_distribution.loc[all_tables.outcome_distribution["Cohort Semester"].isin(selected_cohorts)].copy() if selected_cohorts and not all_tables.outcome_distribution.empty else all_tables.outcome_distribution

    if st.sidebar.button("Write Report Files", use_container_width=True):
        try:
            report = build_new_member_cohort_report(
                database_path=database_path,
                cohort_semesters=selected_cohorts or None,
                all_cohorts=not selected_cohorts,
                manual_status_file=manual_status_file,
                output_dir=DEFAULT_COHORT_OUTPUT_DIR,
            )
            st.sidebar.success(f"Wrote report files to {report.output_dir}.")
        except Exception as exc:
            st.sidebar.error(f"Report write failed: {exc}")

    kpi_frame = build_dashboard_rate_table(outcomes)
    cohort_students = int(kpi_frame["Cohort Students"].sum()) if not kpi_frame.empty else 0
    resolved_students = int(kpi_frame["Resolved Students"].sum()) if not kpi_frame.empty else 0
    manual_review = int(kpi_frame["Needs Manual Review"].sum()) if not kpi_frame.empty else 0
    persisted = int(kpi_frame["Persisted / Active"].sum()) if not kpi_frame.empty else 0
    graduated = int(kpi_frame["Graduated"].sum()) if not kpi_frame.empty else 0

    kpis = st.columns(5)
    with kpis[0]:
        st.metric("New Members", f"{cohort_students:,}")
    with kpis[1]:
        st.metric("Resolved", f"{resolved_students:,}")
    with kpis[2]:
        st.metric("Manual Review", f"{manual_review:,}")
    with kpis[3]:
        st.metric("Persistence Rate", _format_percent(persisted / resolved_students if resolved_students else pd.NA))
    with kpis[4]:
        st.metric("Graduation Rate", _format_percent(graduated / resolved_students if resolved_students else pd.NA))

    rates_tab, outcomes_tab, checker_tab, manual_rows_tab = st.tabs(
        ["Persistence & Graduation", "Outcome Mix", "Manual Checker", "Manual Rows"]
    )

    with rates_tab:
        _render_rate_charts(rate_table)

    with outcomes_tab:
        _render_outcome_distribution(distribution)
        if not outcomes.empty:
            st.dataframe(outcomes, use_container_width=True, hide_index=True)

    with checker_tab:
        _render_manual_checker(review_template.loc[:, ODD_RECORD_COLUMNS] if not review_template.empty else review_template, manual_status_file)

    with manual_rows_tab:
        _render_manual_rows_editor(all_tables.manual_rows, manual_status_file)


if __name__ == "__main__":
    main()
