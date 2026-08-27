from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from app.charts import COLOR_SEQUENCE, PLOTLY_TEMPLATE, persistence_milestone_chart
from app.exports import dataframe_to_csv_bytes
from src.sqlCompile import DEFAULT_OUTPUT_PATH, sqlCompile
from src.sqlCompile_cohort import (
    DEFAULT_COHORT_OUTPUT_DIR,
    DEFAULT_MANUAL_STATUS_PATH,
    MANUAL_STATUS_COLUMNS,
    append_manual_status_rows,
    build_new_member_cohort_report,
    completed_manual_status_rows,
    write_manual_status_rows,
)
from src.sqlCompile_dashboard import (
    MANUAL_CHECKER_COLUMNS,
    MANUAL_CHECKER_SELECT_COLUMN,
    ODD_RECORD_COLUMNS,
    SQL_COMPILE_ALL_TIME_LABEL,
    build_dashboard_rate_table,
    build_manual_checker_queue,
    build_sql_compile_milestone_dashboard,
    load_dashboard_tables,
    odd_record_editor_to_manual_rows,
)
from src.persistence_outcomes import PERSISTENCE_OUTCOME_ORDER


st.set_page_config(
    page_title="FSL sqlCompile Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)


STATUS_OPTIONS = ["", "D", "G", "RS", "RV", "S", "T", "AL", "H", "CK", "A", "N"]
MANUAL_CHECKER_ROW_ID = "_manual_checker_row_id"
MANUAL_CHECKER_STATE_KEY = "sql_compile_manual_checker_rows"
MANUAL_CHECKER_SIGNATURE_KEY = "sql_compile_manual_checker_signature"
MANUAL_CHECKER_EDITOR_KEY = "sql_compile_manual_checker_editor"


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
        [data-testid="stMetric"] {background: transparent; border: 0; padding: 0.15rem 0 0.5rem 0;}
        [data-testid="stMetricLabel"] p {color: #17213A; font-size: 0.92rem;}
        [data-testid="stMetricValue"] div {color: #17213A; font-size: 2rem; font-weight: 400;}
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


def _unique_nonempty_options(frame: pd.DataFrame, column: str) -> list[str]:
    if frame.empty or column not in frame.columns:
        return []
    values = frame[column].fillna("").astype(str).str.strip()
    return sorted(value for value in values.unique().tolist() if value)


def _manual_checker_signature(review_template: pd.DataFrame) -> tuple[tuple[str, ...], ...]:
    if review_template.empty:
        return ()
    columns = [
        column
        for column in [
            "Cohort Semester",
            "Cohort Chapter",
            "Student ID",
            "Last Known Semester",
            "Last Known Chapter",
            "Last Known Status",
        ]
        if column in review_template.columns
    ]
    if not columns:
        return tuple((str(index),) for index in review_template.index.tolist())
    prepared = review_template.loc[:, columns].fillna("").astype(str)
    return tuple(tuple(row) for row in prepared.to_numpy().tolist())


def _with_manual_checker_row_ids(queue: pd.DataFrame) -> pd.DataFrame:
    result = queue.copy()
    if result.empty:
        result[MANUAL_CHECKER_ROW_ID] = pd.Series(dtype="object")
        return result

    id_columns = ["Cohort Semester", "Cohort Chapter", "Student ID", "Last Known Semester", "Last Known Chapter"]
    row_ids: list[str] = []
    for position, row in result.reset_index(drop=True).iterrows():
        identity = "|".join(str(row.get(column, "")).strip() for column in id_columns)
        row_ids.append(f"{position}|{identity}")
    result[MANUAL_CHECKER_ROW_ID] = row_ids
    return result


def _ensure_manual_checker_state(review_template: pd.DataFrame) -> pd.DataFrame:
    signature = _manual_checker_signature(review_template)
    if st.session_state.get(MANUAL_CHECKER_SIGNATURE_KEY) != signature:
        queue = _with_manual_checker_row_ids(build_manual_checker_queue(review_template))
        st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
        st.session_state[MANUAL_CHECKER_SIGNATURE_KEY] = signature

    stored = st.session_state.get(MANUAL_CHECKER_STATE_KEY, pd.DataFrame(columns=[*MANUAL_CHECKER_COLUMNS, MANUAL_CHECKER_ROW_ID]))
    queue = stored.copy() if isinstance(stored, pd.DataFrame) else pd.DataFrame(stored)
    for column in MANUAL_CHECKER_COLUMNS:
        if column not in queue.columns:
            queue[column] = False if column == MANUAL_CHECKER_SELECT_COLUMN else ""
    if MANUAL_CHECKER_ROW_ID not in queue.columns:
        queue = _with_manual_checker_row_ids(queue)
    queue[MANUAL_CHECKER_SELECT_COLUMN] = queue[MANUAL_CHECKER_SELECT_COLUMN].fillna(False).astype(bool)
    for column in [*ODD_RECORD_COLUMNS, MANUAL_CHECKER_ROW_ID]:
        queue[column] = queue[column].fillna("").astype(str).str.strip()
    queue = queue.loc[:, [*MANUAL_CHECKER_COLUMNS, MANUAL_CHECKER_ROW_ID]].reset_index(drop=True)
    st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
    return queue


def _strip_manual_checker_internal_columns(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.drop(columns=[MANUAL_CHECKER_ROW_ID], errors="ignore").copy()


def _manual_checker_display_frame(frame: pd.DataFrame) -> pd.DataFrame:
    display = frame.loc[:, MANUAL_CHECKER_COLUMNS].copy()
    display.index = frame[MANUAL_CHECKER_ROW_ID].fillna("").astype(str)
    display.index.name = MANUAL_CHECKER_ROW_ID
    return display


def _merge_manual_checker_edits(queue: pd.DataFrame, edited: pd.DataFrame) -> pd.DataFrame:
    if edited.empty:
        return queue

    edited_work = edited.copy()
    if MANUAL_CHECKER_ROW_ID not in edited_work.columns:
        edited_work[MANUAL_CHECKER_ROW_ID] = edited_work.index.astype(str)
    update_columns = [MANUAL_CHECKER_SELECT_COLUMN, "Semester", "Chapter", "Status", "Notes"]
    result = queue.copy().set_index(MANUAL_CHECKER_ROW_ID, drop=False)
    updates = edited_work.set_index(MANUAL_CHECKER_ROW_ID, drop=False)
    shared_ids = result.index.intersection(updates.index)
    for column in update_columns:
        if column in updates.columns:
            result.loc[shared_ids, column] = updates.loc[shared_ids, column]
    result[MANUAL_CHECKER_SELECT_COLUMN] = result[MANUAL_CHECKER_SELECT_COLUMN].fillna(False).astype(bool)
    for column in ["Semester", "Chapter", "Status", "Notes"]:
        result[column] = result[column].fillna("").astype(str).str.strip()
    return result.reset_index(drop=True)


def _filter_manual_checker_rows(
    queue: pd.DataFrame,
    *,
    search_text: str,
    cohort_filter: list[str],
    chapter_filter: list[str],
    last_semester_filter: list[str],
    entry_status_filter: list[str],
    unfinished_only: bool,
) -> pd.DataFrame:
    if queue.empty:
        return queue

    mask = pd.Series(True, index=queue.index)
    if search_text.strip():
        search_columns = [
            "Cohort Semester",
            "Cohort Chapter",
            "Student ID",
            "Last Known Semester",
            "Last Known Chapter",
            "Last Known Status",
            "Semester",
            "Chapter",
            "Status",
            "Notes",
        ]
        haystack = queue[search_columns].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
        mask &= haystack.str.contains(search_text.strip().lower(), regex=False, na=False)
    for column, selected in [
        ("Cohort Semester", cohort_filter),
        ("Cohort Chapter", chapter_filter),
        ("Last Known Semester", last_semester_filter),
    ]:
        if selected:
            mask &= queue[column].isin(selected)
    if entry_status_filter:
        normalized = queue["Status"].fillna("").astype(str).str.strip()
        selected_statuses = set(entry_status_filter)
        status_mask = pd.Series(False, index=queue.index)
        if "Blank" in selected_statuses:
            status_mask |= normalized.eq("")
        selected_statuses.discard("Blank")
        if selected_statuses:
            status_mask |= normalized.isin(selected_statuses)
        mask &= status_mask
    if unfinished_only:
        mask &= queue["Status"].fillna("").astype(str).str.strip().eq("")
    return queue.loc[mask].copy()


def _completed_manual_checker_rows(queue: pd.DataFrame) -> pd.DataFrame:
    manual_rows = odd_record_editor_to_manual_rows(_strip_manual_checker_internal_columns(queue))
    return completed_manual_status_rows(manual_rows)


def _saved_manual_rows_for_queue(manual_rows: pd.DataFrame, queue: pd.DataFrame) -> pd.DataFrame:
    if manual_rows.empty or queue.empty:
        return pd.DataFrame(columns=MANUAL_STATUS_COLUMNS)
    keys = set(
        tuple(row)
        for row in queue.loc[:, ["Cohort Semester", "Cohort Chapter", "Student ID"]]
        .fillna("")
        .astype(str)
        .to_numpy()
        .tolist()
    )
    prepared = manual_rows.copy()
    for column in ["Cohort Semester", "Cohort Chapter", "Student ID"]:
        if column not in prepared.columns:
            prepared[column] = ""
        prepared[column] = prepared[column].fillna("").astype(str).str.strip()
    mask = prepared.apply(
        lambda row: (row["Cohort Semester"], row["Cohort Chapter"], row["Student ID"]) in keys,
        axis=1,
    )
    return prepared.loc[mask, MANUAL_STATUS_COLUMNS].reset_index(drop=True)


def _selected_cohorts(rate_table: pd.DataFrame) -> list[str]:
    options = rate_table["Cohort Semester"].dropna().astype(str).tolist() if "Cohort Semester" in rate_table.columns else []
    options = [option for option in options if option]
    return options


def _cohort_filter(options: list[str]) -> tuple[list[str], str]:
    if not options:
        return [], SQL_COMPILE_ALL_TIME_LABEL

    mode = st.sidebar.radio(
        "Cohort selection",
        options=["All semesters", "Single semester", "Semester group"],
        index=0,
    )
    if mode == "All semesters":
        return options, SQL_COMPILE_ALL_TIME_LABEL
    if mode == "Single semester":
        selected = st.sidebar.selectbox("New-member semester", options=options, index=len(options) - 1)
        return [selected], str(selected)

    selected = st.sidebar.multiselect("New-member semesters", options=options, default=options)
    if not selected:
        return [], "No Semesters"
    if len(selected) == len(options):
        return selected, SQL_COMPILE_ALL_TIME_LABEL
    if len(selected) == 1:
        return selected, str(selected[0])
    return selected, f"{len(selected)} Semesters"


def _render_rate_charts(rate_table: pd.DataFrame, milestone_dashboard: dict[str, object], selected_label: str) -> None:
    if rate_table.empty:
        st.warning("No new-member cohorts were available. Run `python sqlCompile.py --all-semesters` after your roster path is configured.")
        return

    chart_frame = milestone_dashboard.get("chart_frame", pd.DataFrame())
    table_frame = milestone_dashboard.get("table_frame", pd.DataFrame())
    title = f"Persistence and Graduation for {selected_label}"
    subtitle = "ALL distinction | Roster outcomes first | Manual corrections last | Explicit graduation evidence only"
    st.plotly_chart(
        persistence_milestone_chart(chart_frame, title=title, subtitle=subtitle),
        use_container_width=True,
    )

    if not table_frame.empty:
        count_columns = ["Measured Students", *[f"{outcome} Count" for outcome in PERSISTENCE_OUTCOME_ORDER]]
        st.dataframe(
            _format_display_frame(table_frame, percent_cols=PERSISTENCE_OUTCOME_ORDER, integer_cols=count_columns),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Resolved Rate Summary")
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


def _render_manual_checker(review_template: pd.DataFrame, manual_status_file: Path, manual_rows: pd.DataFrame) -> None:
    st.subheader("Manual Checker")
    st.caption("Work the odd-record queue here. Select rows, fill the verified outcome, and save completed decisions to the manual status CSV.")

    queue = _ensure_manual_checker_state(review_template)
    saved_for_queue = _saved_manual_rows_for_queue(manual_rows, queue)

    if queue.empty:
        st.success("No odd records are currently waiting for manual form review.")
        if not saved_for_queue.empty:
            with st.expander(f"Saved manual decisions for this selection ({len(saved_for_queue):,})", expanded=False):
                st.dataframe(saved_for_queue, use_container_width=True, hide_index=True)
        return

    filter_cols = st.columns([1.4, 1, 1, 1])
    with filter_cols[0]:
        search_text = st.text_input("Search queue", placeholder="Student ID, chapter, semester, status, note", key="sql_compile_manual_checker_search")
    with filter_cols[1]:
        cohort_filter = st.multiselect("Cohort", options=_unique_nonempty_options(queue, "Cohort Semester"), key="sql_compile_manual_checker_cohort_filter")
    with filter_cols[2]:
        chapter_filter = st.multiselect("Chapter", options=_unique_nonempty_options(queue, "Cohort Chapter"), key="sql_compile_manual_checker_chapter_filter")
    with filter_cols[3]:
        last_semester_filter = st.multiselect("Last seen", options=_unique_nonempty_options(queue, "Last Known Semester"), key="sql_compile_manual_checker_last_seen_filter")

    detail_filter_cols = st.columns([1, 1, 2])
    with detail_filter_cols[0]:
        entry_status_filter = st.multiselect(
            "Entered status",
            options=["Blank", *[option for option in STATUS_OPTIONS if option]],
            key="sql_compile_manual_checker_status_filter",
        )
    with detail_filter_cols[1]:
        unfinished_only = st.checkbox("Only unfinished", value=False, key="sql_compile_manual_checker_unfinished_only")
    with detail_filter_cols[2]:
        st.caption(f"Manual file: `{manual_status_file}`")

    visible_queue = _filter_manual_checker_rows(
        queue,
        search_text=search_text,
        cohort_filter=cohort_filter,
        chapter_filter=chapter_filter,
        last_semester_filter=last_semester_filter,
        entry_status_filter=entry_status_filter,
        unfinished_only=unfinished_only,
    )
    st.caption(f"Showing {len(visible_queue):,} of {len(queue):,} odd record(s).")

    if visible_queue.empty:
        st.warning("No odd records match the current filters.")
    else:
        editor_height = min(820, max(320, 92 + (len(visible_queue) * 35)))
        edited = st.data_editor(
            _manual_checker_display_frame(visible_queue),
            use_container_width=True,
            hide_index=True,
            height=editor_height,
            num_rows="fixed",
            column_config={
                MANUAL_CHECKER_SELECT_COLUMN: st.column_config.CheckboxColumn("Select"),
                "Status": st.column_config.SelectboxColumn(
                    "Status",
                    options=STATUS_OPTIONS,
                ),
                "Semester": st.column_config.TextColumn("Correct Semester"),
                "Chapter": st.column_config.TextColumn("Correct Chapter"),
                "Notes": st.column_config.TextColumn("Notes"),
            },
            disabled=["Cohort Semester", "Cohort Chapter", "Student ID", "Last Known Semester", "Last Known Chapter", "Last Known Status"],
            key=MANUAL_CHECKER_EDITOR_KEY,
        )
        queue = _merge_manual_checker_edits(queue, edited)
        st.session_state[MANUAL_CHECKER_STATE_KEY] = queue

    selected_mask = queue[MANUAL_CHECKER_SELECT_COLUMN].fillna(False).astype(bool)
    selected_queue = queue.loc[selected_mask].copy()
    completed_rows = _completed_manual_checker_rows(queue)
    completed_selected_rows = _completed_manual_checker_rows(selected_queue)

    metric_cols = st.columns(5)
    with metric_cols[0]:
        st.metric("Queue records", f"{len(queue):,}")
    with metric_cols[1]:
        st.metric("Visible records", f"{len(visible_queue):,}")
    with metric_cols[2]:
        st.metric("Selected", f"{len(selected_queue):,}")
    with metric_cols[3]:
        st.metric("Ready to save", f"{len(completed_rows):,}")
    with metric_cols[4]:
        st.metric("Saved for queue", f"{len(saved_for_queue):,}")

    selection_cols = st.columns(4)
    visible_ids = set(visible_queue[MANUAL_CHECKER_ROW_ID].tolist()) if not visible_queue.empty else set()
    with selection_cols[0]:
        if st.button("Select Visible", use_container_width=True, disabled=not visible_ids):
            queue.loc[queue[MANUAL_CHECKER_ROW_ID].isin(visible_ids), MANUAL_CHECKER_SELECT_COLUMN] = True
            st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
            st.rerun()
    with selection_cols[1]:
        if st.button("Select Unfinished", use_container_width=True, disabled=not visible_ids):
            unfinished_visible = (
                queue[MANUAL_CHECKER_ROW_ID].isin(visible_ids)
                & queue["Status"].fillna("").astype(str).str.strip().eq("")
            )
            queue.loc[unfinished_visible, MANUAL_CHECKER_SELECT_COLUMN] = True
            st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
            st.rerun()
    with selection_cols[2]:
        if st.button("Clear Selection", use_container_width=True, disabled=selected_queue.empty):
            queue[MANUAL_CHECKER_SELECT_COLUMN] = False
            st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
            st.rerun()
    with selection_cols[3]:
        st.download_button(
            "Download Selected",
            data=dataframe_to_csv_bytes(_strip_manual_checker_internal_columns(selected_queue)),
            file_name="sql_compile_manual_queue_selected.csv",
            mime="text/csv",
            use_container_width=True,
            disabled=selected_queue.empty,
        )

    batch_cols = st.columns([0.8, 1, 1, 1.8])
    with batch_cols[0]:
        batch_status = st.selectbox("Batch status", options=STATUS_OPTIONS, key="sql_compile_manual_checker_batch_status")
    with batch_cols[1]:
        batch_semester = st.text_input("Batch semester", key="sql_compile_manual_checker_batch_semester")
    with batch_cols[2]:
        batch_chapter = st.text_input("Batch chapter", key="sql_compile_manual_checker_batch_chapter")
    with batch_cols[3]:
        batch_notes = st.text_input("Batch notes", key="sql_compile_manual_checker_batch_notes")

    action_cols = st.columns(5)
    with action_cols[0]:
        if st.button("Apply to Selected", type="primary", use_container_width=True, disabled=selected_queue.empty):
            mask = queue[MANUAL_CHECKER_SELECT_COLUMN].fillna(False).astype(bool)
            if batch_status:
                queue.loc[mask, "Status"] = batch_status
            if batch_semester.strip():
                queue.loc[mask, "Semester"] = batch_semester.strip()
            if batch_chapter.strip():
                queue.loc[mask, "Chapter"] = batch_chapter.strip()
            if batch_notes.strip():
                queue.loc[mask, "Notes"] = batch_notes.strip()
            st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
            st.rerun()
    with action_cols[1]:
        if st.button("Use Last Seen", use_container_width=True, disabled=selected_queue.empty):
            mask = queue[MANUAL_CHECKER_SELECT_COLUMN].fillna(False).astype(bool)
            blank_semester = queue["Semester"].fillna("").astype(str).str.strip().eq("")
            blank_chapter = queue["Chapter"].fillna("").astype(str).str.strip().eq("")
            queue.loc[mask & blank_semester, "Semester"] = queue.loc[mask & blank_semester, "Last Known Semester"]
            queue.loc[mask & blank_chapter, "Chapter"] = queue.loc[mask & blank_chapter, "Last Known Chapter"]
            st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
            st.rerun()
    with action_cols[2]:
        if st.button("Clear Selected", use_container_width=True, disabled=selected_queue.empty):
            mask = queue[MANUAL_CHECKER_SELECT_COLUMN].fillna(False).astype(bool)
            queue.loc[mask, ["Semester", "Status", "Notes"]] = ""
            st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
            st.rerun()
    with action_cols[3]:
        if st.button("Save Selected", use_container_width=True, disabled=completed_selected_rows.empty):
            try:
                path, saved = append_manual_status_rows(completed_selected_rows, manual_status_file)
                if saved:
                    st.success(f"Saved {saved:,} selected manual row(s) to {path}.")
                    st.rerun()
                else:
                    st.warning("Fill in at least Student ID, Semester, and Status before saving.")
            except OSError as exc:
                st.error(f"Could not save manual rows. Close the CSV if it is open, then try again. Details: {exc}")
    with action_cols[4]:
        if st.button("Save All Ready", use_container_width=True, disabled=completed_rows.empty):
            try:
                path, saved = append_manual_status_rows(completed_rows, manual_status_file)
                if saved:
                    st.success(f"Saved {saved:,} completed manual row(s) to {path}.")
                    st.rerun()
                else:
                    st.warning("Fill in at least Student ID, Semester, and Status before saving.")
            except OSError as exc:
                st.error(f"Could not save manual rows. Close the CSV if it is open, then try again. Details: {exc}")

    utility_cols = st.columns(4)
    with utility_cols[0]:
        if st.button("Reset Checker Edits", use_container_width=True):
            st.session_state[MANUAL_CHECKER_SIGNATURE_KEY] = None
            st.rerun()
    with utility_cols[1]:
        st.download_button(
            "Download Visible Queue",
            data=dataframe_to_csv_bytes(_strip_manual_checker_internal_columns(visible_queue)),
            file_name="sql_compile_manual_queue_visible.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with utility_cols[2]:
        st.download_button(
            "Download Ready Rows",
            data=dataframe_to_csv_bytes(completed_rows),
            file_name="sql_compile_manual_rows_ready.csv",
            mime="text/csv",
            use_container_width=True,
            disabled=completed_rows.empty,
        )
    with utility_cols[3]:
        st.download_button(
            "Download Saved Rows",
            data=dataframe_to_csv_bytes(saved_for_queue),
            file_name="sql_compile_manual_rows_saved_for_queue.csv",
            mime="text/csv",
            use_container_width=True,
            disabled=saved_for_queue.empty,
        )

    with st.expander(f"Ready manual rows preview ({len(completed_rows):,})", expanded=not completed_rows.empty):
        if completed_rows.empty:
            st.caption("No completed manual rows yet.")
        else:
            st.dataframe(completed_rows, use_container_width=True, hide_index=True)

    with st.expander(f"Saved manual decisions for this selection ({len(saved_for_queue):,})", expanded=False):
        if saved_for_queue.empty:
            st.caption("No saved manual decisions are currently tied to this queue.")
        else:
            st.dataframe(saved_for_queue, use_container_width=True, hide_index=True)


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
                options=STATUS_OPTIONS,
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
    selected_cohorts, selected_label = _cohort_filter(cohort_options)
    rate_table = all_tables.rate_table.loc[all_tables.rate_table["Cohort Semester"].isin(selected_cohorts)].copy() if selected_cohorts else all_tables.rate_table.iloc[0:0].copy()
    outcomes = all_tables.outcomes.loc[all_tables.outcomes["Cohort Semester"].isin(selected_cohorts)].copy() if selected_cohorts and not all_tables.outcomes.empty else all_tables.outcomes.iloc[0:0].copy()
    review_template = all_tables.manual_entry_template.loc[all_tables.manual_entry_template["Cohort Semester"].isin(selected_cohorts)].copy() if selected_cohorts and not all_tables.manual_entry_template.empty else all_tables.manual_entry_template.iloc[0:0].copy()
    distribution = all_tables.outcome_distribution.loc[all_tables.outcome_distribution["Cohort Semester"].isin(selected_cohorts)].copy() if selected_cohorts and not all_tables.outcome_distribution.empty else all_tables.outcome_distribution.iloc[0:0].copy()
    milestone_dashboard = build_sql_compile_milestone_dashboard(
        all_tables.timeline,
        all_tables.outcomes,
        selected_cohorts,
        selection_label=selected_label,
    )

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
    milestone_meta = milestone_dashboard.get("meta", {})
    cohort_students = int(milestone_meta.get("students", 0) or 0)

    kpis = st.columns(4)
    with kpis[0]:
        st.metric("Cohort size", f"{cohort_students:,}")
    with kpis[1]:
        st.metric("Selected cohort", selected_label)
    with kpis[2]:
        st.metric("Council view", "ALL")
    with kpis[3]:
        st.metric("Latest measurable milestone", str(milestone_meta.get("max_milestone") or "Unknown"))

    rates_tab, outcomes_tab, checker_tab, manual_rows_tab = st.tabs(
        ["Persistence & Graduation", "Outcome Mix", "Manual Checker", "Manual Rows"]
    )

    with rates_tab:
        _render_rate_charts(kpi_frame, milestone_dashboard, selected_label)

    with outcomes_tab:
        _render_outcome_distribution(distribution)
        if not outcomes.empty:
            st.dataframe(outcomes, use_container_width=True, hide_index=True)

    with checker_tab:
        _render_manual_checker(
            review_template.loc[:, ODD_RECORD_COLUMNS] if not review_template.empty else review_template,
            manual_status_file,
            all_tables.manual_rows,
        )

    with manual_rows_tab:
        _render_manual_rows_editor(all_tables.manual_rows, manual_status_file)


if __name__ == "__main__":
    main()
