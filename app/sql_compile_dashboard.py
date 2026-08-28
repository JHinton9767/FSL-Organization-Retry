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
    LAST_KNOWN_STATUS_COLUMNS,
    MANUAL_CHECKER_COLUMNS,
    MANUAL_CHECKER_SELECT_COLUMN,
    SQL_COMPILE_ALL_TIME_LABEL,
    build_dashboard_rate_table,
    build_manual_checker_queue,
    build_sql_compile_milestone_dashboard,
    load_dashboard_tables,
    odd_record_editor_to_manual_rows,
)
from src.sqlCompile_legacy_manual import (
    LEGACY_MANUAL_FILE_NAMES,
    import_legacy_manual_decisions,
    load_legacy_manual_decision_rows,
)
from src.persistence_outcomes import PERSISTENCE_OUTCOME_ORDER


st.set_page_config(
    page_title="FSL sqlCompile Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)


STATUS_OPTIONS = ["", "D", "G", "RS", "RV", "S", "T", "AL", "H", "CK", "A", "N"]
SECTION_OPTIONS = ["Persistence & Graduation", "Outcome Mix", "Manual Checker", "Manual Rows"]
SECTION_KEY = "sql_compile_dashboard_section"
DASHBOARD_DATA_REFRESH_KEY = "sql_compile_dashboard_data_refresh_token"
MILESTONE_DASHBOARD_CACHE_KEY = "sql_compile_milestone_dashboard_cache"
MILESTONE_DASHBOARD_CACHE_LIMIT = 8
MANUAL_CHECKER_FILTERED_DOWNLOAD_KEY = "sql_compile_manual_checker_filtered_download"
MANUAL_CHECKER_ROW_ID = "_manual_checker_row_id"
MANUAL_CHECKER_STATE_KEY = "sql_compile_manual_checker_rows"
MANUAL_CHECKER_SIGNATURE_KEY = "sql_compile_manual_checker_signature"
MANUAL_CHECKER_EDITOR_KEY = "sql_compile_manual_checker_editor"
MANUAL_CHECKER_EDITOR_VERSION_KEY = "sql_compile_manual_checker_editor_version"
MANUAL_CHECKER_PAGE_KEY = "sql_compile_manual_checker_page"
MANUAL_CHECKER_PAGE_SIZE_OPTIONS = [50, 100, 250, 500]
MANUAL_CHECKER_DEFAULT_PAGE_SIZE = 100


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


def _mtime_ns(path: Path) -> int:
    try:
        return int(path.stat().st_mtime_ns)
    except OSError:
        return 0


def _dashboard_data_refresh_token() -> int:
    return int(st.session_state.get(DASHBOARD_DATA_REFRESH_KEY, 0) or 0)


def _request_dashboard_data_refresh() -> None:
    st.session_state[DASHBOARD_DATA_REFRESH_KEY] = _dashboard_data_refresh_token() + 1
    st.session_state[MANUAL_CHECKER_SIGNATURE_KEY] = None
    st.session_state.pop(MILESTONE_DASHBOARD_CACHE_KEY, None)
    _refresh_manual_checker_editor()


@st.cache_data(show_spinner="Loading sqlCompile dashboard data...")
def _cached_load_dashboard_tables(
    database_path_text: str,
    database_mtime_ns: int,
    manual_status_file_text: str,
    dashboard_refresh_token: int,
):
    del database_mtime_ns, dashboard_refresh_token
    return load_dashboard_tables(
        database_path=Path(database_path_text),
        manual_status_file=Path(manual_status_file_text),
        all_cohorts=True,
    )


def _session_cached_milestone_dashboard(
    timeline: pd.DataFrame,
    outcomes: pd.DataFrame,
    *,
    selected_cohorts: list[str],
    selected_chapters: list[str],
    selected_label: str,
    database_path: Path,
    manual_status_file: Path,
) -> dict[str, object]:
    key = (
        str(database_path.resolve()),
        _mtime_ns(database_path),
        str(manual_status_file.resolve()),
        _dashboard_data_refresh_token(),
        tuple(str(cohort).strip() for cohort in selected_cohorts),
        tuple(str(chapter).strip() for chapter in selected_chapters),
        str(selected_label),
    )
    cache = st.session_state.get(MILESTONE_DASHBOARD_CACHE_KEY)
    if not isinstance(cache, dict):
        cache = {}
        st.session_state[MILESTONE_DASHBOARD_CACHE_KEY] = cache
    if key not in cache:
        with st.spinner("Calculating P&G milestone chart..."):
            cache[key] = build_sql_compile_milestone_dashboard(
                timeline,
                outcomes,
                selected_cohorts,
                selected_chapters=selected_chapters,
                selection_label=selected_label,
            )
        while len(cache) > MILESTONE_DASHBOARD_CACHE_LIMIT:
            cache.pop(next(iter(cache)))
    return cache[key]


def _format_percent(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value):.1%}"


def _unique_nonempty_options(frame: pd.DataFrame, column: str) -> list[str]:
    if frame.empty or column not in frame.columns:
        return []
    values = frame[column].fillna("").astype(str).str.strip()
    return sorted(value for value in values.unique().tolist() if value)


def _manual_checker_outcome_options(queue: pd.DataFrame) -> list[str]:
    if queue.empty or "Last Known Outcome Bucket" not in queue.columns:
        return []
    present = set(queue["Last Known Outcome Bucket"].fillna("").astype(str).str.strip())
    ordered = [outcome for outcome in PERSISTENCE_OUTCOME_ORDER if outcome in present]
    extras = sorted(value for value in present if value and value not in PERSISTENCE_OUTCOME_ORDER)
    return [*ordered, *extras]


def _manual_checker_signature(checker_template: pd.DataFrame) -> tuple[object, ...]:
    if checker_template.empty:
        return (0, (), 0)
    columns = [
        column
        for column in [
            "Cohort Semester",
            "Cohort Chapter",
            "Student ID",
            "Student Name",
            "Last Known Semester",
            "Last Known Chapter",
            "Last Known Status",
            "Last Known Outcome Bucket",
            "Needs Manual Form Review",
            "Manual Status Applied",
        ]
        if column in checker_template.columns
    ]
    if not columns:
        return (len(checker_template), tuple(str(index) for index in checker_template.index.tolist()), 0)
    prepared = checker_template.loc[:, columns].fillna("").astype(str)
    signature_hash = int(pd.util.hash_pandas_object(prepared, index=False).sum())
    return (len(prepared), tuple(columns), signature_hash)


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


def _ensure_manual_checker_state(checker_template: pd.DataFrame) -> pd.DataFrame:
    signature = _manual_checker_signature(checker_template)
    if st.session_state.get(MANUAL_CHECKER_SIGNATURE_KEY) != signature:
        queue = _with_manual_checker_row_ids(build_manual_checker_queue(checker_template))
        st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
        st.session_state[MANUAL_CHECKER_SIGNATURE_KEY] = signature
        _refresh_manual_checker_editor()

    stored = st.session_state.get(MANUAL_CHECKER_STATE_KEY, pd.DataFrame(columns=[*MANUAL_CHECKER_COLUMNS, MANUAL_CHECKER_ROW_ID]))
    queue = stored.copy() if isinstance(stored, pd.DataFrame) else pd.DataFrame(stored)
    for column in MANUAL_CHECKER_COLUMNS:
        if column not in queue.columns:
            queue[column] = False if column == MANUAL_CHECKER_SELECT_COLUMN else ""
    if MANUAL_CHECKER_ROW_ID not in queue.columns:
        queue = _with_manual_checker_row_ids(queue)
    queue[MANUAL_CHECKER_SELECT_COLUMN] = queue[MANUAL_CHECKER_SELECT_COLUMN].fillna(False).astype(bool)
    for column in [*LAST_KNOWN_STATUS_COLUMNS, MANUAL_CHECKER_ROW_ID]:
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


def _manual_checker_page(frame: pd.DataFrame, page_size: int, page_number: int) -> tuple[pd.DataFrame, int, int, int, int]:
    total_rows = len(frame)
    page_size = max(int(page_size), 1)
    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    current_page = min(max(int(page_number), 1), total_pages)
    start = (current_page - 1) * page_size
    end = min(start + page_size, total_rows)
    return frame.iloc[start:end].copy(), total_pages, current_page, start, end


def _manual_checker_editor_key(page_queue: pd.DataFrame) -> str:
    version = int(st.session_state.get(MANUAL_CHECKER_EDITOR_VERSION_KEY, 0) or 0)
    if page_queue.empty or MANUAL_CHECKER_ROW_ID not in page_queue.columns:
        return f"{MANUAL_CHECKER_EDITOR_KEY}_{version}"
    row_ids = page_queue[MANUAL_CHECKER_ROW_ID].fillna("").astype(str)
    signature_hash = int(pd.util.hash_pandas_object(row_ids, index=False).sum())
    return f"{MANUAL_CHECKER_EDITOR_KEY}_{version}_{len(page_queue)}_{signature_hash}"


def _manual_checker_row_id_signature(frame: pd.DataFrame) -> tuple[int, int]:
    if frame.empty or MANUAL_CHECKER_ROW_ID not in frame.columns:
        return (0, 0)
    row_ids = frame[MANUAL_CHECKER_ROW_ID].fillna("").astype(str)
    return (len(row_ids), int(pd.util.hash_pandas_object(row_ids, index=False).sum()))


def _refresh_manual_checker_editor() -> None:
    st.session_state[MANUAL_CHECKER_EDITOR_VERSION_KEY] = int(
        st.session_state.get(MANUAL_CHECKER_EDITOR_VERSION_KEY, 0) or 0
    ) + 1


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
    outcome_filter: list[str],
    entry_status_filter: list[str],
    needs_review_only: bool,
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
            "Student Name",
            "Last Known Semester",
            "Last Known Chapter",
            "Last Known Status",
            "Last Known Outcome Bucket",
            "Needs Manual Form Review",
            "Manual Status Applied",
            "Semester",
            "Chapter",
            "Status",
            "Notes",
        ]
        available_search_columns = [column for column in search_columns if column in queue.columns]
        haystack = queue[available_search_columns].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
        mask &= haystack.str.contains(search_text.strip().lower(), regex=False, na=False)
    for column, selected in [
        ("Cohort Semester", cohort_filter),
        ("Cohort Chapter", chapter_filter),
        ("Last Known Semester", last_semester_filter),
        ("Last Known Outcome Bucket", outcome_filter),
    ]:
        if selected and column in queue.columns:
            mask &= queue[column].isin(selected)
    if needs_review_only and "Needs Manual Form Review" in queue.columns:
        mask &= queue["Needs Manual Form Review"].fillna("").astype(str).str.strip().str.lower().isin({"yes", "true", "1", "y"})
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


def _manual_checker_ready_mask(queue: pd.DataFrame) -> pd.Series:
    if queue.empty:
        return pd.Series(False, index=queue.index)
    student_id = queue.get("Student ID", pd.Series("", index=queue.index)).fillna("").astype(str).str.strip()
    semester = queue.get("Semester", pd.Series("", index=queue.index)).fillna("").astype(str).str.strip()
    status = queue.get("Status", pd.Series("", index=queue.index)).fillna("").astype(str).str.strip()
    return student_id.ne("") & semester.ne("") & status.ne("")


def _saved_manual_rows_for_queue(manual_rows: pd.DataFrame, queue: pd.DataFrame) -> pd.DataFrame:
    if manual_rows.empty or queue.empty:
        return pd.DataFrame(columns=MANUAL_STATUS_COLUMNS)
    prepared = manual_rows.copy()
    queue_keys = queue.copy()
    key_columns = ["Cohort Semester", "Cohort Chapter", "Student ID"]
    for column in key_columns:
        if column not in prepared.columns:
            prepared[column] = ""
        prepared[column] = prepared[column].fillna("").astype(str).str.strip()
        if column not in queue_keys.columns:
            queue_keys[column] = ""
        queue_keys[column] = queue_keys[column].fillna("").astype(str).str.strip()
    keys = queue_keys[key_columns].agg("\x1f".join, axis=1).drop_duplicates()
    prepared_keys = prepared[key_columns].agg("\x1f".join, axis=1)
    mask = prepared_keys.isin(set(keys.tolist()))
    return prepared.loc[mask, MANUAL_STATUS_COLUMNS].reset_index(drop=True)


def _render_legacy_manual_importer(manual_status_file: Path) -> None:
    with st.expander("Reuse Legacy Manual Decisions", expanded=False):
        legacy_path_text = st.text_input(
            "Legacy dashboard project root, config folder, or file",
            value=".",
            key="sql_compile_legacy_manual_path",
        )
        legacy_path = Path(legacy_path_text).expanduser()
        import_action_cols = st.columns([1, 1, 2])
        with import_action_cols[0]:
            preview_requested = st.button("Preview Legacy Decisions", use_container_width=True)
        with import_action_cols[1]:
            import_requested = st.button("Append Legacy Decisions", type="primary", use_container_width=True)
        with import_action_cols[2]:
            st.caption("Point this at the old dashboard project root, its config folder, or one exported manual-check CSV/XLSX.")

        if not preview_requested and not import_requested:
            return

        try:
            loaded = load_legacy_manual_decision_rows(legacy_path)
        except Exception as exc:
            st.error(f"Could not read legacy manual decisions: {exc}")
            return

        total_source_rows = sum(loaded.source_counts.values())
        total_converted_rows = len(loaded.rows)
        import_cols = st.columns(3)
        with import_cols[0]:
            st.metric("Legacy source rows", f"{total_source_rows:,}")
        with import_cols[1]:
            st.metric("Importable rows", f"{total_converted_rows:,}")
        with import_cols[2]:
            st.metric("Destination rows file", manual_status_file.name)

        source_summary = pd.DataFrame(
            [
                {
                    "Legacy Source": source_name,
                    "Source Rows": loaded.source_counts.get(source_name, 0),
                    "Importable Rows": loaded.converted_counts.get(source_name, 0),
                    "Skipped / Not Status Rows": loaded.skipped_counts.get(source_name, 0),
                    "Looked For": "; ".join(str(path) for path in loaded.searched_paths.get(source_name, [])),
                }
                for source_name in LEGACY_MANUAL_FILE_NAMES
            ]
        )
        st.dataframe(source_summary, use_container_width=True, hide_index=True, height=250)

        if loaded.rows.empty:
            st.info("No completed legacy outcome decisions were found at that path.")
            return

        st.dataframe(loaded.rows.head(250), use_container_width=True, hide_index=True, height=300)
        if import_requested:
            try:
                result = import_legacy_manual_decisions(legacy_path, manual_status_file)
                _request_dashboard_data_refresh()
                st.success(f"Imported {result.saved_rows:,} legacy manual row(s) into {result.manual_status_path}.")
                st.rerun()
            except OSError as exc:
                st.error(f"Could not append legacy decisions. Close the manual CSV if it is open, then try again. Details: {exc}")
        st.download_button(
            "Download Import Preview",
            data=dataframe_to_csv_bytes(loaded.rows),
            file_name="sql_compile_legacy_manual_import_preview.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.caption("Roster exclusions are intentionally not imported because the sqlCompile manual file stores outcomes, not source-row exclusions.")


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


def _selected_chapters(outcomes: pd.DataFrame) -> list[str]:
    if outcomes.empty or "Cohort Chapter" not in outcomes.columns:
        return []
    values = outcomes["Cohort Chapter"].fillna("").astype(str).str.strip()
    return sorted(value for value in values.unique().tolist() if value)


def _chapter_filter(options: list[str]) -> tuple[list[str], str]:
    if not options:
        return [], "No Chapters"

    mode = st.sidebar.radio(
        "P&G chapter selection",
        options=["All chapters", "Chapter group"],
        index=0,
    )
    if mode == "All chapters":
        return options, "ALL"

    selected = st.sidebar.multiselect("P&G chapters", options=options, default=options)
    if not selected:
        return [], "No Chapters"
    if len(selected) == len(options):
        return selected, "ALL"
    if len(selected) == 1:
        return selected, str(selected[0])
    return selected, f"{len(selected)} Chapters"


def _filter_by_chapters(frame: pd.DataFrame, selected_chapters: list[str]) -> pd.DataFrame:
    if frame.empty or "Cohort Chapter" not in frame.columns:
        return frame.copy()
    selected = {str(value).strip() for value in selected_chapters if str(value).strip()}
    if not selected:
        return frame.iloc[0:0].copy()
    return frame.loc[frame["Cohort Chapter"].fillna("").astype(str).str.strip().isin(selected)].copy()


def _future_row_style(frame: pd.DataFrame):
    def style_row(row: pd.Series) -> list[str]:
        status = str(row.get("Milestone Status", "") or "").strip()
        if status == "Future":
            return ["background-color: #E5E7EB; color: #475569;" for _ in row]
        if status == "Partially Future":
            return ["background-color: #F8FAFC; color: #17213A;" for _ in row]
        return ["" for _ in row]

    return frame.style.apply(style_row, axis=1)


def _sort_detail_rows(frame: pd.DataFrame, sort_mode: str) -> pd.DataFrame:
    result = frame.copy()
    if result.empty:
        return result
    result["_row_order"] = range(len(result))
    if sort_mode == "Chapter then semester":
        sort_columns = [column for column in ["Cohort Chapter", "_row_order"] if column in result.columns]
        result = result.sort_values(sort_columns, na_position="last")
    elif sort_mode == "Milestone then chapter":
        sort_columns = [column for column in ["Milestone Sort", "Cohort Chapter", "_row_order"] if column in result.columns]
        result = result.sort_values(sort_columns, na_position="last")
    return result.drop(columns=["_row_order"], errors="ignore").reset_index(drop=True)


def _render_rate_charts(
    rate_table: pd.DataFrame,
    milestone_dashboard: dict[str, object],
    selected_label: str,
    *,
    chapter_label: str,
    chapter_rate_table: pd.DataFrame,
) -> None:
    if rate_table.empty:
        st.warning("No new-member cohorts were available. Run `python sqlCompile.py --all-semesters` after your roster path is configured.")
        return

    chart_frame = milestone_dashboard.get("chart_frame", pd.DataFrame())
    table_frame = milestone_dashboard.get("table_frame", pd.DataFrame())
    detail_frame = milestone_dashboard.get("detail_frame", pd.DataFrame())
    title = f"Persistence and Graduation for {selected_label}"
    subtitle = (
        f"{chapter_label} chapters | Share of eligible cohort | Roster outcomes first | "
        "Manual corrections last | Explicit graduation evidence only"
    )
    st.plotly_chart(
        persistence_milestone_chart(chart_frame, title=title, subtitle=subtitle),
        use_container_width=True,
    )
    note = str(milestone_dashboard.get("meta", {}).get("note", "") or "").strip()
    if note:
        st.caption(note)

    if not table_frame.empty:
        count_columns = [
            "Measured Students",
            "Future Students",
            *[f"{outcome} Count" for outcome in PERSISTENCE_OUTCOME_ORDER],
        ]
        milestone_display = _format_display_frame(
            table_frame,
            percent_cols=PERSISTENCE_OUTCOME_ORDER,
            integer_cols=count_columns,
        )
        st.dataframe(
            _future_row_style(milestone_display),
            use_container_width=True,
            hide_index=True,
        )

    if isinstance(detail_frame, pd.DataFrame) and not detail_frame.empty:
        st.subheader("Milestone Rows by Semester and Chapter")
        sort_mode = st.selectbox(
            "P&G row sort",
            options=["Semester then chapter", "Chapter then semester", "Milestone then chapter"],
            index=0,
            key="sql_compile_pg_detail_sort",
        )
        detail_columns = [
            column
            for column in [
                "Cohort Semester",
                "Cohort Chapter",
                "Milestone",
                "Milestone Status",
                "Measured Students",
                "Future Students",
                *PERSISTENCE_OUTCOME_ORDER,
                *[f"{outcome} Count" for outcome in PERSISTENCE_OUTCOME_ORDER],
            ]
            if column in detail_frame.columns
        ]
        detail_count_columns = [
            "Measured Students",
            "Future Students",
            *[f"{outcome} Count" for outcome in PERSISTENCE_OUTCOME_ORDER],
        ]
        detail_display = _format_display_frame(
            _sort_detail_rows(detail_frame.loc[:, detail_columns], sort_mode),
            percent_cols=PERSISTENCE_OUTCOME_ORDER,
            integer_cols=detail_count_columns,
        )
        st.dataframe(
            _future_row_style(detail_display),
            use_container_width=True,
            hide_index=True,
            height=430,
        )
        st.download_button(
            "Download P&G Detail Rows",
            data=dataframe_to_csv_bytes(detail_frame.loc[:, detail_columns]),
            file_name="sql_compile_pg_semester_chapter_rows.csv",
            mime="text/csv",
            use_container_width=True,
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

    if not chapter_rate_table.empty:
        st.subheader("Resolved Rate Summary by Semester and Chapter")
        st.dataframe(
            _format_display_frame(
                chapter_rate_table,
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
            height=360,
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


def _render_manual_checker(checker_template: pd.DataFrame, manual_status_file: Path, manual_rows: pd.DataFrame) -> None:
    st.subheader("Manual Checker")
    st.caption("Review all selected students by last-known status. Filter by P&G bucket, fill verified corrections, and save completed decisions to the manual status CSV.")

    _render_legacy_manual_importer(manual_status_file)

    queue = _ensure_manual_checker_state(checker_template)
    saved_for_queue = _saved_manual_rows_for_queue(manual_rows, queue)

    if queue.empty:
        st.success("No student status records are available for the current cohort selection.")
        if not saved_for_queue.empty:
            with st.expander(f"Saved manual decisions for this selection ({len(saved_for_queue):,})", expanded=False):
                st.dataframe(saved_for_queue, use_container_width=True, hide_index=True)
        return

    filter_cols = st.columns([1.4, 1, 1, 1, 1])
    with filter_cols[0]:
        search_text = st.text_input("Search queue", placeholder="Student ID, name, chapter, semester, status, outcome, note", key="sql_compile_manual_checker_search")
    with filter_cols[1]:
        cohort_filter = st.multiselect("Cohort", options=_unique_nonempty_options(queue, "Cohort Semester"), key="sql_compile_manual_checker_cohort_filter")
    with filter_cols[2]:
        chapter_filter = st.multiselect("Chapter", options=_unique_nonempty_options(queue, "Cohort Chapter"), key="sql_compile_manual_checker_chapter_filter")
    with filter_cols[3]:
        last_semester_filter = st.multiselect("Last seen", options=_unique_nonempty_options(queue, "Last Known Semester"), key="sql_compile_manual_checker_last_seen_filter")
    with filter_cols[4]:
        outcome_filter = st.multiselect("Last outcome", options=_manual_checker_outcome_options(queue), key="sql_compile_manual_checker_outcome_filter")

    detail_filter_cols = st.columns([1, 1, 2])
    with detail_filter_cols[0]:
        entry_status_filter = st.multiselect(
            "Entered status",
            options=["Blank", *[option for option in STATUS_OPTIONS if option]],
            key="sql_compile_manual_checker_status_filter",
        )
    with detail_filter_cols[1]:
        needs_review_only = st.checkbox("Only needs review", value=False, key="sql_compile_manual_checker_needs_review_only")
    with detail_filter_cols[2]:
        unfinished_only = st.checkbox("Only unfinished", value=False, key="sql_compile_manual_checker_unfinished_only")
        st.caption(f"Manual file: `{manual_status_file}`")

    filtered_queue = _filter_manual_checker_rows(
        queue,
        search_text=search_text,
        cohort_filter=cohort_filter,
        chapter_filter=chapter_filter,
        last_semester_filter=last_semester_filter,
        outcome_filter=outcome_filter,
        entry_status_filter=entry_status_filter,
        needs_review_only=needs_review_only,
        unfinished_only=unfinished_only,
    )

    pager_cols = st.columns([0.8, 0.8, 2.4])
    with pager_cols[0]:
        page_size = st.selectbox(
            "Rows per page",
            options=MANUAL_CHECKER_PAGE_SIZE_OPTIONS,
            index=MANUAL_CHECKER_PAGE_SIZE_OPTIONS.index(MANUAL_CHECKER_DEFAULT_PAGE_SIZE),
            key="sql_compile_manual_checker_page_size",
        )
    total_pages = max(1, (len(filtered_queue) + int(page_size) - 1) // int(page_size))
    st.session_state[MANUAL_CHECKER_PAGE_KEY] = min(
        max(int(st.session_state.get(MANUAL_CHECKER_PAGE_KEY, 1) or 1), 1),
        total_pages,
    )
    with pager_cols[1]:
        page_number = st.number_input(
            "Page",
            min_value=1,
            max_value=total_pages,
            step=1,
            key=MANUAL_CHECKER_PAGE_KEY,
        )
    page_queue, total_pages, page_number, page_start, page_end = _manual_checker_page(
        filtered_queue,
        int(page_size),
        int(page_number),
    )
    with pager_cols[2]:
        if filtered_queue.empty:
            st.caption(f"Showing 0 of {len(queue):,} student status record(s).")
        else:
            st.caption(
                f"Showing rows {page_start + 1:,}-{page_end:,} of {len(filtered_queue):,} filtered "
                f"record(s), from {len(queue):,} total."
            )

    if filtered_queue.empty:
        st.warning("No student status records match the current filters.")
    else:
        editor_height = min(820, max(320, 92 + (len(page_queue) * 35)))
        st.caption("Check or edit rows, then update the page once. This keeps individual checkbox clicks from rerunning the full dashboard.")
        with st.form(f"{_manual_checker_editor_key(page_queue)}_form"):
            edited = st.data_editor(
                _manual_checker_display_frame(page_queue),
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
                    "Last Known Outcome Bucket": st.column_config.TextColumn("Last Outcome"),
                    "Needs Manual Form Review": st.column_config.TextColumn("Needs Review"),
                    "Manual Status Applied": st.column_config.TextColumn("Manual Applied"),
                    "Student Name": st.column_config.TextColumn("Student Name"),
                    "Semester": st.column_config.TextColumn("Correct Semester"),
                    "Chapter": st.column_config.TextColumn("Correct Chapter"),
                    "Notes": st.column_config.TextColumn("Notes"),
                },
                disabled=[
                    column
                    for column in LAST_KNOWN_STATUS_COLUMNS
                    if column not in {"Semester", "Chapter", "Status", "Notes"}
                ],
                key=_manual_checker_editor_key(page_queue),
            )
            page_update_requested = st.form_submit_button("Update Page Edits", use_container_width=True)
        if page_update_requested:
            queue = _merge_manual_checker_edits(queue, edited)
            st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
            st.success(f"Updated {len(page_queue):,} visible row(s).")

    selected_mask = queue[MANUAL_CHECKER_SELECT_COLUMN].fillna(False).astype(bool)
    selected_queue = queue.loc[selected_mask].copy()
    ready_mask = _manual_checker_ready_mask(queue)
    ready_count = int(ready_mask.sum())
    completed_selected_rows = _completed_manual_checker_rows(selected_queue)

    metric_cols = st.columns(6)
    with metric_cols[0]:
        st.metric("Queue records", f"{len(queue):,}")
    with metric_cols[1]:
        st.metric("Filtered records", f"{len(filtered_queue):,}")
    with metric_cols[2]:
        st.metric("Selected", f"{len(selected_queue):,}")
    with metric_cols[3]:
        st.metric("Ready selected", f"{len(completed_selected_rows):,}")
    with metric_cols[4]:
        st.metric("Ready total", f"{ready_count:,}")
    with metric_cols[5]:
        st.metric("Saved for queue", f"{len(saved_for_queue):,}")

    selection_cols = st.columns(4)
    page_ids = set(page_queue[MANUAL_CHECKER_ROW_ID].tolist()) if not page_queue.empty else set()
    with selection_cols[0]:
        if st.button("Select Page", use_container_width=True, disabled=not page_ids):
            queue.loc[queue[MANUAL_CHECKER_ROW_ID].isin(page_ids), MANUAL_CHECKER_SELECT_COLUMN] = True
            st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
            _refresh_manual_checker_editor()
            st.rerun()
    with selection_cols[1]:
        if st.button("Select Unfinished Page", use_container_width=True, disabled=not page_ids):
            unfinished_visible = (
                queue[MANUAL_CHECKER_ROW_ID].isin(page_ids)
                & queue["Status"].fillna("").astype(str).str.strip().eq("")
            )
            queue.loc[unfinished_visible, MANUAL_CHECKER_SELECT_COLUMN] = True
            st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
            _refresh_manual_checker_editor()
            st.rerun()
    with selection_cols[2]:
        if st.button("Clear Selection", use_container_width=True, disabled=selected_queue.empty):
            queue[MANUAL_CHECKER_SELECT_COLUMN] = False
            st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
            _refresh_manual_checker_editor()
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
            _refresh_manual_checker_editor()
            st.rerun()
    with action_cols[1]:
        if st.button("Use Last Seen", use_container_width=True, disabled=selected_queue.empty):
            mask = queue[MANUAL_CHECKER_SELECT_COLUMN].fillna(False).astype(bool)
            blank_semester = queue["Semester"].fillna("").astype(str).str.strip().eq("")
            blank_chapter = queue["Chapter"].fillna("").astype(str).str.strip().eq("")
            queue.loc[mask & blank_semester, "Semester"] = queue.loc[mask & blank_semester, "Last Known Semester"]
            queue.loc[mask & blank_chapter, "Chapter"] = queue.loc[mask & blank_chapter, "Last Known Chapter"]
            st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
            _refresh_manual_checker_editor()
            st.rerun()
    with action_cols[2]:
        if st.button("Clear Selected", use_container_width=True, disabled=selected_queue.empty):
            mask = queue[MANUAL_CHECKER_SELECT_COLUMN].fillna(False).astype(bool)
            queue.loc[mask, ["Semester", "Status", "Notes"]] = ""
            st.session_state[MANUAL_CHECKER_STATE_KEY] = queue
            _refresh_manual_checker_editor()
            st.rerun()
    with action_cols[3]:
        if st.button("Save Selected", use_container_width=True, disabled=completed_selected_rows.empty):
            try:
                path, saved = append_manual_status_rows(completed_selected_rows, manual_status_file)
                if saved:
                    st.success(f"Saved {saved:,} selected manual row(s) to {path}.")
                    st.caption("The dashboard keeps cached data for speed. Use Refresh Dashboard Data when you want rates and saved flags recalculated.")
                else:
                    st.warning("Fill in at least Student ID, Semester, and Status before saving.")
            except OSError as exc:
                st.error(f"Could not save manual rows. Close the CSV if it is open, then try again. Details: {exc}")
    with action_cols[4]:
        if st.button("Save All Ready", use_container_width=True, disabled=ready_count == 0):
            try:
                completed_rows = _completed_manual_checker_rows(queue.loc[ready_mask].copy())
                path, saved = append_manual_status_rows(completed_rows, manual_status_file)
                if saved:
                    st.success(f"Saved {saved:,} completed manual row(s) to {path}.")
                    st.caption("The dashboard keeps cached data for speed. Use Refresh Dashboard Data when you want rates and saved flags recalculated.")
                else:
                    st.warning("Fill in at least Student ID, Semester, and Status before saving.")
            except OSError as exc:
                st.error(f"Could not save manual rows. Close the CSV if it is open, then try again. Details: {exc}")

    utility_cols = st.columns(4)
    with utility_cols[0]:
        if st.button("Reset Checker Edits", use_container_width=True):
            st.session_state[MANUAL_CHECKER_SIGNATURE_KEY] = None
            _refresh_manual_checker_editor()
            st.rerun()
    with utility_cols[1]:
        filtered_signature = _manual_checker_row_id_signature(filtered_queue)
        if st.button("Prepare Filtered CSV", use_container_width=True, disabled=filtered_queue.empty):
            st.session_state[MANUAL_CHECKER_FILTERED_DOWNLOAD_KEY] = {
                "signature": filtered_signature,
                "data": dataframe_to_csv_bytes(_strip_manual_checker_internal_columns(filtered_queue)),
            }
            st.success(f"Prepared {len(filtered_queue):,} filtered row(s) for download.")
        filtered_download = st.session_state.get(MANUAL_CHECKER_FILTERED_DOWNLOAD_KEY, {})
        filtered_download_data = b""
        if isinstance(filtered_download, dict) and filtered_download.get("signature") == filtered_signature:
            filtered_download_data = filtered_download.get("data", b"") or b""
        st.download_button(
            "Download Filtered Queue",
            data=filtered_download_data,
            file_name="sql_compile_manual_queue_filtered.csv",
            mime="text/csv",
            use_container_width=True,
            disabled=not filtered_download_data,
        )
    with utility_cols[2]:
        st.download_button(
            "Download Ready Selected",
            data=dataframe_to_csv_bytes(completed_selected_rows),
            file_name="sql_compile_manual_rows_ready_selected.csv",
            mime="text/csv",
            use_container_width=True,
            disabled=completed_selected_rows.empty,
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

    with st.expander(f"Ready selected rows preview ({len(completed_selected_rows):,})", expanded=not completed_selected_rows.empty):
        if completed_selected_rows.empty:
            st.caption("No selected completed manual rows yet.")
        else:
            st.dataframe(completed_selected_rows, use_container_width=True, hide_index=True)

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
            _request_dashboard_data_refresh()
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
            _request_dashboard_data_refresh()
            st.sidebar.success(f"Compiled {result.row_count:,} rows from {result.source_file_count:,} Excel file(s).")
            st.rerun()
        except Exception as exc:
            st.sidebar.error(f"Compile failed: {exc}")
    if st.sidebar.button("Refresh Dashboard Data", use_container_width=True):
        _request_dashboard_data_refresh()
        st.rerun()
    st.sidebar.caption("Manual saves are cached for speed. Refresh dashboard data when you want saved statuses reflected in rates and flags.")

    if not database_path.exists():
        st.error("No sqlCompile SQLite file was found. Run `python sqlCompile.py --all-semesters` first, or use the sidebar button after your roster paths are configured.")
        return

    try:
        all_tables = _cached_load_dashboard_tables(
            str(database_path.resolve()),
            _mtime_ns(database_path),
            str(manual_status_file.resolve()),
            _dashboard_data_refresh_token(),
        )
    except Exception as exc:
        st.error(f"Could not load sqlCompile dashboard data: {exc}")
        return

    cohort_options = _selected_cohorts(all_tables.rate_table)
    selected_cohorts, selected_label = _cohort_filter(cohort_options)
    section = st.radio("Dashboard section", options=SECTION_OPTIONS, horizontal=True, key=SECTION_KEY)
    base_rate_table = all_tables.rate_table.loc[all_tables.rate_table["Cohort Semester"].isin(selected_cohorts)].copy() if selected_cohorts else all_tables.rate_table.iloc[0:0].copy()
    base_outcomes = all_tables.outcomes.loc[all_tables.outcomes["Cohort Semester"].isin(selected_cohorts)].copy() if selected_cohorts and not all_tables.outcomes.empty else all_tables.outcomes.iloc[0:0].copy()
    selected_chapters = _selected_chapters(base_outcomes)
    chapter_label = "ALL"
    if section == "Persistence & Graduation":
        selected_chapters, chapter_label = _chapter_filter(selected_chapters)
    outcomes = _filter_by_chapters(base_outcomes, selected_chapters) if section == "Persistence & Graduation" else base_outcomes
    rate_table = build_dashboard_rate_table(outcomes) if section == "Persistence & Graduation" else base_rate_table

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

    cohort_students = int(
        outcomes.loc[:, ["Cohort Semester", "Student ID"]].drop_duplicates().shape[0]
        if not outcomes.empty and {"Cohort Semester", "Student ID"}.issubset(outcomes.columns)
        else 0
    )

    kpis = st.columns(4)
    with kpis[0]:
        st.metric("Cohort size", f"{cohort_students:,}")
    with kpis[1]:
        st.metric("Selected cohort", selected_label)
    with kpis[2]:
        st.metric("Council view", "ALL")
    with kpis[3]:
        st.metric("Chapter view", chapter_label)

    if section == "Persistence & Graduation":
        milestone_dashboard = _session_cached_milestone_dashboard(
            all_tables.timeline,
            all_tables.outcomes,
            selected_cohorts=selected_cohorts,
            selected_chapters=selected_chapters,
            selected_label=selected_label,
            database_path=database_path,
            manual_status_file=manual_status_file,
        )
        kpi_frame = build_dashboard_rate_table(outcomes)
        chapter_rate_table = build_dashboard_rate_table(
            outcomes,
            group_columns=["Cohort Semester", "Cohort Chapter"],
        )
        _render_rate_charts(
            kpi_frame,
            milestone_dashboard,
            selected_label,
            chapter_label=chapter_label,
            chapter_rate_table=chapter_rate_table,
        )

    elif section == "Outcome Mix":
        distribution = all_tables.outcome_distribution.loc[all_tables.outcome_distribution["Cohort Semester"].isin(selected_cohorts)].copy() if selected_cohorts and not all_tables.outcome_distribution.empty else all_tables.outcome_distribution.iloc[0:0].copy()
        _render_outcome_distribution(distribution)
        if not outcomes.empty:
            st.dataframe(outcomes, use_container_width=True, hide_index=True)

    elif section == "Manual Checker":
        checker_template = all_tables.manual_checker_template.loc[all_tables.manual_checker_template["Cohort Semester"].isin(selected_cohorts)].copy() if selected_cohorts and not all_tables.manual_checker_template.empty else all_tables.manual_checker_template.iloc[0:0].copy()
        _render_manual_checker(
            checker_template.loc[:, LAST_KNOWN_STATUS_COLUMNS] if not checker_template.empty else checker_template,
            manual_status_file,
            all_tables.manual_rows,
        )

    else:
        _render_manual_rows_editor(all_tables.manual_rows, manual_status_file)


if __name__ == "__main__":
    main()
