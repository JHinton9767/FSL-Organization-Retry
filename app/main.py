from __future__ import annotations

from datetime import datetime
import os
from typing import Dict, List

import pandas as pd
import streamlit as st

from app.analysis import (
    apply_longitudinal_filters,
    apply_summary_filters,
    build_advisor_intervention_queue,
    available_dimensions,
    build_chapter_health_dashboard,
    build_graduation_denominator_comparison,
    build_persistence_dashboard,
    build_comparison_table,
    build_controlled_comparison,
    build_distribution_table,
    build_gpa_trend_with_coverage,
    build_observed_term_series,
    build_retention_dashboard,
    build_roster_disappearance_tracker,
    build_scatter_frame,
    build_summary_time_series,
    chapter_health_options,
    filter_options,
    persistence_checkpoint_sort_value,
    persistence_cohort_options,
    persistence_cohort_sort_key,
    stakeholder_summary,
    summarize_metric_by_group,
    PERSISTENCE_COUNCIL_OPTIONS,
)
from app.charts import bar_chart, box_plot, histogram, line_chart, persistence_milestone_chart, scatter_chart, stacked_bar_chart
from app.config_loader import (
    MANUAL_ROSTER_CORRECTION_COLUMNS,
    MANUAL_ADJUSTMENTS_PATH,
    MANUAL_REVIEW_ACTIONS_PATH,
    MANUAL_REVIEW_QUEUE_COLUMNS,
    MANUAL_ROSTER_CORRECTIONS_PATH,
    MANUAL_TRANSCRIPTS_PATH,
    OUTCOME_OVERRIDE_COLUMNS,
    ROSTER_EXCLUSION_COLUMNS,
    GRADUATION_EVIDENCE_COLUMNS,
    append_graduation_evidence,
    append_manual_review_actions,
    append_outcome_overrides,
    append_roster_exclusions,
    REVIEW_STATUS_OPTIONS,
    append_manual_adjustments,
    append_manual_roster_corrections,
    build_manual_corrections_package,
    dedupe_manual_review_queue_by_cohort,
    empty_manual_roster_corrections,
    ensure_manual_transcript_files,
    find_manual_correction_conflicts,
    graduated_alumni_rows_to_manual_corrections,
    import_manual_corrections_package,
    load_graduation_evidence,
    load_manual_roster_corrections,
    load_manual_review_actions,
    load_outcome_overrides,
    load_roster_exclusions,
    load_metric_catalog,
    load_settings,
    load_status_code_map,
    manual_transcript_path_for_correction,
    normalize_graduation_evidence,
    normalize_manual_roster_corrections,
    normalize_outcome_overrides,
    normalize_roster_exclusions,
    prepare_manual_corrections_workspace,
    save_manual_roster_corrections,
)
from app.exports import dataframe_to_csv_bytes, figure_to_html_bytes, figure_to_png_bytes
from app.io_utils import parse_term_label, safe_slug
from app.data_loader import discover_dataset_versions, load_analysis_bundle, load_manual_corrections_bundle, scan_preloaded_sources, select_default_dataset
from app.metrics_engine import (
    ALL_STUDENTS_LABEL,
    RESOLVED_OUTCOMES_ONLY_LABEL,
    available_metrics,
    compute_metric_views,
    format_metric_value,
    metric_by_key,
    metric_caption,
)
from app.models import DataSourceStatus, MetricDefinition
from app.presets import list_presets, load_preset, save_preset
from app.status_framework import FULL_POPULATION_LABEL, outcome_population_summary
from src.persistence_outcomes import PERSISTENCE_OUTCOME_ORDER


st.set_page_config(
    page_title="FSL Academic Outcomes Analytics",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)


CURRENT_ACTIVE_METRIC_KEY = "active_member_count"
CURRENT_ACTIVE_DIMENSION_OVERRIDES = {
    "chapter": "current_active_chapter",
    "chapter_group": "current_active_chapter_group",
    "council": "current_active_council",
    "org_type": "current_active_org_type",
    "family": "current_active_family",
    "custom_group": "current_active_custom_group",
    "chapter_size_band": "current_active_chapter_size_band",
    "active_membership_group": "current_active_membership_group",
}
PERSISTENCE_DEFAULT_DISTINCTION = "ALL"
FILTER_LIST_STATE_KEYS = [
    "chapters",
    "chapter_groups",
    "custom_groups",
    "councils",
    "org_types",
    "families",
    "join_terms",
    "statuses",
    "resolved_outcome_groups",
    "majors",
    "pell_groups",
    "transfer_groups",
    "estimated_join_stages",
    "high_hours_groups",
    "active_groups",
    "chapter_size_bands",
    "snapshot_groups",
    "observed_terms",
]
MANUAL_STAGED_CORRECTIONS_KEY = "manual_staged_corrections"
MANUAL_SAVE_MODE_KEY = "manual_correction_save_mode"
MANUAL_STAGE_MODE_LABEL = "Stage changes (fast batch)"
MANUAL_IMMEDIATE_MODE_LABEL = "Save immediately"


def _display_metric_value(value: object, format_code: str, missing: str = "n/a") -> str:
    if value is None or pd.isna(value):
        return missing
    return format_metric_value(value, format_code)


def _format_display_frame(
    frame: pd.DataFrame,
    *,
    percent_cols: list[str] | tuple[str, ...] = (),
    one_decimal_cols: list[str] | tuple[str, ...] = (),
    decimal_cols: list[str] | tuple[str, ...] = (),
    integer_cols: list[str] | tuple[str, ...] = (),
) -> pd.DataFrame:
    display = frame.copy()
    for column in percent_cols:
        if column in display.columns:
            display[column] = display[column].map(lambda value: "" if pd.isna(value) else f"{value:.1%}")
    for column in one_decimal_cols:
        if column in display.columns:
            display[column] = display[column].map(lambda value: "" if pd.isna(value) else f"{value:.1f}")
    for column in decimal_cols:
        if column in display.columns:
            display[column] = display[column].map(lambda value: "" if pd.isna(value) else f"{value:.2f}")
    for column in integer_cols:
        if column in display.columns:
            display[column] = display[column].map(lambda value: "" if pd.isna(value) else f"{int(value):,}")
    return display


def _unique_text_options(frame: pd.DataFrame, column: str) -> list[str]:
    if column not in frame.columns:
        return []
    return sorted(
        frame[column]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )


def _source_status_rows(statuses: List[DataSourceStatus]) -> list[dict[str, object]]:
    rows = []
    for status in statuses:
        rows.append(
            {
                "Source": status.label,
                "Available": "Yes" if status.available else "No",
                "Selected Path": str(status.selected_path) if status.selected_path else str(status.root_path),
                "Warnings": " | ".join(status.warnings) if status.warnings else "",
            }
        )
    return rows


def _source_file_status_rows(statuses: List[DataSourceStatus]) -> list[dict[str, object]]:
    rows = []
    for status in statuses:
        for file_status in status.files:
            rows.append(
                {
                    "Source": status.label,
                    "File": file_status.label,
                    "Required": "Yes" if file_status.required else "No",
                    "Exists": "Yes" if file_status.exists else "No",
                    "Last Modified": file_status.last_modified,
                    "Path": str(file_status.path),
                    "Warning": file_status.warning,
                }
            )
    return rows


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
          <div class="txst-persistence-subtitle">Organization Join Term Cohorts</div>
          <div class="txst-persistence-rule">
            <span></span><span></span><span></span><span></span><span></span><span></span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _default_persistence_cohort(cohort_options: List[str], longitudinal: pd.DataFrame) -> str:
    if not cohort_options:
        return ""
    if longitudinal.empty or "observed_term" not in longitudinal.columns:
        return cohort_options[-1]

    observed_sort = longitudinal["observed_term"].map(lambda value: parse_term_label(value)["sort_value"])
    observed_sort = pd.to_numeric(observed_sort, errors="coerce").dropna()
    if observed_sort.empty:
        return cohort_options[-1]

    max_observed_sort = int(observed_sort.max())
    ranked_options: list[tuple[int, int, str]] = []
    for option in cohort_options:
        best_offset = -1
        for offset in range(6, -1, -1):
            target_sort = persistence_checkpoint_sort_value(option, offset)
            if target_sort is not None and target_sort <= max_observed_sort:
                best_offset = offset
                break
        ranked_options.append((best_offset, persistence_cohort_sort_key(option), option))

    ranked_options.sort(key=lambda item: (item[0], item[1]))
    return ranked_options[-1][2]


def _render_persistence_and_graduation_view(bundle) -> None:
    summary = bundle.summary.copy()
    longitudinal = bundle.longitudinal.copy()
    cohort_options = persistence_cohort_options(summary)

    _persistence_header()
    st.caption("This view tracks organization join-term cohorts across eight roster-aligned outcomes, with dated manual corrections applied after roster evidence.")

    if not cohort_options:
        st.warning("No organization join-term cohorts were available for the persistence and graduation view.")
        return

    default_cohort = _default_persistence_cohort(cohort_options, longitudinal)
    if "persistence_cohort_term" not in st.session_state or st.session_state["persistence_cohort_term"] not in cohort_options:
        st.session_state["persistence_cohort_term"] = default_cohort or cohort_options[-1]
    if "persistence_distinction" not in st.session_state:
        st.session_state["persistence_distinction"] = PERSISTENCE_DEFAULT_DISTINCTION

    filter_cols = st.columns([1.2, 1, 1.2])
    with filter_cols[0]:
        cohort_term = st.selectbox("Students entering in", options=cohort_options, key="persistence_cohort_term")
    with filter_cols[1]:
        distinction = st.selectbox("Council distinction", options=PERSISTENCE_COUNCIL_OPTIONS, key="persistence_distinction")
    with filter_cols[2]:
        st.markdown(
            """
            <div class="txst-note">
            Filters use the configured chapter-to-council mapping and mapped organization type fields.
            <strong>FRA</strong> and <strong>SOR</strong> come from mapped fraternity/sorority classifications, and <strong>MGC</strong> also accepts legacy <strong>MCG</strong> spellings when they appear in historical data.
            </div>
            """,
            unsafe_allow_html=True,
        )

    dashboard = build_persistence_dashboard(summary, longitudinal, cohort_term, distinction)
    cohort_frame = dashboard["cohort"]
    chart_frame = dashboard["chart_frame"]
    table_frame = dashboard["table_frame"]
    meta = dashboard["meta"]

    if cohort_frame.empty:
        st.warning("No students matched the selected cohort and council distinction.")
        return

    stat_cols = st.columns(4)
    with stat_cols[0]:
        st.metric("Cohort size", f"{int(meta['students']):,}")
    with stat_cols[1]:
        st.metric("Selected cohort", str(meta["cohort_term"]))
    with stat_cols[2]:
        st.metric("Council view", str(meta["distinction"]))
    with stat_cols[3]:
        st.metric("Latest measurable milestone", str(meta["max_milestone"] or "Unknown"))

    title = f"Persistence and Graduation for {cohort_term}"
    subtitle = (
        f"{distinction} distinction | Roster outcomes first | Manual corrections last | Explicit graduation evidence only"
    )
    chart = persistence_milestone_chart(chart_frame, title=title, subtitle=subtitle)
    st.plotly_chart(chart, use_container_width=True)
    _save_chart_downloads(chart, f"persistence_graduation_{safe_slug(cohort_term)}_{safe_slug(distinction)}")

    if not table_frame.empty:
        st.dataframe(
            _format_display_frame(table_frame, percent_cols=PERSISTENCE_OUTCOME_ORDER),
            use_container_width=True,
            hide_index=True,
        )

    with st.expander("Graduation rate denominator toggle", expanded=True):
        st.caption(
            "This uses the same selected cohort and council distinction above. "
            "Resolved-only is best for comparing final outcomes; full-population is the conservative rate that keeps active and unknown students in the denominator."
        )
        toggle_cols = st.columns([1, 1])
        with toggle_cols[0]:
            denominator_view = st.radio(
                "Denominator view",
                options=[RESOLVED_OUTCOMES_ONLY_LABEL, ALL_STUDENTS_LABEL, "Side-by-side"],
                horizontal=True,
                key="persistence_graduation_denominator_view",
            )
        with toggle_cols[1]:
            breakdown_options = {
                "Overall": None,
                "Chapter": "chapter",
                "Council": "council",
                "Fraternity / Sorority": "org_type",
                "Join Term": "join_term",
            }
            breakdown_label = st.selectbox(
                "Breakdown",
                options=list(breakdown_options.keys()),
                key="persistence_graduation_denominator_breakdown",
            )

        denominator_table = build_graduation_denominator_comparison(cohort_frame, breakdown_options[breakdown_label])
        if denominator_table.empty:
            st.caption("No graduation denominator comparison is available for this cohort.")
        else:
            display_table = denominator_table.copy()
            if denominator_view == RESOLVED_OUTCOMES_ONLY_LABEL:
                display_table["Selected Graduation Rate"] = display_table["Graduation Rate (Resolved Outcomes Only)"]
                display_table["Selected Denominator"] = display_table["Resolved Outcomes"]
            elif denominator_view == ALL_STUDENTS_LABEL:
                display_table["Selected Graduation Rate"] = display_table["Graduation Rate (Full Population)"]
                display_table["Selected Denominator"] = display_table["Total Unique Students"]
            else:
                display_table["Selected Graduation Rate"] = pd.NA
                display_table["Selected Denominator"] = pd.NA
            ordered_columns = [
                "Group",
                "Selected Graduation Rate",
                "Selected Denominator",
                "Explicit Graduates",
                "Total Unique Students",
                "Resolved Outcomes",
                "Still Active",
                "Unknown / Unresolved",
                "Other / Unmapped",
                "Graduation Rate (Resolved Outcomes Only)",
                "Graduation Rate (Full Population)",
                "Unknown Share",
            ]
            display_table = display_table[[column for column in ordered_columns if column in display_table.columns]]
            st.dataframe(
                _format_display_frame(
                    display_table,
                    percent_cols=[
                        "Selected Graduation Rate",
                        "Graduation Rate (Resolved Outcomes Only)",
                        "Graduation Rate (Full Population)",
                        "Unknown Share",
                    ],
                    integer_cols=[
                        "Selected Denominator",
                        "Explicit Graduates",
                        "Total Unique Students",
                        "Resolved Outcomes",
                        "Still Active",
                        "Unknown / Unresolved",
                        "Other / Unmapped",
                    ],
                ),
                use_container_width=True,
                hide_index=True,
            )

    st.caption(meta["note"])
    st.caption("Caution: this page uses organization-entry cohorts rather than true first-time-in-college cohorts. It is designed to match the institutional presentation format as closely as the available FSL data allows.")


def _render_chapter_health_dashboard(bundle) -> None:
    summary = bundle.summary.copy()
    longitudinal = bundle.longitudinal.copy()
    chapter_options = chapter_health_options(summary, longitudinal)

    st.title("Chapter Health Dashboard")
    st.caption("One chapter at a time, using historical and current canonical data together. This view keeps inactive and historical chapters available instead of limiting the dashboard to only currently active organizations.")

    if not chapter_options:
        st.warning("No chapter options were available in the loaded dataset.")
        return

    if "chapter_health_chapter" not in st.session_state or st.session_state["chapter_health_chapter"] not in chapter_options:
        st.session_state["chapter_health_chapter"] = chapter_options[0]

    selected_chapter = st.selectbox("Chapter", options=chapter_options, key="chapter_health_chapter")
    dashboard = build_chapter_health_dashboard(summary, longitudinal, selected_chapter)
    meta = dashboard["meta"]
    kpis = dashboard["kpis"]

    if not kpis:
        st.warning("No chapter-level data matched the selected chapter.")
        return

    info_cols = st.columns(5)
    with info_cols[0]:
        st.metric("Council", meta["council"] or "Unknown")
    with info_cols[1]:
        st.metric("Type", meta["org_type"] or "Unknown")
    with info_cols[2]:
        st.metric("Current Status", "Active" if meta["is_currently_active"] else "Historical / Inactive")
    with info_cols[3]:
        st.metric("Latest Active Roster", meta["latest_current_roster_term"] or "Not current")
    with info_cols[4]:
        st.metric("Last Observed Term", meta["last_observed_term"] or "Unknown")

    st.caption(meta["notes"])

    top_kpi_cols = st.columns(4)
    with top_kpi_cols[0]:
        st.metric("Current active members", f"{int(kpis['current_active_members']):,}")
    with top_kpi_cols[1]:
        st.metric("Students ever observed", f"{int(kpis['students_ever_observed']):,}")
    with top_kpi_cols[2]:
        st.metric("Students entering chapter", f"{int(kpis['students_entering_chapter']):,}")
    with top_kpi_cols[3]:
        st.metric("Resolved grad rate", _display_metric_value(kpis["resolved_graduation_rate"], "percent"))

    second_kpi_cols = st.columns(4)
    with second_kpi_cols[0]:
        st.metric("Full-pop grad rate", _display_metric_value(kpis["full_population_graduation_rate"], "percent"))
    with second_kpi_cols[1]:
        st.metric("Next-fall retention", _display_metric_value(kpis["next_fall_retention_rate"], "percent"))
    with second_kpi_cols[2]:
        st.metric("Avg first-year GPA", _display_metric_value(kpis["average_first_year_gpa"], "decimal"))
    with second_kpi_cols[3]:
        unresolved_label = f"{int(kpis['roster_disappeared_unknown']):,} / {int(kpis['unknown_outcomes']):,}"
        st.metric("Roster disappeared / unknown", unresolved_label)

    risk_flags = dashboard["risk_flags"]
    st.subheader("Chapter risk flags")
    if risk_flags.empty:
        st.success("No chapter-level risk flags were triggered by the current advisor/risk heuristics.")
    else:
        severity_cols = st.columns(3)
        with severity_cols[0]:
            st.metric("High flags", f"{int(risk_flags['Severity'].eq('High').sum()):,}")
        with severity_cols[1]:
            st.metric("Medium flags", f"{int(risk_flags['Severity'].eq('Medium').sum()):,}")
        with severity_cols[2]:
            st.metric("Monitor flags", f"{int(risk_flags['Severity'].eq('Monitor').sum()):,}")

        for row in risk_flags.itertuples(index=False):
            message = f"{row.Flag}: {row.Details}"
            if row.Severity == "High":
                st.error(message)
            elif row.Severity == "Medium":
                st.warning(message)
            else:
                st.info(message)

        st.dataframe(risk_flags, use_container_width=True, hide_index=True)

    overview_tab, cohorts_tab, review_tab = st.tabs(["Overview", "Cohorts", "Review"])

    with overview_tab:
        trend_cols = st.columns(2)
        with trend_cols[0]:
            yearly_trend = dashboard["yearly_trend"]
            if not yearly_trend.empty:
                headcount_chart = line_chart(
                    yearly_trend,
                    x="Year",
                    y="Distinct Students",
                    color=None,
                    title=f"Distinct students observed over time: {selected_chapter}",
                )
                st.plotly_chart(headcount_chart, use_container_width=True)
                _save_chart_downloads(headcount_chart, f"chapter_health_headcount_{safe_slug(selected_chapter)}")
            else:
                st.caption("No yearly headcount trend is available for this chapter.")

        with trend_cols[1]:
            gpa_trend = dashboard["yearly_gpa_trend"]
            if not gpa_trend.empty:
                gpa_chart = line_chart(
                    gpa_trend,
                    x="Year",
                    y="Value",
                    color="Metric",
                    title=f"GPA trend over time: {selected_chapter}",
                )
                st.plotly_chart(gpa_chart, use_container_width=True)
                _save_chart_downloads(gpa_chart, f"chapter_health_gpa_{safe_slug(selected_chapter)}")
            else:
                st.caption("No GPA trend is available for this chapter.")

        st.subheader("Latest outcome mix for students entering this chapter")
        outcome_breakdown = dashboard["outcome_breakdown"]
        if not outcome_breakdown.empty:
            outcome_chart = bar_chart(
                outcome_breakdown,
                x="Outcome",
                y="Share",
                color=None,
                title=f"Outcome mix for {selected_chapter}",
                y_format="percent",
            )
            st.plotly_chart(outcome_chart, use_container_width=True)
            _save_chart_downloads(outcome_chart, f"chapter_health_outcomes_{safe_slug(selected_chapter)}")
            st.dataframe(_format_display_frame(outcome_breakdown, percent_cols=["Share"]), use_container_width=True, hide_index=True)
        else:
            st.caption("No entry-student outcome breakdown is available for this chapter.")

    with cohorts_tab:
        st.subheader("Entry cohort health")
        cohort_table = dashboard["cohort_table"]
        if cohort_table.empty:
            st.caption("No chapter-entry cohorts were available for this chapter.")
        else:
            st.dataframe(
                _format_display_frame(
                    cohort_table,
                    percent_cols=["Resolved Graduation Rate", "Full Population Graduation Rate", "Next Fall Retention"],
                    decimal_cols=["Average First-Year GPA", "Average Cumulative GPA"],
                ),
                use_container_width=True,
                hide_index=True,
            )

    with review_tab:
        st.subheader("Current active members")
        current_active_students = dashboard["current_active_students"]
        if current_active_students.empty:
            st.caption("This chapter does not currently appear as active on the latest roster.")
        else:
            st.dataframe(
                _format_display_frame(current_active_students, percent_cols=["Data Completeness Rate"]),
                use_container_width=True,
                hide_index=True,
            )

        st.subheader("Students needing review")
        review_students = dashboard["review_students"]
        if review_students.empty:
            st.caption("No unresolved or roster-disappeared entry students are currently flagged for this chapter.")
        else:
            st.dataframe(
                _format_display_frame(review_students, percent_cols=["Data Completeness Rate"]),
                use_container_width=True,
                hide_index=True,
            )


def _render_roster_disappearance_tracker(bundle) -> None:
    summary = bundle.summary.copy()

    st.title("Roster Disappearance Tracker")
    st.caption(
        "This view isolates students classified as `Roster Dissapeared/Unknown`: they are not treated as graduates, "
        "but their chapter roster coverage appears to have disappeared before a confirmed final outcome was found."
    )

    base_tracker = build_roster_disappearance_tracker(summary)
    base_students = base_tracker["student_table"]
    if base_students.empty:
        st.success("No roster-disappeared unknown students were found in the current canonical summary.")
        return

    filter_cols = st.columns(3)
    with filter_cols[0]:
        council_options = ["All"] + _unique_text_options(base_students, "Council")
        selected_council = st.selectbox("Council", options=council_options, key="roster_disappearance_council")
    with filter_cols[1]:
        chapter_options = ["All"] + _unique_text_options(base_students, "Chapter")
        selected_chapter = st.selectbox("Chapter", options=chapter_options, key="roster_disappearance_chapter")
    with filter_cols[2]:
        student_search = st.text_input("Search students", placeholder="Name, Banner ID, or chapter", key="roster_disappearance_search")

    filtered_summary = summary.copy()
    if selected_council != "All":
        council_masks = []
        for column in ["council", "current_active_council", "chapter_group"]:
            if column in filtered_summary.columns:
                council_masks.append(filtered_summary[column].fillna("").astype(str).str.strip().eq(selected_council))
        if council_masks:
            council_mask = council_masks[0]
            for mask in council_masks[1:]:
                council_mask = council_mask | mask
            filtered_summary = filtered_summary.loc[council_mask].copy()
        else:
            filtered_summary = filtered_summary.iloc[0:0].copy()
    if selected_chapter != "All":
        chapter_masks = []
        for column in ["initial_chapter", "latest_chapter", "chapter", "current_active_chapter"]:
            if column in filtered_summary.columns:
                chapter_masks.append(filtered_summary[column].fillna("").astype(str).str.strip().eq(selected_chapter))
        if chapter_masks:
            chapter_mask = chapter_masks[0]
            for mask in chapter_masks[1:]:
                chapter_mask = chapter_mask | mask
            filtered_summary = filtered_summary.loc[chapter_mask].copy()
        else:
            filtered_summary = filtered_summary.iloc[0:0].copy()

    tracker = build_roster_disappearance_tracker(filtered_summary)
    meta = tracker["meta"]
    student_table = tracker["student_table"]
    if student_search and not student_table.empty:
        search_haystack = student_table.fillna("").astype(str).agg(" ".join, axis=1).str.lower()
        student_table = student_table.loc[search_haystack.str.contains(student_search.lower(), regex=False, na=False)].copy()

    kpi_cols = st.columns(4)
    with kpi_cols[0]:
        st.metric("Affected students", f"{int(meta['affected_students']):,}")
    with kpi_cols[1]:
        st.metric("Affected chapters", f"{int(meta['affected_chapters']):,}")
    with kpi_cols[2]:
        st.metric("Filtered student base", f"{int(meta['total_students']):,}")
    with kpi_cols[3]:
        st.metric("Affected share", _display_metric_value(meta["affected_share"], "percent"))

    st.info(
        "Use this as a cleanup list, not as a graduation list. These students stay unknown until a manual correction, roster status, transcript note, or other explicit evidence resolves them."
    )

    chapter_rollup = tracker["chapter_rollup"]
    if not chapter_rollup.empty:
        top_chapters = chapter_rollup.head(25)
        chart = bar_chart(
            top_chapters,
            x="Chapter",
            y="Affected Students",
            color="Council" if "Council" in top_chapters.columns else None,
            title="Roster-disappeared unknowns by chapter",
        )
        st.plotly_chart(chart, use_container_width=True)
        _save_chart_downloads(chart, "roster_disappearance_by_chapter")

    rollup_tab, cohort_tab, student_tab = st.tabs(["Chapter Rollup", "Timing", "Student Detail"])

    with rollup_tab:
        st.subheader("Affected chapters")
        if chapter_rollup.empty:
            st.caption("No chapter rollup is available for the current filters.")
        else:
            st.dataframe(
                _format_display_frame(chapter_rollup, percent_cols=["Average Data Completeness"]),
                use_container_width=True,
                hide_index=True,
            )

    with cohort_tab:
        timing_cols = st.columns(2)
        with timing_cols[0]:
            st.subheader("By join term")
            cohort_rollup = tracker["cohort_rollup"]
            if cohort_rollup.empty:
                st.caption("No join-term rollup is available.")
            else:
                st.dataframe(cohort_rollup, use_container_width=True, hide_index=True)
        with timing_cols[1]:
            st.subheader("By last observed organization term")
            last_observed_rollup = tracker["last_observed_rollup"]
            if last_observed_rollup.empty:
                st.caption("No last-observed rollup is available.")
            else:
                st.dataframe(last_observed_rollup, use_container_width=True, hide_index=True)

    with student_tab:
        st.subheader("Students to investigate")
        if student_table.empty:
            st.caption("No students match the current filters.")
        else:
            st.dataframe(
                _format_display_frame(student_table, percent_cols=["Data Completeness Rate"]),
                use_container_width=True,
                hide_index=True,
            )
            st.download_button(
                "Download roster disappearance students CSV",
                data=dataframe_to_csv_bytes(student_table),
                file_name="roster_disappearance_students.csv",
                mime="text/csv",
                use_container_width=True,
            )


def _render_retention_and_gpa_dashboard(bundle) -> None:
    summary = bundle.summary.copy()
    longitudinal = bundle.longitudinal.copy()

    st.title("Retention & GPA Trends")
    st.caption(
        "This view separates organization retention, academic continuation, and GPA coverage. "
        "Rates use explicit measurable denominators, and GPA trends show how much of the roster actually has grade data."
    )

    retention_group_options = {
        "Overall": None,
        "Initial Chapter": "initial_chapter",
        "Latest Chapter": "chapter",
        "Council": "council",
        "Fraternity / Sorority": "org_type",
        "Join Term": "join_term",
    }
    gpa_group_options = {
        "Overall": None,
        "Chapter": "chapter",
        "Council": "council",
        "Fraternity / Sorority": "org_type",
    }

    retention_tab, gpa_tab = st.tabs(["Retention Rates", "GPA Trends"])

    with retention_tab:
        control_cols = st.columns([1.2, 1])
        with control_cols[0]:
            retention_group_label = st.selectbox(
                "Retention breakdown",
                options=list(retention_group_options.keys()),
                index=1,
                key="retention_dashboard_group",
            )
        with control_cols[1]:
            retention_min_n = st.slider("Minimum measurable denominator", min_value=1, max_value=100, value=5, key="retention_dashboard_min_n")

        retention_table = build_retention_dashboard(
            summary,
            group_field=retention_group_options[retention_group_label],
            min_denominator=retention_min_n,
        )
        if retention_table.empty:
            st.warning("No retention groups met the current measurable-denominator rule.")
        else:
            metric_cols = st.columns(4)
            overall = build_retention_dashboard(summary, group_field=None, min_denominator=1)
            overall_row = overall.iloc[0] if not overall.empty else pd.Series(dtype="object")
            with metric_cols[0]:
                st.metric("Org retention denominator", f"{int(overall_row.get('Organization Retention Denominator', 0) or 0):,}")
            with metric_cols[1]:
                st.metric("Org retention", _display_metric_value(overall_row.get("Organization Retention Rate"), "percent"))
            with metric_cols[2]:
                st.metric("Academic continuation denominator", f"{int(overall_row.get('Academic Continuation Denominator', 0) or 0):,}")
            with metric_cols[3]:
                st.metric("Academic continuation", _display_metric_value(overall_row.get("Academic Continuation Rate"), "percent"))

            chart_source = retention_table.head(25).melt(
                id_vars=["Group"],
                value_vars=["Organization Retention Rate", "Academic Continuation Rate"],
                var_name="Rate Type",
                value_name="Rate",
            )
            retention_chart = bar_chart(
                chart_source,
                x="Group",
                y="Rate",
                color="Rate Type",
                title="Next-fall organization retention versus academic continuation",
                y_format="percent",
            )
            st.plotly_chart(retention_chart, use_container_width=True)
            _save_chart_downloads(retention_chart, "retention_rate_comparison")

            st.dataframe(
                _format_display_frame(
                    retention_table,
                    percent_cols=["Organization Retention Rate", "Academic Continuation Rate"],
                    integer_cols=[
                        "Students",
                        "Organization Retention Denominator",
                        "Retained In Organization Next Fall",
                        "Academic Continuation Denominator",
                        "Academically Continued Next Fall",
                        "Explicit Graduates",
                        "Still Active",
                        "Unknown / Unresolved",
                    ],
                ),
                use_container_width=True,
                hide_index=True,
            )
            st.download_button(
                "Download retention table CSV",
                data=dataframe_to_csv_bytes(retention_table),
                file_name="retention_rates.csv",
                mime="text/csv",
                use_container_width=True,
            )

        st.info(
            "Organization retention means the student appears on a roster in the next-fall checkpoint. "
            "Academic continuation means the student has academic evidence in that checkpoint. "
            "They answer different questions, so the app keeps them side by side instead of blending them."
        )

    with gpa_tab:
        gpa_controls = st.columns([1, 1, 1])
        with gpa_controls[0]:
            gpa_group_label = st.selectbox("GPA segment", options=list(gpa_group_options.keys()), index=1, key="gpa_dashboard_group")
        gpa_table = build_gpa_trend_with_coverage(longitudinal, segment_field=gpa_group_options[gpa_group_label])
        with gpa_controls[1]:
            gpa_measure = st.selectbox(
                "Trend measure",
                options=["Average Term GPA", "Average Cumulative GPA", "Term GPA Coverage"],
                key="gpa_dashboard_measure",
            )
        with gpa_controls[2]:
            available_segments = _unique_text_options(gpa_table, "Segment")
            default_segments = available_segments[:6] if gpa_group_label != "Overall" else available_segments
            selected_segments = st.multiselect(
                "Segments",
                options=available_segments,
                default=default_segments,
                key="gpa_dashboard_segments",
            )

        if gpa_table.empty:
            st.warning("No GPA trend rows were available in the longitudinal canonical table.")
        else:
            gpa_view = gpa_table.copy()
            if selected_segments:
                gpa_view = gpa_view.loc[gpa_view["Segment"].isin(selected_segments)].copy()
            elif gpa_group_label != "Overall":
                gpa_view = gpa_view.iloc[0:0].copy()

            coverage_cols = st.columns(4)
            with coverage_cols[0]:
                st.metric("Terms shown", f"{gpa_view['Observed Term'].nunique():,}" if not gpa_view.empty else "0")
            with coverage_cols[1]:
                st.metric("Roster student-term rows", f"{int(gpa_view['Roster Students'].sum()):,}" if not gpa_view.empty else "0")
            with coverage_cols[2]:
                st.metric("Rows with term GPA", f"{int(gpa_view['Students With Term GPA'].sum()):,}" if not gpa_view.empty else "0")
            with coverage_cols[3]:
                weighted_denominator = float(gpa_view["Roster Students"].replace(0, pd.NA).dropna().sum()) if not gpa_view.empty else 0
                weighted_coverage = (float(gpa_view["Students With Term GPA"].sum()) / weighted_denominator) if weighted_denominator else pd.NA
                st.metric("Weighted GPA coverage", _display_metric_value(weighted_coverage, "percent"))

            if gpa_view.empty:
                st.caption("No GPA segments match the current selection.")
            else:
                gpa_chart = line_chart(
                    gpa_view,
                    x="Observed Term",
                    y=gpa_measure,
                    color="Segment",
                    title=f"{gpa_measure} over time",
                    y_format="percent" if gpa_measure == "Term GPA Coverage" else "",
                )
                st.plotly_chart(gpa_chart, use_container_width=True)
                _save_chart_downloads(gpa_chart, "gpa_trend_with_coverage")

                st.dataframe(
                    _format_display_frame(
                        gpa_view,
                        percent_cols=["Term GPA Coverage"],
                        decimal_cols=["Average Term GPA", "Average Cumulative GPA"],
                        one_decimal_cols=["Average Passed Hours", "Average Cumulative Hours"],
                        integer_cols=["Roster Students", "Academic Students", "Students With Term GPA"],
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
                st.download_button(
                    "Download GPA trend CSV",
                    data=dataframe_to_csv_bytes(gpa_view),
                    file_name="gpa_trends_with_coverage.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

        st.info(
            "GPA coverage is `students with term GPA / roster students` when roster rows exist for a segment-term. "
            "This keeps years with incomplete grade files from looking cleaner than they really are."
        )


def _render_advisor_help_dashboard(bundle) -> None:
    summary = bundle.summary.copy()

    st.title("Advisor Help")
    st.caption(
        "This queue focuses on currently active members who may need outreach or cleanup follow-up soon. "
        "Flags are heuristics built from current-active status, GPA, and data-quality signals, so they should guide conversations rather than replace advisor judgment."
    )

    dashboard = build_advisor_intervention_queue(summary)
    queue = dashboard["queue"]
    chapter_rollup = dashboard["chapter_rollup"]
    meta = dashboard["meta"]

    kpi_cols = st.columns(5)
    with kpi_cols[0]:
        st.metric("Current active students", f"{int(meta['current_active_students']):,}")
    with kpi_cols[1]:
        st.metric("Flagged students", f"{int(meta['flagged_students']):,}")
    with kpi_cols[2]:
        st.metric("High priority", f"{int(meta['high_priority_students']):,}")
    with kpi_cols[3]:
        st.metric("Medium priority", f"{int(meta['medium_priority_students']):,}")
    with kpi_cols[4]:
        st.metric("Monitor", f"{int(meta['monitor_students']):,}")

    if queue.empty:
        st.success("No currently active students were flagged by the current advisor-help heuristics.")
        return

    filter_cols = st.columns(3)
    council_options = ["All"] + _unique_text_options(queue, "Council")
    chapter_options = ["All"] + _unique_text_options(queue, "Current Chapter")
    priority_options = ["High", "Medium", "Monitor"]

    with filter_cols[0]:
        selected_council = st.selectbox("Council", options=council_options, key="advisor_help_council")
    with filter_cols[1]:
        selected_chapter = st.selectbox("Chapter", options=chapter_options, key="advisor_help_chapter")
    with filter_cols[2]:
        selected_priorities = st.multiselect("Priority", options=priority_options, default=priority_options, key="advisor_help_priority")

    queue_view = queue.copy()
    if selected_council != "All":
        queue_view = queue_view.loc[queue_view["Council"].eq(selected_council)].copy()
    if selected_chapter != "All":
        queue_view = queue_view.loc[queue_view["Current Chapter"].eq(selected_chapter)].copy()
    if selected_priorities:
        queue_view = queue_view.loc[queue_view["Priority"].isin(selected_priorities)].copy()
    else:
        queue_view = queue_view.iloc[0:0].copy()

    rollup_view = chapter_rollup.copy()
    if selected_council != "All":
        rollup_view = rollup_view.loc[rollup_view["Council"].eq(selected_council)].copy()
    if selected_chapter != "All":
        rollup_view = rollup_view.loc[rollup_view["Current Chapter"].eq(selected_chapter)].copy()

    chapter_tab, student_tab = st.tabs(["Chapter Summary", "Student Queue"])

    with chapter_tab:
        st.subheader("Where advisor flags are concentrated")
        if rollup_view.empty:
            st.caption("No chapter-level advisor flags matched the current filter.")
        else:
            st.dataframe(
                _format_display_frame(rollup_view, one_decimal_cols=["Average Risk Score"]),
                use_container_width=True,
                hide_index=True,
            )

    with student_tab:
        st.subheader("Students needing outreach or cleanup follow-up")
        if queue_view.empty:
            st.caption("No active students matched the current advisor-help filters.")
        else:
            st.dataframe(
                _format_display_frame(
                    queue_view,
                    percent_cols=["Data Completeness Rate"],
                    decimal_cols=["Average Cumulative GPA", "Average First-Year GPA"],
                ),
                use_container_width=True,
                hide_index=True,
            )


def _manual_correction_row_from_summary(row: pd.Series) -> dict[str, object]:
    final_status_term = row.get("graduation_term", "") or row.get("last_observed_academic_term", "") or row.get("last_observed_org_term", "")
    final_status = row.get("latest_outcome_bucket", "") or row.get("status_group", "") or row.get("latest_roster_status_bucket", "")
    organization_join_term = row.get("join_term", "") or row.get("join_term_code", "")
    return {
        "student_id": row.get("student_id", ""),
        "last_name": row.get("last_name", ""),
        "first_name": row.get("first_name", ""),
        "organization_join_term": organization_join_term,
        "organization_name": row.get("current_active_chapter", "") or row.get("latest_chapter", "") or row.get("chapter", ""),
        "leaving_organization_term": row.get("last_observed_org_term", "") or row.get("last_observed_org_term_code", ""),
        "final_status_term": final_status_term,
        "final_status": final_status,
        "exclude_from_roster_calculations": "",
    }


def _manual_review_key(row: pd.Series) -> str:
    student_id = str(row.get("student_id", "") or "").strip().upper()
    if student_id:
        return student_id
    last_name = str(row.get("last_name", "") or "").strip().lower()
    first_name = str(row.get("first_name", "") or "").strip().lower()
    if last_name or first_name:
        return f"{last_name}|{first_name}"
    return str(row.get("student_name", "") or "").strip().lower()


def _manual_correction_identity_set(corrections: pd.DataFrame) -> set[str]:
    if corrections.empty:
        return set()
    frame = corrections.copy()
    keys = []
    for _, row in frame.iterrows():
        keys.append(_manual_review_key(row))
    return {key for key in keys if key}


def _registry_identity_set(*frames: pd.DataFrame) -> set[str]:
    keys: set[str] = set()
    for frame in frames:
        if frame is None or frame.empty:
            continue
        for _, row in frame.iterrows():
            key = _manual_review_key(row)
            if key:
                keys.add(key)
    return keys


def _queue_reason_for_row(row: pd.Series) -> str:
    reasons: list[str] = []
    if not _graduation_mentioned_for_row(row):
        reasons.append("No graduation mention found")
    unknown_value = str(row.get("is_unknown_outcome", "") or "").strip().lower()
    is_unknown = unknown_value in {"true", "1", "yes", "y"}
    if is_unknown or str(row.get("outcome_resolution_group", "")).lower().find("unknown") >= 0:
        reasons.append("Unknown outcome")
    if str(row.get("chapter_assignment_source", "")).lower().find("unresolved") >= 0:
        reasons.append("Unresolved chapter")
    if str(row.get("chapter_assignment_source", "")).lower().find("inferred") >= 0:
        reasons.append("Inferred chapter")
    completeness = pd.to_numeric(pd.Series([row.get("data_completeness_rate", pd.NA)]), errors="coerce").iloc[0]
    if pd.notna(completeness) and completeness < 0.75:
        reasons.append("Incomplete data")
    if not str(row.get("student_id", "") or "").strip():
        reasons.append("Missing student ID")
    return "; ".join(reasons) or "Review recommended"


def _graduation_mentioned_for_row(row: pd.Series) -> bool:
    if str(row.get("is_graduated", "") or "").strip().lower() in {"true", "1", "yes", "y"}:
        return True
    if str(row.get("graduation_evidence_confirmed", "") or "").strip().lower() in {"yes", "true", "1", "y"}:
        return True
    if str(row.get("graduation_status_without_evidence", "") or "").strip().lower() in {"yes", "true", "1", "y"}:
        return True
    text = " ".join(
        str(row.get(column, "") or "")
        for column in [
            "latest_outcome_bucket",
            "latest_roster_status_bucket",
            "outcome_evidence_source",
            "graduation_term",
            "graduation_term_code",
            "graduated_eventual",
            "graduated_4yr",
            "graduated_5yr",
            "graduated_6yr",
            "ambiguity_flags",
            "manual_review_reason",
        ]
    ).lower()
    return "graduat" in text or "degree awarded" in text or "degree confer" in text


def _build_manual_assignment_queue(summary: pd.DataFrame, decision_keys: set[str]) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame(columns=MANUAL_REVIEW_QUEUE_COLUMNS)

    if "student_id" not in summary.columns:
        return pd.DataFrame(columns=MANUAL_REVIEW_QUEUE_COLUMNS)

    has_student_id = summary["student_id"].fillna("").astype(str).str.strip().ne("")
    graduation_mentioned = summary.apply(_graduation_mentioned_for_row, axis=1)
    candidates = summary.loc[has_student_id & ~graduation_mentioned].copy()
    rows: list[dict[str, object]] = []
    for _, row in candidates.iterrows():
        review_key = _manual_review_key(row)
        if not review_key:
            continue
        correction_row = pd.Series(_manual_correction_row_from_summary(row))
        rows.append(
            {
                "review_key": review_key,
                "student_id": row.get("student_id", ""),
                "last_name": row.get("last_name", ""),
                "first_name": row.get("first_name", ""),
                "student_name": row.get("student_name", ""),
                "chapter": row.get("current_active_chapter", "") or row.get("latest_chapter", "") or row.get("chapter", ""),
                "join_term": row.get("join_term", ""),
                "last_observed_org_term": row.get("last_observed_org_term", ""),
                "latest_outcome_bucket": row.get("latest_outcome_bucket", ""),
                "outcome_resolution_group": row.get("outcome_resolution_group", ""),
                "queue_reason": _queue_reason_for_row(row),
                "assigned_to": "",
                "review_status": "Needs Review",
                "needs_transcript": "No",
                "review_notes": "",
                "has_manual_correction": "Yes" if review_key in decision_keys else "No",
                "transcript_file_exists": "Yes" if manual_transcript_path_for_correction(correction_row).exists() else "No",
                "updated_at": "",
            }
        )

    queue = pd.DataFrame(rows)
    if queue.empty:
        return pd.DataFrame(columns=MANUAL_REVIEW_QUEUE_COLUMNS)
    return queue.drop_duplicates(subset=["review_key"], keep="first").reset_index(drop=True)


def _canonical_review_queue_for_app(bundle, decision_keys: set[str]) -> pd.DataFrame:
    tables = getattr(bundle, "tables", {})
    canonical = tables.get("manual_review_queue", pd.DataFrame())
    if canonical is None or canonical.empty:
        return pd.DataFrame(columns=MANUAL_REVIEW_QUEUE_COLUMNS)
    summary = getattr(bundle, "summary", pd.DataFrame()).copy()
    summary_lookup = {}
    if not summary.empty and "student_id" in summary.columns:
        for _, summary_row in summary.iterrows():
            student_id = str(summary_row.get("student_id", "") or "").strip().upper()
            if student_id and student_id not in summary_lookup:
                summary_lookup[student_id] = summary_row
    rows: list[dict[str, object]] = []
    for _, row in canonical.iterrows():
        student_id = str(row.get("student_id", "") or "").strip()
        summary_row = summary_lookup.get(student_id.upper(), pd.Series(dtype="object"))
        review_key = str(row.get("review_id", "") or "").strip() or student_id or str(row.get("normalized_student_id", "") or "").strip()
        if not review_key:
            continue
        rows.append(
            {
                "review_key": review_key,
                "student_id": student_id,
                "last_name": "",
                "first_name": "",
                "student_name": summary_row.get("student_name", ""),
                "chapter": row.get("chapter", "") or summary_row.get("chapter", ""),
                "join_term": summary_row.get("join_term", ""),
                "last_observed_org_term": summary_row.get("last_observed_org_term", row.get("term", "")),
                "latest_outcome_bucket": row.get("current_outcome_bucket", "") or summary_row.get("latest_outcome_bucket", ""),
                "outcome_resolution_group": row.get("current_outcome_bucket", ""),
                "academic_year": "",
                "term": row.get("term", ""),
                "organization": row.get("organization", ""),
                "issue_type": row.get("issue_type", ""),
                "outcome_bucket": row.get("current_outcome_bucket", ""),
                "priority": row.get("priority", ""),
                "input_group_id": row.get("input_group_id", ""),
                "source_file": row.get("source_file", ""),
                "source_sheet": row.get("source_sheet", ""),
                "evidence_summary": row.get("evidence_summary", ""),
                "suggested_action": row.get("suggested_action", ""),
                "queue_reason": row.get("issue_description", "") or row.get("issue_type", ""),
                "assigned_to": row.get("reviewed_by", ""),
                "review_status": "Corrected" if str(row.get("manual_adjustment_status", "")).lower() == "applied" else "Needs Review",
                "needs_transcript": "No",
                "review_notes": row.get("reviewer_notes", ""),
                "has_manual_correction": "Yes" if student_id and student_id.upper() in decision_keys else "No",
                "transcript_file_exists": "No",
                "updated_at": row.get("reviewed_at", ""),
            }
        )
    if not rows:
        return pd.DataFrame(columns=MANUAL_REVIEW_QUEUE_COLUMNS)
    return pd.DataFrame(rows).drop_duplicates(subset=["review_key"], keep="first").reset_index(drop=True)


def _merge_saved_review_queue(generated: pd.DataFrame, saved: pd.DataFrame) -> pd.DataFrame:
    if generated.empty:
        return pd.DataFrame(columns=MANUAL_REVIEW_QUEUE_COLUMNS)
    result = generated.copy()
    if not saved.empty:
        keep_columns = ["review_key", "assigned_to", "review_status", "needs_transcript", "review_notes", "updated_at"]
        saved_subset = saved[[column for column in keep_columns if column in saved.columns]].copy()
        result = result.merge(saved_subset, on="review_key", how="left", suffixes=("", "_saved"))
        for column in ["assigned_to", "review_status", "needs_transcript", "review_notes", "updated_at"]:
            saved_column = f"{column}_saved"
            if saved_column in result.columns:
                result[column] = result[saved_column].where(result[saved_column].fillna("").astype(str).str.strip().ne(""), result[column])
                result = result.drop(columns=[saved_column])
    return dedupe_manual_review_queue_by_cohort(result[MANUAL_REVIEW_QUEUE_COLUMNS].fillna("").astype(str)).reset_index(drop=True)


def _manual_review_action_rows(
    queue_rows: pd.DataFrame,
    assigned_to: str = "",
    review_status: str = "",
    needs_transcript: str = "",
    note: str = "",
    has_manual_correction: str = "",
) -> pd.DataFrame:
    if queue_rows is None or queue_rows.empty:
        return pd.DataFrame(columns=MANUAL_REVIEW_QUEUE_COLUMNS)
    rows = queue_rows.copy()
    for column in MANUAL_REVIEW_QUEUE_COLUMNS:
        if column not in rows.columns:
            rows[column] = ""
    rows = rows[MANUAL_REVIEW_QUEUE_COLUMNS].fillna("").astype(str)
    for column in MANUAL_REVIEW_QUEUE_COLUMNS:
        rows[column] = rows[column].str.strip()
    if assigned_to:
        rows["assigned_to"] = assigned_to
    if review_status:
        rows["review_status"] = review_status
    if needs_transcript:
        rows["needs_transcript"] = needs_transcript
    if note:
        rows["review_notes"] = rows["review_notes"].where(rows["review_notes"].str.strip().ne(""), note)
    if has_manual_correction:
        rows["has_manual_correction"] = has_manual_correction
    rows["updated_at"] = datetime.now().isoformat(timespec="seconds")
    return dedupe_manual_review_queue_by_cohort(rows)


def _append_manual_review_actions(
    queue_rows: pd.DataFrame,
    assigned_to: str = "",
    review_status: str = "",
    needs_transcript: str = "",
    note: str = "",
    has_manual_correction: str = "",
) -> dict[str, object]:
    actions = _manual_review_action_rows(
        queue_rows,
        assigned_to=assigned_to,
        review_status=review_status,
        needs_transcript=needs_transcript,
        note=note,
        has_manual_correction=has_manual_correction,
    )
    if actions.empty:
        return {"path": MANUAL_REVIEW_ACTIONS_PATH, "incoming_rows": 0, "saved_rows": len(load_manual_review_actions())}
    return append_manual_review_actions(actions)


def _warn_if_manual_action_fallback(result: dict[str, object]) -> None:
    if result.get("used_fallback"):
        attempted = result.get("attempted_path", MANUAL_REVIEW_ACTIONS_PATH)
        fallback = result.get("path", "")
        st.warning(
            f"{attempted} is locked or not writable, so the action log was saved to {fallback}. "
            "Close the locked CSV when you can; the app and pipeline will still read the pending action file."
        )


def _manual_queue_metrics(queue: pd.DataFrame, corrections: pd.DataFrame) -> dict[str, int]:
    if queue.empty:
        return {
            "queue_total": 0,
            "needs_review": 0,
            "in_progress": 0,
            "corrected": len(corrections),
            "needs_transcript": 0,
            "transcripts_ready": 0,
        }
    review_status = queue["review_status"].fillna("").astype(str)
    return {
        "queue_total": int(len(queue)),
        "needs_review": int(review_status.eq("Needs Review").sum()),
        "in_progress": int(review_status.eq("In Progress").sum()),
        "corrected": int(queue["has_manual_correction"].fillna("").astype(str).str.lower().eq("yes").sum()),
        "needs_transcript": int(queue["needs_transcript"].fillna("").astype(str).str.lower().eq("yes").sum()),
        "transcripts_ready": int(queue["transcript_file_exists"].fillna("").astype(str).str.lower().eq("yes").sum()),
    }


def _manual_corrections_from_queue_rows(queue_rows: pd.DataFrame, final_status: str = "", exclude_from_roster: bool = False) -> pd.DataFrame:
    if queue_rows is None or queue_rows.empty:
        return empty_manual_roster_corrections()

    rows = queue_rows.copy()
    for column in MANUAL_REVIEW_QUEUE_COLUMNS:
        if column not in rows.columns:
            rows[column] = ""
    rows = rows[MANUAL_REVIEW_QUEUE_COLUMNS].fillna("").astype(str)
    for column in MANUAL_REVIEW_QUEUE_COLUMNS:
        rows[column] = rows[column].str.strip()

    status_series = pd.Series(final_status, index=rows.index) if final_status else rows["latest_outcome_bucket"]
    status_term = rows["last_observed_org_term"].where(status_series.astype(str).str.strip().ne(""), "")
    corrections = pd.DataFrame(
        {
            "student_id": rows["student_id"],
            "last_name": rows["last_name"],
            "first_name": rows["first_name"],
            "organization_join_term": rows["join_term"],
            "organization_name": rows["chapter"],
            "leaving_organization_term": rows["last_observed_org_term"],
            "final_status_term": status_term,
            "final_status": status_series,
            "exclude_from_roster_calculations": "Yes" if exclude_from_roster else "",
        }
    )
    if exclude_from_roster:
        corrections["final_status_term"] = ""
        corrections["final_status"] = ""
    return normalize_manual_roster_corrections(corrections)


def _registry_base_from_queue_rows(queue_rows: pd.DataFrame) -> pd.DataFrame:
    if queue_rows is None or queue_rows.empty:
        return pd.DataFrame(columns=MANUAL_REVIEW_QUEUE_COLUMNS)
    rows = queue_rows.copy()
    for column in MANUAL_REVIEW_QUEUE_COLUMNS:
        if column not in rows.columns:
            rows[column] = ""
    rows = rows[MANUAL_REVIEW_QUEUE_COLUMNS].fillna("").astype(str)
    for column in MANUAL_REVIEW_QUEUE_COLUMNS:
        rows[column] = rows[column].str.strip()
    return rows


def _graduation_evidence_from_queue_rows(queue_rows: pd.DataFrame, entered_by: str = "", note: str = "") -> pd.DataFrame:
    rows = _registry_base_from_queue_rows(queue_rows)
    if rows.empty:
        return pd.DataFrame(columns=GRADUATION_EVIDENCE_COLUMNS)
    result = pd.DataFrame(
        {
            "student_id": rows["student_id"],
            "organization_name": rows["chapter"].where(rows["chapter"].ne(""), rows["organization"]),
            "organization_join_term": rows["join_term"],
            "graduation_term": rows["last_observed_org_term"].where(rows["last_observed_org_term"].ne(""), rows["term"]),
            "evidence_source": "Manual review: explicit graduation evidence confirmed",
            "notes": note or rows["review_notes"].where(rows["review_notes"].ne(""), rows["queue_reason"]),
            "entered_by": entered_by,
            "entered_at": datetime.now().isoformat(timespec="seconds"),
        }
    )
    return normalize_graduation_evidence(result)


def _outcome_overrides_from_queue_rows(queue_rows: pd.DataFrame, final_status: str, entered_by: str = "", note: str = "") -> pd.DataFrame:
    rows = _registry_base_from_queue_rows(queue_rows)
    if rows.empty:
        return pd.DataFrame(columns=OUTCOME_OVERRIDE_COLUMNS)
    result = pd.DataFrame(
        {
            "student_id": rows["student_id"],
            "organization_name": rows["chapter"].where(rows["chapter"].ne(""), rows["organization"]),
            "organization_join_term": rows["join_term"],
            "final_status": final_status,
            "final_status_term": rows["last_observed_org_term"].where(rows["last_observed_org_term"].ne(""), rows["term"]),
            "reason": note or f"Manual review set outcome to {final_status}.",
            "evidence_source": rows["queue_reason"].where(rows["queue_reason"].ne(""), "Manual review"),
            "entered_by": entered_by,
            "entered_at": datetime.now().isoformat(timespec="seconds"),
        }
    )
    return normalize_outcome_overrides(result)


def _roster_exclusions_from_queue_rows(queue_rows: pd.DataFrame, entered_by: str = "", note: str = "") -> pd.DataFrame:
    rows = _registry_base_from_queue_rows(queue_rows)
    if rows.empty:
        return pd.DataFrame(columns=ROSTER_EXCLUSION_COLUMNS)
    result = pd.DataFrame(
        {
            "student_id": rows["student_id"],
            "organization_name": rows["chapter"].where(rows["chapter"].ne(""), rows["organization"]),
            "term": rows["term"].where(rows["term"].ne(""), rows["last_observed_org_term"].where(rows["last_observed_org_term"].ne(""), rows["join_term"])),
            "source_file": rows["source_file"],
            "source_sheet": rows["source_sheet"],
            "reason": note or rows["queue_reason"].where(rows["queue_reason"].ne(""), "Manual review roster exclusion"),
            "entered_by": entered_by,
            "entered_at": datetime.now().isoformat(timespec="seconds"),
        }
    )
    return normalize_roster_exclusions(result)


def _graduation_evidence_from_corrections(corrections: pd.DataFrame, entered_by: str = "", note: str = "") -> pd.DataFrame:
    cleaned = normalize_manual_roster_corrections(corrections)
    if cleaned.empty:
        return pd.DataFrame(columns=GRADUATION_EVIDENCE_COLUMNS)
    result = pd.DataFrame(
        {
            "student_id": cleaned["student_id"],
            "organization_name": cleaned["organization_name"],
            "organization_join_term": cleaned["organization_join_term"],
            "graduation_term": cleaned["final_status_term"],
            "evidence_source": "Graduated alumni list",
            "notes": note or "Graduated alumni batch entered in app.",
            "entered_by": entered_by,
            "entered_at": datetime.now().isoformat(timespec="seconds"),
        }
    )
    return normalize_graduation_evidence(result)


def _manual_adjustments_from_corrections(corrections: pd.DataFrame, reviewer: str = "", reason: str = "") -> pd.DataFrame:
    cleaned = normalize_manual_roster_corrections(corrections)
    if cleaned.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    created_at = datetime.now().isoformat(timespec="seconds")
    for _, row in cleaned.iterrows():
        student_id = str(row.get("student_id", "") or "").strip().upper()
        base = {
            "adjustment_id": "",
            "student_id": student_id,
            "normalized_student_id": student_id,
            "reason": reason or "Saved from Manual Roster Corrections app.",
            "evidence": "Manual reviewer correction",
            "source_file": str(MANUAL_ROSTER_CORRECTIONS_PATH),
            "source_sheet": "Manual Corrections App",
            "reviewer": reviewer,
            "created_at": created_at,
            "active": "Yes",
        }
        final_status = str(row.get("final_status", "") or "").strip()
        if final_status:
            rows.append(
                {
                    **base,
                    "adjustment_type": "outcome_override",
                    "field_to_override": "final_outcome_bucket",
                    "original_value": "",
                    "adjusted_value": final_status,
                }
            )
        organization = str(row.get("organization_name", "") or "").strip()
        if organization:
            rows.append(
                {
                    **base,
                    "adjustment_type": "chapter_override",
                    "field_to_override": "latest_known_chapter",
                    "original_value": "",
                    "adjusted_value": organization,
                }
            )
    return pd.DataFrame(rows)


def _append_manual_adjustments_from_corrections(corrections: pd.DataFrame, reviewer: str = "", reason: str = "") -> None:
    adjustments = _manual_adjustments_from_corrections(corrections, reviewer=reviewer, reason=reason)
    if adjustments.empty:
        return
    append_manual_adjustments(adjustments)


def _manual_correction_key_series(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype="object")
    return frame[MANUAL_ROSTER_CORRECTION_COLUMNS].fillna("").astype(str).agg("\u241f".join, axis=1)


def _new_or_changed_manual_rows(frame: pd.DataFrame, existing: pd.DataFrame) -> pd.DataFrame:
    incoming = normalize_manual_roster_corrections(frame)
    if incoming.empty or existing.empty:
        return incoming
    existing_keys = set(_manual_correction_key_series(normalize_manual_roster_corrections(existing)))
    return incoming.loc[~_manual_correction_key_series(incoming).isin(existing_keys)].reset_index(drop=True)


def _staged_manual_corrections() -> pd.DataFrame:
    staged = st.session_state.get(MANUAL_STAGED_CORRECTIONS_KEY, [])
    if isinstance(staged, pd.DataFrame):
        return normalize_manual_roster_corrections(staged)
    return normalize_manual_roster_corrections(pd.DataFrame(staged))


def _set_staged_manual_corrections(frame: pd.DataFrame) -> None:
    st.session_state[MANUAL_STAGED_CORRECTIONS_KEY] = normalize_manual_roster_corrections(frame).to_dict("records")


def _stage_manual_corrections(frame: pd.DataFrame) -> pd.DataFrame:
    incoming = normalize_manual_roster_corrections(frame)
    if incoming.empty:
        return incoming
    combined = pd.concat([_staged_manual_corrections(), incoming], ignore_index=True)
    combined = combined.drop_duplicates(subset=MANUAL_ROSTER_CORRECTION_COLUMNS, keep="last").reset_index(drop=True)
    _set_staged_manual_corrections(combined)
    return incoming


def _clear_staged_manual_corrections() -> None:
    st.session_state[MANUAL_STAGED_CORRECTIONS_KEY] = []


def _mark_manual_queue_corrected(corrections: pd.DataFrame, assigned_to: str = "", note: str = "") -> None:
    if corrections.empty:
        return

    cleaned = normalize_manual_roster_corrections(corrections)
    if cleaned.empty:
        return

    rows: list[dict[str, object]] = []
    for _, row in cleaned.iterrows():
        review_key = _manual_review_key(row)
        if not review_key:
            continue
        rows.append(
            {
                "review_key": review_key,
                "student_id": row.get("student_id", ""),
                "last_name": row.get("last_name", ""),
                "first_name": row.get("first_name", ""),
                "chapter": row.get("organization_name", ""),
                "join_term": row.get("organization_join_term", ""),
                "last_observed_org_term": row.get("leaving_organization_term", ""),
                "latest_outcome_bucket": row.get("final_status", ""),
            }
        )
    if rows:
        _append_manual_review_actions(
            pd.DataFrame(rows),
            assigned_to=assigned_to,
            review_status="Corrected",
            note=note,
            has_manual_correction="Yes",
        )


def _commit_staged_manual_corrections(assigned_to: str = "") -> dict[str, object]:
    staged = _staged_manual_corrections()
    if staged.empty:
        return {"saved_path": None, "saved_rows": 0, "created_transcripts": []}

    append_result = append_manual_roster_corrections(staged)
    saved_path = append_result["path"]
    _append_manual_adjustments_from_corrections(staged, reviewer=assigned_to, reason="Manual correction committed from staged changes.")
    created_transcripts = ensure_manual_transcript_files(staged)
    _mark_manual_queue_corrected(staged, assigned_to=assigned_to, note="Manual correction committed from staged changes.")
    _clear_staged_manual_corrections()
    return {"saved_path": saved_path, "saved_rows": append_result["appended_rows"], "created_transcripts": created_transcripts}


def _manual_save_mode_is_staged() -> bool:
    return st.session_state.get(MANUAL_SAVE_MODE_KEY, MANUAL_STAGE_MODE_LABEL) == MANUAL_STAGE_MODE_LABEL


def _save_manual_correction_batch(corrections: pd.DataFrame, assigned_to: str = "", note: str = "") -> dict[str, object]:
    cleaned = normalize_manual_roster_corrections(corrections)
    if cleaned.empty:
        return {"saved_path": None, "saved_rows": 0, "created_transcripts": []}

    append_result = append_manual_roster_corrections(cleaned)
    saved_path = append_result["path"]
    _append_manual_adjustments_from_corrections(cleaned, reviewer=assigned_to, reason=note or "Manual correction batch saved.")
    created_transcripts = ensure_manual_transcript_files(cleaned)
    _mark_manual_queue_corrected(cleaned, assigned_to=assigned_to, note=note)
    return {"saved_path": saved_path, "saved_rows": append_result["appended_rows"], "created_transcripts": created_transcripts}


def _manual_correction_review_tables(bundle) -> dict[str, pd.DataFrame]:
    summary = getattr(bundle, "summary", pd.DataFrame()).copy()
    tables = getattr(bundle, "tables", {})
    review: dict[str, pd.DataFrame] = {}

    if not summary.empty:
        masks = []
        if "is_unknown_outcome" in summary.columns:
            masks.append(_truthy_mask(summary["is_unknown_outcome"]))
        if "chapter_assignment_source" in summary.columns:
            masks.append(summary["chapter_assignment_source"].fillna("").astype(str).str.contains("unresolved|inferred", case=False, na=False))
        if "data_completeness_rate" in summary.columns:
            masks.append(pd.to_numeric(summary["data_completeness_rate"], errors="coerce").lt(0.75))
        if "student_id" in summary.columns:
            masks.append(summary["student_id"].fillna("").astype(str).str.strip().eq(""))

        if masks:
            combined = masks[0]
            for mask in masks[1:]:
                combined = combined | mask
            columns = [
                column
                for column in [
                    "student_id",
                    "student_name",
                    "last_name",
                    "first_name",
                    "chapter",
                    "latest_chapter",
                    "join_term",
                    "last_observed_org_term",
                    "latest_outcome_bucket",
                    "outcome_resolution_group",
                    "chapter_assignment_source",
                    "data_completeness_rate",
                ]
                if column in summary.columns
            ]
            review["Incomplete or unresolved students"] = summary.loc[combined, columns].head(150)

    roster = tables.get("roster_term", pd.DataFrame())
    if roster is not None and not roster.empty and {"student_id", "term_code"}.issubset(roster.columns):
        duplicate_counts = (
            roster.groupby(["student_id", "term_code"], dropna=False)
            .size()
            .reset_index(name="Roster Rows")
            .loc[lambda frame: frame["Roster Rows"].gt(1)]
            .sort_values("Roster Rows", ascending=False)
            .head(100)
        )
        if not duplicate_counts.empty:
            review["Multiple roster rows for same student/term"] = duplicate_counts

    for key, label in [
        ("unresolved_chapter_review", "Unresolved chapter review"),
        ("identity_exceptions", "Identity exceptions"),
        ("term_exceptions", "Term exceptions"),
        ("status_exceptions", "Status exceptions"),
        ("chapter_conflicts", "Chapter conflicts"),
        ("outcome_exceptions", "Outcome exceptions"),
        ("missing_evidence_cases", "Missing evidence cases"),
    ]:
        frame = tables.get(key)
        if frame is not None and not frame.empty:
            review[label] = frame.head(150)
    return review


def _render_manual_review_panel(bundle) -> None:
    st.subheader("Weird / incomplete records")
    st.caption("Use these as cleanup targets. They are pulled from the current canonical QA and student summary tables.")
    review_tables = _manual_correction_review_tables(bundle)
    if not review_tables:
        st.success("No weird or incomplete record tables were found in the current bundle.")
    for label, frame in review_tables.items():
        with st.expander(f"{label} ({len(frame):,})", expanded=label == "Incomplete or unresolved students"):
            st.dataframe(frame, use_container_width=True, hide_index=True)

    with st.expander("Graduation-rate display options to decide", expanded=False):
        st.write("- **Resolved-only ranking:** rank chapters only on students with confirmed final outcomes. Cleanest for comparing chapters, but hides unresolved burden unless shown beside it.")
        st.write("- **Conservative full-cohort rate:** keep unknowns in the denominator. Most cautious and honest, but can punish chapters/years with bad historical records.")
        st.write("- **Two-column default:** show resolved-only graduation rate as the main ranking and full-cohort conservative rate plus unknown share beside it. This is my recommendation for showing Greek Life fairly without hiding data quality.")


def _open_local_text_file(path) -> str:
    try:
        os.startfile(path)  # type: ignore[attr-defined]
        return ""
    except Exception as exc:
        return str(exc)


def _requested_app_mode() -> str:
    env_mode = os.environ.get("FSL_APP_MODE", "")
    query_mode = ""
    try:
        query_value = st.query_params.get("mode", "")
        if isinstance(query_value, list):
            query_mode = str(query_value[0]) if query_value else ""
        else:
            query_mode = str(query_value)
    except Exception:
        query_mode = ""
    return (env_mode or query_mode).strip().lower()


def _manual_workspace_summary() -> pd.DataFrame:
    workspace = prepare_manual_corrections_workspace()
    return pd.DataFrame(
        [
            {"Item": "Graduation Evidence CSV", "Path": str(workspace["graduation_evidence_path"])},
            {"Item": "Outcome Overrides CSV", "Path": str(workspace["outcome_overrides_path"])},
            {"Item": "Roster Exclusions CSV", "Path": str(workspace["roster_exclusions_path"])},
            {"Item": "Legacy Correction CSV", "Path": str(workspace["corrections_path"])},
            {"Item": "Manual Adjustments CSV", "Path": str(workspace.get("adjustments_path", MANUAL_ADJUSTMENTS_PATH))},
            {"Item": "Checked Queue Actions CSV", "Path": str(workspace.get("review_actions_path", MANUAL_REVIEW_ACTIONS_PATH))},
            {"Item": "Legacy Assignment Queue CSV", "Path": str(workspace["review_queue_path"])},
            {"Item": "Transcript Paste-In Folder", "Path": str(workspace["transcript_folder"])},
            {"Item": "Canonical Latest Folder", "Path": str(MANUAL_ROSTER_CORRECTIONS_PATH.parent.parent / "output" / "canonical" / "latest")},
        ]
    )


def _render_manual_corrections_editor(bundle) -> None:
    workspace = prepare_manual_corrections_workspace()
    st.title("Odd Record Decisions")
    st.caption(
        "The queue is regenerated from canonical data. Only decisions you explicitly save are persisted in small registry files "
        "and reapplied whenever the canonical pipeline is rebuilt."
    )

    corrections = load_manual_roster_corrections()
    graduation_evidence = load_graduation_evidence()
    outcome_overrides = load_outcome_overrides()
    roster_exclusions = load_roster_exclusions()
    decision_keys = _registry_identity_set(corrections, graduation_evidence, outcome_overrides, roster_exclusions)
    staged_corrections = _staged_manual_corrections()
    summary = getattr(bundle, "summary", pd.DataFrame()).copy()
    saved_actions = load_manual_review_actions()
    generated_queue = pd.concat(
        [
            _build_manual_assignment_queue(summary, decision_keys),
            _canonical_review_queue_for_app(bundle, decision_keys),
        ],
        ignore_index=True,
    )
    if not generated_queue.empty:
        for column in MANUAL_REVIEW_QUEUE_COLUMNS:
            if column not in generated_queue.columns:
                generated_queue[column] = ""
        generated_queue = dedupe_manual_review_queue_by_cohort(
            generated_queue[MANUAL_REVIEW_QUEUE_COLUMNS].drop_duplicates(subset=["review_key"], keep="first")
        )
    review_queue = _merge_saved_review_queue(generated_queue, saved_actions)
    staged_keys = _manual_correction_identity_set(staged_corrections)
    if staged_keys and not review_queue.empty:
        review_queue.loc[review_queue["review_key"].isin(staged_keys), "has_manual_correction"] = "Staged"

    registry_decision_count = len(graduation_evidence) + len(outcome_overrides) + len(roster_exclusions)
    metrics = _manual_queue_metrics(review_queue, pd.DataFrame(index=range(registry_decision_count + len(corrections))))
    info_cols = st.columns(7)
    with info_cols[0]:
        st.metric("Saved decisions", f"{registry_decision_count:,}")
    with info_cols[1]:
        st.metric("Legacy corrections", f"{len(corrections):,}")
    with info_cols[2]:
        st.metric("Queue records", f"{metrics['queue_total']:,}")
    with info_cols[3]:
        st.metric("Needs review", f"{metrics['needs_review']:,}")
    with info_cols[4]:
        st.metric("In progress", f"{metrics['in_progress']:,}")
    with info_cols[5]:
        st.metric("Needs transcript", f"{metrics['needs_transcript']:,}")
    with info_cols[6]:
        st.metric("Transcript files", f"{metrics['transcripts_ready']:,}")

    with st.expander("Helper quick start", expanded=True):
        st.write("1. Search for the student by Banner ID, name, or chapter.")
        st.write("2. Check only rows you reviewed.")
        st.write("3. Use Graduated for explicit graduation proof, another status for an outcome override, or roster exclusion for bad source rows.")
        st.write("4. A matching transcript `.txt` file can still be created when you need transcript text.")
        st.write("5. Use **Download helper package** to send back the registry CSVs and transcript text files.")
        st.dataframe(_manual_workspace_summary(), use_container_width=True, hide_index=True)

        action_cols = st.columns(3)
        with action_cols[0]:
            if st.button("Open correction CSV folder", use_container_width=True):
                error = _open_local_text_file(workspace["corrections_path"].parent)
                if error:
                    st.warning(f"Could not open folder automatically: {error}")
        with action_cols[1]:
            if st.button("Open transcript folder", use_container_width=True):
                error = _open_local_text_file(workspace["transcript_folder"])
                if error:
                    st.warning(f"Could not open folder automatically: {error}")
        with action_cols[2]:
            st.download_button(
                "Download helper package",
                data=build_manual_corrections_package(),
                file_name="manual_corrections_package.zip",
                mime="application/zip",
                use_container_width=True,
            )

    st.info(
        "Work the Assignment Queue first when possible. Search is still available when you need to jump to a specific student. "
        "The visible queue is regenerated from canonical data; the saved records are the small decision registries. "
        "Use Graduated only when you have explicit graduation evidence. Use roster exclusions only when the source roster row itself should be ignored by the canonical pipeline."
    )

    with st.expander(f"Pending staged changes ({len(staged_corrections):,})", expanded=not staged_corrections.empty):
        st.caption("These changes are fast in-memory drafts. They are not applied to the canonical pipeline until you commit them to the manual corrections CSV.")
        if staged_corrections.empty:
            st.caption("No staged changes yet.")
        else:
            staged_display = staged_corrections.copy()
            staged_display.insert(0, "delete_row", False)
            edited_staged = st.data_editor(
                staged_display,
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                column_config={
                    "delete_row": st.column_config.CheckboxColumn("x", help="Check this and update staged list to drop the pending row."),
                    "exclude_from_roster_calculations": st.column_config.CheckboxColumn("Exclude From Roster Calculations"),
                },
                key="manual_staged_corrections_editor",
            )
            staged_action_cols = st.columns(4)
            with staged_action_cols[0]:
                if st.button("Update staged list", use_container_width=True):
                    _set_staged_manual_corrections(edited_staged)
                    st.success("Updated the pending staged list.")
                    st.rerun()
            with staged_action_cols[1]:
                open_transcripts_after_commit = st.checkbox("Open created transcript files", value=False, key="manual_open_transcripts_after_batch")
                if st.button("Commit staged changes", type="primary", use_container_width=True):
                    result = _commit_staged_manual_corrections(st.session_state.get("manual_helper_initials", ""))
                    created_transcripts = result["created_transcripts"]
                    open_errors = {}
                    if open_transcripts_after_commit:
                        open_errors = {path: error for path in created_transcripts if (error := _open_local_text_file(path))}
                    st.success(
                        f"Committed {result['saved_rows']:,} staged correction row(s) to {result['saved_path']}. "
                        "Rerun `py run_canonical_pipeline.py --refresh-source-cache` when you want these applied to canonical outputs."
                    )
                    if created_transcripts:
                        st.info(f"Created {len(created_transcripts):,} transcript template(s).")
                    if open_errors:
                        st.warning("Some transcript files were created but could not be opened automatically: " + "; ".join(f"{path.name}: {error}" for path, error in open_errors.items()))
                    st.rerun()
            with staged_action_cols[2]:
                if st.button("Clear staged changes", use_container_width=True):
                    _clear_staged_manual_corrections()
                    st.warning("Cleared pending staged changes.")
                    st.rerun()
            with staged_action_cols[3]:
                st.download_button(
                    "Download staged CSV",
                    data=dataframe_to_csv_bytes(staged_corrections),
                    file_name="manual_roster_corrections_staged.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

    queue_tab, correction_tab, package_tab, review_tab = st.tabs(["Assignment Queue", "Decision Registries", "Import / Export", "Weird Records"])

    with queue_tab:
        helper_initials = st.text_input("Helper initials / owner", value=st.session_state.get("manual_helper_initials", ""), key="manual_helper_initials")
        queue_filters = st.columns(4)
        with queue_filters[0]:
            status_filter = st.multiselect("Review status", options=REVIEW_STATUS_OPTIONS, default=["Needs Review", "In Progress", "Waiting on Transcript"])
        with queue_filters[1]:
            owner_options = _unique_text_options(review_queue, "assigned_to")
            owner_filter = st.multiselect("Assigned to", options=owner_options)
        with queue_filters[2]:
            transcript_filter = st.selectbox("Needs transcript", options=["All", "Yes", "No"])
        with queue_filters[3]:
            reason_search = st.text_input("Reason contains", placeholder="unknown, inferred, incomplete")

        evidence_filters = st.columns(5)
        with evidence_filters[0]:
            join_filter = st.multiselect("Join semester", options=sorted(_unique_text_options(review_queue, "join_term"), key=persistence_cohort_sort_key))
        with evidence_filters[1]:
            year_filter = st.multiselect("Academic year", options=_unique_text_options(review_queue, "academic_year"))
        with evidence_filters[2]:
            term_filter = st.multiselect("Term", options=_unique_text_options(review_queue, "term"))
        with evidence_filters[3]:
            org_filter = st.multiselect("Organization", options=_unique_text_options(review_queue, "organization"))
        with evidence_filters[4]:
            chapter_filter = st.multiselect("Chapter", options=_unique_text_options(review_queue, "chapter"))

        issue_filters = st.columns(4)
        with issue_filters[0]:
            issue_filter = st.multiselect("Issue type", options=_unique_text_options(review_queue, "issue_type"))
        with issue_filters[1]:
            outcome_filter = st.multiselect("Outcome bucket", options=_unique_text_options(review_queue, "outcome_bucket"))
        with issue_filters[2]:
            priority_filter = st.multiselect("Priority", options=_unique_text_options(review_queue, "priority"))
        with issue_filters[3]:
            queue_sort = st.selectbox("Sort", options=["Join semester", "Priority", "Status", "Chapter"], index=0)

        queue_view = review_queue.copy()
        if status_filter:
            queue_view = queue_view.loc[queue_view["review_status"].isin(status_filter)]
        if owner_filter:
            queue_view = queue_view.loc[queue_view["assigned_to"].isin(owner_filter)]
        if transcript_filter != "All":
            queue_view = queue_view.loc[queue_view["needs_transcript"].eq(transcript_filter)]
        for column, selected in [
            ("join_term", join_filter),
            ("academic_year", year_filter),
            ("term", term_filter),
            ("organization", org_filter),
            ("chapter", chapter_filter),
            ("issue_type", issue_filter),
            ("outcome_bucket", outcome_filter),
            ("priority", priority_filter),
        ]:
            if selected and column in queue_view.columns:
                queue_view = queue_view.loc[queue_view[column].isin(selected)]
        if reason_search:
            queue_view = queue_view.loc[queue_view["queue_reason"].fillna("").astype(str).str.contains(reason_search, case=False, regex=False, na=False)]
        if not queue_view.empty:
            queue_view = queue_view.copy()
            queue_view["_join_sort"] = queue_view["join_term"].map(persistence_cohort_sort_key)
            if queue_sort == "Priority":
                queue_view["_priority_sort"] = queue_view["priority"].map({"High": 0, "Medium": 1, "Monitor": 2}).fillna(9)
                queue_view = queue_view.sort_values(["_priority_sort", "_join_sort", "student_name", "student_id"], na_position="last")
            elif queue_sort == "Status":
                queue_view = queue_view.sort_values(["review_status", "_join_sort", "student_name", "student_id"], na_position="last")
            elif queue_sort == "Chapter":
                queue_view = queue_view.sort_values(["chapter", "_join_sort", "student_name", "student_id"], na_position="last")
            else:
                queue_view = queue_view.sort_values(["_join_sort", "student_name", "student_id"], na_position="last")
            queue_view = queue_view.drop(columns=["_join_sort", "_priority_sort"], errors="ignore")

        st.subheader("Editable assignment queue")
        st.caption("Check one or more rows, then click one action. The full queue is disposable; only checked rows are saved to the small action file.")
        select_all_visible = st.checkbox("Precheck all visible queue rows", value=False, key="manual_queue_precheck_visible")
        queue_editor_frame = queue_view.copy()
        queue_editor_frame.insert(0, "select_row", select_all_visible)
        edited_queue = st.data_editor(
            queue_editor_frame,
            num_rows="fixed",
            use_container_width=True,
            hide_index=True,
            height=820,
            column_config={
                "select_row": st.column_config.CheckboxColumn("Select", help="Check rows to batch-stage or batch-save the same outcome."),
                "review_status": st.column_config.SelectboxColumn("Review Status", options=REVIEW_STATUS_OPTIONS),
                "needs_transcript": st.column_config.SelectboxColumn("Needs Transcript", options=["Yes", "No"]),
                "assigned_to": st.column_config.TextColumn("Assigned To"),
                "review_notes": st.column_config.TextColumn("Review Notes", width="large"),
            },
            key="manual_review_queue_editor",
        )

        selected_mask = _truthy_mask(edited_queue["select_row"])
        selected_queue_rows = edited_queue.loc[selected_mask].drop(columns=["select_row"], errors="ignore").copy()
        selected_review_keys = selected_queue_rows["review_key"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().tolist()
        st.caption(f"Selected rows: {len(selected_review_keys):,} / visible rows: {len(queue_view):,}")
        if not selected_queue_rows.empty:
            first_selected = selected_queue_rows.iloc[0]
            selected_student_id = str(first_selected.get("student_id", "") or "").strip()
            selected_normalized_id = str(first_selected.get("normalized_student_id", "") or selected_student_id).strip()
            evidence_tables = getattr(bundle, "tables", {})
            with st.expander("Evidence for first selected row", expanded=False):
                for table_key, label in [
                    ("student_source_appearances", "Source appearances"),
                    ("student_longitudinal_tracking", "Longitudinal tracking"),
                    ("graduation_status_audit", "Graduation audit"),
                ]:
                    evidence = evidence_tables.get(table_key, pd.DataFrame())
                    if evidence is None or evidence.empty:
                        continue
                    evidence_view = evidence.copy()
                    if "normalized_student_id" in evidence_view.columns and selected_normalized_id:
                        evidence_view = evidence_view.loc[evidence_view["normalized_student_id"].fillna("").astype(str).str.strip().eq(selected_normalized_id)]
                    elif "student_id" in evidence_view.columns and selected_student_id:
                        evidence_view = evidence_view.loc[evidence_view["student_id"].fillna("").astype(str).str.strip().eq(selected_student_id)]
                    if not evidence_view.empty:
                        st.caption(label)
                        st.dataframe(evidence_view.head(200), use_container_width=True, hide_index=True)

        def _apply_batch_status(final_status: str) -> None:
            if final_status == "Graduated":
                registry_rows = _graduation_evidence_from_queue_rows(selected_queue_rows, entered_by=helper_initials)
                result = append_graduation_evidence(registry_rows)
                saved_label = "graduation evidence"
            else:
                registry_rows = _outcome_overrides_from_queue_rows(selected_queue_rows, final_status=final_status, entered_by=helper_initials)
                result = append_outcome_overrides(registry_rows)
                saved_label = "outcome override"
            if registry_rows.empty:
                st.warning("No selected queue rows had a valid A# and enough information to save.")
                return
            action_result = _append_manual_review_actions(
                selected_queue_rows,
                assigned_to=helper_initials,
                review_status="Corrected",
                note=f"Saved as {final_status}.",
                has_manual_correction="Yes",
            )
            _warn_if_manual_action_fallback(action_result)
            st.success(f"Saved {result['appended_rows']:,} {saved_label} row(s) to {result['path']}. Rerun the canonical pipeline to apply them.")
            st.rerun()

        button_disabled = len(selected_review_keys) == 0
        st.caption("Batch outcome buttons")
        quick_statuses = ["Early Alumni", "Inactive", "Dropped", "Resigned", "Revoked", "Suspended", "Transfer", "Unknown", "Graduated"]
        quick_status_cols = st.columns(len(quick_statuses))
        for index, status in enumerate(quick_statuses):
            with quick_status_cols[index]:
                if st.button(status, use_container_width=True, key=f"batch_status_{status.lower()}", disabled=button_disabled):
                    _apply_batch_status(status)

        batch_action_cols = st.columns(3)
        with batch_action_cols[0]:
            if st.button("Exclude selected from roster calcs", use_container_width=True, disabled=button_disabled):
                exclusions = _roster_exclusions_from_queue_rows(selected_queue_rows, entered_by=helper_initials)
                if exclusions.empty:
                    st.warning("No selected queue rows had a valid A# and enough scope to save as roster exclusions.")
                else:
                    result = append_roster_exclusions(exclusions)
                    action_result = _append_manual_review_actions(
                        selected_queue_rows,
                        assigned_to=helper_initials,
                        review_status="Corrected",
                        note="Saved as roster exclusion.",
                        has_manual_correction="Yes",
                    )
                    _warn_if_manual_action_fallback(action_result)
                    st.success(f"Saved {result['appended_rows']:,} roster exclusion row(s) to {result['path']}. Rerun the canonical pipeline to apply them.")
                st.rerun()
        with batch_action_cols[1]:
            if st.button("Create transcript files for selected", use_container_width=True, disabled=button_disabled):
                correction_rows = _manual_corrections_from_queue_rows(selected_queue_rows)
                created = ensure_manual_transcript_files(correction_rows)
                action_result = _append_manual_review_actions(
                    selected_queue_rows,
                    assigned_to=helper_initials,
                    review_status="Waiting on Transcript",
                    needs_transcript="Yes",
                    note="Transcript template requested from assignment queue.",
                )
                _warn_if_manual_action_fallback(action_result)
                st.success(f"Created {len(created):,} missing transcript template(s) for selected row(s). Existing files were not overwritten.")
                st.rerun()
        with batch_action_cols[2]:
            if st.button("Mark selected skipped / no change", use_container_width=True, disabled=button_disabled):
                result = _append_manual_review_actions(
                    selected_queue_rows,
                    assigned_to=helper_initials,
                    review_status="Skipped / No Change",
                    note="Marked skipped / no change from assignment queue.",
                )
                _warn_if_manual_action_fallback(result)
                st.success(f"Saved {len(selected_queue_rows):,} skipped / no-change action row(s) to {result['path']}.")
                st.rerun()

        if st.button("Save queue updates", use_container_width=True):
            if selected_queue_rows.empty:
                st.warning("Check the row(s) you want to save first. Unchecked queue edits stay temporary.")
            else:
                result = _append_manual_review_actions(selected_queue_rows, assigned_to=helper_initials)
                _warn_if_manual_action_fallback(result)
                st.success(f"Saved {len(selected_queue_rows):,} checked queue action row(s) to {result['path']}.")
                st.rerun()

    with correction_tab:
        st.subheader("Saved decision registries")
        registry_cols = st.columns(3)
        with registry_cols[0]:
            st.metric("Graduation evidence", f"{len(graduation_evidence):,}")
            st.dataframe(graduation_evidence, use_container_width=True, hide_index=True, height=220)
        with registry_cols[1]:
            st.metric("Outcome overrides", f"{len(outcome_overrides):,}")
            st.dataframe(outcome_overrides, use_container_width=True, hide_index=True, height=220)
        with registry_cols[2]:
            st.metric("Roster exclusions", f"{len(roster_exclusions):,}")
            st.dataframe(roster_exclusions, use_container_width=True, hide_index=True, height=220)

        editor_frame = corrections.copy()
        for column in MANUAL_ROSTER_CORRECTION_COLUMNS:
            if column not in editor_frame.columns:
                editor_frame[column] = ""
        editor_frame = editor_frame[MANUAL_ROSTER_CORRECTION_COLUMNS]
        with st.expander("Graduated alumni batch", expanded=False):
            batch_defaults = st.columns(2)
            with batch_defaults[0]:
                alumni_default_org = st.text_input("Default organization", key="manual_alumni_default_org")
            with batch_defaults[1]:
                alumni_default_grad_term = st.text_input("Default graduation term", key="manual_alumni_default_grad_term")
            alumni_input = st.data_editor(
                pd.DataFrame(
                    columns=[
                        "student_id",
                        "student_name",
                        "last_name",
                        "first_name",
                        "organization_name",
                        "initiation_date",
                        "final_status_term",
                        "organization_join_term",
                    ]
                ),
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                key="graduated_alumni_batch_editor",
                column_config={
                    "student_id": st.column_config.TextColumn("Student ID"),
                    "student_name": st.column_config.TextColumn("Student Name"),
                    "last_name": st.column_config.TextColumn("Last Name"),
                    "first_name": st.column_config.TextColumn("First Name"),
                    "organization_name": st.column_config.TextColumn("Organization"),
                    "initiation_date": st.column_config.TextColumn("Initiation Date"),
                    "final_status_term": st.column_config.TextColumn("Graduation Term"),
                    "organization_join_term": st.column_config.TextColumn("Organization Join Term"),
                },
            )
            alumni_corrections = graduated_alumni_rows_to_manual_corrections(
                alumni_input,
                default_organization=alumni_default_org,
                default_graduation_term=alumni_default_grad_term,
                summary=summary,
            )
            if not alumni_corrections.empty:
                st.dataframe(alumni_corrections, use_container_width=True, hide_index=True)
            elif not alumni_input.dropna(how="all").empty:
                st.warning(
                    "No rows could be converted. Rows without a valid A# must uniquely match an existing student by exact name and organization."
                )
            alumni_action_cols = st.columns(2)
            with alumni_action_cols[0]:
                if st.button("Save alumni evidence", use_container_width=True, disabled=alumni_corrections.empty):
                    evidence_rows = _graduation_evidence_from_corrections(
                        alumni_corrections,
                        entered_by=st.session_state.get("manual_helper_initials", ""),
                    )
                    result = append_graduation_evidence(evidence_rows)
                    st.success(f"Saved {result['appended_rows']:,} graduated alumni evidence row(s) to {result['path']}.")
                    st.rerun()
            with alumni_action_cols[1]:
                evidence_preview = _graduation_evidence_from_corrections(
                    alumni_corrections,
                    entered_by=st.session_state.get("manual_helper_initials", ""),
                )
                st.download_button(
                    "Download evidence preview",
                    data=dataframe_to_csv_bytes(evidence_preview),
                    file_name="graduation_evidence_preview.csv",
                    mime="text/csv",
                    use_container_width=True,
                    disabled=evidence_preview.empty,
                )
        st.divider()
        st.subheader("Legacy correction sheet")
        st.caption("This remains for backward compatibility. New queue decisions should use the registry buttons above.")
        search = st.text_input("Find a student to edit", placeholder="Type Banner ID, first name, last name, or chapter")
        if search and not summary.empty:
            haystack_columns = [
                column
                for column in ["student_id", "student_name", "first_name", "last_name", "chapter", "latest_chapter", "join_term"]
                if column in summary.columns
            ]
            if haystack_columns:
                haystack = summary[haystack_columns].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
                matches = summary.loc[haystack.str.contains(search.lower(), regex=False, na=False)].copy()
                if not matches.empty:
                    draft_row = pd.DataFrame([_manual_correction_row_from_summary(matches.iloc[0])])
                    editor_frame = pd.concat([draft_row, editor_frame], ignore_index=True)
                    display_columns = [
                        column
                        for column in [
                            "student_id",
                            "student_name",
                            "first_name",
                            "last_name",
                            "chapter",
                            "current_active_chapter",
                            "join_term",
                            "last_observed_org_term",
                            "latest_outcome_bucket",
                            "outcome_resolution_group",
                        ]
                        if column in matches.columns
                    ]
                    st.caption("Best matches. The first row below was copied into the top correction row.")
                    st.dataframe(matches[display_columns].head(10), use_container_width=True, hide_index=True)
                else:
                    st.caption("No matching student was found in the current canonical summary.")

        with st.form("manual_roster_corrections_form"):
            editor_display = editor_frame.copy()
            editor_display.insert(0, "delete_row", "")
            if "exclude_from_roster_calculations" in editor_display.columns:
                editor_display["exclude_from_roster_calculations"] = _truthy_mask(editor_display["exclude_from_roster_calculations"])
            edited = st.data_editor(
                editor_display,
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                column_config={
                    "delete_row": st.column_config.CheckboxColumn("x", help="Check this and save to remove this correction row."),
                    "student_id": st.column_config.TextColumn("Student ID"),
                    "last_name": st.column_config.TextColumn("Last Name"),
                    "first_name": st.column_config.TextColumn("First Name"),
                    "organization_join_term": st.column_config.TextColumn("Organization Join Term"),
                    "organization_name": st.column_config.TextColumn("Organization Name"),
                    "leaving_organization_term": st.column_config.TextColumn("Leaving Organization Term"),
                    "final_status_term": st.column_config.TextColumn("Final Status Term"),
                    "final_status": st.column_config.TextColumn("Final Status"),
                    "exclude_from_roster_calculations": st.column_config.CheckboxColumn(
                        "Exclude From Roster Calculations",
                        help=(
                            "Persistently removes matching raw roster rows from canonical roster-based calculations. "
                            "If Organization Join Term and Final/Leaving Term are supplied, that inclusive term range is removed; "
                            "if no terms are supplied, all matching rows for the student/org are removed."
                        ),
                    ),
                },
            )
            form_action_cols = st.columns(2)
            with form_action_cols[0]:
                staged = st.form_submit_button("Stage new/changed rows", use_container_width=True)
            with form_action_cols[1]:
                saved = st.form_submit_button("Save full sheet to CSV now", use_container_width=True)

        if staged:
            staged_rows = _new_or_changed_manual_rows(edited, corrections)
            staged_rows = _stage_manual_corrections(staged_rows)
            if staged_rows.empty:
                st.info("No new or changed correction rows were found to stage.")
            else:
                st.success(f"Staged {len(staged_rows):,} new or changed correction row(s). Commit staged changes when you are ready.")
            st.rerun()

        if saved:
            saved_path = save_manual_roster_corrections(edited)
            saved_corrections = load_manual_roster_corrections()
            _append_manual_adjustments_from_corrections(
                saved_corrections,
                reviewer=st.session_state.get("manual_helper_initials", ""),
                reason="Manual correction sheet saved.",
            )
            created_transcripts = ensure_manual_transcript_files(saved_corrections)
            open_errors = {path: error for path in created_transcripts if (error := _open_local_text_file(path))}
            st.success(f"Saved corrections to {saved_path}. Rerun `py run_canonical_pipeline.py --refresh-source-cache` when you want these applied to the canonical outputs.")
            if created_transcripts:
                st.info(
                    f"Created {len(created_transcripts):,} transcript text file(s) in {MANUAL_TRANSCRIPTS_PATH}. "
                    "New files were opened when the local operating system allowed it."
                )
                st.dataframe(pd.DataFrame({"Transcript File": [str(path) for path in created_transcripts]}), use_container_width=True, hide_index=True)
            if open_errors:
                st.warning("Some transcript files were created but could not be opened automatically: " + "; ".join(f"{path.name}: {error}" for path, error in open_errors.items()))
            st.download_button(
                "Download updated helper package",
                data=build_manual_corrections_package(),
                file_name="manual_corrections_package.zip",
                mime="application/zip",
                use_container_width=True,
                key="download_updated_manual_package",
            )

        if not corrections.empty:
            st.download_button(
                "Download saved corrections CSV",
                data=dataframe_to_csv_bytes(corrections),
                file_name="manual_roster_corrections.csv",
                mime="text/csv",
            )

    with package_tab:
        st.subheader("Import returned helper package")
        uploaded_package = st.file_uploader("Upload manual_corrections_package.zip", type=["zip"])
        if uploaded_package is not None and st.button("Merge uploaded package", use_container_width=True):
            result = import_manual_corrections_package(uploaded_package.getvalue())
            st.success(
                f"Merged {result['incoming_rows']:,} incoming correction row(s). "
                f"Master correction file now has {result['merged_rows']:,} row(s). "
                f"Imported {result['transcript_imported']:,} transcript file(s)."
            )
            conflicts = result.get("conflicts", pd.DataFrame())
            if isinstance(conflicts, pd.DataFrame) and not conflicts.empty:
                st.warning("Potential conflicting corrections were found. Review these before applying the next canonical rebuild.")
                st.dataframe(conflicts, use_container_width=True, hide_index=True)

        st.subheader("Duplicate / conflict check")
        conflict_frame = find_manual_correction_conflicts(load_manual_roster_corrections())
        if conflict_frame.empty:
            st.success("No conflicting manual correction rows were found.")
        else:
            st.warning("Multiple different correction rows exist for the same student/name.")
            st.dataframe(conflict_frame, use_container_width=True, hide_index=True)
        st.download_button(
            "Download current helper package",
            data=build_manual_corrections_package(),
            file_name="manual_corrections_package.zip",
            mime="application/zip",
            use_container_width=True,
        )

    with review_tab:
        _render_manual_review_panel(bundle)


def _analysis_summary_for_metric(summary: pd.DataFrame, metric: MetricDefinition) -> pd.DataFrame:
    if summary.empty or metric.key != CURRENT_ACTIVE_METRIC_KEY or "current_active_flag" not in summary.columns:
        return summary
    result = summary.copy()
    for base_column, override_column in CURRENT_ACTIVE_DIMENSION_OVERRIDES.items():
        if override_column in result.columns:
            result[base_column] = result[override_column]
    return result


def _metric_frame_for_metric(summary: pd.DataFrame, metric: MetricDefinition) -> pd.DataFrame:
    if summary.empty or metric.key != CURRENT_ACTIVE_METRIC_KEY or "current_active_flag" not in summary.columns:
        return summary
    return summary.loc[_truthy_mask(summary["current_active_flag"])].copy()


def _reset_state_for_dataset(version_key: str, metrics: List[MetricDefinition], dimension_map: Dict[str, str], summary: pd.DataFrame, longitudinal: pd.DataFrame, metadata: Dict[str, object]) -> None:
    if st.session_state.get("loaded_dataset_key") == version_key:
        return

    st.session_state["loaded_dataset_key"] = version_key
    st.session_state["metric_key"] = metrics[0].key if metrics else ""
    first_dimension = next(iter(dimension_map.keys()), "chapter")
    st.session_state["group_field"] = first_dimension
    st.session_state["compare_field"] = "chapter" if "chapter" in dimension_map else first_dimension
    st.session_state["compare_values"] = []
    st.session_state["control_field"] = "None"
    st.session_state["population"] = "FSL Only"
    st.session_state["outcome_population_view"] = ALL_STUDENTS_LABEL
    st.session_state["min_n"] = 5

    numeric_join_years = pd.to_numeric(summary.get("join_year", pd.Series(dtype=float)), errors="coerce").dropna()
    join_min = int(numeric_join_years.min()) if not numeric_join_years.empty else 2010
    join_max = int(numeric_join_years.max()) if not numeric_join_years.empty else datetime.now().year
    st.session_state["join_year_range"] = (join_min, join_max)

    numeric_grad_years = pd.to_numeric(summary.get("graduation_year", pd.Series(dtype=float)), errors="coerce").dropna()
    grad_min = int(numeric_grad_years.min()) if not numeric_grad_years.empty else join_min
    grad_max = int(numeric_grad_years.max()) if not numeric_grad_years.empty else join_max
    st.session_state["graduation_year_range"] = (grad_min, grad_max)

    numeric_observed_years = pd.to_numeric(longitudinal.get("observed_year", pd.Series(dtype=float)), errors="coerce").dropna()
    obs_min = int(numeric_observed_years.min()) if not numeric_observed_years.empty else join_min
    obs_max = int(numeric_observed_years.max()) if not numeric_observed_years.empty else join_max
    st.session_state["observed_year_range"] = (obs_min, obs_max)

    for key in FILTER_LIST_STATE_KEYS:
        st.session_state[key] = []

    if not metadata.get("available_campus_baseline"):
        st.session_state["population"] = "FSL Only"


def _collect_filters() -> Dict[str, object]:
    return {
        "population": st.session_state.get("population", "FSL Only"),
        "join_year_range": st.session_state.get("join_year_range"),
        "graduation_year_range": st.session_state.get("graduation_year_range"),
        "observed_year_range": st.session_state.get("observed_year_range"),
        **{key: st.session_state.get(key, []) for key in FILTER_LIST_STATE_KEYS},
    }


def _apply_preset(name: str) -> None:
    payload = load_preset(name)
    for key, value in payload.get("filters", {}).items():
        st.session_state[key] = value
    for key in ["metric_key", "group_field", "compare_field", "compare_values", "control_field", "outcome_population_view"]:
        if key in payload:
            st.session_state[key] = payload[key]


def _save_chart_downloads(figure, key_prefix: str) -> None:
    col1, col2 = st.columns(2)
    with col1:
        try:
            st.download_button(
                "Download chart PNG",
                data=figure_to_png_bytes(figure),
                file_name=f"{safe_slug(key_prefix)}.png",
                mime="image/png",
                key=f"{key_prefix}_png",
            )
        except Exception:
            st.caption("PNG export requires `kaleido` in the local environment.")
    with col2:
        st.download_button(
            "Download chart HTML",
            data=figure_to_html_bytes(figure),
            file_name=f"{safe_slug(key_prefix)}.html",
            mime="text/html",
            key=f"{key_prefix}_html",
        )


def _render_source_scan(statuses: List[DataSourceStatus]) -> None:
    rows = _source_status_rows(statuses)
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_source_file_status(statuses: List[DataSourceStatus]) -> None:
    rows = _source_file_status_rows(statuses)
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_data_status_panel(bundle, source_statuses: List[DataSourceStatus]) -> None:
    with st.expander("Data Status", expanded=False):
        st.write(f"**Active dataset:** {bundle.version.label}")
        st.write(f"**Startup behavior:** The app auto-loaded the highest-priority valid dataset it found in the local project folders.")
        file_rows = [
            {
                "File": status.label,
                "Path": str(status.path),
                "Required": "Yes" if status.required else "No",
                "Loaded": "Yes" if status.loaded else "No",
                "Exists": "Yes" if status.exists else "No",
                "Rows": status.row_count if status.row_count is not None else "",
                "Last Modified": status.last_modified,
                "Warning": status.warning,
            }
            for status in bundle.data_status
        ]
        if file_rows:
            st.dataframe(pd.DataFrame(file_rows), use_container_width=True, hide_index=True)
        if bundle.metadata.get("validation_warnings"):
            for warning in bundle.metadata["validation_warnings"]:
                st.warning(warning)
        st.subheader("Discovered Local Sources")
        _render_source_scan(source_statuses)
        if any(status.files for status in source_statuses):
            st.subheader("Expected Files")
            _render_source_file_status(source_statuses)


def _render_startup_failure(message: str, source_statuses: List[DataSourceStatus], detail: str = "") -> None:
    st.title("FSL Academic Outcomes Analytics")
    st.error(message)
    if detail:
        st.write(detail)
    st.subheader("Detected Local Data Sources")
    _render_source_scan(source_statuses)
    if any(status.files for status in source_statuses):
        st.subheader("Expected Files")
        _render_source_file_status(source_statuses)


def _population_transparency_frame(metric: MetricDefinition, metric_views: dict[str, object], filtered_summary: pd.DataFrame) -> pd.DataFrame:
    if metric.key == CURRENT_ACTIVE_METRIC_KEY:
        all_result = metric_views["all"]
        latest_term = filtered_summary.get("current_active_roster_term", pd.Series("", dtype="object"))
        latest_term_label = latest_term.fillna("").astype(str).str.strip().replace("", pd.NA).dropna()
        return pd.DataFrame(
            [
                {
                    "Population View": ALL_STUDENTS_LABEL,
                    "Population Definition": "Most Recent Roster Only",
                    "Metric Value": all_result["value"],
                    "Formatted Value": format_metric_value(all_result["value"], metric.format),
                    "Numerator": all_result["numerator"],
                    "Denominator": all_result["denominator"],
                    "Students Included": all_result["students"],
                    "Latest Roster Term": latest_term_label.iloc[0] if not latest_term_label.empty else "Unknown",
                    "Resolved Count": pd.NA,
                    "Still Active Excluded": pd.NA,
                    "Truly Unknown Excluded": pd.NA,
                    "Other / Unmapped Excluded": pd.NA,
                    "Excluded Total": pd.NA,
                }
            ]
        )

    population_summary = outcome_population_summary(filtered_summary)
    all_result = metric_views["all"]
    resolved_result = metric_views["resolved_only"]

    return pd.DataFrame(
        [
            {
                "Population View": ALL_STUDENTS_LABEL,
                "Population Definition": all_result.get("population_definition", FULL_POPULATION_LABEL),
                "Metric Value": all_result["value"],
                "Formatted Value": format_metric_value(all_result["value"], metric.format),
                "Numerator": all_result["numerator"],
                "Denominator": all_result["denominator"],
                "Students Included": all_result["students"],
                "Resolved Count": metric_views["resolved_n"],
                "Still Active Excluded": metric_views["still_active_n"],
                "Truly Unknown Excluded": metric_views["truly_unknown_n"],
                "Other / Unmapped Excluded": metric_views["other_unmapped_n"],
                "Excluded Total": metric_views["excluded_n"],
            },
            {
                "Population View": RESOLVED_OUTCOMES_ONLY_LABEL,
                "Population Definition": resolved_result.get("population_definition", RESOLVED_OUTCOMES_ONLY_LABEL),
                "Metric Value": resolved_result["value"],
                "Formatted Value": format_metric_value(resolved_result["value"], metric.format),
                "Numerator": resolved_result["numerator"],
                "Denominator": resolved_result["denominator"],
                "Students Included": resolved_result["students"],
                "Resolved Count": metric_views["resolved_n"],
                "Still Active Excluded": metric_views["still_active_n"],
                "Truly Unknown Excluded": metric_views["truly_unknown_n"],
                "Other / Unmapped Excluded": metric_views["other_unmapped_n"],
                "Excluded Total": metric_views["excluded_n"],
            },
        ]
    )


def _render_population_summary(metric: MetricDefinition, metric_views: dict[str, object], filtered_summary: pd.DataFrame) -> pd.DataFrame:
    if metric.key == CURRENT_ACTIVE_METRIC_KEY:
        all_result = metric_views["all"]
        latest_term = filtered_summary.get("current_active_roster_term", pd.Series("", dtype="object"))
        latest_term_code = filtered_summary.get("current_active_roster_term_code", pd.Series("", dtype="object"))
        latest_term_value = latest_term.fillna("").astype(str).str.strip().replace("", pd.NA).dropna()
        if latest_term_value.empty:
            latest_term_value = latest_term_code.fillna("").astype(str).str.strip().replace("", pd.NA).dropna()
        latest_term_text = latest_term_value.iloc[0] if not latest_term_value.empty else "Unknown"
        historical_active_count = int(_truthy_mask(filtered_summary.get("active_flag", pd.Series(False, index=filtered_summary.index))).sum())
        current_columns = st.columns(4)
        with current_columns[0]:
            st.metric("Current Active Students", format_metric_value(all_result["value"], metric.format))
        with current_columns[1]:
            st.metric("Most Recent Roster Term", latest_term_text)
        with current_columns[2]:
            st.metric("Historical Latest-Status Active", format_metric_value(historical_active_count, "integer"))
        with current_columns[3]:
            st.metric("Inflation Removed", format_metric_value(max(historical_active_count - int(all_result["value"]), 0), "integer"))
        st.caption("Current active counts use only the single most recent roster term. Historical rosters remain available for cohort and trend analysis, but they do not roll forward into this present-day count.")
        transparency = _population_transparency_frame(metric, metric_views, filtered_summary)
        st.dataframe(transparency, use_container_width=True, hide_index=True)
        return transparency

    population_summary = outcome_population_summary(filtered_summary)
    all_result = metric_views["all"]
    resolved_result = metric_views["resolved_only"]

    population_columns = st.columns(6)
    with population_columns[0]:
        st.metric(ALL_STUDENTS_LABEL, format_metric_value(population_summary["all_students"], "integer"))
    with population_columns[1]:
        st.metric(RESOLVED_OUTCOMES_ONLY_LABEL, format_metric_value(population_summary["resolved_students"], "integer"))
    with population_columns[2]:
        st.metric("Still Active", format_metric_value(population_summary["still_active_students"], "integer"))
    with population_columns[3]:
        st.metric("Truly Unknown", format_metric_value(population_summary["unknown_students"], "integer"))
    with population_columns[4]:
        st.metric("Other / Unmapped", format_metric_value(population_summary["other_unmapped_students"], "integer"))
    with population_columns[5]:
        st.metric(
            f"{metric.display_name} ({RESOLVED_OUTCOMES_ONLY_LABEL})",
            format_metric_value(resolved_result["value"], metric.format),
        )
    st.caption(
        f"Full population result: {all_result['numerator']} / {all_result['denominator']} = {format_metric_value(all_result['value'], metric.format)} | "
        f"Resolved-only result: {resolved_result['numerator']} / {resolved_result['denominator']} = {format_metric_value(resolved_result['value'], metric.format)}"
    )

    transparency = _population_transparency_frame(metric, metric_views, filtered_summary)
    st.caption(
        "Full Population keeps the entire filtered cohort in the denominator. "
        "Resolved Outcomes Only keeps the same metric formula but excludes Still Active, Truly Unknown / Unresolved, and Other / Unmapped students."
    )
    st.dataframe(transparency, use_container_width=True, hide_index=True)
    return transparency


def _truthy_mask(series: pd.Series) -> pd.Series:
    lowered = series.fillna("").astype(str).str.strip().str.lower()
    return lowered.eq("true") | lowered.eq("yes") | lowered.eq("1")


def _audit_tables(summary: pd.DataFrame, bundle) -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    if summary.empty:
        return tables

    def _count_table(column: str, label: str) -> None:
        if column not in summary.columns:
            return
        counts = (
            summary[column]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace("", "Unknown")
            .value_counts(dropna=False)
            .rename_axis(label)
            .reset_index(name="Student Count")
        )
        tables[label] = counts

    _count_table("latest_outcome_bucket", "Raw Outcome Bucket")
    _count_table("latest_roster_status_bucket", "Raw Roster Status")
    _count_table("outcome_resolution_group", "Standardized Outcome Group")
    _count_table("chapter_assignment_source", "Chapter Assignment Source")

    summary_breakdown = outcome_population_summary(summary)
    tables["Outcome Population Audit"] = pd.DataFrame(
        [
            {"Measure": "All Students", "Student Count": summary_breakdown["all_students"]},
            {"Measure": "Resolved Outcomes", "Student Count": summary_breakdown["resolved_students"]},
            {"Measure": "Graduated", "Student Count": summary_breakdown["graduated_students"]},
            {"Measure": "Resolved Non-Graduate Exit", "Student Count": summary_breakdown["known_non_graduate_exit_students"]},
            {"Measure": "Still Active", "Student Count": summary_breakdown["still_active_students"]},
            {"Measure": "Truly Unknown / Unresolved", "Student Count": summary_breakdown["unknown_students"]},
            {"Measure": "Other / Unmapped", "Student Count": summary_breakdown["other_unmapped_students"]},
            {"Measure": "Excluded From Resolved-Only", "Student Count": summary_breakdown["excluded_students"]},
        ]
    )

    chapter_unresolved = pd.DataFrame(
        [
            {
                "Measure": "Rows with unresolved chapter assignment",
                "Student Count": int(
                    (
                        summary.get("chapter_assignment_source", pd.Series("", index=summary.index, dtype="object"))
                        .fillna("")
                        .astype(str)
                        .str.strip()
                        .eq("unresolved")
                    ).sum()
                ),
            },
            {
                "Measure": "Students reclassified by standardized taxonomy",
                "Student Count": int(
                    (
                        summary.get("latest_outcome_bucket", pd.Series("", index=summary.index, dtype="object"))
                        .fillna("")
                        .astype(str)
                        .str.strip()
                        .replace("", "Unknown")
                        .ne(
                            summary.get("outcome_resolution_group", pd.Series("", index=summary.index, dtype="object"))
                            .fillna("")
                            .astype(str)
                            .str.strip()
                        )
                    ).sum()
                ),
            },
            {
                "Measure": "Roster disappeared / unknown students",
                "Student Count": int(
                    summary.get("roster_disappeared_unknown_flag", pd.Series("", index=summary.index, dtype="object"))
                    .fillna("")
                    .astype(str)
                    .str.strip()
                    .eq("Yes")
                    .sum()
                ),
            },
        ]
    )
    tables["Classification Audit"] = chapter_unresolved
    graduation_audit = getattr(bundle, "tables", {}).get("graduation_status_audit")
    if graduation_audit is not None and not graduation_audit.empty:
        tables["Graduation Evidence Audit"] = graduation_audit
    transcript_audit = getattr(bundle, "tables", {}).get("transcript_parse_audit")
    if transcript_audit is not None and not transcript_audit.empty:
        tables["Transcript Text Audit"] = transcript_audit
    transcript_issues = getattr(bundle, "tables", {}).get("transcript_parse_issues")
    if transcript_issues is not None and not transcript_issues.empty:
        tables["Transcript Text Issues"] = transcript_issues

    roster_term = getattr(bundle, "tables", {}).get("roster_term")
    full_summary = getattr(bundle, "summary", summary)
    if roster_term is not None and not roster_term.empty and "current_active_flag" in full_summary.columns:
        roster = roster_term.copy()
        roster["term_code"] = roster["term_code"].fillna("").astype(str).str.strip()
        roster = roster.loc[roster["term_code"].ne("")].copy()
        if not roster.empty:
            roster["_term_sort"] = roster["term_code"].map(lambda value: parse_term_label(value)["sort_value"])
            latest_term_sort = roster["_term_sort"].max()
            latest_roster = roster.loc[roster["_term_sort"].eq(latest_term_sort)].copy()
            latest_term_code = latest_roster["term_code"].iloc[0] if not latest_roster.empty else ""
            latest_term_label = latest_roster["term_label"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna()
            latest_term_text = latest_term_label.iloc[0] if not latest_term_label.empty else str(latest_term_code)
            latest_active = latest_roster.loc[
                latest_roster["org_status_bucket"].fillna("").astype(str).isin(["Active", "New Member"])
            ].copy()
            current_active_students = int(
                full_summary.loc[_truthy_mask(full_summary["current_active_flag"]), "student_id"]
                .fillna("")
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .nunique()
            )
            historical_active_students = int(
                full_summary.loc[_truthy_mask(full_summary.get("active_flag", pd.Series(False, index=full_summary.index))), "student_id"]
                .fillna("")
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .nunique()
            )
            tables["Current Active Audit"] = pd.DataFrame(
                [
                    {"Measure": "Most recent roster term", "Value": latest_term_text, "Notes": "Authoritative source for present-day active membership."},
                    {"Measure": "Rows in most recent roster term", "Value": int(len(latest_roster)), "Notes": "All roster rows in the selected latest term after canonical conflict resolution."},
                    {"Measure": "Unique students in most recent roster term", "Value": int(latest_roster["student_id"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()), "Notes": ""},
                    {"Measure": "Current active students (latest roster only)", "Value": current_active_students, "Notes": "Used by the current active metric and chapter current-headcount views."},
                    {"Measure": "Historical latest-status active students", "Value": historical_active_students, "Notes": "Broader historical count kept only for historical/outcome context."},
                    {"Measure": "Inflation difference removed", "Value": max(historical_active_students - current_active_students, 0), "Notes": "Difference between historical latest-status actives and latest-roster-only actives."},
                ]
            )
            source_breakdown = (
                latest_roster.groupby("source_file", dropna=False)
                .agg(
                    **{
                        "Roster Rows": ("student_id", "size"),
                        "Unique Students": ("student_id", lambda series: series.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
                    }
                )
                .reset_index()
                .rename(columns={"source_file": "Source File"})
            )
            if not latest_active.empty:
                active_by_source = (
                    latest_active.groupby("source_file", dropna=False)["student_id"]
                    .apply(lambda series: series.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique())
                    .rename("Current Active Students")
                    .reset_index()
                    .rename(columns={"source_file": "Source File"})
                )
                source_breakdown = source_breakdown.merge(active_by_source, on="Source File", how="left")
            else:
                source_breakdown["Current Active Students"] = 0
            tables["Current Active Source Files"] = source_breakdown.fillna({"Current Active Students": 0})
            chapter_breakdown = (
                full_summary.loc[_truthy_mask(full_summary["current_active_flag"])]
                .groupby("current_active_chapter", dropna=False)["student_id"]
                .apply(lambda series: series.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique())
                .reset_index(name="Current Active Students")
                .rename(columns={"current_active_chapter": "Chapter"})
            )
            chapter_breakdown["Chapter"] = chapter_breakdown["Chapter"].fillna("").astype(str).str.strip().replace("", "Unknown")
            tables["Current Active Chapter Counts"] = chapter_breakdown.sort_values(["Current Active Students", "Chapter"], ascending=[False, True]).reset_index(drop=True)

    for key in ["identity_exceptions", "term_exceptions", "status_exceptions", "chapter_conflicts", "outcome_exceptions", "missing_evidence_cases", "unresolved_chapter_review", "qa_checks"]:
        if key in bundle.tables:
            tables[key] = bundle.tables[key]
    return tables


def _render_advanced_analytics(
    bundle,
    source_statuses: List[DataSourceStatus],
    metric: MetricDefinition,
    metrics: List[MetricDefinition],
    settings: Dict[str, object],
    dimension_map: Dict[str, str],
    group_field: str,
    compare_field: str,
    compare_values: List[str],
    outcome_population_view: str,
    filtered_summary: pd.DataFrame,
    metric_summary: pd.DataFrame,
    filtered_longitudinal: pd.DataFrame,
    group_summary: pd.DataFrame,
    comparison_table: pd.DataFrame,
    controlled_table: pd.DataFrame,
    metric_views: dict[str, object],
) -> None:
    st.title("Fraternity / Sorority Life Academic Outcomes Analytics")
    st.caption(f"Dataset: {bundle.version.label}")
    st.caption("Prepared files are loaded automatically from the local project folders at startup.")
    if bundle.notes:
        with st.expander("Dataset notes and caveats", expanded=False):
            for note in bundle.notes:
                st.write(f"- {note}")
    _render_data_status_panel(bundle, source_statuses)

    st.info(metric_caption(metric))
    if metric.key == CURRENT_ACTIVE_METRIC_KEY:
        st.caption("Charts and rankings for this metric use only the most recent roster. Historical rosters still remain in the dataset for cohort and trend work, but they do not contribute to the present-day active count.")
    else:
        st.caption(
            f"Charts and rank ordering currently use: {outcome_population_view}. "
            "Every major table now shows the full-population and resolved-only denominators side by side where practical."
        )
    population_transparency = _render_population_summary(metric, metric_views, filtered_summary if metric.key == CURRENT_ACTIVE_METRIC_KEY else metric_summary)
    audit_tables = _audit_tables(filtered_summary, bundle)

    overview_tab, comparison_tab, ranking_tab, trend_tab, distribution_tab, audit_tab, export_tab, definition_tab = st.tabs(
        ["Overview", "Comparisons", "Rankings", "Trends", "Distributions", "Audit", "Data & Export", "Metric Definitions"]
    )

    with overview_tab:
        st.subheader("Current cohort and chapter view")
        if not group_summary.empty:
            chart = bar_chart(
                group_summary,
                x="Group",
                y="Metric Value",
                color=None,
                title=f"{metric.display_name} by {dimension_map[group_field]} ({outcome_population_view})",
                y_format=metric.format,
            )
            st.plotly_chart(chart, use_container_width=True)
            _save_chart_downloads(chart, "overview_group_metric")
            st.dataframe(group_summary, use_container_width=True, hide_index=True)
        else:
            st.warning("No groups met the current minimum-N rule for this metric.")

        st.subheader("Stakeholder notes")
        for note in stakeholder_summary(group_summary, metric, population_label=outcome_population_view):
            st.write(f"- {note}")

    with comparison_tab:
        st.subheader("Side-by-side comparisons")
        if comparison_table.empty:
            st.caption("Select one or more comparison groups in the sidebar to populate this view.")
        else:
            comparison_chart = bar_chart(
                comparison_table,
                x="Comparison Group",
                y="Metric Value",
                color=None,
                title=f"{metric.display_name} comparison ({outcome_population_view})",
                y_format=metric.format,
            )
            st.plotly_chart(comparison_chart, use_container_width=True)
            _save_chart_downloads(comparison_chart, "comparison_metric")
            st.dataframe(comparison_table, use_container_width=True, hide_index=True)

        if not controlled_table.empty:
            st.subheader("Controlled comparison")
            controlled_chart = bar_chart(
                controlled_table,
                x="Control Group",
                y="Metric Value",
                color="Comparison Group",
                title=f"{metric.display_name} within {dimension_map[st.session_state['control_field']]} ({outcome_population_view})",
                y_format=metric.format,
            )
            st.plotly_chart(controlled_chart, use_container_width=True)
            _save_chart_downloads(controlled_chart, "controlled_comparison")
            st.dataframe(controlled_table, use_container_width=True, hide_index=True)

    with ranking_tab:
        st.subheader("Ranking table")
        if group_summary.empty:
            st.caption("No groups met the current minimum-N rule for the ranking table.")
        else:
            ranking_direction = st.radio("Ordering", options=["Highest first", "Lowest first"], horizontal=True)
            sort_options = {
                "Selected metric value": "Metric Value",
                f"Resolved-only {metric.display_name}": f"Metric Value ({RESOLVED_OUTCOMES_ONLY_LABEL})",
                f"Full-population {metric.display_name}": f"Metric Value ({ALL_STUDENTS_LABEL})",
                "Resolved count": "Resolved Count",
                "Still active count": "Still Active Count",
                "Truly unknown count": "Truly Unknown Count",
                "Excluded count": "Excluded Count",
            }
            default_sort_label = f"Resolved-only {metric.display_name}" if metric.category.lower() == "graduation" else "Selected metric value"
            sort_label = st.selectbox("Sort by", options=list(sort_options.keys()), index=list(sort_options.keys()).index(default_sort_label))
            sort_column = sort_options[sort_label]
            ranked = group_summary.sort_values(sort_column, ascending=(ranking_direction == "Lowest first")).reset_index(drop=True)
            st.caption("What this tells us: graduation-focused rankings are easiest to read when resolved-only rates are separated from still-active and truly unknown students.")
            st.dataframe(ranked, use_container_width=True, hide_index=True)

        scatter_source = build_scatter_frame(
            metric_summary,
            metric,
            group_field,
            st.session_state["min_n"],
            population_label=outcome_population_view,
        )
        if not scatter_source.empty:
            scatter = scatter_chart(
                scatter_source,
                x="Population Students",
                y="Metric Value",
                size="Students",
                color=None,
                title=f"Group size versus performance ({outcome_population_view})",
                y_format=metric.format,
            )
            st.plotly_chart(scatter, use_container_width=True)
            _save_chart_downloads(scatter, "ranking_scatter")

    with trend_tab:
        st.subheader("Join cohort trend")
        summary_time_field = "join_year" if "join_year" in metric_summary.columns else "join_term"
        summary_trend = build_summary_time_series(
            metric_summary,
            metric,
            time_field=summary_time_field,
            segment_field=group_field,
            min_n=st.session_state["min_n"],
            population_label=outcome_population_view,
        )
        if not summary_trend.empty:
            join_trend_chart = line_chart(
                summary_trend,
                x="Time",
                y="Metric Value",
                color="Segment",
                title=f"{metric.display_name} over join cohorts ({outcome_population_view})",
                y_format=metric.format,
            )
            st.plotly_chart(join_trend_chart, use_container_width=True)
            _save_chart_downloads(join_trend_chart, "join_cohort_trend")
        else:
            st.caption("No join-cohort trend data is available for this metric after the current filters.")

        st.subheader("Observed term trend")
        observed_measure = st.selectbox(
            "Observed term measure",
            options=["Headcount", "Average Term GPA", "Average Cumulative GPA", "Average Passed Hours", "Average Cumulative Hours"],
        )
        observed_trend = build_observed_term_series(
            filtered_longitudinal,
            observed_measure,
            group_field,
            summary=metric_summary,
            population_label=outcome_population_view,
        )
        if not observed_trend.empty:
            observed_chart = line_chart(
                observed_trend,
                x="Observed Term",
                y="Metric Value",
                color="Segment",
                title=f"{observed_measure} over observed terms ({outcome_population_view})",
            )
            st.plotly_chart(observed_chart, use_container_width=True)
            _save_chart_downloads(observed_chart, "observed_term_trend")
        else:
            st.caption("Observed-term trends require longitudinal data in the selected bundle.")

    with distribution_tab:
        st.subheader("Category distributions")
        distribution_options = [
            column
            for column in [
                "status_group",
                "outcome_resolution_group",
                "first_academic_standing_bucket",
                "active_membership_group",
                "pell_group",
                "transfer_group",
                "estimated_join_stage",
                "chapter_size_band",
            ]
            if column in metric_summary.columns
        ]
        if distribution_options:
            distribution_field = st.selectbox(
                "Distribution field",
                options=distribution_options,
                format_func=lambda key: key.replace("_", " ").title(),
            )
            distribution_table = build_distribution_table(
                metric_summary,
                group_field,
                distribution_field,
                st.session_state["min_n"],
                population_label=outcome_population_view,
            )
            if not distribution_table.empty:
                distribution_chart = stacked_bar_chart(
                    distribution_table,
                    x="Group",
                    y="Share",
                    color="Category",
                    title=f"{distribution_field.replace('_', ' ').title()} by {dimension_map[group_field]} ({outcome_population_view})",
                )
                st.plotly_chart(distribution_chart, use_container_width=True)
                _save_chart_downloads(distribution_chart, "distribution_chart")
                st.dataframe(distribution_table, use_container_width=True, hide_index=True)
            else:
                st.caption("No distribution data is available for the current filters.")
        else:
            st.caption("No categorical distribution fields are available in the current filtered dataset.")

        numeric_options = [
            column
            for column in [
                "average_term_gpa",
                "average_cumulative_gpa",
                "total_cumulative_hours",
                "entry_cumulative_hours",
                "estimated_pre_org_hours_txst",
                "first_year_passed_hours",
            ]
            if column in metric_summary.columns and pd.to_numeric(metric_summary[column], errors="coerce").dropna().shape[0] > 0
        ]
        if numeric_options:
            numeric_field = st.selectbox(
                "Numeric field",
                options=numeric_options,
                format_func=lambda key: key.replace("_", " ").title(),
            )
            numeric_frame = metric_summary if outcome_population_view == ALL_STUDENTS_LABEL else metric_summary.loc[
                metric_summary["resolved_outcomes_only_flag"].fillna(False)
            ].copy()
            if numeric_frame.empty:
                st.caption("No numeric distribution data is available for the selected outcome population view.")
            else:
                hist_chart = histogram(
                    numeric_frame,
                    x=numeric_field,
                    color=None,
                    title=f"Distribution of {numeric_field.replace('_', ' ').title()} ({outcome_population_view})",
                )
                box_chart = box_plot(
                    numeric_frame,
                    x=group_field,
                    y=numeric_field,
                    color=None,
                    title=f"{numeric_field.replace('_', ' ').title()} by {dimension_map[group_field]} ({outcome_population_view})",
                )
                st.plotly_chart(hist_chart, use_container_width=True)
                _save_chart_downloads(hist_chart, "numeric_histogram")
                st.plotly_chart(box_chart, use_container_width=True)
                _save_chart_downloads(box_chart, "numeric_boxplot")
        else:
            st.caption("No numeric distribution fields are available in the current filtered dataset.")

    with audit_tab:
        st.subheader("Data quality and denominator audit")
        st.caption("How to read this: these tables separate resolved outcomes, still-active students, and truly unknown students so denominator changes stay visible.")
        for label, frame in audit_tables.items():
            if frame is None or frame.empty:
                continue
            st.markdown(f"**{label}**")
            st.dataframe(frame, use_container_width=True, hide_index=True)

    with export_tab:
        with st.expander("Where the old spreadsheet reports went", expanded=False):
            st.caption("The old standalone workbook builders have been retired. Their review workflows now live here so the app and canonical pipeline use one source of truth.")
            st.dataframe(
                pd.DataFrame(
                    [
                        {"Former workbook/report": "Master roster / roster grades", "App replacement": "Filtered Students + Filtered Longitudinal export"},
                        {"Former workbook/report": "Member tenure report", "App replacement": "Overview, Trends, and Chapter Health cohort views"},
                        {"Former workbook/report": "Chapter history workbooks", "App replacement": "Chapter Health dashboard and current-active audit tables"},
                        {"Former workbook/report": "Full academic record priority list", "App replacement": "Advisor Help intervention queue"},
                        {"Former workbook/report": "Unresolved outcome year report", "App replacement": "Audit tab, Graduation Evidence Audit, and unresolved outcome exports"},
                        {"Former workbook/report": "Executive report", "App replacement": "Persistence & Graduation landing page, comparisons, rankings, and app workbook export"},
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )
        st.subheader("Filtered tables")
        export_columns = [
            column
            for column in [
                "student_id",
                "student_name",
                "chapter",
                "current_active_chapter",
                "chapter_assignment_source",
                "chapter_assignment_confidence",
                "chapter_assignment_notes",
                "chapter_group",
                "current_active_chapter_group",
                "council",
                "current_active_council",
                "org_type",
                "current_active_org_type",
                "join_term",
                "join_year",
                "status_group",
                "current_active_flag",
                "current_active_membership_group",
                "current_active_roster_term",
                "current_active_source_file",
                "current_active_source_sheet",
                "outcome_resolution_group",
                "is_resolved_outcome",
                "is_active_outcome",
                "is_unknown_outcome",
                "is_graduated",
                "is_known_non_graduate_exit",
                "resolved_outcomes_only_flag",
                "resolved_outcome_excluded_flag",
                "resolved_outcome_exclusion_reason",
                "outcome_evidence_source",
                "graduation_evidence_confirmed",
                "graduation_status_without_evidence",
                "graduation_status_corrected_flag",
                "graduation_status_correction_reason",
                "roster_disappeared_unknown_flag",
                "graduated_eventual",
                "graduated_eventual_measurable",
                "graduated_4yr",
                "graduated_4yr_measurable",
                "graduated_5yr",
                "graduated_5yr_measurable",
                "graduated_6yr",
                "graduated_6yr_measurable",
                "graduation_term",
                "graduation_year",
                "major",
                "pell_group",
                "transfer_group",
                "estimated_join_stage",
                "average_term_gpa",
                "average_cumulative_gpa",
                "total_cumulative_hours",
                "data_completeness_rate",
            ]
            if column in metric_summary.columns
        ]
        summary_export = metric_summary[export_columns].copy()
        st.dataframe(summary_export, use_container_width=True, hide_index=True)

        st.download_button(
            "Download filtered students CSV",
            data=dataframe_to_csv_bytes(summary_export),
            file_name="filtered_students.csv",
            mime="text/csv",
        )

    with definition_tab:
        st.subheader("About this metric")
        st.write(f"**Metric:** {metric.display_name}")
        st.write(f"**Internal key:** `{metric.key}`")
        st.write(f"**Category:** {metric.category}")
        st.write(f"**Logic source:** {metric.logic_source}")
        st.write(f"**Numerator:** {metric.numerator_label or metric.numerator_field or 'See notes'}")
        st.write(f"**Denominator:** {metric.denominator_label or metric.denominator_field or 'See notes'}")
        st.write(f"**Minimum sample-size guidance:** {metric.min_sample_size}")
        st.write(f"**Notes:** {metric.notes or 'None'}")
        st.write(f"**Limitations:** {metric.limitations or 'None'}")
        excluded_groups = ", ".join(settings.get("outcome_resolution", {}).get("resolved_only_excluded_groups", []))
        st.write("**Full Population view:** Uses the entire filtered student group as the comparison population.")
        st.write(
            "**Resolved Outcomes Only view:** Uses the same formula after excluding students classified as "
            "Still Active, Truly Unknown / Unresolved, or Other / Unmapped by the configured status framework."
        )
        st.write("**Interpretation note:** Resolved-only results are usually the best default for final-outcome metrics such as graduation rates, while full-population views show the broader unresolved burden.")
        st.write(f"**Resolved-only excluded groups:** {excluded_groups or 'Configured in app settings'}")

        st.subheader("Available metrics")
        metric_table = pd.DataFrame(
            [
                {
                    "Key": item.key,
                    "Display Name": item.display_name,
                    "Category": item.category,
                    "Source Table": item.source_table,
                    "Logic Source": item.logic_source,
                    "Minimum N": item.min_sample_size,
                    "Population Views": f"{ALL_STUDENTS_LABEL} + {RESOLVED_OUTCOMES_ONLY_LABEL}",
                }
                for item in metrics
            ]
        )
        st.dataframe(metric_table, use_container_width=True, hide_index=True)

        if "qa_checks" in bundle.tables:
            st.subheader("Canonical QA table")
            st.dataframe(bundle.tables["qa_checks"], use_container_width=True, hide_index=True)


def main() -> None:
    settings = load_settings()
    metric_catalog = load_metric_catalog()
    status_code_map = load_status_code_map()

    source_statuses = scan_preloaded_sources()
    versions = discover_dataset_versions()
    version = select_default_dataset(versions)

    st.sidebar.title("FSL Analytics")
    st.sidebar.caption("Persistence and graduation landing page with advanced chapter, cohort, and campus analytics behind it.")
    st.sidebar.caption("The app reads pre-positioned local project files on startup.")

    if version is None:
        _render_startup_failure(
            "No valid prepared dataset was found in the expected local project folders. "
            "Run the external prep pipeline, place the finished files in their documented folders, and relaunch the app.",
            source_statuses,
        )
        return

    st.sidebar.caption(f"Auto-loaded dataset: {version.label}")
    app_mode = _requested_app_mode()

    try:
        if app_mode in {"manual", "corrections", "manual_corrections", "helper"}:
            bundle = load_manual_corrections_bundle(
                version=version,
                metric_definitions=metric_catalog,
                settings=settings,
            )
        else:
            bundle = load_analysis_bundle(
                version=version,
                metric_definitions=metric_catalog,
                settings=settings,
                status_code_map=status_code_map,
            )
    except Exception as exc:
        _render_startup_failure(
            "A prepared dataset was found, but it could not be loaded cleanly. "
            "Check the generated files, rerun the external prep workflow if needed, and relaunch the app.",
            source_statuses,
            detail=f"**Load error:** `{exc}`",
        )
        return

    if app_mode in {"manual", "corrections", "manual_corrections", "helper"}:
        st.sidebar.success("Manual Corrections Mode")
        st.sidebar.caption("This mode skips the analytics setup and opens directly to roster cleanup.")
        st.sidebar.caption(f"Dataset: {version.label}")
        _render_manual_corrections_editor(bundle)
        _render_data_status_panel(bundle, source_statuses)
        return

    metrics = available_metrics(bundle.metric_definitions, bundle.summary, bundle.longitudinal)
    if not metrics:
        st.title("FSL Academic Outcomes Analytics")
        st.error("No metrics were available for the selected dataset.")
        return

    dimension_map = available_dimensions(bundle.summary)
    if not dimension_map:
        st.title("FSL Academic Outcomes Analytics")
        st.error("No grouping dimensions were available for the selected dataset.")
        return
    _reset_state_for_dataset(version.key, metrics, dimension_map, bundle.summary, bundle.longitudinal, bundle.metadata)

    with st.sidebar.expander("Presets", expanded=False):
        preset_names = list_presets()
        preset_name = st.selectbox("Load preset", options=[""] + preset_names)
        if st.button("Apply preset", use_container_width=True, disabled=not preset_name):
            _apply_preset(preset_name)
            st.rerun()
        save_name = st.text_input("Save current filters as")
        if st.button("Save preset", use_container_width=True, disabled=not save_name):
            payload = {
                "metric_key": st.session_state.get("metric_key"),
                "group_field": st.session_state.get("group_field"),
                "compare_field": st.session_state.get("compare_field"),
                "compare_values": st.session_state.get("compare_values", []),
                "control_field": st.session_state.get("control_field", "None"),
                "outcome_population_view": st.session_state.get("outcome_population_view", ALL_STUDENTS_LABEL),
                "filters": _collect_filters(),
            }
            path = save_preset(save_name, payload)
            st.success(f"Saved preset to {path.name}.")

    with st.sidebar.expander("Advanced Analysis Setup", expanded=False):
        metric_key = st.selectbox(
            "Metric",
            options=[metric.key for metric in metrics],
            format_func=lambda key: metric_by_key(metrics, key).display_name,
            key="metric_key",
        )
        metric = metric_by_key(metrics, metric_key)
        analysis_summary = _analysis_summary_for_metric(bundle.summary, metric)
        if metric.category.lower() == "graduation":
            previous_metric = st.session_state.get("_auto_population_metric")
            if previous_metric != metric_key and st.session_state.get("outcome_population_view", ALL_STUDENTS_LABEL) == ALL_STUDENTS_LABEL:
                st.session_state["outcome_population_view"] = RESOLVED_OUTCOMES_ONLY_LABEL
            st.session_state["_auto_population_metric"] = metric_key
        elif metric.key == CURRENT_ACTIVE_METRIC_KEY:
            st.session_state["outcome_population_view"] = ALL_STUDENTS_LABEL
            st.session_state["_auto_population_metric"] = metric_key
        group_field = st.selectbox(
            "Aggregation level",
            options=list(dimension_map.keys()),
            format_func=lambda key: dimension_map[key],
            key="group_field",
        )
        compare_field = st.selectbox(
            "Compare groups by",
            options=list(dimension_map.keys()),
            format_func=lambda key: dimension_map[key],
            key="compare_field",
        )
        compare_values = st.multiselect(
            "Specific groups to compare",
            options=filter_options(analysis_summary, compare_field),
            key="compare_values",
        )
        control_options = ["None"] + [key for key in dimension_map.keys() if key != compare_field]
        st.selectbox(
            "Controlled comparison",
            options=control_options,
            format_func=lambda key: "No control" if key == "None" else dimension_map[key],
            key="control_field",
        )
        st.selectbox(
            "Metric population view",
            options=[ALL_STUDENTS_LABEL] if metric.key == CURRENT_ACTIVE_METRIC_KEY else [ALL_STUDENTS_LABEL, RESOLVED_OUTCOMES_ONLY_LABEL],
            key="outcome_population_view",
        )
        if metric.category.lower() == "graduation":
            st.caption("Graduation-focused views default to Resolved Outcomes Only so active and unresolved students do not dominate the ranking.")
        elif metric.key == CURRENT_ACTIVE_METRIC_KEY:
            st.caption("Current active counts are locked to the most recent roster only and are not recalculated from historical activeness.")
        max_min_n = int(settings.get("max_min_sample_size", 50))
        default_min_n = min(int(settings.get("default_min_sample_size", 5)), max_min_n)
        st.slider("Minimum N", min_value=1, max_value=max_min_n, value=default_min_n, key="min_n")
        population_options = ["FSL Only", "All Students"]
        if bundle.metadata.get("available_campus_baseline"):
            population_options.append("Campus Baseline Only")
        st.selectbox("Population", options=population_options, key="population")

    with st.sidebar.expander("Filters", expanded=False):
        join_years = pd.to_numeric(analysis_summary.get("join_year", pd.Series(dtype=float)), errors="coerce").dropna()
        if not join_years.empty:
            st.slider(
                "Join year range",
                min_value=int(join_years.min()),
                max_value=int(join_years.max()),
                value=st.session_state.get("join_year_range", (int(join_years.min()), int(join_years.max()))),
                key="join_year_range",
            )
        grad_years = pd.to_numeric(analysis_summary.get("graduation_year", pd.Series(dtype=float)), errors="coerce").dropna()
        if not grad_years.empty:
            st.slider(
                "Graduation year range",
                min_value=int(grad_years.min()),
                max_value=int(grad_years.max()),
                value=st.session_state.get("graduation_year_range", (int(grad_years.min()), int(grad_years.max()))),
                key="graduation_year_range",
            )

        observed_years = pd.to_numeric(bundle.longitudinal.get("observed_year", pd.Series(dtype=float)), errors="coerce").dropna()
        if not observed_years.empty:
            st.slider(
                "Observed year range",
                min_value=int(observed_years.min()),
                max_value=int(observed_years.max()),
                value=st.session_state.get("observed_year_range", (int(observed_years.min()), int(observed_years.max()))),
                key="observed_year_range",
            )

        filter_specs = [
            ("chapters", "chapter", "Chapters"),
            ("chapter_groups", "chapter_group", "Chapter groups"),
            ("custom_groups", "custom_group", "Custom groups"),
            ("councils", "council", "Councils"),
            ("org_types", "org_type", "Fraternity / Sorority"),
            ("families", "family", "Organization families"),
            ("join_terms", "join_term", "Join terms"),
            ("statuses", "status_group", "Latest statuses"),
            ("resolved_outcome_groups", "outcome_resolution_group", "Outcome resolution"),
            ("majors", "major_group", "Majors"),
            ("pell_groups", "pell_group", "Pell groups"),
            ("transfer_groups", "transfer_group", "Transfer groups"),
            ("estimated_join_stages", "estimated_join_stage", "Estimated join stages"),
            ("high_hours_groups", "high_hours_group", "Hours groups"),
            ("active_groups", "active_membership_group", "Membership activity"),
            ("chapter_size_bands", "chapter_size_band", "Chapter size bands"),
            ("snapshot_groups", "snapshot_group", "Snapshot match status"),
        ]
        for state_key, column, label in filter_specs:
            options = filter_options(analysis_summary, column)
            if options:
                st.multiselect(label, options=options, key=state_key)

        observed_terms = filter_options(bundle.longitudinal, "observed_term")
        if observed_terms:
            st.multiselect("Observed terms", options=observed_terms, key="observed_terms")

    filters = _collect_filters()
    filtered_summary = apply_summary_filters(analysis_summary, filters)
    metric_summary = _metric_frame_for_metric(filtered_summary, metric)
    filtered_longitudinal = apply_longitudinal_filters(bundle.longitudinal, metric_summary, filters)

    outcome_population_view = st.session_state["outcome_population_view"]
    metric_views = compute_metric_views(metric_summary, metric)
    group_summary = summarize_metric_by_group(
        metric_summary,
        metric,
        group_field,
        st.session_state["min_n"],
        population_label=outcome_population_view,
    )
    comparison_table = build_comparison_table(
        metric_summary,
        metric,
        compare_field,
        compare_values,
        st.session_state["min_n"],
        population_label=outcome_population_view,
    )
    controlled_table = build_controlled_comparison(
        metric_summary,
        metric,
        compare_field,
        compare_values,
        st.session_state["control_field"],
        st.session_state["min_n"],
        population_label=outcome_population_view,
    ) if st.session_state["control_field"] != "None" else pd.DataFrame()
    landing_tab, retention_gpa_tab, chapter_health_tab, roster_disappearance_tab, advisor_help_tab, corrections_tab, advanced_tab = st.tabs(
        ["Persistence & Graduation", "Retention & GPA", "Chapter Health", "Roster Disappearances", "Advisor Help", "Manual Corrections", "Advanced Analytics"]
    )

    with landing_tab:
        _render_persistence_and_graduation_view(bundle)

    with retention_gpa_tab:
        _render_retention_and_gpa_dashboard(bundle)

    with chapter_health_tab:
        _render_chapter_health_dashboard(bundle)

    with roster_disappearance_tab:
        _render_roster_disappearance_tracker(bundle)

    with advisor_help_tab:
        _render_advisor_help_dashboard(bundle)

    with corrections_tab:
        _render_manual_corrections_editor(bundle)

    with advanced_tab:
        _render_advanced_analytics(
            bundle=bundle,
            source_statuses=source_statuses,
            metric=metric,
            metrics=metrics,
            settings=settings,
            dimension_map=dimension_map,
            group_field=group_field,
            compare_field=compare_field,
            compare_values=compare_values,
            outcome_population_view=outcome_population_view,
            filtered_summary=filtered_summary,
            metric_summary=metric_summary,
            filtered_longitudinal=filtered_longitudinal,
            group_summary=group_summary,
            comparison_table=comparison_table,
            controlled_table=controlled_table,
            metric_views=metric_views,
        )


if __name__ == "__main__":
    main()
