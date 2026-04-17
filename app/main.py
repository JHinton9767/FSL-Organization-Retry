from __future__ import annotations

from datetime import datetime
from typing import Dict, List

import pandas as pd
import streamlit as st

from app.analysis import (
    apply_longitudinal_filters,
    apply_summary_filters,
    available_dimensions,
    build_comparison_table,
    build_controlled_comparison,
    build_distribution_table,
    build_observed_term_series,
    build_scatter_frame,
    build_summary_time_series,
    filter_options,
    stakeholder_summary,
    summarize_metric_by_group,
)
from app.charts import bar_chart, box_plot, histogram, line_chart, scatter_chart, stacked_bar_chart
from app.config_loader import load_metric_catalog, load_settings, load_status_code_map
from app.exports import dataframe_to_csv_bytes, figure_to_html_bytes, figure_to_png_bytes, frames_to_excel_bytes
from app.io_utils import safe_slug
from app.legacy_bridge import discover_dataset_versions, load_analysis_bundle, scan_preloaded_sources, select_default_dataset
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


st.set_page_config(
    page_title="FSL Academic Outcomes Analytics",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)


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

    for key in [
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
    ]:
        st.session_state[key] = []

    if not metadata.get("available_campus_baseline"):
        st.session_state["population"] = "FSL Only"


def _collect_filters() -> Dict[str, object]:
    return {
        "population": st.session_state.get("population", "FSL Only"),
        "join_year_range": st.session_state.get("join_year_range"),
        "graduation_year_range": st.session_state.get("graduation_year_range"),
        "observed_year_range": st.session_state.get("observed_year_range"),
        "chapters": st.session_state.get("chapters", []),
        "chapter_groups": st.session_state.get("chapter_groups", []),
        "custom_groups": st.session_state.get("custom_groups", []),
        "councils": st.session_state.get("councils", []),
        "org_types": st.session_state.get("org_types", []),
        "families": st.session_state.get("families", []),
        "join_terms": st.session_state.get("join_terms", []),
        "statuses": st.session_state.get("statuses", []),
        "resolved_outcome_groups": st.session_state.get("resolved_outcome_groups", []),
        "majors": st.session_state.get("majors", []),
        "pell_groups": st.session_state.get("pell_groups", []),
        "transfer_groups": st.session_state.get("transfer_groups", []),
        "estimated_join_stages": st.session_state.get("estimated_join_stages", []),
        "high_hours_groups": st.session_state.get("high_hours_groups", []),
        "active_groups": st.session_state.get("active_groups", []),
        "chapter_size_bands": st.session_state.get("chapter_size_bands", []),
        "snapshot_groups": st.session_state.get("snapshot_groups", []),
        "observed_terms": st.session_state.get("observed_terms", []),
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
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_source_file_status(statuses: List[DataSourceStatus]) -> None:
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


def _population_transparency_frame(metric: MetricDefinition, metric_views: dict[str, object], filtered_summary: pd.DataFrame) -> pd.DataFrame:
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
        ]
    )
    tables["Classification Audit"] = chapter_unresolved

    for key in ["identity_exceptions", "term_exceptions", "status_exceptions", "chapter_conflicts", "outcome_exceptions", "missing_evidence_cases", "unresolved_chapter_review", "qa_checks"]:
        if key in bundle.tables:
            tables[key] = bundle.tables[key]
    return tables


def main() -> None:
    settings = load_settings()
    metric_catalog = load_metric_catalog()
    status_code_map = load_status_code_map()

    source_statuses = scan_preloaded_sources()
    versions = discover_dataset_versions()
    version = select_default_dataset(versions)

    st.sidebar.title("FSL Analytics")
    st.sidebar.caption("Interactive chapter, cohort, and campus comparison workspace.")
    st.sidebar.caption("The app reads pre-positioned local project files on startup.")

    if version is None:
        st.title("FSL Academic Outcomes Analytics")
        st.error(
            "No valid prepared dataset was found in the expected local project folders. "
            "Run the external prep pipeline, place the finished files in their documented folders, and relaunch the app."
        )
        st.subheader("Detected Local Data Sources")
        _render_source_scan(source_statuses)
        if any(status.files for status in source_statuses):
            st.subheader("Expected Files")
            _render_source_file_status(source_statuses)
        return

    st.sidebar.caption(f"Auto-loaded dataset: {version.label}")

    try:
        bundle = load_analysis_bundle(
            version=version,
            metric_definitions=metric_catalog,
            settings=settings,
            status_code_map=status_code_map,
        )
    except Exception as exc:
        st.title("FSL Academic Outcomes Analytics")
        st.error(
            "A prepared dataset was found, but it could not be loaded cleanly. "
            "Check the generated files, rerun the external prep workflow if needed, and relaunch the app."
        )
        st.write(f"**Load error:** `{exc}`")
        st.subheader("Detected Local Data Sources")
        _render_source_scan(source_statuses)
        if any(status.files for status in source_statuses):
            st.subheader("Expected Files")
            _render_source_file_status(source_statuses)
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

    with st.sidebar.expander("Analysis Setup", expanded=True):
        metric_key = st.selectbox(
            "Metric",
            options=[metric.key for metric in metrics],
            format_func=lambda key: metric_by_key(metrics, key).display_name,
            key="metric_key",
        )
        metric = metric_by_key(metrics, metric_key)
        if metric.category.lower() == "graduation":
            previous_metric = st.session_state.get("_auto_population_metric")
            if previous_metric != metric_key and st.session_state.get("outcome_population_view", ALL_STUDENTS_LABEL) == ALL_STUDENTS_LABEL:
                st.session_state["outcome_population_view"] = RESOLVED_OUTCOMES_ONLY_LABEL
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
            options=filter_options(bundle.summary, compare_field),
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
            options=[ALL_STUDENTS_LABEL, RESOLVED_OUTCOMES_ONLY_LABEL],
            key="outcome_population_view",
        )
        if metric.category.lower() == "graduation":
            st.caption("Graduation-focused views default to Resolved Outcomes Only so active and unresolved students do not dominate the ranking.")
        max_min_n = int(settings.get("max_min_sample_size", 50))
        default_min_n = min(int(settings.get("default_min_sample_size", 5)), max_min_n)
        st.slider("Minimum N", min_value=1, max_value=max_min_n, value=default_min_n, key="min_n")
        population_options = ["FSL Only", "All Students"]
        if bundle.metadata.get("available_campus_baseline"):
            population_options.append("Campus Baseline Only")
        st.selectbox("Population", options=population_options, key="population")

    with st.sidebar.expander("Filters", expanded=False):
        join_years = pd.to_numeric(bundle.summary.get("join_year", pd.Series(dtype=float)), errors="coerce").dropna()
        if not join_years.empty:
            st.slider(
                "Join year range",
                min_value=int(join_years.min()),
                max_value=int(join_years.max()),
                value=st.session_state.get("join_year_range", (int(join_years.min()), int(join_years.max()))),
                key="join_year_range",
            )
        grad_years = pd.to_numeric(bundle.summary.get("graduation_year", pd.Series(dtype=float)), errors="coerce").dropna()
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
            options = filter_options(bundle.summary, column)
            if options:
                st.multiselect(label, options=options, key=state_key)

        observed_terms = filter_options(bundle.longitudinal, "observed_term")
        if observed_terms:
            st.multiselect("Observed terms", options=observed_terms, key="observed_terms")

    filters = _collect_filters()
    filtered_summary = apply_summary_filters(bundle.summary, filters)
    filtered_longitudinal = apply_longitudinal_filters(bundle.longitudinal, filtered_summary, filters)

    outcome_population_view = st.session_state["outcome_population_view"]
    metric_views = compute_metric_views(filtered_summary, metric)
    group_summary = summarize_metric_by_group(
        filtered_summary,
        metric,
        group_field,
        st.session_state["min_n"],
        population_label=outcome_population_view,
    )
    comparison_table = build_comparison_table(
        filtered_summary,
        metric,
        compare_field,
        compare_values,
        st.session_state["min_n"],
        population_label=outcome_population_view,
    )
    controlled_table = build_controlled_comparison(
        filtered_summary,
        metric,
        compare_field,
        compare_values,
        st.session_state["control_field"],
        st.session_state["min_n"],
        population_label=outcome_population_view,
    ) if st.session_state["control_field"] != "None" else pd.DataFrame()

    st.title("Fraternity / Sorority Life Academic Outcomes Analytics")
    st.caption(f"Dataset: {bundle.version.label}")
    st.caption("Prepared files are loaded automatically from the local project folders at startup.")
    if bundle.notes:
        with st.expander("Dataset notes and caveats", expanded=False):
            for note in bundle.notes:
                st.write(f"- {note}")
    _render_data_status_panel(bundle, source_statuses)

    st.info(metric_caption(metric))
    st.caption(
        f"Charts and rank ordering currently use: {outcome_population_view}. "
        "Every major table now shows the full-population and resolved-only denominators side by side where practical."
    )
    population_transparency = _render_population_summary(metric, metric_views, filtered_summary)

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
            filtered_summary,
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
        summary_time_field = "join_year" if "join_year" in filtered_summary.columns else "join_term"
        summary_trend = build_summary_time_series(
            filtered_summary,
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
            summary=filtered_summary,
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
            if column in filtered_summary.columns
        ]
        if distribution_options:
            distribution_field = st.selectbox(
                "Distribution field",
                options=distribution_options,
                format_func=lambda key: key.replace("_", " ").title(),
            )
            distribution_table = build_distribution_table(
                filtered_summary,
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
            if column in filtered_summary.columns and pd.to_numeric(filtered_summary[column], errors="coerce").dropna().shape[0] > 0
        ]
        if numeric_options:
            numeric_field = st.selectbox(
                "Numeric field",
                options=numeric_options,
                format_func=lambda key: key.replace("_", " ").title(),
            )
            numeric_frame = filtered_summary if outcome_population_view == ALL_STUDENTS_LABEL else filtered_summary.loc[
                filtered_summary["resolved_outcomes_only_flag"].fillna(False)
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
        audit_tables = _audit_tables(filtered_summary, bundle)
        for label, frame in audit_tables.items():
            if frame is None or frame.empty:
                continue
            st.markdown(f"**{label}**")
            st.dataframe(frame, use_container_width=True, hide_index=True)

    with export_tab:
        st.subheader("Filtered tables")
        export_columns = [
            column
            for column in [
                "student_id",
                "student_name",
                "chapter",
                "chapter_assignment_source",
                "chapter_assignment_confidence",
                "chapter_assignment_notes",
                "chapter_group",
                "council",
                "org_type",
                "join_term",
                "join_year",
                "status_group",
                "outcome_resolution_group",
                "is_resolved_outcome",
                "is_active_outcome",
                "is_unknown_outcome",
                "is_graduated",
                "is_known_non_graduate_exit",
                "resolved_outcomes_only_flag",
                "resolved_outcome_excluded_flag",
                "resolved_outcome_exclusion_reason",
                "major",
                "pell_group",
                "transfer_group",
                "estimated_join_stage",
                "average_term_gpa",
                "average_cumulative_gpa",
                "total_cumulative_hours",
                "data_completeness_rate",
            ]
            if column in filtered_summary.columns
        ]
        summary_export = filtered_summary[export_columns].copy()
        st.dataframe(summary_export, use_container_width=True, hide_index=True)

        export_frames = {
            "Filtered Students": summary_export,
            "Population Summary": population_transparency,
            "Group Summary": group_summary,
            "Comparison Table": comparison_table,
            "Controlled Comparison": controlled_table,
            "Filtered Longitudinal": filtered_longitudinal,
            "Audit Tables": pd.concat(_audit_tables(filtered_summary, bundle).values(), ignore_index=True) if _audit_tables(filtered_summary, bundle) else pd.DataFrame(),
        }
        csv_col, xlsx_col = st.columns(2)
        with csv_col:
            st.download_button(
                "Download filtered students CSV",
                data=dataframe_to_csv_bytes(summary_export),
                file_name="filtered_students.csv",
                mime="text/csv",
            )
        with xlsx_col:
            st.download_button(
                "Download current workbook",
                data=frames_to_excel_bytes(export_frames),
                file_name="analytics_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
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

        if "metric_definitions" in bundle.tables:
            st.subheader("Legacy metric definition table")
            st.dataframe(bundle.tables["metric_definitions"], use_container_width=True, hide_index=True)
        if "qa_checks" in bundle.tables:
            st.subheader("Legacy QA table")
            st.dataframe(bundle.tables["qa_checks"], use_container_width=True, hide_index=True)
        if "snapshot_merge_qa" in bundle.tables:
            st.subheader("Snapshot merge QA table")
            st.dataframe(bundle.tables["snapshot_merge_qa"], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
