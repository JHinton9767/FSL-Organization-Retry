from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from src.persistence_outcomes import PERSISTENCE_OUTCOME_ORDER


PLOTLY_TEMPLATE = "plotly_white"
COLOR_SEQUENCE = [
    "#0B3954",
    "#BFD7EA",
    "#FF6663",
    "#E0FF4F",
    "#4B6858",
    "#F4D35E",
]
PERSISTENCE_OUTCOME_COLORS = {
    "Active": "#5C1418",
    "Early Alumni": "#C69026",
    "Inactive/Suspended": "#D97706",
    "Dropped/Resigned": "#7C3AED",
    "Revoked": "#9F1D35",
    "Transfer": "#0B6C94",
    "Chapter Kicked": "#475569",
    "Unknown": "#B7B4AA",
    "Graduated": "#2F7D4A",
}
PERSISTENCE_DARK_TEXT_OUTCOMES = {"Early Alumni", "Unknown"}


def _finalize_figure(fig: go.Figure, y_format: str = "", **layout_updates: object) -> go.Figure:
    fig.update_layout(template=PLOTLY_TEMPLATE, **layout_updates)
    if y_format == "percent":
        fig.update_yaxes(tickformat=".0%")
    return fig


def empty_figure(message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=message, showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
    return _finalize_figure(fig, height=420)


def _build_plotly_figure(
    builder,
    frame: pd.DataFrame,
    empty_message: str,
    y_format: str = "",
    layout_updates: dict[str, object] | None = None,
    **kwargs: object,
) -> go.Figure:
    if frame.empty:
        return empty_figure(empty_message)
    fig = builder(
        frame,
        template=PLOTLY_TEMPLATE,
        color_discrete_sequence=COLOR_SEQUENCE,
        **kwargs,
    )
    return _finalize_figure(
        fig,
        y_format=y_format,
        xaxis_title="",
        legend_title="",
        **(layout_updates or {}),
    )


def bar_chart(frame: pd.DataFrame, x: str, y: str, color: str | None, title: str, y_format: str = "") -> go.Figure:
    return _build_plotly_figure(
        px.bar,
        frame,
        "No data available for the current selection.",
        x=x,
        y=y,
        color=color,
        title=title,
        y_format=y_format,
    )


def line_chart(frame: pd.DataFrame, x: str, y: str, color: str | None, title: str, y_format: str = "") -> go.Figure:
    return _build_plotly_figure(
        px.line,
        frame,
        "No trend data is available for the current selection.",
        x=x,
        y=y,
        color=color,
        markers=True,
        title=title,
        y_format=y_format,
    )


def stacked_bar_chart(frame: pd.DataFrame, x: str, y: str, color: str, title: str) -> go.Figure:
    return _build_plotly_figure(
        px.bar,
        frame,
        "No distribution data is available for the current selection.",
        x=x,
        y=y,
        color=color,
        title=title,
        y_format="percent",
        layout_updates={"barmode": "stack"},
    )


def scatter_chart(frame: pd.DataFrame, x: str, y: str, size: str | None, color: str | None, title: str, y_format: str = "") -> go.Figure:
    return _build_plotly_figure(
        px.scatter,
        frame,
        "No comparison data is available for the current selection.",
        x=x,
        y=y,
        size=size,
        color=color,
        title=title,
        hover_name="Group" if "Group" in frame.columns else None,
        y_format=y_format,
    )


def histogram(frame: pd.DataFrame, x: str, color: str | None, title: str) -> go.Figure:
    return _build_plotly_figure(
        px.histogram,
        frame,
        "No distribution data is available for the current selection.",
        x=x,
        color=color,
        nbins=25,
        title=title,
    )


def box_plot(frame: pd.DataFrame, x: str | None, y: str, color: str | None, title: str) -> go.Figure:
    return _build_plotly_figure(
        px.box,
        frame,
        "No distribution data is available for the current selection.",
        x=x,
        y=y,
        color=color,
        title=title,
    )


def persistence_milestone_chart(frame: pd.DataFrame, title: str, subtitle: str = "") -> go.Figure:
    if frame.empty:
        return empty_figure("No persistence or graduation data is available for the selected cohort.")

    fig = go.Figure()
    for outcome in PERSISTENCE_OUTCOME_ORDER:
        subset = frame.loc[frame["Outcome"].eq(outcome)].copy()
        if subset.empty:
            continue
        customdata_columns = ["Count"]
        hovertemplate = f"{outcome}<br>%{{x}}<br>%{{y:.1%}}<br>n=%{{customdata[0]:,}}<extra></extra>"
        if "Denominator" in subset.columns:
            customdata_columns.append("Denominator")
            hovertemplate = (
                f"{outcome}<br>%{{x}}<br>%{{y:.1%}}"
                "<br>n=%{customdata[0]:,}<br>measured=%{customdata[1]:,}<extra></extra>"
            )
        fig.add_bar(
            x=subset["Milestone"],
            y=subset["Share"],
            name=outcome,
            marker_color=PERSISTENCE_OUTCOME_COLORS[outcome],
            text=subset["Label"],
            textposition="inside",
            textfont={"color": "#2F2E2A" if outcome in PERSISTENCE_DARK_TEXT_OUTCOMES else "white", "size": 11},
            customdata=subset[customdata_columns],
            hovertemplate=hovertemplate,
        )

    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        barmode="stack",
        height=610,
        title={"text": title + (f"<br><sup>{subtitle}</sup>" if subtitle else ""), "x": 0.01, "xanchor": "left"},
        legend={"orientation": "h", "yanchor": "top", "y": -0.08, "xanchor": "left", "x": 0.0},
        xaxis_title="",
        yaxis_title="Share of cohort",
        margin={"l": 24, "r": 24, "t": 90, "b": 120},
    )
    fig.update_yaxes(tickformat=".0%", range=[0, 1])
    return fig
