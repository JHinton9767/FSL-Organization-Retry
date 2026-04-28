from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


PLOTLY_TEMPLATE = "plotly_white"
COLOR_SEQUENCE = [
    "#0B3954",
    "#BFD7EA",
    "#FF6663",
    "#E0FF4F",
    "#4B6858",
    "#F4D35E",
]


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
