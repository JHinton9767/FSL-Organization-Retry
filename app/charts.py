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


def empty_figure(message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(text=message, showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
    fig.update_layout(template=PLOTLY_TEMPLATE, height=420)
    return fig


def bar_chart(frame: pd.DataFrame, x: str, y: str, color: str | None, title: str, y_format: str = "") -> go.Figure:
    if frame.empty:
        return empty_figure("No data available for the current selection.")
    fig = px.bar(
        frame,
        x=x,
        y=y,
        color=color,
        template=PLOTLY_TEMPLATE,
        title=title,
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_layout(xaxis_title="", legend_title="")
    if y_format == "percent":
        fig.update_yaxes(tickformat=".0%")
    return fig


def line_chart(frame: pd.DataFrame, x: str, y: str, color: str | None, title: str, y_format: str = "") -> go.Figure:
    if frame.empty:
        return empty_figure("No trend data is available for the current selection.")
    fig = px.line(
        frame,
        x=x,
        y=y,
        color=color,
        markers=True,
        template=PLOTLY_TEMPLATE,
        title=title,
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_layout(xaxis_title="", legend_title="")
    if y_format == "percent":
        fig.update_yaxes(tickformat=".0%")
    return fig


def stacked_bar_chart(frame: pd.DataFrame, x: str, y: str, color: str, title: str) -> go.Figure:
    if frame.empty:
        return empty_figure("No distribution data is available for the current selection.")
    fig = px.bar(
        frame,
        x=x,
        y=y,
        color=color,
        template=PLOTLY_TEMPLATE,
        title=title,
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_layout(barmode="stack", xaxis_title="", legend_title="")
    fig.update_yaxes(tickformat=".0%")
    return fig


def scatter_chart(frame: pd.DataFrame, x: str, y: str, size: str | None, color: str | None, title: str, y_format: str = "") -> go.Figure:
    if frame.empty:
        return empty_figure("No comparison data is available for the current selection.")
    fig = px.scatter(
        frame,
        x=x,
        y=y,
        size=size,
        color=color,
        template=PLOTLY_TEMPLATE,
        title=title,
        color_discrete_sequence=COLOR_SEQUENCE,
        hover_name="Group" if "Group" in frame.columns else None,
    )
    fig.update_layout(xaxis_title="", legend_title="")
    if y_format == "percent":
        fig.update_yaxes(tickformat=".0%")
    return fig


def histogram(frame: pd.DataFrame, x: str, color: str | None, title: str) -> go.Figure:
    if frame.empty:
        return empty_figure("No distribution data is available for the current selection.")
    fig = px.histogram(
        frame,
        x=x,
        color=color,
        nbins=25,
        template=PLOTLY_TEMPLATE,
        title=title,
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_layout(xaxis_title="", legend_title="")
    return fig


def box_plot(frame: pd.DataFrame, x: str | None, y: str, color: str | None, title: str) -> go.Figure:
    if frame.empty:
        return empty_figure("No distribution data is available for the current selection.")
    fig = px.box(
        frame,
        x=x,
        y=y,
        color=color,
        template=PLOTLY_TEMPLATE,
        title=title,
        color_discrete_sequence=COLOR_SEQUENCE,
    )
    fig.update_layout(xaxis_title="", legend_title="")
    return fig
