from __future__ import annotations

import pandas as pd  # type: ignore[import-untyped]
import plotly.graph_objects as go  # type: ignore[import-untyped]
from plotly.subplots import make_subplots  # type: ignore[import-untyped]


COMPOUND_COLORS: dict[str, str] = {
    "SOFT": "#d62828",
    "MEDIUM": "#f4a261",
    "HARD": "#e9ecef",
    "INTERMEDIATE": "#2a9d8f",
    "WET": "#457b9d",
    "UNKNOWN": "#6c757d",
}

STINT_SYMBOLS: list[str] = [
    "circle",
    "square",
    "diamond",
    "triangle-up",
    "triangle-down",
    "cross",
]


def _compound_color(compound: str) -> str:
    return COMPOUND_COLORS.get(compound.upper(), COMPOUND_COLORS["UNKNOWN"])


def _stint_symbol(stint_display: str) -> str:
    if stint_display == "UNKNOWN":
        return "x"
    try:
        stint_number = int(float(stint_display))
    except ValueError:
        return "x"
    return STINT_SYMBOLS[(stint_number - 1) % len(STINT_SYMBOLS)]


def build_lap_time_trend_figure(
    plot_rows: pd.DataFrame,
    fit_rows: pd.DataFrame,
    selected_drivers: tuple[str, str],
    *,
    polynomial_degree: int,
) -> go.Figure:
    driver_a, driver_b = selected_drivers
    figure = make_subplots(rows=1, cols=2, subplot_titles=selected_drivers, shared_yaxes=True)

    for column_index, driver in enumerate((driver_a, driver_b), start=1):
        driver_rows = plot_rows[plot_rows["driver"] == driver].copy()
        figure.add_trace(
            go.Scatter(
                x=driver_rows["lap_number"],
                y=driver_rows["lap_time_s"],
                mode="markers",
                name=f"{driver} laps",
                marker={
                    "color": [_compound_color(str(compound)) for compound in driver_rows["compound_display"]],
                    "symbol": [_stint_symbol(str(stint)) for stint in driver_rows["stint_display"]],
                    "size": 9,
                },
                customdata=driver_rows[["compound_display", "stint_display"]],
                hovertemplate=(
                    "Lap %{x}<br>"
                    "Lap time %{y:.3f}s<br>"
                    "Compound %{customdata[0]}<br>"
                    "Stint %{customdata[1]}<extra></extra>"
                ),
                showlegend=False,
            ),
            row=1,
            col=column_index,
        )

        driver_fit = fit_rows[fit_rows["driver"] == driver].copy()
        if not driver_fit.empty:
            figure.add_trace(
                go.Scatter(
                    x=driver_fit["lap_number"],
                    y=driver_fit["fit_lap_time_s"],
                    mode="lines",
                    name=f"{driver} fit",
                    line={"width": 2},
                    showlegend=False,
                    hovertemplate="Fit lap %{x:.1f}<br>%{y:.3f}s<extra></extra>",
                ),
                row=1,
                col=column_index,
            )

        figure.update_xaxes(title_text="Lap", row=1, col=column_index)

    figure.update_yaxes(title_text="Lap time (s)", row=1, col=1)
    figure.update_layout(
        title=f"Lap Time Trends (Polynomial degree {polynomial_degree})",
        margin={"l": 32, "r": 24, "t": 64, "b": 32},
        height=480,
    )
    return figure


def build_cumulative_delta_figure(delta_plot_rows: pd.DataFrame) -> go.Figure:
    driver_a = str(delta_plot_rows.iloc[0]["driver_a"])
    driver_b = str(delta_plot_rows.iloc[0]["driver_b"])
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=delta_plot_rows["lap_number"],
            y=delta_plot_rows["cum_delta_s"],
            mode="lines+markers",
            name="Cumulative delta",
            hovertemplate="Lap %{x}<br>Delta %{y:.3f}s<extra></extra>",
        )
    )
    figure.update_layout(
        title=f"Cumulative Delta ({driver_a} - {driver_b})",
        margin={"l": 32, "r": 24, "t": 64, "b": 32},
        height=420,
    )
    figure.update_xaxes(title_text="Lap")
    figure.update_yaxes(title_text="Cumulative delta (s)")
    return figure


def build_per_lap_delta_figure(delta_plot_rows: pd.DataFrame) -> go.Figure:
    driver_a = str(delta_plot_rows.iloc[0]["driver_a"])
    driver_b = str(delta_plot_rows.iloc[0]["driver_b"])
    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=delta_plot_rows["lap_number"],
            y=delta_plot_rows["lap_delta_s"],
            name="Per-lap delta",
            hovertemplate="Lap %{x}<br>Delta %{y:.3f}s<extra></extra>",
        )
    )
    figure.update_layout(
        title=f"Per-Lap Delta ({driver_a} - {driver_b})",
        margin={"l": 32, "r": 24, "t": 64, "b": 32},
        height=420,
    )
    figure.update_xaxes(title_text="Lap")
    figure.update_yaxes(title_text="Lap delta (s)")
    return figure
