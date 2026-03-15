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


def build_track_compare_figure(track_compare_rows: pd.DataFrame) -> go.Figure:
    driver_a = str(track_compare_rows.iloc[0]["driver_a"])
    driver_b = str(track_compare_rows.iloc[0]["driver_b"])
    color_map = {
        driver_a: "#d62828",
        driver_b: "#1d3557",
        pd.NA: "#6c757d",
        "": "#6c757d",
    }
    figure = go.Figure()

    for row_index in range(1, len(track_compare_rows)):
        start_row = track_compare_rows.iloc[row_index - 1]
        end_row = track_compare_rows.iloc[row_index]
        faster_driver = end_row["faster_driver_segment"]
        color = color_map.get(faster_driver, "#6c757d")
        figure.add_trace(
            go.Scatter(
                x=[start_row["plot_x"], end_row["plot_x"]],
                y=[start_row["plot_y"], end_row["plot_y"]],
                mode="lines",
                line={"color": color, "width": 4},
                hovertemplate=(
                    f"Distance {float(end_row['distance_m']):.1f} m<br>"
                    f"Faster: {faster_driver if pd.notna(faster_driver) else 'Tie'}<extra></extra>"
                ),
                showlegend=False,
            )
        )

    figure.update_layout(
        title=f"Track Comparison ({driver_a} vs {driver_b})",
        margin={"l": 24, "r": 24, "t": 64, "b": 24},
        height=560,
    )
    figure.update_xaxes(visible=False)
    figure.update_yaxes(visible=False, scaleanchor="x", scaleratio=1)
    return figure


def build_telemetry_compare_figure(telemetry_compare_rows: pd.DataFrame) -> go.Figure:
    driver_a = str(telemetry_compare_rows.iloc[0]["driver_a"])
    driver_b = str(telemetry_compare_rows.iloc[0]["driver_b"])
    figure = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=("Throttle", "Brake", "Speed"),
    )

    series_config = [
        ("throttle_a", "throttle_b", "Throttle", "Throttle"),
        ("brake_a", "brake_b", "Brake", "Brake"),
        ("speed_a", "speed_b", "Speed", "Speed"),
    ]
    for row_index, (column_a, column_b, title, y_axis_title) in enumerate(series_config, start=1):
        figure.add_trace(
            go.Scatter(
                x=telemetry_compare_rows["distance_m"],
                y=telemetry_compare_rows[column_a],
                mode="lines",
                name=driver_a,
                showlegend=row_index == 1,
            ),
            row=row_index,
            col=1,
        )
        figure.add_trace(
            go.Scatter(
                x=telemetry_compare_rows["distance_m"],
                y=telemetry_compare_rows[column_b],
                mode="lines",
                name=driver_b,
                showlegend=row_index == 1,
            ),
            row=row_index,
            col=1,
        )
        figure.update_yaxes(title_text=y_axis_title, row=row_index, col=1)

    figure.update_xaxes(title_text="Distance (m)", row=3, col=1)
    figure.update_layout(
        title=f"Telemetry Comparison ({driver_a} vs {driver_b})",
        margin={"l": 40, "r": 24, "t": 64, "b": 32},
        height=760,
    )
    return figure
