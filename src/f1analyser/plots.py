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
FIT_COLORS: list[str] = ["#1d3557", "#457b9d", "#2a9d8f", "#6d597a", "#e76f51"]


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
    shown_compounds: set[str] = set()
    shown_stints: set[str] = set()

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
                showlegend=True,
                legendgroup=f"{driver}-laps",
            ),
            row=1,
            col=column_index,
        )

        for compound in sorted(driver_rows["compound_display"].dropna().astype("string").unique().tolist()):
            if compound in shown_compounds:
                continue
            shown_compounds.add(compound)
            figure.add_trace(
                go.Scatter(
                    x=[None],
                    y=[None],
                    mode="markers",
                    marker={"color": _compound_color(str(compound)), "size": 9, "symbol": "circle"},
                    name=f"Compound {compound}",
                    showlegend=True,
                ),
                row=1,
                col=column_index,
            )

        for stint_display in sorted(driver_rows["stint_display"].dropna().astype("string").unique().tolist()):
            if stint_display in shown_stints:
                continue
            shown_stints.add(stint_display)
            figure.add_trace(
                go.Scatter(
                    x=[None],
                    y=[None],
                    mode="markers",
                    marker={"color": "#6c757d", "size": 9, "symbol": _stint_symbol(str(stint_display))},
                    name=f"Stint {stint_display}",
                    showlegend=True,
                ),
                row=1,
                col=column_index,
            )

        driver_fit = fit_rows[fit_rows["driver"] == driver].copy()
        for fit_index, stint_display in enumerate(
            sorted(driver_fit["stint_display"].dropna().astype("string").unique().tolist())
        ):
            stint_fit = driver_fit[driver_fit["stint_display"] == stint_display].copy()
            if stint_fit.empty:
                continue
            figure.add_trace(
                go.Scatter(
                    x=stint_fit["lap_number"],
                    y=stint_fit["fit_lap_time_s"],
                    mode="lines",
                    name=f"{driver} fit stint {stint_display}",
                    line={"width": 2, "color": FIT_COLORS[fit_index % len(FIT_COLORS)]},
                    showlegend=True,
                    legendgroup=f"{driver}-fit-{stint_display}",
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
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0.0},
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
    for sector_number in [1, 2, 3]:
        sector_rows = track_compare_rows[track_compare_rows["sector_number"] == sector_number].copy()
        if sector_rows.empty:
            continue
        faster_driver = sector_rows["faster_driver_segment"].iloc[0]
        color = color_map.get(faster_driver, "#6c757d")
        figure.add_trace(
            go.Scatter(
                x=sector_rows["plot_x"],
                y=sector_rows["plot_y"],
                mode="lines",
                line={"color": color, "width": 5},
                name=f"Sector {sector_number}",
                showlegend=False,
                hovertemplate=(
                    f"Sector {sector_number}<br>"
                    f"Faster: {faster_driver if pd.notna(faster_driver) else 'Tie'}<extra></extra>"
                ),
            )
        )

    for legend_label, color in [(driver_a, color_map[driver_a]), (driver_b, color_map[driver_b])]:
        figure.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="lines",
                line={"color": color, "width": 5},
                name=legend_label,
                showlegend=True,
            )
        )

    figure.update_layout(
        title=f"Track Comparison ({driver_a} vs {driver_b})",
        margin={"l": 24, "r": 24, "t": 64, "b": 24},
        height=560,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "left", "x": 0.0},
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
