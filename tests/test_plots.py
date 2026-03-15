from __future__ import annotations

import pandas as pd

from f1analyser.plots import (
    build_cumulative_delta_figure,
    build_lap_time_trend_figure,
    build_telemetry_compare_figure,
    build_per_lap_delta_figure,
    build_track_compare_figure,
)


def test_build_lap_time_trend_figure_renders_driver_scatter_and_fit_traces() -> None:
    plot_rows = pd.DataFrame(
        {
            "driver": pd.Series(["VER", "VER", "NOR"], dtype="string"),
            "lap_number": [1, 2, 1],
            "lap_time_s": [80.0, 80.5, 79.8],
            "compound_display": pd.Series(["MEDIUM", "MEDIUM", "SOFT"], dtype="string"),
            "stint_display": pd.Series(["1", "1", "1"], dtype="string"),
        }
    )
    fit_rows = pd.DataFrame(
        {
            "driver": pd.Series(["VER", "NOR"], dtype="string"),
            "lap_number": [1.5, 1.0],
            "fit_lap_time_s": [80.2, 79.8],
        }
    )

    figure = build_lap_time_trend_figure(
        plot_rows,
        fit_rows,
        ("VER", "NOR"),
        polynomial_degree=2,
    )

    assert len(figure.data) == 4
    assert figure.layout.title.text == "Lap Time Trends (Polynomial degree 2)"


def test_build_delta_figures_render_expected_titles_and_axes() -> None:
    delta_plot_rows = pd.DataFrame(
        {
            "lap_number": [1, 2],
            "driver_a": pd.Series(["VER", "VER"], dtype="string"),
            "driver_b": pd.Series(["NOR", "NOR"], dtype="string"),
            "lap_delta_s": [0.5, -0.2],
            "cum_delta_s": [0.5, 0.3],
        }
    )

    cumulative_figure = build_cumulative_delta_figure(delta_plot_rows)
    per_lap_figure = build_per_lap_delta_figure(delta_plot_rows)

    assert len(cumulative_figure.data) == 1
    assert cumulative_figure.layout.title.text == "Cumulative Delta (VER - NOR)"
    assert cumulative_figure.layout.yaxis.title.text == "Cumulative delta (s)"
    assert len(per_lap_figure.data) == 1
    assert per_lap_figure.layout.title.text == "Per-Lap Delta (VER - NOR)"
    assert per_lap_figure.layout.yaxis.title.text == "Lap delta (s)"


def test_build_track_compare_figure_renders_segment_lines() -> None:
    compare_rows = pd.DataFrame(
        {
            "driver_a": pd.Series(["VER", "VER", "VER"], dtype="string"),
            "driver_b": pd.Series(["NOR", "NOR", "NOR"], dtype="string"),
            "distance_m": [0.0, 10.0, 20.0],
            "plot_x": [0.0, 1.0, 2.0],
            "plot_y": [0.0, 0.5, 1.0],
            "faster_driver_segment": pd.Series(["VER", "NOR", pd.NA], dtype="string"),
        }
    )

    figure = build_track_compare_figure(compare_rows)

    assert len(figure.data) == 2
    assert figure.layout.title.text == "Track Comparison (VER vs NOR)"


def test_build_telemetry_compare_figure_renders_three_stacked_panels() -> None:
    telemetry_compare_rows = pd.DataFrame(
        {
            "driver_a": pd.Series(["VER", "VER"], dtype="string"),
            "driver_b": pd.Series(["NOR", "NOR"], dtype="string"),
            "distance_m": [0.0, 10.0],
            "speed_a": [100.0, 105.0],
            "speed_b": [98.0, 103.0],
            "throttle_a": [70.0, 80.0],
            "throttle_b": [68.0, 77.0],
            "brake_a": [0.0, 5.0],
            "brake_b": [1.0, 3.0],
        }
    )

    figure = build_telemetry_compare_figure(telemetry_compare_rows)

    assert len(figure.data) == 6
    assert figure.layout.title.text == "Telemetry Comparison (VER vs NOR)"
    assert figure.layout.yaxis.title.text == "Throttle"
    assert figure.layout.yaxis2.title.text == "Brake"
    assert figure.layout.yaxis3.title.text == "Speed"
