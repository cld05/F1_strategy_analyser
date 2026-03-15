from __future__ import annotations

import pandas as pd

from f1analyser.plots import build_lap_time_trend_figure


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
