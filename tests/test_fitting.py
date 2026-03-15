from __future__ import annotations

import pandas as pd
import pytest

from f1analyser.fitting import LapTrendError, build_lap_trend_inputs


def _fixture_laps_filtered() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "driver": pd.Series(
                ["VER", "VER", "VER", "VER", "NOR", "NOR", "NOR", "NOR"],
                dtype="string",
            ),
            "lap_number": pd.Series([1, 2, 3, 4, 1, 2, 3, 4], dtype="Int64"),
            "lap_time_s": pd.Series([80.0, 80.2, 80.4, 80.6, 79.8, 80.0, 80.3, 80.7], dtype="float64"),
            "compound": pd.Series(
                ["MEDIUM", "MEDIUM", pd.NA, "MEDIUM", "SOFT", "SOFT", "SOFT", "SOFT"],
                dtype="string",
            ),
            "stint": pd.Series([1, 1, 2, 2, 1, 1, 1, 2], dtype="Int64"),
            "include_for_lap_time_plot": [True, True, True, True, True, True, True, True],
            "include_for_fit": [True, True, True, True, True, True, True, False],
        }
    )


def test_build_lap_trend_inputs_uses_filtered_rows_and_warns_on_missing_context() -> None:
    plot_rows, fit_rows, stint_insights, diagnostics = build_lap_trend_inputs(
        _fixture_laps_filtered(),
        ["VER", "NOR"],
        polynomial_degree=2,
    )

    assert set(plot_rows["driver"].tolist()) == {"VER", "NOR"}
    assert plot_rows["lap_number"].tolist() == [1, 2, 3, 4, 1, 2, 3, 4]
    assert not fit_rows.empty
    assert set(fit_rows["stint_display"].tolist()) == {"1"}
    assert not stint_insights.empty
    assert diagnostics.warnings == [
        "Missing compound values for VER; using UNKNOWN in lap trend plot.",
        "Insufficient fit points for VER stint 1 at degree 2; fit curve omitted.",
        "Insufficient fit points for VER stint 2 at degree 2; fit curve omitted.",
        "Insufficient fit points for NOR stint 2 at degree 2; fit curve omitted.",
    ]


def test_build_lap_trend_inputs_requires_positive_degree() -> None:
    with pytest.raises(LapTrendError):
        build_lap_trend_inputs(_fixture_laps_filtered(), ["VER", "NOR"], polynomial_degree=0)
