from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from f1analyser.delta import REQUIRED_DELTA_COLUMNS, DeltaLapsError, build_delta_laps


def _fixture_laps_filtered() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "driver": pd.Series(["VER", "VER", "VER", "NOR", "NOR", "NOR", "NOR"], dtype="string"),
            "lap_number": pd.Series([1, 2, 3, 1, 2, 3, 4], dtype="Int64"),
            "lap_time_s": pd.Series([80.0, 81.0, 82.0, 79.5, 81.5, np.nan, 83.0], dtype="float64"),
            "is_valid_lap_time": [True, True, True, True, True, False, True],
            "include_for_delta_plot": [True, False, True, True, True, False, True],
            "filter_reason_list": [
                [],
                ["pit_lap"],
                [],
                [],
                [],
                ["missing_lap_time"],
                [],
            ],
        }
    )


def test_build_delta_laps_schema_and_sign_convention() -> None:
    delta_laps = build_delta_laps(_fixture_laps_filtered(), ["VER", "NOR"])

    assert list(delta_laps.columns) == REQUIRED_DELTA_COLUMNS
    assert delta_laps["driver_a"].tolist() == ["VER", "VER", "VER", "VER"]
    assert delta_laps["driver_b"].tolist() == ["NOR", "NOR", "NOR", "NOR"]
    assert delta_laps["lap_number"].tolist() == [1, 2, 3, 4]
    assert delta_laps.loc[0, "lap_delta_s"] == pytest.approx(0.5)
    assert delta_laps.loc[0, "cum_delta_s"] == pytest.approx(0.5)
    assert bool(delta_laps.loc[0, "valid_for_delta"]) is True


def test_build_delta_laps_marks_filtered_and_missing_rows() -> None:
    delta_laps = build_delta_laps(_fixture_laps_filtered(), ["VER", "NOR"])

    assert bool(delta_laps.loc[1, "valid_for_delta"]) is False
    assert delta_laps.loc[1, "exclude_reason"] == "driver_a_pit_lap"
    assert bool(delta_laps.loc[2, "valid_for_delta"]) is False
    assert delta_laps.loc[2, "exclude_reason"] == "invalid_driver_b_lap_time,driver_b_missing_lap_time"
    assert bool(delta_laps.loc[3, "valid_for_delta"]) is False
    assert delta_laps.loc[3, "exclude_reason"] == "missing_driver_a_lap"


def test_build_delta_laps_cumulative_times_stop_only_on_missing_pair_rows() -> None:
    delta_laps = build_delta_laps(_fixture_laps_filtered(), ["VER", "NOR"])

    assert delta_laps.loc[0, "cum_time_a_s"] == pytest.approx(80.0)
    assert delta_laps.loc[1, "cum_time_a_s"] == pytest.approx(161.0)
    assert pd.isna(delta_laps.loc[2, "cum_time_a_s"])
    assert pd.isna(delta_laps.loc[3, "cum_time_a_s"])
    assert delta_laps.loc[1, "cum_delta_s"] == pytest.approx(0.0)


def test_build_delta_laps_requires_exactly_two_drivers() -> None:
    with pytest.raises(DeltaLapsError):
        build_delta_laps(_fixture_laps_filtered(), ["VER"])
