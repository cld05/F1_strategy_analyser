from __future__ import annotations

import pandas as pd

from f1analyser.filters import build_laps_filtered


def _fixture_laps() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "season": [2024, 2024, 2024],
            "round": [9, 9, 9],
            "session_type": pd.Series(["Race", "Race", "Race"], dtype="string"),
            "event_name": pd.Series(["Monaco Grand Prix"] * 3, dtype="string"),
            "driver": pd.Series(["VER", "VER", "NOR"], dtype="string"),
            "driver_number": pd.Series(["1", "1", "4"], dtype="string"),
            "team": pd.Series(["Red Bull", "Red Bull", "McLaren"], dtype="string"),
            "lap_number": pd.Series([1, 2, 1], dtype="Int64"),
            "lap_time_s": pd.Series([80.1, None, 81.2], dtype="float64"),
            "compound": pd.Series(["MEDIUM", "MEDIUM", "SOFT"], dtype="string"),
            "tyre_life": pd.Series([1.0, 2.0, 1.0], dtype="Float64"),
            "stint": pd.Series([1, 1, 1], dtype="Int64"),
            "track_status": pd.Series(["1", "4", "1"], dtype="string"),
            "position": pd.Series([1.0, 1.0, 2.0], dtype="Float64"),
            "pit_in_time": [pd.NaT, pd.NaT, pd.NaT],
            "pit_out_time": [pd.NaT, pd.NaT, pd.NaT],
            "is_pit_in_lap": [False, False, False],
            "is_pit_out_lap": [False, True, False],
            "is_pit_lap": [False, True, False],
            "is_sc_vsc_lap": [False, True, False],
            "is_valid_lap_time": [True, False, True],
        }
    )


def test_build_laps_filtered_marks_reasons_and_warnings() -> None:
    filtered, diagnostics = build_laps_filtered(_fixture_laps(), exclude_sc_vsc=True)

    assert "filter_reason_list" in filtered.columns
    assert "include_for_lap_time_plot" in filtered.columns
    assert "include_for_delta_plot" in filtered.columns
    assert "include_for_fit" in filtered.columns
    assert diagnostics.warnings == ["Dropped 1 laps from filtered views."]
    assert len(diagnostics.dropped_laps) == 1
    assert filtered.loc[1, "filter_reason_list"] == ["missing_lap_time", "pit_lap", "sc_vsc_lap"]
    assert bool(filtered.loc[1, "include_for_lap_time_plot"]) is False


def test_build_laps_filtered_sc_vsc_toggle_is_optional() -> None:
    filtered, diagnostics = build_laps_filtered(_fixture_laps(), exclude_sc_vsc=False)

    assert filtered.loc[1, "filter_reason_list"] == ["missing_lap_time", "pit_lap"]
    assert diagnostics.warnings == ["Dropped 1 laps from filtered views."]
