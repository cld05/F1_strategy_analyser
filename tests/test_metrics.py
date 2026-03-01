from __future__ import annotations

import math

import pandas as pd

from f1analyser.laps import classify_clean_laps
from f1analyser.metrics import compute_pit_loss_per_stop, compute_stint_metrics
from f1analyser.pits_stints import detect_pits


def _base_laps() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "driver": ["VER"] * 8,
            "lap_number": [1, 2, 3, 4, 5, 6, 7, 8],
            "lap_time": [100.0, 101.0, 120.0, 130.0, 102.0, 103.0, 104.0, 105.0],
            "track_status": ["1", "1", "1", "1", "1", "1", "1", "1"],
            "pit_in_time": [pd.NA, pd.NA, 1.0, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA],
            "pit_out_time": [pd.NA, pd.NA, pd.NA, 1.0, pd.NA, pd.NA, pd.NA, pd.NA],
            "compound": ["MEDIUM"] * 8,
            "tyre_life": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        }
    )


def test_compute_pit_loss_per_stop_computable() -> None:
    laps = classify_clean_laps(_base_laps())
    pits = detect_pits(laps)

    pit_loss = compute_pit_loss_per_stop(laps, pits)

    row = pit_loss.iloc[0]
    assert bool(row["is_computable"]) is True
    assert int(row["n_pre"]) == 2
    assert int(row["n_post"]) == 3
    assert math.isclose(float(row["baseline_s"]), 102.0)
    assert math.isclose(float(row["pit_loss_s"]), 46.0)


def test_compute_pit_loss_per_stop_noncomputable_first_lap() -> None:
    laps = pd.DataFrame(
        {
            "driver": ["VER", "VER", "VER", "VER"],
            "lap_number": [1, 2, 3, 4],
            "lap_time": [120.0, 130.0, 102.0, 103.0],
            "track_status": ["1", "1", "1", "1"],
            "pit_in_time": [1.0, pd.NA, pd.NA, pd.NA],
            "pit_out_time": [pd.NA, 1.0, pd.NA, pd.NA],
            "compound": ["MEDIUM"] * 4,
            "tyre_life": [1.0, 2.0, 3.0, 4.0],
        }
    )
    classified = classify_clean_laps(laps)
    pits = detect_pits(classified)

    pit_loss = compute_pit_loss_per_stop(classified, pits)

    row = pit_loss.iloc[0]
    assert bool(row["is_computable"]) is False
    assert "both sides" in str(row["reason_not_computable"])


def test_compute_pit_loss_sc_vsc_toggle() -> None:
    laps = _base_laps().copy()
    laps.loc[laps["lap_number"] == 3, "track_status"] = "4"
    classified = classify_clean_laps(laps)
    pits = detect_pits(classified)

    included = compute_pit_loss_per_stop(classified, pits, include_sc_vsc_in_aggregate=True)
    excluded = compute_pit_loss_per_stop(classified, pits, include_sc_vsc_in_aggregate=False)

    assert bool(included.iloc[0]["is_sc_vsc_affected"]) is True
    assert bool(included.iloc[0]["included_in_aggregate"]) is True
    assert bool(excluded.iloc[0]["included_in_aggregate"]) is False


def test_compute_stint_metrics_with_threshold_behavior() -> None:
    laps = pd.DataFrame(
        {
            "driver": ["VER"] * 8,
            "lap_number": [1, 2, 3, 4, 5, 6, 7, 8],
            "lap_time": [100.0, 101.0, 102.0, 103.0, 104.0, 111.0, None, None],
            "is_clean": [True, True, True, True, True, False, False, False],
        }
    )
    stints = pd.DataFrame(
        {
            "driver": ["VER", "VER"],
            "stint_id": [1, 2],
            "start_lap": [1, 6],
            "end_lap": [5, 8],
            "warnings": ["", ""],
        }
    )

    out = compute_stint_metrics(laps, stints)

    first = out[out["stint_id"] == 1].iloc[0]
    second = out[out["stint_id"] == 2].iloc[0]

    assert math.isclose(float(first["pace_median_s"]), 102.0)
    assert math.isclose(float(first["deg_slope_s_per_lap"]), 1.0)
    assert math.isclose(float(first["deg_delta_first_last_s"]), 4.0)

    assert pd.isna(second["pace_median_s"])
    assert pd.isna(second["deg_slope_s_per_lap"])
    assert "insufficient clean laps" in str(second["warnings"])
