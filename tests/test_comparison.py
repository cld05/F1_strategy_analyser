from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from f1analyser.comparison import (
    build_comparison_summary,
    build_comparison_windows,
    load_or_build_comparison_tables,
)
from f1analyser.laps import classify_clean_laps
from f1analyser.pits_stints import build_stints, detect_pits


def _build_driver_rows(
    driver: str,
    compounds: list[str],
    pit_in_laps: set[int],
    pit_out_laps: set[int],
    *,
    lap_offset: float = 0.0,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for lap in range(1, len(compounds) + 1):
        rows.append(
            {
                "season": 2025,
                "round": 1,
                "session_type": "Race",
                "driver": driver,
                "driver_id": driver,
                "lap_number": lap,
                "lap_time": 80.0 + lap / 10 + lap_offset,
                "compound": compounds[lap - 1],
                "tyre_life": float(lap),
                "track_status": "1",
                "position": 1.0,
                "gap": 0.0,
                "pit_in_time": 1.0 if lap in pit_in_laps else pd.NA,
                "pit_out_time": 1.0 if lap in pit_out_laps else pd.NA,
                "sector1": 27.0,
                "sector2": 26.0,
                "sector3": 27.0,
            }
        )
    return rows


def _fixture_laps(name: str) -> pd.DataFrame:
    if name == "spain_rd10":
        rows = []
        rows.extend(
            _build_driver_rows(
                "VER",
                ["MEDIUM", "MEDIUM", "MEDIUM", "HARD", "HARD", "HARD", "SOFT", "SOFT", "SOFT"],
                {3, 6},
                {4, 7},
                lap_offset=0.0,
            )
        )
        rows.extend(
            _build_driver_rows(
                "NOR",
                ["MEDIUM", "MEDIUM", "MEDIUM", "MEDIUM", "HARD", "HARD", "HARD", "SOFT", "SOFT"],
                {4, 7},
                {5, 8},
                lap_offset=0.08,
            )
        )
        return pd.DataFrame(rows)

    if name == "austria_rd11":
        rows = []
        rows.extend(
            _build_driver_rows(
                "VER",
                ["SOFT", "SOFT", "MEDIUM", "MEDIUM", "MEDIUM", "HARD", "HARD", "HARD", "SOFT", "SOFT"],
                {2, 5, 8},
                {3, 6, 9},
                lap_offset=0.0,
            )
        )
        rows.extend(
            _build_driver_rows(
                "NOR",
                ["SOFT", "SOFT", "SOFT", "HARD", "HARD", "HARD", "HARD", "MEDIUM", "MEDIUM", "MEDIUM"],
                {3, 7},
                {4, 8},
                lap_offset=0.10,
            )
        )
        return pd.DataFrame(rows)

    if name == "britain_rd12":
        rows = []
        rows.extend(
            _build_driver_rows(
                "VER",
                ["MEDIUM", "MEDIUM", "MEDIUM", "HARD", "HARD", "HARD", "HARD", "SOFT", "SOFT", "SOFT"],
                {3, 7},
                {4, 8},
                lap_offset=0.0,
            )
        )
        rows.extend(
            _build_driver_rows(
                "NOR",
                ["SOFT", "SOFT", "MEDIUM", "MEDIUM", "MEDIUM", "HARD", "HARD", "HARD", "SOFT", "SOFT"],
                {2, 5, 8},
                {3, 6, 9},
                lap_offset=0.15,
            )
        )
        return pd.DataFrame(rows)

    raise AssertionError(f"Unknown fixture: {name}")


def _expectations() -> dict[str, dict[str, object]]:
    path = Path(__file__).parent / "fixtures" / "pit_stint_counts.json"
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


@pytest.mark.parametrize("fixture_name", ["spain_rd10", "austria_rd11", "britain_rd12"])
def test_round_regression_counts_and_residual_bounds(fixture_name: str, tmp_path: Path) -> None:
    expected = _expectations()[fixture_name]

    laps = classify_clean_laps(_fixture_laps(fixture_name))
    pits = detect_pits(laps)
    stints = build_stints(laps, pits)
    windows, summary, _, _, _ = load_or_build_comparison_tables(
        laps,
        pits,
        stints,
        selected_drivers=["VER", "NOR"],
        cache_dir=tmp_path,
    )

    assert not windows.empty
    assert not summary.empty

    for driver in ("VER", "NOR"):
        assert int((pits["driver"] == driver).sum()) == int(expected[driver]["pit_count"])
        assert int((stints["driver"] == driver).sum()) == int(expected[driver]["stint_count"])

    residual = float(summary.iloc[0]["residual_s"])
    assert abs(residual) < float(expected["residual_bound_pair"])


def test_l_common_block_message() -> None:
    laps = pd.DataFrame(
        {
            "season": [2025, 2025],
            "round": [1, 1],
            "session_type": ["Race", "Race"],
            "driver": ["VER", "NOR"],
            "lap_number": [1, 1],
            "lap_time": [pd.NA, pd.NA],
            "is_clean": [True, True],
        }
    )
    pits = pd.DataFrame(columns=["driver", "stop_index", "pit_in_lap", "pit_out_lap"])
    stints = pd.DataFrame(columns=["driver", "stint_id", "start_lap", "end_lap"])

    windows = build_comparison_windows(laps, stints, selected_drivers=["VER", "NOR"])
    summary = build_comparison_summary(laps, pits, stints, windows, selected_drivers=["VER", "NOR"])

    assert int(summary.iloc[0]["L_common"]) == 0
    assert summary.iloc[0]["warnings"] == "driver with less than one lap, no comparison possible"


def test_load_or_build_comparison_tables_cache_first(tmp_path: Path) -> None:
    laps = classify_clean_laps(_fixture_laps("spain_rd10"))
    pits = detect_pits(laps)
    stints = build_stints(laps, pits)

    first_windows, first_summary, first_cache_hit, windows_path, summary_path = (
        load_or_build_comparison_tables(
            laps,
            pits,
            stints,
            selected_drivers=["VER", "NOR"],
            cache_dir=tmp_path,
        )
    )

    assert first_cache_hit is False
    assert windows_path.exists()
    assert summary_path.exists()

    second_windows, second_summary, second_cache_hit, _, _ = load_or_build_comparison_tables(
        laps,
        pits,
        stints,
        selected_drivers=["VER", "NOR"],
        cache_dir=tmp_path,
    )

    assert second_cache_hit is True
    assert len(first_windows) == len(second_windows)
    assert len(first_summary) == len(second_summary)
