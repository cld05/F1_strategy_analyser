from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from f1analyser.laps import classify_clean_laps
from f1analyser.pits_stints import build_stints, detect_pits


def _build_driver_rows(
    driver: str,
    compounds: list[str],
    pit_in_laps: set[int],
    pit_out_laps: set[int],
    *,
    track_status_by_lap: dict[int, str] | None = None,
    tyre_life_by_lap: dict[int, float | None] | None = None,
    missing_lap_time_laps: set[int] | None = None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for lap in range(1, len(compounds) + 1):
        track_status = "1"
        if track_status_by_lap and lap in track_status_by_lap:
            track_status = track_status_by_lap[lap]

        tyre_life: float | None = float(lap)
        if tyre_life_by_lap and lap in tyre_life_by_lap:
            tyre_life = tyre_life_by_lap[lap]

        lap_time: float | None = 80.0 + lap / 10
        if missing_lap_time_laps and lap in missing_lap_time_laps:
            lap_time = None

        rows.append(
            {
                "season": 2025,
                "round": 1,
                "session_type": "Race",
                "driver": driver,
                "driver_id": driver,
                "lap_number": lap,
                "lap_time": lap_time,
                "compound": compounds[lap - 1],
                "tyre_life": tyre_life,
                "track_status": track_status,
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
            )
        )
        rows.extend(
            _build_driver_rows(
                "NOR",
                ["MEDIUM", "MEDIUM", "MEDIUM", "MEDIUM", "HARD", "HARD", "HARD", "SOFT", "SOFT"],
                {4, 7},
                {5, 8},
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
            )
        )
        rows.extend(
            _build_driver_rows(
                "NOR",
                ["SOFT", "SOFT", "SOFT", "HARD", "HARD", "HARD", "HARD", "MEDIUM", "MEDIUM", "MEDIUM"],
                {3, 7},
                {4, 8},
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
            )
        )
        rows.extend(
            _build_driver_rows(
                "NOR",
                ["SOFT", "SOFT", "MEDIUM", "MEDIUM", "MEDIUM", "HARD", "HARD", "HARD", "SOFT", "SOFT"],
                {2, 5, 8},
                {3, 6, 9},
            )
        )
        return pd.DataFrame(rows)

    raise AssertionError(f"Unknown fixture: {name}")


def _expected_counts() -> dict[str, dict[str, dict[str, int]]]:
    fixture_path = Path(__file__).parent / "fixtures" / "pit_stint_counts.json"
    with fixture_path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    return raw


@pytest.mark.parametrize("fixture_name", ["spain_rd10", "austria_rd11", "britain_rd12"])
def test_fixture_pit_and_stint_counts(fixture_name: str) -> None:
    expected = _expected_counts()[fixture_name]
    laps = classify_clean_laps(_fixture_laps(fixture_name))
    pits = detect_pits(laps)
    stints = build_stints(laps, pits)

    for driver, driver_expected in expected.items():
        pit_count = int((pits["driver"] == driver).sum())
        stint_count = int((stints["driver"] == driver).sum())
        assert pit_count == int(driver_expected["pit_count"])
        assert stint_count == int(driver_expected["stint_count"])


def test_pit_missing_marker_rules_and_two_lap_window() -> None:
    rows = []
    rows.extend(
        _build_driver_rows(
            "VER",
            ["MEDIUM", "MEDIUM", "MEDIUM", "MEDIUM", "MEDIUM", "MEDIUM", "MEDIUM"],
            {3},
            set(),
            missing_lap_time_laps={4, 5},
        )
    )
    rows.extend(
        _build_driver_rows(
            "NOR",
            ["SOFT", "SOFT", "SOFT", "SOFT", "SOFT", "SOFT", "SOFT"],
            {5},
            {3, 7},
        )
    )
    laps = classify_clean_laps(pd.DataFrame(rows))
    pits = detect_pits(laps)

    ver_warning = " ".join(pits[pits["driver"] == "VER"]["warnings"].tolist())
    nor_warning = " ".join(pits[pits["driver"] == "NOR"]["warnings"].tolist())

    assert "missing out-lap; assuming pit_out_lap=4" in ver_warning
    assert "missing in-lap; assuming pit_in_lap=2" in nor_warning
    assert "2-lap pit window accepted" in nor_warning


def test_red_flag_tyre_continuity_rule() -> None:
    continuous = pd.DataFrame(
        _build_driver_rows(
            "VER",
            ["MEDIUM", "MEDIUM", "MEDIUM", "MEDIUM", "MEDIUM", "MEDIUM"],
            set(),
            set(),
            track_status_by_lap={2: "5", 3: "5"},
            tyre_life_by_lap={1: 1.0, 4: 4.0},
        )
    )
    split = pd.DataFrame(
        _build_driver_rows(
            "NOR",
            ["SOFT", "SOFT", "SOFT", "SOFT", "SOFT", "SOFT"],
            set(),
            set(),
            track_status_by_lap={2: "5", 3: "5"},
            tyre_life_by_lap={1: 1.0, 4: 1.0},
        )
    )

    laps = classify_clean_laps(pd.concat([continuous, split], ignore_index=True))
    pits = detect_pits(laps)
    stints = build_stints(laps, pits)

    assert int((stints["driver"] == "VER").sum()) == 1
    assert int((stints["driver"] == "NOR").sum()) == 2
