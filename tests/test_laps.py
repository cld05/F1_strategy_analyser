from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import logging

import pandas as pd
import pandas.testing as pdt
import pytest

from f1analyser.laps import (
    REQUIRED_LAPS_COLUMNS,
    build_canonical_laps,
    classify_clean_laps,
    drop_drivers_with_telemetry_gaps,
    load_or_build_canonical_laps,
)


@dataclass
class DummyEvent:
    Year: int = 2024
    RoundNumber: int = 9
    EventDate: datetime = datetime(2024, 5, 26)


class DummyLaps:
    def __init__(self, dataframe: pd.DataFrame) -> None:
        self._dataframe = dataframe

    def pick_drivers(self, drivers: list[str]) -> pd.DataFrame:
        return self._dataframe[self._dataframe["Driver"].isin(drivers)].copy()


class DummySession:
    def __init__(self, laps_df: pd.DataFrame) -> None:
        self.event = DummyEvent()
        self.name = "Race"
        self.laps = DummyLaps(laps_df)


def _fixture_laps() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Driver": ["VER", "VER", "NOR", "NOR"],
            "DriverNumber": [1, 1, 4, 4],
            "LapNumber": [1, 2, 1, 2],
            "LapTime": [
                pd.Timedelta(seconds=80.1),
                pd.Timedelta(seconds=79.9),
                pd.Timedelta(seconds=80.4),
                pd.Timedelta(seconds=80.0),
            ],
            "Compound": ["MEDIUM", "MEDIUM", "SOFT", "SOFT"],
            "TyreLife": [1, 2, 1, 2],
            "TrackStatus": ["1", "1", "1", "1"],
            "Position": [1, 1, 2, 2],
            "GapToLeader": [0.0, 0.0, 1.1, 1.2],
            "PitInTime": [pd.NaT, pd.NaT, pd.NaT, pd.NaT],
            "PitOutTime": [pd.NaT, pd.NaT, pd.NaT, pd.NaT],
            "Sector1Time": [
                pd.Timedelta(seconds=27.0),
                pd.Timedelta(seconds=26.9),
                pd.Timedelta(seconds=27.2),
                pd.Timedelta(seconds=27.0),
            ],
            "Sector2Time": [
                pd.Timedelta(seconds=26.3),
                pd.Timedelta(seconds=26.2),
                pd.Timedelta(seconds=26.4),
                pd.Timedelta(seconds=26.3),
            ],
            "Sector3Time": [
                pd.Timedelta(seconds=26.8),
                pd.Timedelta(seconds=26.8),
                pd.Timedelta(seconds=26.8),
                pd.Timedelta(seconds=26.7),
            ],
        }
    )


def test_build_canonical_laps_schema_types_and_non_empty() -> None:
    session = DummySession(_fixture_laps())

    laps = build_canonical_laps(session, ["VER", "NOR"])

    assert list(laps.columns) == REQUIRED_LAPS_COLUMNS
    assert not laps.empty
    assert len(laps) == 4
    assert set(laps["driver"].tolist()) == {"VER", "NOR"}
    assert laps["season"].dtype == "int64"
    assert laps["round"].dtype == "int64"
    assert str(laps["session_type"].dtype) == "string"
    assert str(laps["driver"].dtype) == "string"
    assert str(laps["lap_number"].dtype) == "Int64"
    assert str(laps["lap_time"].dtype) == "float64"
    assert str(laps["tyre_life"].dtype) == "Float64"
    assert str(laps["position"].dtype) == "Float64"


def test_load_or_build_canonical_laps_uses_cache_first(tmp_path: Path) -> None:
    session = DummySession(_fixture_laps())

    first_df, first_from_cache, cache_path = load_or_build_canonical_laps(
        session,
        ["VER", "NOR"],
        cache_dir=tmp_path,
    )
    assert first_from_cache is False
    assert cache_path.exists()

    class FailingSession(DummySession):
        def __init__(self) -> None:
            self.event = DummyEvent()
            self.name = "Race"
            self.laps = "invalid"

    second_df, second_from_cache, second_cache_path = load_or_build_canonical_laps(
        FailingSession(),
        ["VER", "NOR"],
        cache_dir=tmp_path,
    )

    assert second_from_cache is True
    assert second_cache_path == cache_path
    pdt.assert_frame_equal(first_df, second_df, check_dtype=False)


def test_classify_clean_laps_red_flag_boundary_and_invariants(
    caplog: pytest.LogCaptureFixture,
) -> None:
    base_laps = pd.DataFrame(
        {
            "season": [2024] * 8,
            "round": [9] * 8,
            "session_type": ["Race"] * 8,
            "driver": ["VER"] * 4 + ["NOR"] * 4,
            "driver_id": ["1"] * 4 + ["4"] * 4,
            "lap_number": [1, 2, 3, 4, 1, 2, 3, 4],
            "lap_time": [80.0, 81.0, 82.0, 83.0, 80.5, 81.2, 82.4, 83.0],
            "compound": ["MEDIUM"] * 8,
            "tyre_life": [1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0],
            "track_status": ["1", "5", "1", "1", "1", "3", "1", "1"],
            "position": [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0],
            "gap": [0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0],
            "pit_in_time": [pd.NA] * 8,
            "pit_out_time": [pd.NA] * 8,
            "sector1": [27.0] * 8,
            "sector2": [26.0] * 8,
            "sector3": [27.0] * 8,
        }
    )

    caplog.set_level(logging.WARNING)
    classified = classify_clean_laps(base_laps)

    ver = classified[classified["driver"] == "VER"].sort_values("lap_number", kind="stable")
    assert bool(ver.loc[ver["lap_number"] == 1, "exclude_red_flag_pre"].iloc[0]) is True
    assert bool(ver.loc[ver["lap_number"] == 2, "is_red_flag_lap"].iloc[0]) is True
    assert bool(ver.loc[ver["lap_number"] == 3, "exclude_red_flag_post"].iloc[0]) is True

    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert any("unknown track status code 3" in warning for warning in warnings)

    summary = (
        classified.groupby("driver", as_index=False)[["n_total", "n_clean", "n_excluded"]]
        .sum()
        .sort_values("driver", kind="stable")
    )
    for _, row in summary.iterrows():
        assert int(row["n_clean"]) + int(row["n_excluded"]) == int(row["n_total"])


def test_drop_drivers_with_telemetry_gaps_logs_and_drops(
    caplog: pytest.LogCaptureFixture,
) -> None:
    laps = pd.DataFrame(
        {
            "driver": ["VER"] * 10 + ["NOR"] * 10,
            "lap_number": [*range(1, 11), *range(1, 11)],
            "lap_time": [80.0] * 8 + [pd.NA, pd.NA] + [81.0] * 10,
            "track_status": ["1"] * 20,
            "pit_in_time": [pd.NA] * 20,
            "pit_out_time": [pd.NA] * 20,
        }
    )
    classified = classify_clean_laps(laps)

    caplog.set_level(logging.WARNING)
    filtered, dropped = drop_drivers_with_telemetry_gaps(classified, threshold=0.10)

    assert dropped == ["VER"]
    assert set(filtered["driver"].unique().tolist()) == {"NOR"}
    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert any("telemetry gaps exceed 10% for driver VER" in warning for warning in warnings)
