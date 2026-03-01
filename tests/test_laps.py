from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import pandas.testing as pdt

from f1analyser.laps import REQUIRED_LAPS_COLUMNS, build_canonical_laps, load_or_build_canonical_laps


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
