from __future__ import annotations

import pandas as pd

from f1analyser.telemetry import build_corner_markers, build_telemetry_compare_rows, build_track_compare_rows


class DummyLap:
    def __init__(self, driver: str, lap_number: int, telemetry: pd.DataFrame, **fields: object) -> None:
        self.Driver = driver
        self.LapNumber = lap_number
        self._telemetry = telemetry
        for key, value in fields.items():
            setattr(self, key, value)

    def get_telemetry(self) -> pd.DataFrame:
        return self._telemetry.copy()


def test_build_track_compare_rows_resamples_to_shared_distance_grid() -> None:
    lap_a = DummyLap(
        "VER",
        10,
        pd.DataFrame(
            {
                "Distance": [0.0, 10.0, 20.0, 30.0],
                "X": [0.0, 1.0, 2.0, 3.0],
                "Y": [0.0, 1.0, 1.5, 2.0],
            }
        ),
        Sector1Time=pd.Timedelta(seconds=25.0),
        Sector2Time=pd.Timedelta(seconds=27.0),
        Sector3Time=pd.Timedelta(seconds=28.0),
    )
    lap_b = DummyLap(
        "NOR",
        12,
        pd.DataFrame(
            {
                "Distance": [0.0, 15.0, 30.0],
                "X": [0.0, 1.4, 3.2],
                "Y": [0.0, 1.2, 2.2],
            }
        ),
        Sector1Time=pd.Timedelta(seconds=24.0),
        Sector2Time=pd.Timedelta(seconds=28.0),
        Sector3Time=pd.Timedelta(seconds=29.0),
    )

    compare_rows, diagnostics = build_track_compare_rows(lap_a, lap_b, distance_step_m=10.0)

    assert compare_rows["distance_m"].tolist() == [0.0, 10.0, 20.0, 30.0]
    assert compare_rows["driver_a"].tolist() == ["VER"] * 4
    assert compare_rows["driver_b"].tolist() == ["NOR"] * 4
    assert compare_rows["sector_number"].tolist() == [1, 2, 3, 3]
    assert compare_rows["sector_time_driver_a_s"].iloc[0] == 25.0
    assert compare_rows["sector_time_driver_b_s"].iloc[0] == 24.0
    assert compare_rows["sector_delta_s"].iloc[0] == 1.0
    assert diagnostics.warnings == []


def test_build_track_compare_rows_segment_classification_is_deterministic() -> None:
    lap_a = DummyLap(
        "VER",
        4,
        pd.DataFrame(
            {
                "Distance": [0.0, 10.0, 20.0],
                "X": [0.0, 1.0, 2.0],
                "Y": [0.0, 0.5, 1.0],
            }
        ),
        Sector1Time=pd.Timedelta(seconds=24.0),
        Sector2Time=pd.Timedelta(seconds=30.0),
        Sector3Time=pd.Timedelta(seconds=26.0),
    )
    lap_b = DummyLap(
        "NOR",
        7,
        pd.DataFrame(
            {
                "Distance": [0.0, 10.0, 20.0],
                "X": [0.0, 1.2, 2.3],
                "Y": [0.0, 0.6, 1.1],
            }
        ),
        Sector1Time=pd.Timedelta(seconds=25.0),
        Sector2Time=pd.Timedelta(seconds=29.0),
        Sector3Time=pd.Timedelta(seconds=26.0),
    )

    compare_rows, _diagnostics = build_track_compare_rows(lap_a, lap_b, distance_step_m=10.0)

    assert compare_rows["faster_driver_segment"].iloc[0] == "VER"
    assert compare_rows["faster_driver_segment"].iloc[1] == "NOR"
    assert pd.isna(compare_rows["faster_driver_segment"].iloc[2])


def test_build_telemetry_compare_rows_aligns_distance_and_merges_channels() -> None:
    lap_a = DummyLap(
        "VER",
        5,
        pd.DataFrame(
            {
                "Distance": [0.0, 10.0, 20.0],
                "Speed": [100.0, 105.0, 110.0],
                "Throttle": [70.0, 80.0, 90.0],
                "Brake": [0.0, 5.0, 0.0],
            }
        ),
    )
    lap_b = DummyLap(
        "NOR",
        8,
        pd.DataFrame(
            {
                "Distance": [0.0, 5.0, 20.0],
                "Speed": [98.0, 101.0, 112.0],
                "Throttle": [68.0, 75.0, 88.0],
                "Brake": [1.0, 3.0, 0.0],
            }
        ),
    )

    compare_rows, diagnostics = build_telemetry_compare_rows(lap_a, lap_b, distance_step_m=10.0)

    assert compare_rows["distance_m"].tolist() == [0.0, 10.0, 20.0]
    assert list(compare_rows.columns) == [
        "driver_a",
        "driver_b",
        "lap_a",
        "lap_b",
        "distance_m",
        "speed_a",
        "speed_b",
        "throttle_a",
        "throttle_b",
        "brake_a",
        "brake_b",
        "delta_speed",
        "faster_driver_segment",
    ]
    assert compare_rows["delta_speed"].iloc[0] == 2.0
    assert diagnostics.warnings == []


def test_build_corner_markers_uses_available_official_corner_metadata() -> None:
    corners = pd.DataFrame(
        {
            "Number": [1, 2],
            "Letter": ["", "A"],
            "Distance": [120.0, 340.0],
        }
    )

    class DummySession:
        def get_circuit_info(self) -> object:
            return type("CircuitInfo", (), {"corners": corners})()

    markers, warnings = build_corner_markers(DummySession())

    assert warnings == []
    assert markers is not None
    assert markers["corner_label"].tolist() == ["1", "2A"]
