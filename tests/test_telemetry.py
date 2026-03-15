from __future__ import annotations

import pandas as pd

from f1analyser.telemetry import build_track_compare_rows


class DummyLap:
    def __init__(self, driver: str, lap_number: int, telemetry: pd.DataFrame) -> None:
        self.Driver = driver
        self.LapNumber = lap_number
        self._telemetry = telemetry

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
                "Speed": [100.0, 105.0, 110.0, 108.0],
            }
        ),
    )
    lap_b = DummyLap(
        "NOR",
        12,
        pd.DataFrame(
            {
                "Distance": [0.0, 15.0, 30.0],
                "X": [0.0, 1.4, 3.2],
                "Y": [0.0, 1.2, 2.2],
                "Speed": [99.0, 107.0, 111.0],
            }
        ),
    )

    compare_rows, diagnostics = build_track_compare_rows(lap_a, lap_b, distance_step_m=10.0)

    assert compare_rows["distance_m"].tolist() == [0.0, 10.0, 20.0, 30.0]
    assert compare_rows["driver_a"].tolist() == ["VER"] * 4
    assert compare_rows["driver_b"].tolist() == ["NOR"] * 4
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
                "Speed": [100.0, 102.0, 99.0],
            }
        ),
    )
    lap_b = DummyLap(
        "NOR",
        7,
        pd.DataFrame(
            {
                "Distance": [0.0, 10.0, 20.0],
                "X": [0.0, 1.2, 2.3],
                "Y": [0.0, 0.6, 1.1],
                "Speed": [98.0, 104.0, 99.0],
            }
        ),
    )

    compare_rows, _diagnostics = build_track_compare_rows(lap_a, lap_b, distance_step_m=10.0)

    assert compare_rows["faster_driver_segment"].iloc[0] == "VER"
    assert compare_rows["faster_driver_segment"].iloc[1] == "NOR"
    assert pd.isna(compare_rows["faster_driver_segment"].iloc[2])
