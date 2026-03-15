from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd  # type: ignore[import-untyped]


@dataclass(frozen=True)
class TrackCompareDiagnostics:
    warnings: list[str]


class TrackCompareError(ValueError):
    """Raised when track comparison inputs are invalid."""


def _lap_number_value(lap: Any) -> int:
    value = getattr(lap, "LapNumber", None)
    if value is None and isinstance(lap, pd.Series):
        value = lap.get("LapNumber")
    if value is None or pd.isna(value):
        raise TrackCompareError("Selected lap is missing LapNumber.")
    return int(value)


def _driver_value(lap: Any) -> str:
    value = getattr(lap, "Driver", None)
    if value is None and isinstance(lap, pd.Series):
        value = lap.get("Driver")
    if value is None or pd.isna(value):
        raise TrackCompareError("Selected lap is missing driver code.")
    return str(value)


def _lap_telemetry(lap: Any) -> pd.DataFrame:
    telemetry = lap.get_telemetry()
    required_columns = {"Distance", "X", "Y"}
    missing = required_columns.difference(telemetry.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise TrackCompareError(f"Telemetry is missing required columns: {missing_list}")

    cleaned = telemetry.loc[:, ["Distance", "X", "Y"]].copy()
    cleaned = cleaned.dropna(subset=["Distance", "X", "Y"]).sort_values("Distance", kind="stable")
    cleaned = cleaned.drop_duplicates(subset="Distance", keep="first")
    if len(cleaned) < 2:
        raise TrackCompareError("Telemetry must contain at least 2 valid distance samples.")
    return cleaned.reset_index(drop=True)


def _sector_time_seconds(lap: Any, field_name: str) -> float:
    value = getattr(lap, field_name, None)
    if value is None and isinstance(lap, pd.Series):
        value = lap.get(field_name)
    if value is None or pd.isna(value):
        raise TrackCompareError(f"Selected lap is missing {field_name}.")
    return float(pd.to_timedelta(value).total_seconds())


def _telemetry_compare_source(lap: Any) -> pd.DataFrame:
    telemetry = lap.get_telemetry()
    required_columns = {"Distance", "Speed", "Throttle", "Brake"}
    missing = required_columns.difference(telemetry.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise TrackCompareError(f"Telemetry is missing required columns: {missing_list}")

    cleaned = telemetry.loc[:, ["Distance", "Speed", "Throttle", "Brake"]].copy()
    cleaned = cleaned.dropna(subset=["Distance", "Speed", "Throttle", "Brake"]).sort_values("Distance", kind="stable")
    cleaned = cleaned.drop_duplicates(subset="Distance", keep="first")
    if len(cleaned) < 2:
        raise TrackCompareError("Telemetry compare source must contain at least 2 valid distance samples.")
    return cleaned.reset_index(drop=True)


def _interpolate_series(distances: np.ndarray, telemetry: pd.DataFrame, column: str) -> np.ndarray:
    source_distance = telemetry["Distance"].to_numpy(dtype="float64")
    source_values = telemetry[column].to_numpy(dtype="float64")
    return np.asarray(np.interp(distances, source_distance, source_values), dtype="float64")


def build_track_compare_rows(
    lap_a: Any,
    lap_b: Any,
    *,
    distance_step_m: float = 10.0,
) -> tuple[pd.DataFrame, TrackCompareDiagnostics]:
    if distance_step_m <= 0:
        raise TrackCompareError("Distance step must be positive.")

    telemetry_a = _lap_telemetry(lap_a)
    telemetry_b = _lap_telemetry(lap_b)
    max_common_distance = min(
        float(telemetry_a["Distance"].max()),
        float(telemetry_b["Distance"].max()),
    )
    if max_common_distance <= 0:
        raise TrackCompareError("Telemetry distance must be positive.")

    sample_distance = np.arange(0.0, max_common_distance + distance_step_m, distance_step_m, dtype="float64")
    sample_distance[-1] = max_common_distance

    x_a = _interpolate_series(sample_distance, telemetry_a, "X")
    y_a = _interpolate_series(sample_distance, telemetry_a, "Y")
    x_b = _interpolate_series(sample_distance, telemetry_b, "X")
    y_b = _interpolate_series(sample_distance, telemetry_b, "Y")

    driver_a = _driver_value(lap_a)
    driver_b = _driver_value(lap_b)
    sector_times_a = np.array(
        [
            _sector_time_seconds(lap_a, "Sector1Time"),
            _sector_time_seconds(lap_a, "Sector2Time"),
            _sector_time_seconds(lap_a, "Sector3Time"),
        ],
        dtype="float64",
    )
    sector_times_b = np.array(
        [
            _sector_time_seconds(lap_b, "Sector1Time"),
            _sector_time_seconds(lap_b, "Sector2Time"),
            _sector_time_seconds(lap_b, "Sector3Time"),
        ],
        dtype="float64",
    )
    total_a = float(sector_times_a.sum())
    total_b = float(sector_times_b.sum())
    sector_boundaries = np.array(
        [
            0.0,
            ((sector_times_a[0] / total_a) + (sector_times_b[0] / total_b)) / 2.0,
            ((sector_times_a[:2].sum() / total_a) + (sector_times_b[:2].sum() / total_b)) / 2.0,
            1.0,
        ],
        dtype="float64",
    ) * max_common_distance
    sector_numbers = np.searchsorted(sector_boundaries[1:], sample_distance, side="right") + 1
    sector_numbers = np.clip(sector_numbers, 1, 3)
    sector_winners = np.where(
        sector_times_a < sector_times_b,
        driver_a,
        np.where(sector_times_b < sector_times_a, driver_b, pd.NA),
    )
    faster_driver = pd.Series([sector_winners[sector - 1] for sector in sector_numbers], dtype="string")

    compare_rows = pd.DataFrame(
        {
            "driver_a": pd.Series([driver_a] * len(sample_distance), dtype="string"),
            "driver_b": pd.Series([driver_b] * len(sample_distance), dtype="string"),
            "lap_a": pd.Series([_lap_number_value(lap_a)] * len(sample_distance), dtype="Int64"),
            "lap_b": pd.Series([_lap_number_value(lap_b)] * len(sample_distance), dtype="Int64"),
            "distance_m": sample_distance,
            "x_a": x_a,
            "y_a": y_a,
            "x_b": x_b,
            "y_b": y_b,
            "plot_x": (x_a + x_b) / 2.0,
            "plot_y": (y_a + y_b) / 2.0,
            "sector_number": pd.Series(sector_numbers, dtype="Int64"),
            "faster_driver_segment": faster_driver,
        }
    )

    warnings: list[str] = []
    if float(telemetry_a["Distance"].max()) != float(telemetry_b["Distance"].max()):
        warnings.append("Telemetry comparison truncated to the shorter shared lap distance.")

    return compare_rows, TrackCompareDiagnostics(warnings=warnings)


def build_telemetry_compare_rows(
    lap_a: Any,
    lap_b: Any,
    *,
    distance_step_m: float = 5.0,
) -> tuple[pd.DataFrame, TrackCompareDiagnostics]:
    if distance_step_m <= 0:
        raise TrackCompareError("Distance step must be positive.")

    telemetry_a = _telemetry_compare_source(lap_a)
    telemetry_b = _telemetry_compare_source(lap_b)
    max_common_distance = min(
        float(telemetry_a["Distance"].max()),
        float(telemetry_b["Distance"].max()),
    )
    if max_common_distance <= 0:
        raise TrackCompareError("Telemetry distance must be positive.")

    sample_distance = np.arange(0.0, max_common_distance + distance_step_m, distance_step_m, dtype="float64")
    sample_distance[-1] = max_common_distance

    speed_a = _interpolate_series(sample_distance, telemetry_a, "Speed")
    speed_b = _interpolate_series(sample_distance, telemetry_b, "Speed")
    throttle_a = _interpolate_series(sample_distance, telemetry_a, "Throttle")
    throttle_b = _interpolate_series(sample_distance, telemetry_b, "Throttle")
    brake_a = _interpolate_series(sample_distance, telemetry_a, "Brake")
    brake_b = _interpolate_series(sample_distance, telemetry_b, "Brake")

    driver_a = _driver_value(lap_a)
    driver_b = _driver_value(lap_b)
    faster_driver = np.where(speed_a > speed_b, driver_a, np.where(speed_b > speed_a, driver_b, pd.NA))

    compare_rows = pd.DataFrame(
        {
            "driver_a": pd.Series([driver_a] * len(sample_distance), dtype="string"),
            "driver_b": pd.Series([driver_b] * len(sample_distance), dtype="string"),
            "lap_a": pd.Series([_lap_number_value(lap_a)] * len(sample_distance), dtype="Int64"),
            "lap_b": pd.Series([_lap_number_value(lap_b)] * len(sample_distance), dtype="Int64"),
            "distance_m": sample_distance,
            "speed_a": speed_a,
            "speed_b": speed_b,
            "throttle_a": throttle_a,
            "throttle_b": throttle_b,
            "brake_a": brake_a,
            "brake_b": brake_b,
            "delta_speed": speed_a - speed_b,
            "faster_driver_segment": pd.Series(faster_driver, dtype="string"),
        }
    )

    warnings: list[str] = []
    if float(telemetry_a["Distance"].max()) != float(telemetry_b["Distance"].max()):
        warnings.append("Telemetry comparison truncated to the shorter shared lap distance.")

    return compare_rows, TrackCompareDiagnostics(warnings=warnings)
