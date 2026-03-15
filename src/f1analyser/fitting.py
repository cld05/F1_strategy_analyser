from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd  # type: ignore[import-untyped]


@dataclass(frozen=True)
class LapTrendDiagnostics:
    warnings: list[str]
    stint_insights: pd.DataFrame


class LapTrendError(ValueError):
    """Raised when lap trend inputs are invalid."""


def _normalize_driver_codes(drivers: Sequence[str]) -> tuple[str, str]:
    normalized = [driver.strip().upper() for driver in drivers if driver.strip()]
    unique = list(dict.fromkeys(normalized))
    if len(unique) != 2:
        raise LapTrendError("Exactly 2 unique driver codes are required.")
    return unique[0], unique[1]


def _fit_stint_curve(stint_rows: pd.DataFrame, polynomial_degree: int) -> pd.DataFrame:
    fit_source = stint_rows[stint_rows["include_for_fit"]].copy()
    if len(fit_source) < polynomial_degree + 1:
        return pd.DataFrame(columns=["driver", "stint_display", "lap_number", "fit_lap_time_s"])

    x_values = fit_source["lap_number"].astype("float64").to_numpy()
    y_values = fit_source["lap_time_s"].astype("float64").to_numpy()
    coefficients = np.polyfit(x_values, y_values, deg=polynomial_degree)
    x_fit = np.linspace(x_values.min(), x_values.max(), num=100)
    y_fit = np.polyval(coefficients, x_fit)
    return pd.DataFrame(
        {
            "driver": pd.Series([str(fit_source.iloc[0]["driver"])] * len(x_fit), dtype="string"),
            "stint_display": pd.Series([str(fit_source.iloc[0]["stint_display"])] * len(x_fit), dtype="string"),
            "lap_number": x_fit,
            "fit_lap_time_s": y_fit,
        }
    )


def _stint_trend_label(delta_seconds: float) -> str:
    if delta_seconds <= -0.15:
        return "improving"
    if delta_seconds >= 0.15:
        return "degrading"
    return "stable"


def build_lap_trend_inputs(
    laps_filtered: pd.DataFrame,
    selected_drivers: Sequence[str],
    *,
    polynomial_degree: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, LapTrendDiagnostics]:
    if polynomial_degree < 1:
        raise LapTrendError("Polynomial degree must be at least 1.")

    driver_a, driver_b = _normalize_driver_codes(selected_drivers)
    filtered = laps_filtered[laps_filtered["driver"].isin([driver_a, driver_b])].copy()
    if filtered.empty:
        raise LapTrendError("No filtered laps found for the selected drivers.")

    plot_rows = filtered[filtered["include_for_lap_time_plot"]].copy()
    if plot_rows.empty:
        raise LapTrendError("No filtered laps available for lap trend plots.")

    plot_rows["compound_display"] = plot_rows["compound"].fillna("UNKNOWN").astype("string")
    plot_rows["stint_display"] = plot_rows["stint"].astype("string").fillna("UNKNOWN")

    warnings: list[str] = []
    insight_rows: list[dict[str, object]] = []
    for driver in (driver_a, driver_b):
        driver_rows = plot_rows[plot_rows["driver"] == driver]
        if driver_rows.empty:
            warnings.append(f"No filtered lap-time plot rows available for {driver}.")
            continue
        if driver_rows["compound"].isna().any():
            warnings.append(f"Missing compound values for {driver}; using UNKNOWN in lap trend plot.")
        if driver_rows["stint"].isna().any():
            warnings.append(f"Missing stint values for {driver}; using UNKNOWN in lap trend plot.")
        for stint_display in driver_rows["stint_display"].dropna().astype("string").unique().tolist():
            stint_rows = driver_rows[driver_rows["stint_display"] == stint_display].copy()
            fit_source_count = int(stint_rows["include_for_fit"].sum())
            if fit_source_count < polynomial_degree + 1:
                warnings.append(
                    f"Insufficient fit points for {driver} stint {stint_display} at degree {polynomial_degree}; fit curve omitted."
                )
                continue

            fit_curve = _fit_stint_curve(stint_rows, polynomial_degree)
            fit_start = float(fit_curve["fit_lap_time_s"].iloc[0])
            fit_end = float(fit_curve["fit_lap_time_s"].iloc[-1])
            fit_delta = fit_end - fit_start
            insight_rows.append(
                {
                    "driver": driver,
                    "stint_display": stint_display,
                    "fit_points": fit_source_count,
                    "fit_start_s": fit_start,
                    "fit_end_s": fit_end,
                    "fit_delta_s": fit_delta,
                    "trend": _stint_trend_label(fit_delta),
                }
            )

    fit_frames = []
    for driver in (driver_a, driver_b):
        driver_rows = plot_rows[plot_rows["driver"] == driver].copy()
        for stint_display in driver_rows["stint_display"].dropna().astype("string").unique().tolist():
            fit_frames.append(
                _fit_stint_curve(
                    driver_rows[driver_rows["stint_display"] == stint_display].copy(),
                    polynomial_degree,
                )
            )
    non_empty_fit_frames = [frame for frame in fit_frames if not frame.empty]
    fit_rows = (
        pd.concat(non_empty_fit_frames, ignore_index=True)
        if non_empty_fit_frames
        else pd.DataFrame(columns=["driver", "stint_display", "lap_number", "fit_lap_time_s"])
    )
    stint_insights = pd.DataFrame(
        insight_rows,
        columns=["driver", "stint_display", "fit_points", "fit_start_s", "fit_end_s", "fit_delta_s", "trend"],
    )

    return (
        plot_rows.reset_index(drop=True),
        fit_rows.reset_index(drop=True),
        stint_insights.reset_index(drop=True),
        LapTrendDiagnostics(warnings=warnings, stint_insights=stint_insights.reset_index(drop=True)),
    )
