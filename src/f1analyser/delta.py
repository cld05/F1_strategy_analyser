from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd  # type: ignore[import-untyped]

REQUIRED_DELTA_COLUMNS: list[str] = [
    "lap_number",
    "driver_a",
    "driver_b",
    "lap_time_a_s",
    "lap_time_b_s",
    "lap_delta_s",
    "cum_time_a_s",
    "cum_time_b_s",
    "cum_delta_s",
    "valid_for_delta",
    "exclude_reason",
]


class DeltaLapsError(ValueError):
    """Raised when delta table inputs are invalid."""


def _normalize_driver_codes(drivers: Sequence[str]) -> tuple[str, str]:
    normalized = [driver.strip().upper() for driver in drivers if driver.strip()]
    unique = list(dict.fromkeys(normalized))
    if len(unique) != 2:
        raise DeltaLapsError("Exactly 2 unique driver codes are required.")
    return unique[0], unique[1]


def _first_row_by_lap(driver_laps: pd.DataFrame) -> dict[int, pd.Series]:
    sorted_laps = driver_laps.sort_values("lap_number", kind="stable")
    rows: dict[int, pd.Series] = {}
    for _, row in sorted_laps.iterrows():
        lap_number = row["lap_number"]
        if pd.isna(lap_number):
            continue
        rows[int(lap_number)] = row
    return rows


def _row_reasons(row: pd.Series | None, driver_label: str) -> list[str]:
    if row is None:
        return [f"missing_{driver_label}_lap"]

    reasons: list[str] = []
    if not bool(row["is_valid_lap_time"]):
        reasons.append(f"invalid_{driver_label}_lap_time")

    filter_reasons = row.get("filter_reason_list", [])
    if isinstance(filter_reasons, list):
        reasons.extend(f"{driver_label}_{reason}" for reason in filter_reasons)
    return reasons


def build_delta_laps(
    laps_filtered: pd.DataFrame,
    selected_drivers: Sequence[str],
) -> pd.DataFrame:
    driver_a, driver_b = _normalize_driver_codes(selected_drivers)

    if laps_filtered.empty:
        raise DeltaLapsError("Filtered laps table is empty.")

    filtered = laps_filtered[laps_filtered["driver"].isin([driver_a, driver_b])].copy()
    if filtered.empty:
        raise DeltaLapsError("No filtered laps found for the selected drivers.")

    laps_a = filtered[filtered["driver"] == driver_a].copy()
    laps_b = filtered[filtered["driver"] == driver_b].copy()
    if laps_a.empty or laps_b.empty:
        raise DeltaLapsError("Both selected drivers must have filtered laps.")

    rows_a = _first_row_by_lap(laps_a)
    rows_b = _first_row_by_lap(laps_b)
    lap_numbers = sorted(set(rows_a).union(rows_b))
    if not lap_numbers:
        raise DeltaLapsError("No comparable lap numbers found for the selected drivers.")

    output_rows: list[dict[str, object]] = []
    for lap_number in lap_numbers:
        row_a = rows_a.get(lap_number)
        row_b = rows_b.get(lap_number)

        lap_time_a = float(row_a["lap_time_s"]) if row_a is not None and pd.notna(row_a["lap_time_s"]) else np.nan
        lap_time_b = float(row_b["lap_time_s"]) if row_b is not None and pd.notna(row_b["lap_time_s"]) else np.nan
        both_lap_times_present = pd.notna(lap_time_a) and pd.notna(lap_time_b)

        include_a = row_a is not None and bool(row_a.get("include_for_delta_plot", False))
        include_b = row_b is not None and bool(row_b.get("include_for_delta_plot", False))
        valid_for_delta = bool(include_a and include_b and both_lap_times_present)

        reasons = _row_reasons(row_a, "driver_a") + _row_reasons(row_b, "driver_b")
        deduped_reasons = list(dict.fromkeys(reasons))

        output_rows.append(
            {
                "lap_number": lap_number,
                "driver_a": driver_a,
                "driver_b": driver_b,
                "lap_time_a_s": lap_time_a,
                "lap_time_b_s": lap_time_b,
                "lap_delta_s": lap_time_a - lap_time_b if both_lap_times_present else np.nan,
                "cum_time_a_s": np.nan,
                "cum_time_b_s": np.nan,
                "cum_delta_s": np.nan,
                "valid_for_delta": valid_for_delta,
                "exclude_reason": pd.NA if valid_for_delta else ",".join(deduped_reasons),
            }
        )

    delta_laps = pd.DataFrame(output_rows, columns=REQUIRED_DELTA_COLUMNS)
    valid_time_mask = delta_laps["lap_time_a_s"].notna() & delta_laps["lap_time_b_s"].notna()
    delta_laps.loc[valid_time_mask, "cum_time_a_s"] = delta_laps.loc[valid_time_mask, "lap_time_a_s"].cumsum()
    delta_laps.loc[valid_time_mask, "cum_time_b_s"] = delta_laps.loc[valid_time_mask, "lap_time_b_s"].cumsum()
    delta_laps.loc[valid_time_mask, "cum_delta_s"] = (
        delta_laps.loc[valid_time_mask, "cum_time_a_s"] - delta_laps.loc[valid_time_mask, "cum_time_b_s"]
    )
    delta_laps["exclude_reason"] = delta_laps["exclude_reason"].astype("string")
    return delta_laps
