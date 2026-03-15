from __future__ import annotations

from dataclasses import dataclass

import pandas as pd  # type: ignore[import-untyped]


@dataclass(frozen=True)
class FilterDiagnostics:
    warnings: list[str]
    dropped_laps: pd.DataFrame


def build_laps_filtered(
    laps: pd.DataFrame,
    *,
    exclude_sc_vsc: bool,
) -> tuple[pd.DataFrame, FilterDiagnostics]:
    filtered = laps.copy()

    reason_lists: list[list[str]] = []
    for row in filtered.itertuples(index=False):
        reasons: list[str] = []
        if not bool(row.is_valid_lap_time):
            reasons.append("missing_lap_time")
        if bool(row.is_pit_lap):
            reasons.append("pit_lap")
        if exclude_sc_vsc and bool(row.is_sc_vsc_lap):
            reasons.append("sc_vsc_lap")
        reason_lists.append(reasons)

    filtered["filter_reason_list"] = reason_lists
    filtered["include_for_lap_time_plot"] = filtered["filter_reason_list"].map(lambda reasons: len(reasons) == 0)
    filtered["include_for_delta_plot"] = filtered["filter_reason_list"].map(lambda reasons: len(reasons) == 0)
    filtered["include_for_fit"] = filtered["filter_reason_list"].map(lambda reasons: len(reasons) == 0)
    filtered["filter_reason"] = filtered["filter_reason_list"].map(
        lambda reasons: ",".join(reasons) if reasons else pd.NA
    ).astype("string")

    dropped_laps = filtered[~filtered["include_for_lap_time_plot"]].copy()
    warnings: list[str] = []
    if not dropped_laps.empty:
        warnings.append(f"Dropped {len(dropped_laps)} laps from filtered views.")

    diagnostics = FilterDiagnostics(
        warnings=warnings,
        dropped_laps=dropped_laps.reset_index(drop=True),
    )
    return filtered.reset_index(drop=True), diagnostics
