from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from f1analyser.laps import CanonicalLapsError
else:
    try:
        from f1analyser.laps import CanonicalLapsError
    except ModuleNotFoundError:
        from laps import CanonicalLapsError  # type: ignore[import-not-found]

LOGGER = logging.getLogger(__name__)
_SC_VSC_CODES = {"4", "6", "7"}


def _status_has_sc_vsc(status: Any) -> bool:
    if status is None or pd.isna(status):
        return False
    text = str(status)
    return any(code in text for code in _SC_VSC_CODES)


def _driver_lap_row(driver_laps: pd.DataFrame, lap_number: int) -> pd.Series[Any] | None:
    match = driver_laps[driver_laps["lap_number"] == lap_number]
    if match.empty:
        return None
    return match.iloc[0]


def compute_pit_loss_per_stop(
    laps: pd.DataFrame,
    pits: pd.DataFrame,
    *,
    include_sc_vsc_in_aggregate: bool = True,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Compute per-stop pit loss using fixed baseline windows."""
    active_logger = logger or LOGGER

    required_laps = {"driver", "lap_number", "lap_time", "track_status", "is_clean"}
    missing_laps = required_laps.difference(laps.columns)
    if missing_laps:
        missing_list = ", ".join(sorted(missing_laps))
        raise CanonicalLapsError(f"Cannot compute pit loss; laps missing columns: {missing_list}")

    required_pits = {"driver", "stop_index", "pit_in_lap", "pit_out_lap"}
    missing_pits = required_pits.difference(pits.columns)
    if missing_pits:
        missing_list = ", ".join(sorted(missing_pits))
        raise CanonicalLapsError(f"Cannot compute pit loss; pits missing columns: {missing_list}")

    rows: list[dict[str, Any]] = []

    for _, pit in pits.sort_values(["driver", "stop_index"], kind="stable").iterrows():
        driver = str(pit["driver"])
        stop_index = int(pit["stop_index"])
        pit_in = pit["pit_in_lap"]
        pit_out = pit["pit_out_lap"]

        reason_not_computable: str | None = None
        baseline_s: float | None = None
        inlap_time_s: float | None = None
        outlap_time_s: float | None = None
        pit_loss_s: float | None = None
        n_pre = 0
        n_post = 0
        n_total_used = 0

        driver_laps = laps[laps["driver"] == driver].sort_values("lap_number", kind="stable")
        in_row = _driver_lap_row(driver_laps, int(pit_in)) if pd.notna(pit_in) else None
        out_row = _driver_lap_row(driver_laps, int(pit_out)) if pd.notna(pit_out) else None

        is_sc_vsc_affected = False
        if in_row is not None:
            is_sc_vsc_affected = is_sc_vsc_affected or _status_has_sc_vsc(in_row.get("track_status"))
        if out_row is not None:
            is_sc_vsc_affected = is_sc_vsc_affected or _status_has_sc_vsc(out_row.get("track_status"))

        if in_row is None or out_row is None:
            reason_not_computable = "missing in-lap or out-lap row"
        else:
            inlap_value = in_row.get("lap_time")
            outlap_value = out_row.get("lap_time")
            if pd.isna(inlap_value) or pd.isna(outlap_value):
                reason_not_computable = "in-lap or out-lap missing/invalid lap_time"
            else:
                inlap_time_s = float(inlap_value)
                outlap_time_s = float(outlap_value)

                clean_pre = driver_laps[
                    (driver_laps["lap_number"] < int(pit_in)) & (driver_laps["is_clean"] == True)  # noqa: E712
                ].sort_values("lap_number", ascending=False, kind="stable")
                clean_post = driver_laps[
                    (driver_laps["lap_number"] > int(pit_out)) & (driver_laps["is_clean"] == True)  # noqa: E712
                ].sort_values("lap_number", ascending=True, kind="stable")

                pre_values = clean_pre["lap_time"].dropna().head(3).astype(float).tolist()
                post_values = clean_post["lap_time"].dropna().head(3).astype(float).tolist()

                n_pre = len(pre_values)
                n_post = len(post_values)
                n_total_used = n_pre + n_post

                if n_pre < 1 or n_post < 1:
                    reason_not_computable = "baseline requires clean laps on both sides"
                elif n_total_used < 3:
                    reason_not_computable = "baseline requires at least 3 clean laps"
                else:
                    baseline_s = float(np.median(pre_values + post_values))
                    pit_loss_s = (inlap_time_s - baseline_s) + (outlap_time_s - baseline_s)

        is_computable = reason_not_computable is None
        if not is_computable and reason_not_computable is not None:
            active_logger.warning(
                "warning: pit loss not computable for driver %s stop %s: %s",
                driver,
                stop_index,
                reason_not_computable,
            )

        included_in_aggregate = bool(is_computable)
        if is_sc_vsc_affected and not include_sc_vsc_in_aggregate:
            included_in_aggregate = False

        rows.append(
            {
                "driver": driver,
                "stop_index": stop_index,
                "baseline_s": baseline_s,
                "inlap_time_s": inlap_time_s,
                "outlap_time_s": outlap_time_s,
                "pit_loss_s": pit_loss_s,
                "n_pre": n_pre,
                "n_post": n_post,
                "n_total_used": n_total_used,
                "is_sc_vsc_affected": is_sc_vsc_affected,
                "is_computable": is_computable,
                "reason_not_computable": reason_not_computable,
                "included_in_aggregate": included_in_aggregate,
            }
        )

    return pd.DataFrame(rows)


def compute_stint_metrics(
    laps: pd.DataFrame,
    stints: pd.DataFrame,
    *,
    min_clean_laps: int = 3,
) -> pd.DataFrame:
    """Compute pace and degradation metrics per stint."""
    required_laps = {"driver", "lap_number", "lap_time", "is_clean"}
    missing_laps = required_laps.difference(laps.columns)
    if missing_laps:
        missing_list = ", ".join(sorted(missing_laps))
        raise CanonicalLapsError(f"Cannot compute stint metrics; laps missing columns: {missing_list}")

    required_stints = {"driver", "stint_id", "start_lap", "end_lap"}
    missing_stints = required_stints.difference(stints.columns)
    if missing_stints:
        missing_list = ", ".join(sorted(missing_stints))
        raise CanonicalLapsError(f"Cannot compute stint metrics; stints missing columns: {missing_list}")

    enriched = stints.copy()
    enriched["pace_median_s"] = pd.NA
    enriched["deg_slope_s_per_lap"] = pd.NA
    enriched["deg_delta_first_last_s"] = pd.NA

    if "warnings" not in enriched.columns:
        enriched["warnings"] = ""

    for idx, stint in enriched.iterrows():
        driver = str(stint["driver"])
        start_lap = int(stint["start_lap"])
        end_lap = int(stint["end_lap"])

        stint_laps = laps[
            (laps["driver"] == driver)
            & (laps["lap_number"] >= start_lap)
            & (laps["lap_number"] <= end_lap)
        ].sort_values("lap_number", kind="stable")

        clean = stint_laps[(stint_laps["is_clean"] == True) & stint_laps["lap_time"].notna()]  # noqa: E712
        if len(clean) < min_clean_laps:
            warning = f"insufficient clean laps (<{min_clean_laps}) for pace/degradation"
            existing = str(enriched.at[idx, "warnings"] or "")
            enriched.at[idx, "warnings"] = (f"{existing} | {warning}").strip(" |")
            continue

        lap_times = clean["lap_time"].astype(float).to_numpy()
        lap_numbers = clean["lap_number"].astype(float).to_numpy()

        pace = float(np.median(lap_times))
        slope = float(np.polyfit(lap_numbers, lap_times, 1)[0])
        delta = float(lap_times[-1] - lap_times[0])

        enriched.at[idx, "pace_median_s"] = pace
        enriched.at[idx, "deg_slope_s_per_lap"] = slope
        enriched.at[idx, "deg_delta_first_last_s"] = delta

    return enriched
