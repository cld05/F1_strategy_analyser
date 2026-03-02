from __future__ import annotations

import logging
from typing import Any

import pandas as pd

try:
    from f1analyser.laps import CanonicalLapsError
except ModuleNotFoundError:
    from laps import CanonicalLapsError

LOGGER = logging.getLogger(__name__)


def _is_red_flag(track_status: Any) -> bool:
    if track_status is None:
        return False
    text = str(track_status)
    return "5" in text


def detect_pits(
    laps: pd.DataFrame,
    *,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Detect pit events from pit markers and apply missing-marker rules."""
    active_logger = logger or LOGGER
    required = {"driver", "lap_number", "pit_in_time", "pit_out_time", "lap_time"}
    missing = required.difference(laps.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise CanonicalLapsError(f"Cannot detect pits; missing required columns: {missing_list}")

    pit_rows: list[dict[str, Any]] = []

    for driver, driver_rows in laps.groupby("driver", sort=False):
        ordered = driver_rows.sort_values("lap_number", kind="stable")
        in_laps = ordered.loc[ordered["pit_in_time"].notna(), "lap_number"].astype("Int64").tolist()
        out_laps = ordered.loc[ordered["pit_out_time"].notna(), "lap_number"].astype("Int64").tolist()
        out_used: set[int] = set()

        events: list[tuple[int, int | None, list[str]]] = []

        for in_lap_value in in_laps:
            if pd.isna(in_lap_value):
                continue
            in_lap = int(in_lap_value)
            matching_out = next((ol for ol in out_laps if ol >= in_lap and ol not in out_used), None)
            warnings: list[str] = []
            if matching_out is None:
                post = ordered[ordered["lap_number"] > in_lap]
                has_post_time = bool(post["lap_time"].notna().any())
                has_post_after_two = bool((post.loc[post["lap_time"].notna(), "lap_number"] > in_lap + 2).any())

                if not has_post_time:
                    warning = f"warning: possible DNF after in-pit at lap {in_lap}"
                    warnings.append(warning)
                    active_logger.warning(warning)
                    events.append((in_lap, None, warnings))
                    continue

                if has_post_after_two:
                    assumed_out = in_lap + 1
                    warning = (
                        "warning: missing out-lap; assuming pit_out_lap="
                        f"{assumed_out} based on subsequent lap times"
                    )
                    warnings.append(warning)
                    active_logger.warning(warning)
                    matching_out = assumed_out
                else:
                    warning = f"warning: missing out-lap after in-pit at lap {in_lap}"
                    warnings.append(warning)
                    active_logger.warning(warning)
                    events.append((in_lap, None, warnings))
                    continue

            if matching_out is not None:
                out_used.add(int(matching_out))
                events.append((in_lap, int(matching_out), warnings))

        for out_lap_value in out_laps:
            if pd.isna(out_lap_value):
                continue
            out_lap = int(out_lap_value)
            if out_lap in out_used:
                continue
            assumed_in = max(1, out_lap - 1)
            warning = f"warning: missing in-lap; assuming pit_in_lap={assumed_in}"
            active_logger.warning(warning)
            events.append((assumed_in, out_lap, [warning]))

        events.sort(key=lambda item: item[0])

        for idx, (pit_in_lap, pit_out_lap, warnings) in enumerate(events, start=1):
            event_warnings = list(warnings)
            laps_stationary: int | None
            if pit_out_lap is None:
                laps_stationary = None
            else:
                laps_stationary = pit_out_lap - pit_in_lap
                if laps_stationary == 2:
                    warning = (
                        "warning: 2-lap pit window accepted for driver "
                        f"{driver} at pit_in_lap={pit_in_lap}, pit_out_lap={pit_out_lap}"
                    )
                    active_logger.warning(warning)
                    event_warnings.append(warning)

            pit_rows.append(
                {
                    "driver": str(driver),
                    "stop_index": idx,
                    "pit_in_lap": pit_in_lap,
                    "pit_out_lap": pit_out_lap,
                    "laps_stationary": laps_stationary,
                    "is_drive_through": False,
                    "has_time_penalty_served": False,
                    "warnings": " | ".join(event_warnings),
                }
            )

    pits = pd.DataFrame(
        pit_rows,
        columns=[
            "driver",
            "stop_index",
            "pit_in_lap",
            "pit_out_lap",
            "laps_stationary",
            "is_drive_through",
            "has_time_penalty_served",
            "warnings",
        ],
    )
    if pits.empty:
        return pits

    return pits.sort_values(["driver", "stop_index"], kind="stable").reset_index(drop=True)


def build_stints(
    laps: pd.DataFrame,
    pits: pd.DataFrame,
    *,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Segment laps into stints by pit boundaries, compound changes, and red-flag tyre rules."""
    active_logger = logger or LOGGER
    required = {"driver", "lap_number", "compound", "track_status", "tyre_life"}
    missing = required.difference(laps.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise CanonicalLapsError(f"Cannot build stints; missing required columns: {missing_list}")

    stint_rows: list[dict[str, Any]] = []

    for driver, driver_rows in laps.groupby("driver", sort=False):
        ordered = driver_rows.sort_values("lap_number", kind="stable").reset_index(drop=True)

        pit_boundaries: set[int] = set()
        if not pits.empty:
            driver_pits = pits[pits["driver"] == driver]
            for _, pit in driver_pits.iterrows():
                out_lap = pit["pit_out_lap"]
                if pd.notna(out_lap):
                    pit_boundaries.add(int(out_lap))

        boundaries: set[int] = set()
        warnings: dict[int, list[str]] = {}

        for i in range(1, len(ordered)):
            curr_lap = int(ordered.loc[i, "lap_number"])
            prev_lap = int(ordered.loc[i - 1, "lap_number"])
            curr_compound = ordered.loc[i, "compound"]
            prev_compound = ordered.loc[i - 1, "compound"]

            if curr_lap in pit_boundaries:
                boundaries.add(curr_lap)

            if pd.notna(curr_compound) and pd.notna(prev_compound) and str(curr_compound) != str(prev_compound):
                boundaries.add(curr_lap)

            curr_red = _is_red_flag(ordered.loc[i, "track_status"])
            prev_red = _is_red_flag(ordered.loc[i - 1, "track_status"])

            if prev_red and not curr_red:
                # First lap after red-flag segment; find pre-red lap
                pre_idx = i - 1
                while pre_idx >= 0 and _is_red_flag(ordered.loc[pre_idx, "track_status"]):
                    pre_idx -= 1
                if pre_idx < 0:
                    continue

                pre_lap = int(ordered.loc[pre_idx, "lap_number"])
                post_lap = curr_lap
                pre_tyre = ordered.loc[pre_idx, "tyre_life"]
                post_tyre = ordered.loc[i, "tyre_life"]
                pre_compound = ordered.loc[pre_idx, "compound"]
                post_compound = ordered.loc[i, "compound"]

                needs_new_stint = False
                if pd.notna(pre_tyre) and pd.notna(post_tyre):
                    delta_tyre = float(post_tyre) - float(pre_tyre)
                    delta_laps = float(post_lap - pre_lap)
                    if float(post_tyre) < float(pre_tyre) or delta_tyre != delta_laps:
                        needs_new_stint = True
                else:
                    if str(pre_compound) != str(post_compound):
                        needs_new_stint = True
                    else:
                        has_pit_in_red = False
                        for red_idx in range(pre_idx + 1, i):
                            red_lap = int(ordered.loc[red_idx, "lap_number"])
                            if red_lap in pit_boundaries:
                                has_pit_in_red = True
                                break
                        if has_pit_in_red:
                            needs_new_stint = True
                        else:
                            warning = (
                                "warning: TyreLife missing across red flag; assuming continuous tyres "
                                f"for driver {driver}"
                            )
                            active_logger.warning(warning)
                            warnings.setdefault(post_lap, []).append(warning)

                if needs_new_stint:
                    boundaries.add(post_lap)

        stint_id = 1
        start_idx = 0
        for i in range(1, len(ordered) + 1):
            next_lap_number = None
            if i < len(ordered):
                next_lap_number = int(ordered.loc[i, "lap_number"])

            if i < len(ordered) and next_lap_number not in boundaries:
                continue

            stint_laps = ordered.iloc[start_idx:i]
            start_lap = int(stint_laps["lap_number"].iloc[0])
            end_lap = int(stint_laps["lap_number"].iloc[-1])

            n_total = int(len(stint_laps))
            if "n_clean" in stint_laps.columns:
                n_clean = int(stint_laps["n_clean"].sum())
            elif "is_clean" in stint_laps.columns:
                n_clean = int(stint_laps["is_clean"].sum())
            else:
                n_clean = n_total
            if "n_excluded" in stint_laps.columns:
                n_excluded = int(stint_laps["n_excluded"].sum())
            elif "is_excluded" in stint_laps.columns:
                n_excluded = int(stint_laps["is_excluded"].sum())
            else:
                n_excluded = 0

            compound = str(stint_laps["compound"].dropna().iloc[0]) if stint_laps["compound"].notna().any() else "Unknown"
            warning_text = ""
            if next_lap_number is not None and next_lap_number in warnings:
                warning_text = " | ".join(warnings[next_lap_number])

            stint_rows.append(
                {
                    "driver": str(driver),
                    "stint_id": stint_id,
                    "compound": compound,
                    "start_lap": start_lap,
                    "end_lap": end_lap,
                    "lap_count": n_total,
                    "n_laps_total": n_total,
                    "n_laps_clean": n_clean,
                    "n_laps_excluded": n_excluded,
                    "warnings": warning_text,
                }
            )

            stint_id += 1
            start_idx = i

    stints = pd.DataFrame(
        stint_rows,
        columns=[
            "driver",
            "stint_id",
            "compound",
            "start_lap",
            "end_lap",
            "lap_count",
            "n_laps_total",
            "n_laps_clean",
            "n_laps_excluded",
            "warnings",
        ],
    )

    return stints.sort_values(["driver", "stint_id"], kind="stable").reset_index(drop=True)
