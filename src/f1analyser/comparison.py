from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import pandas as pd

try:
    from f1analyser.laps import CanonicalLapsError
    from f1analyser.metrics import compute_pit_loss_per_stop
except ModuleNotFoundError:
    from laps import CanonicalLapsError
    from metrics import compute_pit_loss_per_stop

COMPARISON_WINDOWS_COLUMNS: list[str] = [
    "window_id",
    "driver_a",
    "driver_b",
    "stint_a_id",
    "stint_b_id",
    "lap_start",
    "lap_end",
    "n_overlap_total",
    "n_clean_a",
    "n_clean_b",
    "included",
    "exclude_reason",
    "pace_a_window_s",
    "pace_b_window_s",
    "window_delta_s",
]

COMPARISON_SUMMARY_COLUMNS: list[str] = [
    "driver_a",
    "driver_b",
    "L_common",
    "observed_finish_delta_s",
    "pit_delta_sum_s",
    "stint_delta_sum_s",
    "residual_s",
    "residual_ok",
    "residual_threshold_s",
    "warnings",
]

RESIDUAL_THRESHOLD_S = 10.0


def _normalize_driver_codes(drivers: Sequence[str]) -> tuple[str, str]:
    normalized = [driver.strip().upper() for driver in drivers if driver.strip()]
    unique = list(dict.fromkeys(normalized))
    if len(unique) != 2:
        raise CanonicalLapsError("Exactly 2 unique driver codes are required for comparison.")
    return unique[0], unique[1]


def _comparison_cache_paths(
    *,
    cache_dir: Path,
    season: int,
    round_number: int,
    session_type: str,
    driver_a: str,
    driver_b: str,
) -> tuple[Path, Path]:
    drivers_key = "-".join(sorted([driver_a, driver_b]))
    base = f"{season}-{round_number}-{session_type.lower()}-{drivers_key}"
    windows_path = cache_dir / f"comparison-windows-{base}.parquet"
    summary_path = cache_dir / f"comparison-summary-{base}.parquet"
    return windows_path, summary_path


def _season_round_session_type(laps: pd.DataFrame) -> tuple[int, int, str]:
    required = {"season", "round", "session_type"}
    missing = required.difference(laps.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise CanonicalLapsError(f"Missing required laps metadata columns: {missing_list}")

    first = laps.iloc[0]
    return int(first["season"]), int(first["round"]), str(first["session_type"])


def _completed_laps(driver_laps: pd.DataFrame) -> int:
    valid = driver_laps[driver_laps["lap_time"].notna()]
    if valid.empty:
        return 0
    return int(valid["lap_number"].max())


def build_comparison_windows(
    laps: pd.DataFrame,
    stints: pd.DataFrame,
    *,
    selected_drivers: Sequence[str],
) -> pd.DataFrame:
    driver_a, driver_b = _normalize_driver_codes(selected_drivers)

    required_laps = {"driver", "lap_number", "lap_time", "is_clean"}
    missing_laps = required_laps.difference(laps.columns)
    if missing_laps:
        missing_list = ", ".join(sorted(missing_laps))
        raise CanonicalLapsError(f"Cannot build comparison windows; laps missing columns: {missing_list}")

    required_stints = {"driver", "stint_id", "start_lap", "end_lap"}
    missing_stints = required_stints.difference(stints.columns)
    if missing_stints:
        missing_list = ", ".join(sorted(missing_stints))
        raise CanonicalLapsError(f"Cannot build comparison windows; stints missing columns: {missing_list}")

    stints_a = stints[stints["driver"] == driver_a].sort_values("stint_id", kind="stable")
    stints_b = stints[stints["driver"] == driver_b].sort_values("stint_id", kind="stable")

    windows_rows: list[dict[str, Any]] = []
    window_id = 1

    for _, stint_a in stints_a.iterrows():
        for _, stint_b in stints_b.iterrows():
            lap_start = max(int(stint_a["start_lap"]), int(stint_b["start_lap"]))
            lap_end = min(int(stint_a["end_lap"]), int(stint_b["end_lap"]))

            if lap_start > lap_end:
                windows_rows.append(
                    {
                        "window_id": window_id,
                        "driver_a": driver_a,
                        "driver_b": driver_b,
                        "stint_a_id": int(stint_a["stint_id"]),
                        "stint_b_id": int(stint_b["stint_id"]),
                        "lap_start": pd.NA,
                        "lap_end": pd.NA,
                        "n_overlap_total": 0,
                        "n_clean_a": 0,
                        "n_clean_b": 0,
                        "included": False,
                        "exclude_reason": "no lap overlap",
                        "pace_a_window_s": pd.NA,
                        "pace_b_window_s": pd.NA,
                        "window_delta_s": pd.NA,
                    }
                )
                window_id += 1
                continue

            overlap_count = (lap_end - lap_start) + 1
            laps_a = laps[
                (laps["driver"] == driver_a)
                & (laps["lap_number"] >= lap_start)
                & (laps["lap_number"] <= lap_end)
                & (laps["is_clean"] == True)  # noqa: E712
                & laps["lap_time"].notna()
            ]
            laps_b = laps[
                (laps["driver"] == driver_b)
                & (laps["lap_number"] >= lap_start)
                & (laps["lap_number"] <= lap_end)
                & (laps["is_clean"] == True)  # noqa: E712
                & laps["lap_time"].notna()
            ]

            n_clean_a = int(len(laps_a))
            n_clean_b = int(len(laps_b))
            included = min(n_clean_a, n_clean_b) >= 3
            exclude_reason: str | None = None
            pace_a_window_s: float | None = None
            pace_b_window_s: float | None = None
            window_delta_s: float | None = None

            if included:
                pace_a_window_s = float(laps_a["lap_time"].astype(float).median())
                pace_b_window_s = float(laps_b["lap_time"].astype(float).median())
                window_delta_s = (pace_a_window_s - pace_b_window_s) * overlap_count
            else:
                exclude_reason = "insufficient clean overlap (<3)"

            windows_rows.append(
                {
                    "window_id": window_id,
                    "driver_a": driver_a,
                    "driver_b": driver_b,
                    "stint_a_id": int(stint_a["stint_id"]),
                    "stint_b_id": int(stint_b["stint_id"]),
                    "lap_start": lap_start,
                    "lap_end": lap_end,
                    "n_overlap_total": overlap_count,
                    "n_clean_a": n_clean_a,
                    "n_clean_b": n_clean_b,
                    "included": included,
                    "exclude_reason": exclude_reason,
                    "pace_a_window_s": pace_a_window_s,
                    "pace_b_window_s": pace_b_window_s,
                    "window_delta_s": window_delta_s,
                }
            )
            window_id += 1

    return pd.DataFrame(windows_rows, columns=COMPARISON_WINDOWS_COLUMNS)


def build_comparison_summary(
    laps: pd.DataFrame,
    pits: pd.DataFrame,
    stints: pd.DataFrame,
    comparison_windows: pd.DataFrame,
    *,
    selected_drivers: Sequence[str],
) -> pd.DataFrame:
    driver_a, driver_b = _normalize_driver_codes(selected_drivers)

    laps_a = laps[laps["driver"] == driver_a]
    laps_b = laps[laps["driver"] == driver_b]
    completed_a = _completed_laps(laps_a)
    completed_b = _completed_laps(laps_b)
    l_common = min(completed_a, completed_b)

    warnings: list[str] = []
    if l_common < 1:
        warning = "driver with less than one lap, no comparison possible"
        warnings.append(warning)
        return pd.DataFrame(
            [
                {
                    "driver_a": driver_a,
                    "driver_b": driver_b,
                    "L_common": l_common,
                    "observed_finish_delta_s": pd.NA,
                    "pit_delta_sum_s": pd.NA,
                    "stint_delta_sum_s": pd.NA,
                    "residual_s": pd.NA,
                    "residual_ok": False,
                    "residual_threshold_s": RESIDUAL_THRESHOLD_S,
                    "warnings": warning,
                }
            ],
            columns=COMPARISON_SUMMARY_COLUMNS,
        )

    sum_a = float(
        laps_a[(laps_a["lap_number"] <= l_common) & laps_a["lap_time"].notna()]["lap_time"]
        .astype(float)
        .sum()
    )
    sum_b = float(
        laps_b[(laps_b["lap_number"] <= l_common) & laps_b["lap_time"].notna()]["lap_time"]
        .astype(float)
        .sum()
    )
    observed_finish_delta = sum_a - sum_b

    pit_loss = compute_pit_loss_per_stop(laps, pits, include_sc_vsc_in_aggregate=True)
    pit_a = pit_loss[
        (pit_loss["driver"] == driver_a)
        & (pit_loss["is_computable"] == True)  # noqa: E712
        & (pit_loss["included_in_aggregate"] == True)  # noqa: E712
    ]["pit_loss_s"].dropna()
    pit_b = pit_loss[
        (pit_loss["driver"] == driver_b)
        & (pit_loss["is_computable"] == True)  # noqa: E712
        & (pit_loss["included_in_aggregate"] == True)  # noqa: E712
    ]["pit_loss_s"].dropna()

    pit_delta_sum = float(pit_a.astype(float).sum() - pit_b.astype(float).sum())

    included_windows = comparison_windows[comparison_windows["included"] == True]  # noqa: E712
    stint_delta_sum = float(included_windows["window_delta_s"].dropna().astype(float).sum())

    residual_s = observed_finish_delta - (pit_delta_sum + stint_delta_sum)
    residual_ok = abs(residual_s) <= RESIDUAL_THRESHOLD_S
    if not residual_ok:
        warnings.append("unreconciled residual exceeds 10.0s")

    return pd.DataFrame(
        [
            {
                "driver_a": driver_a,
                "driver_b": driver_b,
                "L_common": l_common,
                "observed_finish_delta_s": observed_finish_delta,
                "pit_delta_sum_s": pit_delta_sum,
                "stint_delta_sum_s": stint_delta_sum,
                "residual_s": residual_s,
                "residual_ok": residual_ok,
                "residual_threshold_s": RESIDUAL_THRESHOLD_S,
                "warnings": " | ".join(warnings),
            }
        ],
        columns=COMPARISON_SUMMARY_COLUMNS,
    )


def load_or_build_comparison_tables(
    laps: pd.DataFrame,
    pits: pd.DataFrame,
    stints: pd.DataFrame,
    *,
    selected_drivers: Sequence[str],
    cache_dir: Path | str = Path("cache"),
) -> tuple[pd.DataFrame, pd.DataFrame, bool, Path, Path]:
    if laps.empty:
        raise CanonicalLapsError("Cannot build comparison tables from empty laps dataframe.")

    driver_a, driver_b = _normalize_driver_codes(selected_drivers)
    season, round_number, session_type = _season_round_session_type(laps)

    resolved_cache_dir = Path(cache_dir)
    resolved_cache_dir.mkdir(parents=True, exist_ok=True)

    windows_path, summary_path = _comparison_cache_paths(
        cache_dir=resolved_cache_dir,
        season=season,
        round_number=round_number,
        session_type=session_type,
        driver_a=driver_a,
        driver_b=driver_b,
    )

    if windows_path.exists() and summary_path.exists():
        windows = pd.read_parquet(windows_path)
        summary = pd.read_parquet(summary_path)
        return windows, summary, True, windows_path, summary_path

    windows = build_comparison_windows(laps, stints, selected_drivers=[driver_a, driver_b])
    summary = build_comparison_summary(
        laps,
        pits,
        stints,
        windows,
        selected_drivers=[driver_a, driver_b],
    )

    windows.to_parquet(windows_path, index=False)
    summary.to_parquet(summary_path, index=False)

    return windows, summary, False, windows_path, summary_path
