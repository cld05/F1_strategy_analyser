from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

REQUIRED_LAPS_COLUMNS: list[str] = [
    "season",
    "round",
    "session_type",
    "driver",
    "driver_id",
    "lap_number",
    "lap_time",
    "compound",
    "tyre_life",
    "track_status",
    "position",
    "gap",
    "pit_in_time",
    "pit_out_time",
    "sector1",
    "sector2",
    "sector3",
]


class CanonicalLapsError(ValueError):
    """Raised when canonical laps input data is invalid."""


TRACK_STATUS_KNOWN_CODES: set[str] = {"1", "2", "4", "5", "6", "7"}
SC_VSC_CODES: set[str] = {"4", "6", "7"}
LOGGER = logging.getLogger(__name__)


def _normalize_driver_codes(drivers: Sequence[str]) -> list[str]:
    normalized = [driver.strip().upper() for driver in drivers if driver.strip()]
    unique = list(dict.fromkeys(normalized))
    if len(unique) != 2:
        raise CanonicalLapsError("Exactly 2 unique driver codes are required.")
    return unique


def _event_value(event: Any, key_a: str, key_b: str, fallback: Any) -> Any:
    return getattr(event, key_a, getattr(event, key_b, fallback))


def _to_seconds(series: pd.Series[Any]) -> pd.Series[Any]:
    if pd.api.types.is_datetime64_any_dtype(series):
        # Datetime pit markers are not durations; keep as missing seconds in canonical laps.
        return pd.Series(np.nan, index=series.index, dtype="float64")
    timedelta_series = pd.to_timedelta(series, errors="coerce")
    return timedelta_series.dt.total_seconds()


def _pick_source_series(
    dataframe: pd.DataFrame,
    names: Iterable[str],
    *,
    default: Any,
) -> pd.Series[Any]:
    for name in names:
        if name in dataframe.columns:
            return dataframe[name]
    return pd.Series([default] * len(dataframe), index=dataframe.index)


def _session_identifiers(session: Any) -> tuple[int, int, str]:
    event = getattr(session, "event", None)
    season = int(_event_value(event, "Year", "year", 0) or 0)
    round_number = int(_event_value(event, "RoundNumber", "round", 0) or 0)
    session_type = str(getattr(session, "name", "Race"))
    return season, round_number, session_type


def canonical_laps_cache_path(
    *,
    cache_dir: Path,
    season: int,
    round_number: int,
    session_type: str,
    selected_drivers: Sequence[str],
) -> Path:
    normalized_drivers = _normalize_driver_codes(selected_drivers)
    drivers_key = "-".join(sorted(normalized_drivers))
    file_name = f"laps-{season}-{round_number}-{session_type.lower()}-{drivers_key}.parquet"
    return cache_dir / file_name


def build_canonical_laps(session: Any, selected_drivers: Sequence[str]) -> pd.DataFrame:
    normalized_drivers = _normalize_driver_codes(selected_drivers)
    season, round_number, session_type = _session_identifiers(session)

    session_laps = getattr(session, "laps", None)
    if session_laps is None:
        raise CanonicalLapsError("Loaded session has no laps data.")

    if hasattr(session_laps, "pick_drivers"):
        raw_laps = session_laps.pick_drivers(normalized_drivers).copy()
    elif isinstance(session_laps, pd.DataFrame):
        raw_laps = session_laps[session_laps["Driver"].isin(normalized_drivers)].copy()
    else:
        raise CanonicalLapsError("Session laps data is not in a supported tabular format.")

    if raw_laps.empty:
        raise CanonicalLapsError("No laps found for the selected drivers.")

    canonical = pd.DataFrame(index=raw_laps.index)
    canonical["season"] = pd.Series(season, index=raw_laps.index, dtype="int64")
    canonical["round"] = pd.Series(round_number, index=raw_laps.index, dtype="int64")
    canonical["session_type"] = pd.Series(session_type, index=raw_laps.index, dtype="string")

    canonical["driver"] = _pick_source_series(raw_laps, ["Driver"], default="").astype("string")
    canonical["driver_id"] = _pick_source_series(
        raw_laps,
        ["DriverNumber", "DriverId"],
        default=pd.NA,
    ).astype("string")

    canonical["lap_number"] = pd.to_numeric(
        _pick_source_series(raw_laps, ["LapNumber"], default=pd.NA),
        errors="coerce",
    ).astype("Int64")
    canonical["lap_time"] = _to_seconds(_pick_source_series(raw_laps, ["LapTime"], default=pd.NaT))

    canonical["compound"] = _pick_source_series(raw_laps, ["Compound"], default=pd.NA).astype("string")
    canonical["tyre_life"] = pd.to_numeric(
        _pick_source_series(raw_laps, ["TyreLife"], default=pd.NA),
        errors="coerce",
    ).astype("Float64")
    canonical["track_status"] = _pick_source_series(
        raw_laps,
        ["TrackStatus"],
        default=pd.NA,
    ).astype("string")
    canonical["position"] = pd.to_numeric(
        _pick_source_series(raw_laps, ["Position"], default=pd.NA),
        errors="coerce",
    ).astype("Float64")

    canonical["gap"] = pd.to_numeric(
        _pick_source_series(raw_laps, ["GapToLeader", "GapToReference"], default=pd.NA),
        errors="coerce",
    ).astype("Float64")

    canonical["pit_in_time"] = _to_seconds(_pick_source_series(raw_laps, ["PitInTime"], default=pd.NaT))
    canonical["pit_out_time"] = _to_seconds(_pick_source_series(raw_laps, ["PitOutTime"], default=pd.NaT))

    canonical["sector1"] = _to_seconds(_pick_source_series(raw_laps, ["Sector1Time"], default=pd.NaT))
    canonical["sector2"] = _to_seconds(_pick_source_series(raw_laps, ["Sector2Time"], default=pd.NaT))
    canonical["sector3"] = _to_seconds(_pick_source_series(raw_laps, ["Sector3Time"], default=pd.NaT))

    canonical = canonical[REQUIRED_LAPS_COLUMNS].sort_values(
        by=["driver", "lap_number"],
        kind="stable",
    )
    canonical = canonical.reset_index(drop=True)

    return canonical


def load_or_build_canonical_laps(
    session: Any,
    selected_drivers: Sequence[str],
    *,
    cache_dir: Path | str = Path("cache"),
) -> tuple[pd.DataFrame, bool, Path]:
    season, round_number, session_type = _session_identifiers(session)
    resolved_cache_dir = Path(cache_dir)
    resolved_cache_dir.mkdir(parents=True, exist_ok=True)

    cache_path = canonical_laps_cache_path(
        cache_dir=resolved_cache_dir,
        season=season,
        round_number=round_number,
        session_type=session_type,
        selected_drivers=selected_drivers,
    )

    if cache_path.exists():
        cached = pd.read_parquet(cache_path)
        return cached, True, cache_path

    canonical = build_canonical_laps(session, selected_drivers)
    canonical.to_parquet(cache_path, index=False)
    return canonical, False, cache_path


def _track_status_codes(track_status: str | None) -> set[str]:
    if track_status is None:
        return set()

    stripped = track_status.strip()
    if not stripped or stripped.lower() == "nan":
        return set()

    return {char for char in stripped if char.isdigit()}


def _has_unknown_track_status_code(track_status: str | None) -> bool:
    codes = _track_status_codes(track_status)
    unknown = codes.difference(TRACK_STATUS_KNOWN_CODES)
    return bool(unknown)


def _has_sc_vsc(track_status: str | None) -> bool:
    codes = _track_status_codes(track_status)
    return bool(codes.intersection(SC_VSC_CODES))


def _has_red_flag(track_status: str | None) -> bool:
    return "5" in _track_status_codes(track_status)


def _is_green(track_status: str | None) -> bool:
    return "1" in _track_status_codes(track_status)


def classify_clean_laps(
    laps: pd.DataFrame,
    *,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    """Annotate canonical laps with clean/excluded classification columns."""
    active_logger = logger or LOGGER
    required = {"driver", "lap_number", "lap_time", "track_status", "pit_in_time", "pit_out_time"}
    missing = required.difference(laps.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise CanonicalLapsError(f"Cannot classify laps; missing required columns: {missing_list}")

    classified = laps.copy()
    classified["track_status"] = classified["track_status"].astype("string")
    classified["is_in_lap"] = classified["pit_in_time"].notna()
    classified["is_out_lap"] = classified["pit_out_time"].notna()
    classified["is_sc_vsc_lap"] = classified["track_status"].map(_has_sc_vsc).fillna(False)
    classified["is_red_flag_lap"] = classified["track_status"].map(_has_red_flag).fillna(False)
    classified["has_invalid_lap_time"] = classified["lap_time"].isna()
    classified["exclude_red_flag_pre"] = False
    classified["exclude_red_flag_post"] = False

    unknown_codes = sorted(
        {
            "".join(sorted(_track_status_codes(str(value)).difference(TRACK_STATUS_KNOWN_CODES)))
            for value in classified["track_status"].dropna().unique().tolist()
            if _has_unknown_track_status_code(str(value))
        }
    )
    for unknown in unknown_codes:
        active_logger.warning("warning: unknown track status code %s; treated as green", unknown)

    for _, driver_rows in classified.groupby("driver", sort=False):
        ordered = driver_rows.sort_values("lap_number", kind="stable")
        index_list = ordered.index.to_list()
        for pos, row_idx in enumerate(index_list):
            is_red = bool(ordered.at[row_idx, "is_red_flag_lap"])
            if not is_red:
                continue

            prev_idx = index_list[pos - 1] if pos > 0 else None
            if prev_idx is not None and not bool(ordered.at[prev_idx, "is_red_flag_lap"]):
                classified.at[prev_idx, "exclude_red_flag_pre"] = True

            next_idx = index_list[pos + 1] if pos + 1 < len(index_list) else None
            if next_idx is not None and not bool(ordered.at[next_idx, "is_red_flag_lap"]):
                next_status = str(ordered.at[next_idx, "track_status"])
                if _is_green(next_status):
                    classified.at[next_idx, "exclude_red_flag_post"] = True

    classified["is_excluded"] = (
        classified["is_in_lap"]
        | classified["is_out_lap"]
        | classified["is_sc_vsc_lap"]
        | classified["is_red_flag_lap"]
        | classified["exclude_red_flag_pre"]
        | classified["exclude_red_flag_post"]
        | classified["has_invalid_lap_time"]
    )
    classified["is_clean"] = ~classified["is_excluded"]
    classified["n_total"] = 1
    classified["n_clean"] = classified["is_clean"].astype("int64")
    classified["n_excluded"] = classified["is_excluded"].astype("int64")

    return classified


def drop_drivers_with_telemetry_gaps(
    classified_laps: pd.DataFrame,
    *,
    threshold: float = 0.10,
    logger: logging.Logger | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Drop drivers when missing lap_time rows exceed the configured threshold."""
    active_logger = logger or LOGGER
    if "driver" not in classified_laps.columns or "has_invalid_lap_time" not in classified_laps.columns:
        raise CanonicalLapsError(
            "Cannot drop telemetry-gap drivers; expected driver and has_invalid_lap_time columns."
        )

    dropped_drivers: list[str] = []
    for driver, rows in classified_laps.groupby("driver", sort=False):
        total = len(rows)
        if total == 0:
            continue
        invalid = int(rows["has_invalid_lap_time"].sum())
        gap_ratio = invalid / total
        if gap_ratio > threshold:
            driver_code = str(driver)
            dropped_drivers.append(driver_code)
            active_logger.warning(
                "warning: telemetry gaps exceed 10%% for driver %s; dropping from analysis",
                driver_code,
            )

    if not dropped_drivers:
        return classified_laps.copy(), []

    filtered = classified_laps[~classified_laps["driver"].isin(dropped_drivers)].copy()
    return filtered.reset_index(drop=True), dropped_drivers
