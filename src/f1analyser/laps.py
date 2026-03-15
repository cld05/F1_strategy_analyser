from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd  # type: ignore[import-untyped]

REQUIRED_LAPS_COLUMNS: list[str] = [
    "season",
    "round",
    "session_type",
    "event_name",
    "driver",
    "driver_number",
    "team",
    "lap_number",
    "lap_time_s",
    "compound",
    "tyre_life",
    "stint",
    "track_status",
    "position",
    "pit_in_time",
    "pit_out_time",
    "is_pit_in_lap",
    "is_pit_out_lap",
    "is_pit_lap",
    "is_sc_vsc_lap",
    "is_valid_lap_time",
]

SC_VSC_CODES: set[str] = {"4", "6", "7"}
LOGGER = logging.getLogger(__name__)


class CanonicalLapsError(ValueError):
    """Raised when canonical laps input data is invalid."""


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


def _session_identifiers(session: Any) -> tuple[int, int, str, str]:
    event = getattr(session, "event", None)
    season = int(_event_value(event, "Year", "year", 0) or 0)
    round_number = int(_event_value(event, "RoundNumber", "round", 0) or 0)
    session_type = str(getattr(session, "name", "Race"))
    event_name = str(_event_value(event, "EventName", "name", "Unknown"))
    return season, round_number, session_type, event_name


def _track_status_codes(track_status: str | None) -> set[str]:
    if track_status is None:
        return set()
    stripped = track_status.strip()
    if not stripped or stripped.lower() == "nan":
        return set()
    return {char for char in stripped if char.isdigit()}


def _has_sc_vsc(track_status: str | None) -> bool:
    return bool(_track_status_codes(track_status).intersection(SC_VSC_CODES))


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
    season, round_number, session_type, event_name = _session_identifiers(session)

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
    canonical["event_name"] = pd.Series(event_name, index=raw_laps.index, dtype="string")
    canonical["driver"] = _pick_source_series(raw_laps, ["Driver"], default="").astype("string")
    canonical["driver_number"] = _pick_source_series(
        raw_laps,
        ["DriverNumber"],
        default=pd.NA,
    ).astype("string")
    canonical["team"] = _pick_source_series(raw_laps, ["Team"], default=pd.NA).astype("string")
    canonical["lap_number"] = pd.to_numeric(
        _pick_source_series(raw_laps, ["LapNumber"], default=pd.NA),
        errors="coerce",
    ).astype("Int64")
    canonical["lap_time_s"] = _to_seconds(_pick_source_series(raw_laps, ["LapTime"], default=pd.NaT))
    canonical["compound"] = _pick_source_series(raw_laps, ["Compound"], default=pd.NA).astype("string")
    canonical["tyre_life"] = pd.to_numeric(
        _pick_source_series(raw_laps, ["TyreLife"], default=pd.NA),
        errors="coerce",
    ).astype("Float64")
    canonical["stint"] = pd.to_numeric(
        _pick_source_series(raw_laps, ["Stint"], default=pd.NA),
        errors="coerce",
    ).astype("Int64")
    canonical["track_status"] = _pick_source_series(raw_laps, ["TrackStatus"], default=pd.NA).astype("string")
    canonical["position"] = pd.to_numeric(
        _pick_source_series(raw_laps, ["Position"], default=pd.NA),
        errors="coerce",
    ).astype("Float64")
    canonical["pit_in_time"] = _pick_source_series(raw_laps, ["PitInTime"], default=pd.NaT)
    canonical["pit_out_time"] = _pick_source_series(raw_laps, ["PitOutTime"], default=pd.NaT)
    canonical["is_pit_in_lap"] = canonical["pit_in_time"].notna()
    canonical["is_pit_out_lap"] = canonical["pit_out_time"].notna()
    canonical["is_pit_lap"] = canonical["is_pit_in_lap"] | canonical["is_pit_out_lap"]
    canonical["is_sc_vsc_lap"] = canonical["track_status"].map(lambda value: _has_sc_vsc(str(value))).fillna(False)
    canonical["is_valid_lap_time"] = canonical["lap_time_s"].notna()

    canonical = canonical[REQUIRED_LAPS_COLUMNS].sort_values(
        by=["driver", "lap_number"],
        kind="stable",
    )
    return canonical.reset_index(drop=True)


def load_or_build_canonical_laps(
    session: Any,
    selected_drivers: Sequence[str],
    *,
    cache_dir: Path | str = Path("cache"),
) -> tuple[pd.DataFrame, bool, Path]:
    season, round_number, session_type, _event_name = _session_identifiers(session)
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
        if set(REQUIRED_LAPS_COLUMNS).issubset(cached.columns):
            return cached, True, cache_path
        LOGGER.warning("Cached laps file %s has outdated schema; rebuilding.", cache_path)

    canonical = build_canonical_laps(session, selected_drivers)
    canonical.to_parquet(cache_path, index=False)
    return canonical, False, cache_path
