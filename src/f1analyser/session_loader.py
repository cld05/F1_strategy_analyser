from __future__ import annotations

import importlib
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

LOGGER = logging.getLogger(__name__)
_MIN_SEASON = 2020


class SessionLoadError(RuntimeError):
    """Raised when a race session cannot be loaded after retries."""


@dataclass(frozen=True)
class SessionMetadata:
    season: int
    round_number: int
    event_name: str
    session_name: str
    event_date: str
    circuit_name: str


def current_season() -> int:
    return datetime.now().year


def available_seasons(start: int = _MIN_SEASON) -> list[int]:
    return list(range(start, current_season() + 1))


def _get_fastf1_module() -> Any:
    try:
        return importlib.import_module("fastf1")
    except ModuleNotFoundError as exc:
        raise SessionLoadError(
            "FastF1 is not installed in this environment. Install project dependencies first."
        ) from exc


def load_race_session(
    season: int,
    round_number: int,
    *,
    timeout_seconds: float = 120.0,
    max_retries: int = 2,
    logger: logging.Logger | None = None,
) -> Any:
    if season < _MIN_SEASON or season > current_season():
        raise ValueError(f"Season must be between {_MIN_SEASON} and {current_season()}.")
    if round_number < 1:
        raise ValueError("Round number must be >= 1.")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be > 0.")
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0.")

    active_logger = logger or LOGGER
    attempt = 0

    while True:
        try:
            fastf1_module = _get_fastf1_module()
            session = fastf1_module.get_session(season, round_number, "R")
            _load_with_timeout(session, timeout_seconds)
            return session
        except SessionLoadError:
            raise
        except Exception as exc:
            if attempt >= max_retries:
                raise SessionLoadError(
                    "Failed to load race session "
                    f"(season={season}, round={round_number}) after {attempt + 1} attempts."
                ) from exc
            attempt += 1
            active_logger.warning(
                "Race session load failed (season=%s, round=%s, retry=%s/%s): %s",
                season,
                round_number,
                attempt,
                max_retries,
                exc,
            )


def _load_with_timeout(session: Any, timeout_seconds: float) -> None:
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(session.load)
    try:
        future.result(timeout=timeout_seconds)
    except FuturesTimeoutError as exc:
        future.cancel()
        raise TimeoutError(
            f"Session load exceeded timeout of {timeout_seconds:.1f} seconds"
        ) from exc
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def extract_session_metadata(session: Any) -> SessionMetadata:
    event = getattr(session, "event", None)

    season = int(getattr(event, "Year", getattr(event, "year", 0)) or 0)
    round_number = int(getattr(event, "RoundNumber", getattr(event, "round", 0)) or 0)
    event_name = str(getattr(event, "EventName", getattr(event, "name", "Unknown")))
    session_name = str(getattr(session, "name", "Race"))

    event_date_obj = getattr(event, "EventDate", getattr(event, "date", None))
    event_date = "Unknown"
    if isinstance(event_date_obj, datetime):
        event_date = event_date_obj.date().isoformat()
    elif isinstance(event_date_obj, date):
        event_date = event_date_obj.isoformat()
    elif event_date_obj is not None:
        event_date = str(event_date_obj)

    circuit_name = str(
        getattr(event, "Location", getattr(event, "CircuitShortName", "Unknown"))
    )

    return SessionMetadata(
        season=season,
        round_number=round_number,
        event_name=event_name,
        session_name=session_name,
        event_date=event_date,
        circuit_name=circuit_name,
    )
