from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

LOGGER = logging.getLogger(__name__)


class ChampionshipLoadError(RuntimeError):
    """Raised when championship data cannot be loaded."""


@dataclass
class RoundResult:
    round_number: int
    event_name: str
    results: pd.DataFrame


def _get_fastf1() -> Any:
    try:
        return importlib.import_module("fastf1")
    except ModuleNotFoundError as exc:
        raise ChampionshipLoadError("FastF1 is not installed.") from exc


def _event_is_past(event_date: Any) -> bool:
    today = date.today()
    if isinstance(event_date, pd.Timestamp):
        return bool(event_date.date() < today)
    if isinstance(event_date, datetime):
        return event_date.date() < today
    if isinstance(event_date, date):
        return event_date < today
    return False


def load_season_results(season: int) -> list[RoundResult]:
    """Return race results for all completed rounds of a season."""
    fastf1 = _get_fastf1()
    try:
        schedule = fastf1.get_event_schedule(season, include_testing=False)
    except Exception as exc:
        raise ChampionshipLoadError(f"Cannot fetch schedule for {season}.") from exc

    completed = [
        (int(row["RoundNumber"]), str(row["EventName"]))
        for _, row in schedule.iterrows()
        if int(row["RoundNumber"]) >= 1 and _event_is_past(row["EventDate"])
    ]

    season_results: list[RoundResult] = []
    for round_num, event_name in completed:
        try:
            session = fastf1.get_session(season, round_num, "R")
            session.load(laps=False, telemetry=False, weather=False, messages=False)
            wanted = ["Abbreviation", "FullName", "TeamName", "TeamColor", "Points", "Time", "Status", "Position"]
            available = [c for c in wanted if c in session.results.columns]
            df = session.results[available].copy()
            df["RoundNumber"] = round_num
            df["EventName"] = event_name
            season_results.append(RoundResult(round_num, event_name, df))
        except Exception as exc:
            LOGGER.warning("Skipping round %s (%s): %s", round_num, event_name, exc)

    return season_results


def available_drivers(season_results: list[RoundResult]) -> list[str]:
    if not season_results:
        return []
    drivers: set[str] = set()
    for r in season_results:
        drivers.update(r.results["Abbreviation"].dropna().tolist())
    return sorted(drivers)


def available_teams(season_results: list[RoundResult]) -> list[str]:
    if not season_results:
        return []
    teams: set[str] = set()
    for r in season_results:
        teams.update(r.results["TeamName"].dropna().tolist())
    return sorted(teams)


def build_driver_cumulative_points(
    season_results: list[RoundResult], drivers: list[str]
) -> pd.DataFrame:
    rows = []
    for r in season_results:
        df = r.results
        for driver in drivers:
            mask = df["Abbreviation"] == driver
            if mask.any():
                row_data = df[mask].iloc[0]
                pts = float(row_data["Points"]) if pd.notna(row_data["Points"]) else 0.0
                color = str(row_data["TeamColor"]) if "TeamColor" in df.columns and pd.notna(row_data.get("TeamColor")) else ""
            else:
                pts = 0.0
                color = ""
            rows.append({
                "RoundNumber": r.round_number,
                "EventName": r.event_name,
                "Driver": driver,
                "Points": pts,
                "TeamColor": color,
            })

    if not rows:
        return pd.DataFrame(columns=["RoundNumber", "EventName", "Driver", "Points", "TeamColor", "CumulativePoints"])

    out = pd.DataFrame(rows).sort_values("RoundNumber")
    out["CumulativePoints"] = out.groupby("Driver")["Points"].cumsum()
    return out


def build_team_cumulative_points(
    season_results: list[RoundResult], teams: list[str]
) -> pd.DataFrame:
    rows = []
    for r in season_results:
        df = r.results
        for team in teams:
            mask = df["TeamName"] == team
            pts = float(df.loc[mask, "Points"].sum()) if mask.any() else 0.0
            rows.append({
                "RoundNumber": r.round_number,
                "EventName": r.event_name,
                "Team": team,
                "Points": pts,
            })

    if not rows:
        return pd.DataFrame(columns=["RoundNumber", "EventName", "Team", "Points", "CumulativePoints"])

    out = pd.DataFrame(rows).sort_values("RoundNumber")
    out["CumulativePoints"] = out.groupby("Team")["Points"].cumsum()
    return out


def build_driver_gap_table(
    season_results: list[RoundResult], drivers: list[str]
) -> pd.DataFrame:
    """Finish gap in seconds relative to the fastest classified selected driver per round."""
    rows = []
    for r in season_results:
        df = r.results
        for driver in drivers:
            mask = df["Abbreviation"] == driver
            if not mask.any():
                continue
            row_data = df[mask].iloc[0]
            position = row_data.get("Position") if "Position" in df.columns else None
            time_val = row_data.get("Time") if "Time" in df.columns else None

            if pd.notna(position) and int(position) == 1:
                gap_s: float | None = 0.0
            elif time_val is not None and pd.notna(time_val) and hasattr(time_val, "total_seconds"):
                gap_s = float(time_val.total_seconds())
            else:
                gap_s = None

            rows.append({
                "RoundNumber": r.round_number,
                "EventName": r.event_name,
                "Driver": driver,
                "GapToWinner": gap_s,
            })

    if not rows:
        return pd.DataFrame(columns=["RoundNumber", "EventName", "Driver", "GapSeconds"])

    out = pd.DataFrame(rows).sort_values("RoundNumber")
    min_gap = out.groupby("RoundNumber")["GapToWinner"].transform("min")
    out["GapSeconds"] = out["GapToWinner"] - min_gap
    return out.drop(columns=["GapToWinner"])
