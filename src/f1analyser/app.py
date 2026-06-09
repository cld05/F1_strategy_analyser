from __future__ import annotations

import streamlit as st

from f1analyser.delta import DeltaLapsError, build_delta_laps, build_delta_plot_rows
from f1analyser.exports import export_analysis_artifacts
from f1analyser.filters import build_laps_filtered
from f1analyser.fitting import LapTrendError, build_lap_trend_inputs
from f1analyser.laps import CanonicalLapsError, load_or_build_canonical_laps
from f1analyser.methods import build_methods_table
from f1analyser.plots import (
    build_cumulative_delta_figure,
    build_lap_time_trend_figure,
    build_per_lap_delta_figure,
    build_telemetry_compare_figure,
    build_track_compare_figure,
)
from f1analyser.session_loader import (
    SessionLoadError,
    available_rounds,
    available_seasons,
    extract_session_metadata,
    load_race_session,
)
from f1analyser.telemetry import (
    TrackCompareError,
    build_corner_markers,
    build_telemetry_compare_rows,
    build_track_compare_rows,
)


TRACK_COMPARISON_LIMITATIONS = """
Remaining limitations  
Corner markers depend on FastF1 circuit metadata availability. When missing or incomplete, the telemetry plot renders without corner markers and shows a warning.
Track Comparison still projects the three official sectors onto the sampled telemetry path using sector-time-derived boundaries. It does not use a circuit-native sector geometry map.
""".strip()

LAP_TRENDS_EXPLANATION = """
**What these graphs show**  
These charts show lap time evolution across the race for the two selected drivers. Each point is one filtered lap, plotted as lap number versus lap time. The data is split by stint, so the graphs let you compare how pace changed within each tyre run rather than across the full race as one continuous series.

**How the trends are calculated**  
Only laps that pass the current filtering rules are used. That means pit laps are excluded, and SC/VSC laps may also be excluded depending on the active setting. For each driver and each stint separately, the app fits a polynomial curve to the remaining lap times using `lap number -> lap time` as the relationship. The fitted line is a smoothing curve, not a raw measurement. It is used to highlight the general direction of pace change within that stint.

**What is useful to look at**  
Relevant signals are:
- whether a stint looks stable, improving, or degrading over time
- whether one driver’s lap times rise faster than the other’s within comparable stints
- whether performance drops sharply near the end of a stint
- whether the raw laps broadly follow the fitted curve or show a lot of scatter

In practical terms, these plots are useful for identifying pace consistency, tyre-run deterioration, and differences in stint shape between the two drivers.

**What cannot be concluded from these graphs**  
These graphs do not prove causation. They do not by themselves tell you why lap times changed. They also do not isolate the effect of tyre compound, fuel load, traffic, battery usage, lift-and-coast, setup differences, or race management decisions. The fitted curve is only a mathematical smoothing of filtered lap times, so it should not be interpreted as a physical tyre model or as a precise degradation law. Higher-degree fits can also exaggerate curvature, especially when a stint has few usable laps.

**Main limitations** 
- Trends are fitted per stint, not as a full-race model.
- Results depend on the current filtering choices.
- Sparse stints may not have enough laps for a reliable fit.
- The fit summarizes shape, but the raw lap points remain the primary evidence.
""".strip()

LAP_TRENDS_DEGREE_RECOMMENDATION = """
**Recommended fit degree**  
Recommended fit degree: start with degree 2. Use degree 1 for short or noisy stints, and degree 3 only when a stint has many clean laps and visible curvature. Higher degrees can overfit and should be interpreted cautiously. If the fitted curve looks more complex than the raw lap pattern, the selected degree is likely too high.

NB: Guidance:
- if the fitted line swings more than the raw points suggest, the degree is probably too high
- if different degrees tell very different stories, trust the raw laps more than the fit
- shorter stints should use lower degrees
- the fit is for summarizing shape, not proving a physical degradation model
""".strip()

RACE_DELTA_EXPLANATION = """
**What these graphs show**  
These charts compare the two selected drivers lap by lap across the race. The per-lap delta chart shows the time difference on each comparable lap. The cumulative delta chart shows how those lap-by-lap differences add up over the race. The sign convention is fixed as `driver A - driver B`, so negative values mean driver A was faster and positive values mean driver B was faster.

**How the deltas are calculated**  
The comparison is built by aligning both drivers by lap number. A lap is included in the plotted delta only when both drivers have a valid comparable lap time for that same lap number. If a lap is missing, invalid, or excluded by the current filtering rules, it remains documented in the underlying table but is not used in the plotted delta series. Per-lap delta is calculated as `lap_time_a - lap_time_b`. Cumulative delta is the running sum of those valid per-lap differences.

**What is useful to look at**  
Relevant signals are:
- who gained time overall across the race
- whether the advantage was steady or came in isolated laps
- where the cumulative gap changed direction
- whether the lap-by-lap differences are small and stable or large and irregular

In practical terms, these charts help identify when one driver was consistently quicker, when the race balance shifted, and whether the final gap came from gradual accumulation or a few decisive laps.

**What cannot be concluded from these graphs**  
These charts do not explain why time was gained or lost. They do not isolate the effect of tyre choice, degradation, fuel load, traffic, battery deployment, lift-and-coast, setup differences, pit strategy, or race management decisions. They are descriptive timing comparisons only. They show where the time difference appeared, not the cause of it.

**Main limitations**  
- Only laps that are valid and comparable for both drivers are plotted.
- Missing or excluded laps can affect how much of the race is actually represented in the charts.
- The sign convention must be read carefully: the delta is always `driver A - driver B`.
- The cumulative curve is a running total of valid lap differences, not a causal explanation of race outcome.
""".strip()


@st.cache_data
def _cached_available_rounds(season: int) -> list[tuple[int, str]]:
    return available_rounds(season)


def _render_debug_tables() -> None:
    show_debug = st.checkbox("Show intermediate tables", value=False)
    if not show_debug:
        st.info("Enable the debug toggle to inspect intermediate tables.")
        return

    tables = [
        ("Canonical laps", st.session_state.get("canonical_laps")),
        ("Filtered laps", st.session_state.get("laps_filtered")),
        ("Delta laps", st.session_state.get("delta_laps")),
        ("Telemetry compare", st.session_state.get("telemetry_compare")),
        ("Dropped laps", st.session_state.get("dropped_laps")),
        ("Methods", st.session_state.get("methods_table")),
        ("Exports", st.session_state.get("exports_table")),
    ]
    for title, table in tables:
        if table is not None:
            st.write(title)
            st.dataframe(table, use_container_width=True)


def _render_session_tab() -> None:
    st.subheader("Session loader (Race only)")

    seasons = available_seasons()
    selected_season = st.selectbox(
        "Season",
        options=seasons,
        index=len(seasons) - 1,
    )
    season_int = int(selected_season)

    try:
        rounds = _cached_available_rounds(season_int)
        round_labels = [f"{n} — {name}" for n, name in rounds]
        round_numbers = [n for n, _ in rounds]
        selected_round_label = st.selectbox("Round", options=round_labels)
        selected_round = round_numbers[round_labels.index(selected_round_label)]
    except SessionLoadError:
        st.warning("Could not fetch round list for this season. Enter round number manually.")
        selected_round = int(
            st.number_input("Round", min_value=1, max_value=30, value=1, step=1)
        )

    if st.button("Load race session", type="primary"):
        with st.spinner("Loading FastF1 race session..."):
            try:
                session = load_race_session(
                    season=season_int,
                    round_number=int(selected_round),
                    timeout_seconds=120,
                    max_retries=2,
                )
            except SessionLoadError as exc:
                st.error(str(exc))
                st.session_state.pop("loaded_session", None)
            else:
                st.session_state["loaded_session"] = session
                st.success("Session loaded.")

    loaded_session = st.session_state.get("loaded_session")
    if loaded_session is None:
        st.info("Choose season and round, then load the Race session.")
        return

    metadata = extract_session_metadata(loaded_session)
    st.write("Session metadata")
    st.table(
        {
            "Field": [
                "Season",
                "Round",
                "Event",
                "Session",
                "Date",
                "Circuit",
            ],
            "Value": [
                metadata.season,
                metadata.round_number,
                metadata.event_name,
                metadata.session_name,
                metadata.event_date,
                metadata.circuit_name,
            ],
        }
    )


def _render_driver_tab() -> None:
    st.subheader("Driver selection")

    loaded_session = st.session_state.get("loaded_session")
    if loaded_session is None:
        st.info("Load a Race session first.")
        return

    raw_drivers = sorted(
        {
            str(driver)
            for driver in loaded_session.laps["Driver"].dropna().unique().tolist()
            if str(driver).strip()
        }
    )
    if len(raw_drivers) < 2:
        st.warning("This session has fewer than 2 drivers with laps data.")
        return

    preferred_defaults = [driver for driver in ("RUS", "LEC") if driver in raw_drivers]
    default_drivers = preferred_defaults if len(preferred_defaults) == 2 else raw_drivers[:2]
    selected_drivers = st.multiselect(
        "Select exactly 2 drivers",
        options=raw_drivers,
        default=default_drivers,
        max_selections=2,
    )

    if st.button("Build canonical laps table", type="primary"):
        if len(selected_drivers) != 2:
            st.error("Select exactly 2 drivers before building race comparison data.")
            st.session_state.pop("canonical_laps", None)
            st.session_state.pop("laps_filtered", None)
            st.session_state.pop("delta_laps", None)
            return
        try:
            laps_df, loaded_from_cache, cache_path = load_or_build_canonical_laps(
                loaded_session,
                selected_drivers,
                cache_dir="cache",
            )
        except CanonicalLapsError as exc:
            st.error(str(exc))
            st.session_state.pop("canonical_laps", None)
            st.session_state.pop("laps_filtered", None)
            st.session_state.pop("filter_warnings", None)
            st.session_state.pop("dropped_laps", None)
            st.session_state.pop("delta_laps", None)
        else:
            filtered_laps, diagnostics = build_laps_filtered(
                laps_df,
                exclude_sc_vsc=bool(st.session_state.get("exclude_sc_vsc", False)),
            )
            try:
                delta_laps = build_delta_laps(filtered_laps, selected_drivers)
            except DeltaLapsError as exc:
                st.error(str(exc))
                st.session_state.pop("delta_laps", None)
            else:
                st.session_state["canonical_laps"] = laps_df
                st.session_state["laps_filtered"] = filtered_laps
                st.session_state["delta_laps"] = delta_laps
                st.session_state["filter_warnings"] = diagnostics.warnings
                st.session_state["dropped_laps"] = diagnostics.dropped_laps
                st.session_state["laps_cache_path"] = str(cache_path)
                st.session_state["laps_from_cache"] = loaded_from_cache
                st.session_state["exclude_sc_vsc"] = bool(st.session_state.get("exclude_sc_vsc", False))
                st.session_state["selected_drivers"] = tuple(selected_drivers)
                st.session_state["polynomial_degree"] = int(st.session_state.get("polynomial_degree", 2))
                source_label = "cache" if loaded_from_cache else "session data"
                st.success(f"Canonical laps loaded from {source_label}.")

    canonical_laps = st.session_state.get("canonical_laps")
    if canonical_laps is None:
        st.info("Select two drivers and build the canonical laps table.")
        return

    cache_path = st.session_state.get("laps_cache_path", "")
    from_cache = st.session_state.get("laps_from_cache", False)
    st.caption(f"Cache file: {cache_path} | cache hit: {from_cache}")
    for warning in st.session_state.get("filter_warnings", []):
        st.warning(warning)
    st.dataframe(canonical_laps, use_container_width=True)
    filtered_laps = st.session_state.get("laps_filtered")
    if filtered_laps is not None:
        st.write("Filtered laps")
        st.dataframe(filtered_laps, use_container_width=True)


def _available_driver_lap_numbers(canonical_laps: object, driver: str) -> list[int]:
    if not hasattr(canonical_laps, "__getitem__"):
        return []
    laps_df = canonical_laps
    driver_laps = laps_df[laps_df["driver"] == driver]
    lap_numbers = driver_laps["lap_number"].dropna().astype("int64").tolist()
    return sorted(dict.fromkeys(int(lap) for lap in lap_numbers))


def _default_lap_index(lap_options: list[int]) -> int:
    if not lap_options:
        return 0
    if 2 in lap_options:
        return lap_options.index(2)
    return 0


def _select_session_lap(session: object, driver: str, lap_number: int) -> object:
    session_laps = getattr(session, "laps", None)
    if session_laps is None:
        raise TrackCompareError("Loaded session has no laps data.")
    if hasattr(session_laps, "pick_drivers"):
        driver_laps = session_laps.pick_drivers([driver])
    else:
        driver_laps = session_laps[session_laps["Driver"] == driver]

    matching_laps = driver_laps[driver_laps["LapNumber"] == lap_number]
    if matching_laps.empty:
        raise TrackCompareError(f"Lap {lap_number} for {driver} is not available.")
    return matching_laps.iloc[0]


def _refresh_analysis_tables() -> None:
    canonical_laps = st.session_state.get("canonical_laps")
    selected_drivers = st.session_state.get("selected_drivers")
    if canonical_laps is None or selected_drivers is None:
        return

    filtered_laps, diagnostics = build_laps_filtered(
        canonical_laps,
        exclude_sc_vsc=bool(st.session_state.get("exclude_sc_vsc", False)),
    )
    delta_laps = build_delta_laps(filtered_laps, selected_drivers)
    st.session_state["laps_filtered"] = filtered_laps
    st.session_state["delta_laps"] = delta_laps
    st.session_state["filter_warnings"] = diagnostics.warnings
    st.session_state["dropped_laps"] = diagnostics.dropped_laps


def _sync_exclude_sc_vsc_from(widget_key: str) -> None:
    st.session_state["exclude_sc_vsc"] = bool(st.session_state.get(widget_key, False))


def _shared_exclude_sc_vsc_checkbox(*, label: str, widget_key: str) -> bool:
    current_value = bool(st.session_state.get("exclude_sc_vsc", False))
    st.session_state[widget_key] = current_value
    return bool(
        st.checkbox(
            label,
            key=widget_key,
            on_change=_sync_exclude_sc_vsc_from,
            args=(widget_key,),
        )
    )


def main() -> None:
    st.set_page_config(page_title="F1 Post-Race Analyzer", layout="wide")
    st.title("F1 Post-Race Analyzer — MVP")

    tabs = st.tabs(
        [
            "1) Session",
            "2) Drivers",
            "3) Race delta",
            "4) Lap trends",
            "5) Track comparison",
            "6) Telemetry comparison",
            "7) Methods",
            "8) Debug",
        ]
    )

    with tabs[0]:
        _render_session_tab()

    with tabs[1]:
        _render_driver_tab()

    with tabs[2]:
        st.subheader("Delta plots")
        if st.session_state.get("canonical_laps") is not None:
            _refresh_analysis_tables()
        delta_laps = st.session_state.get("delta_laps")
        if delta_laps is None:
            st.info("Build canonical laps to prepare delta plots.")
        else:
            try:
                delta_plot_rows = build_delta_plot_rows(delta_laps)
            except DeltaLapsError as exc:
                st.error(str(exc))
            if "delta_plot_rows" in locals():
                cumulative_figure = build_cumulative_delta_figure(delta_plot_rows)
                per_lap_figure = build_per_lap_delta_figure(delta_plot_rows)
                st.plotly_chart(cumulative_figure, use_container_width=True)
                st.plotly_chart(per_lap_figure, use_container_width=True)
                st.markdown(RACE_DELTA_EXPLANATION)
            st.dataframe(delta_laps, use_container_width=True)

    with tabs[3]:
        st.subheader("Lap time trends")
        filtered_laps = st.session_state.get("laps_filtered")
        selected_drivers = st.session_state.get("selected_drivers")
        if filtered_laps is None or selected_drivers is None:
            st.info("Build canonical laps to prepare lap trend plots.")
        else:
            exclude_sc_vsc = _shared_exclude_sc_vsc_checkbox(
                label="Exclude SC/VSC laps from comparison and fitting",
                widget_key="exclude_sc_vsc_lap_trends",
            )
            st.caption(f"SC/VSC exclusion active: {exclude_sc_vsc}")
            if st.session_state.get("canonical_laps") is not None:
                _refresh_analysis_tables()
            filtered_laps = st.session_state.get("laps_filtered")
            if filtered_laps is None:
                st.info("Build canonical laps to prepare lap trend plots.")
            else:
                polynomial_degree = st.selectbox(
                    "Polynomial degree",
                    options=[1, 2, 3, 4, 5],
                    index=1,
                )
                st.caption(LAP_TRENDS_DEGREE_RECOMMENDATION)
                st.session_state["polynomial_degree"] = int(polynomial_degree)
                try:
                    plot_rows, fit_rows, stint_insights, diagnostics = build_lap_trend_inputs(
                        filtered_laps,
                        selected_drivers,
                        polynomial_degree=polynomial_degree,
                    )
                except LapTrendError as exc:
                    st.error(str(exc))
                else:
                    st.session_state["lap_trend_plot_rows"] = plot_rows
                    st.session_state["lap_trend_fit_rows"] = fit_rows
                    st.session_state["lap_trend_stint_insights"] = stint_insights
                    for warning in diagnostics.warnings:
                        st.warning(warning)
                    figure = build_lap_time_trend_figure(
                        plot_rows,
                        fit_rows,
                        selected_drivers,
                        polynomial_degree=polynomial_degree,
                    )
                    st.plotly_chart(figure, use_container_width=True)
                    st.markdown(LAP_TRENDS_EXPLANATION)
                    if not stint_insights.empty:
                        st.write("Stint fit insights")
                        st.dataframe(stint_insights, use_container_width=True)

    with tabs[4]:
        track_header, track_help = st.columns([0.94, 0.06])
        with track_header:
            st.subheader("Track comparison")
        with track_help:
            with st.popover("?"):
                st.markdown(TRACK_COMPARISON_LIMITATIONS)
        loaded_session = st.session_state.get("loaded_session")
        canonical_laps = st.session_state.get("canonical_laps")
        selected_drivers = st.session_state.get("selected_drivers")
        if loaded_session is None or canonical_laps is None or selected_drivers is None:
            st.info("Build canonical laps to prepare track comparison.")
        else:
            driver_a, driver_b = selected_drivers
            lap_options_a = _available_driver_lap_numbers(canonical_laps, driver_a)
            lap_options_b = _available_driver_lap_numbers(canonical_laps, driver_b)
            if not lap_options_a or not lap_options_b:
                st.info("No lap numbers are available for one or both selected drivers.")
            else:
                selected_lap_a = st.selectbox(
                    f"Lap for {driver_a}",
                    options=lap_options_a,
                    index=_default_lap_index(lap_options_a),
                    key="track_lap_a",
                )
                selected_lap_b = st.selectbox(
                    f"Lap for {driver_b}",
                    options=lap_options_b,
                    index=_default_lap_index(lap_options_b),
                    key="track_lap_b",
                )
                st.caption(
                    "Lap 1 is usually not a good comparison lap because the race start, launch phase, "
                    "opening-corner traffic, and position changes distort the pace profile. Lap 2 is "
                    "typically a cleaner baseline when available."
                )
                try:
                    lap_a = _select_session_lap(loaded_session, driver_a, int(selected_lap_a))
                    lap_b = _select_session_lap(loaded_session, driver_b, int(selected_lap_b))
                    track_compare_rows, track_diagnostics = build_track_compare_rows(lap_a, lap_b)
                    corner_markers, _corner_warnings = build_corner_markers(loaded_session)
                except TrackCompareError as exc:
                    st.error(str(exc))
                else:
                    for warning in track_diagnostics.warnings:
                        st.warning(warning)
                    if corner_markers is None:
                        st.warning("Track corner numbering is unavailable for this session.")
                    figure = build_track_compare_figure(track_compare_rows)
                    st.plotly_chart(figure, use_container_width=True)

    with tabs[5]:
        st.subheader("Telemetry comparison")
        loaded_session = st.session_state.get("loaded_session")
        canonical_laps = st.session_state.get("canonical_laps")
        selected_drivers = st.session_state.get("selected_drivers")
        if loaded_session is None or canonical_laps is None or selected_drivers is None:
            st.info("Build canonical laps to prepare telemetry comparison.")
        else:
            driver_a, driver_b = selected_drivers
            lap_options_a = _available_driver_lap_numbers(canonical_laps, driver_a)
            lap_options_b = _available_driver_lap_numbers(canonical_laps, driver_b)
            if not lap_options_a or not lap_options_b:
                st.info("No lap numbers are available for one or both selected drivers.")
            else:
                selected_lap_a = st.selectbox(f"Telemetry lap for {driver_a}", options=lap_options_a, key="telemetry_lap_a")
                selected_lap_b = st.selectbox(f"Telemetry lap for {driver_b}", options=lap_options_b, key="telemetry_lap_b")
                try:
                    lap_a = _select_session_lap(loaded_session, driver_a, int(selected_lap_a))
                    lap_b = _select_session_lap(loaded_session, driver_b, int(selected_lap_b))
                    telemetry_compare_rows, telemetry_diagnostics = build_telemetry_compare_rows(lap_a, lap_b)
                    corner_markers, corner_warnings = build_corner_markers(loaded_session)
                except TrackCompareError as exc:
                    st.error(str(exc))
                else:
                    st.session_state["telemetry_compare"] = telemetry_compare_rows
                    for warning in telemetry_diagnostics.warnings:
                        st.warning(warning)
                    for warning in corner_warnings:
                        st.warning(warning)
                    figure = build_telemetry_compare_figure(
                        telemetry_compare_rows,
                        corner_markers=corner_markers,
                    )
                    st.plotly_chart(figure, use_container_width=True)
                    st.dataframe(telemetry_compare_rows, use_container_width=True)

    with tabs[6]:
        st.subheader("Methods")
        selected_drivers = st.session_state.get("selected_drivers")
        if selected_drivers is None:
            st.info("Build canonical laps to populate methods and exports.")
        else:
            exclude_sc_vsc = bool(st.session_state.get("exclude_sc_vsc", False))
            polynomial_degree = int(st.session_state.get("polynomial_degree", 2))
            methods_table = build_methods_table(
                exclude_sc_vsc=exclude_sc_vsc,
                polynomial_degree=polynomial_degree,
            )
            st.session_state["methods_table"] = methods_table
            st.dataframe(methods_table, use_container_width=True)

            if st.button("Export CSV + PDF", type="primary"):
                loaded_session = st.session_state.get("loaded_session")
                canonical_laps = st.session_state.get("canonical_laps")
                laps_filtered = st.session_state.get("laps_filtered")
                delta_laps = st.session_state.get("delta_laps")
                if loaded_session is None or canonical_laps is None or laps_filtered is None or delta_laps is None:
                    st.error("Build the analysis tables before exporting.")
                else:
                    driver_a, driver_b = selected_drivers
                    metadata = extract_session_metadata(loaded_session)
                    warnings = list(st.session_state.get("filter_warnings", []))
                    export_result = export_analysis_artifacts(
                        season=metadata.season,
                        round_number=metadata.round_number,
                        session_type=metadata.session_name,
                        driver_a=driver_a,
                        driver_b=driver_b,
                        laps=canonical_laps,
                        laps_filtered=laps_filtered,
                        delta_laps=delta_laps,
                        telemetry_compare=st.session_state.get("telemetry_compare"),
                        methods=methods_table,
                        warnings=warnings,
                        polynomial_degree=polynomial_degree,
                        exclude_sc_vsc=exclude_sc_vsc,
                        telemetry_lap_a=st.session_state.get("telemetry_lap_a"),
                        telemetry_lap_b=st.session_state.get("telemetry_lap_b"),
                        lap_trend_rows=st.session_state.get("lap_trend_plot_rows"),
                    )
                    st.session_state["exports_table"] = export_result.exports_row
                    st.session_state["run_log_path"] = str(export_result.run_log_path)
                    st.success(f"Exports written to {export_result.csv_path} and {export_result.pdf_path}")
                    st.dataframe(export_result.exports_row, use_container_width=True)

    with tabs[7]:
        st.subheader("Debug panels")
        for warning in st.session_state.get("filter_warnings", []):
            st.warning(warning)
        run_log_path = st.session_state.get("run_log_path")
        if run_log_path:
            st.caption(f"Run log: {run_log_path}")
        _render_debug_tables()


if __name__ == "__main__":
    main()
