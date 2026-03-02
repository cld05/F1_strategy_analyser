from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import plotly.express as px
import streamlit as st

if TYPE_CHECKING:
    from f1analyser.laps import (
        CanonicalLapsError,
        classify_clean_laps,
        drop_drivers_with_telemetry_gaps,
        load_or_build_canonical_laps,
    )
    from f1analyser.metrics import compute_pit_loss_per_stop, compute_stint_metrics
    from f1analyser.pits_stints import build_stints, detect_pits
    from f1analyser.comparison import load_or_build_comparison_tables
    from f1analyser.session_loader import (
        SessionLoadError,
        available_seasons,
        extract_session_metadata,
        load_race_session,
    )
else:
    try:
        from f1analyser.laps import (
            CanonicalLapsError,
            classify_clean_laps,
            drop_drivers_with_telemetry_gaps,
            load_or_build_canonical_laps,
        )
        from f1analyser.metrics import compute_pit_loss_per_stop, compute_stint_metrics
        from f1analyser.pits_stints import build_stints, detect_pits
        from f1analyser.comparison import load_or_build_comparison_tables
        from f1analyser.session_loader import (
            SessionLoadError,
            available_seasons,
            extract_session_metadata,
            load_race_session,
        )
    except ModuleNotFoundError:
        from laps import (  # type: ignore[import-not-found]
            CanonicalLapsError,
            classify_clean_laps,
            drop_drivers_with_telemetry_gaps,
            load_or_build_canonical_laps,
        )
        from metrics import compute_pit_loss_per_stop, compute_stint_metrics  # type: ignore[import-not-found]
        from pits_stints import build_stints, detect_pits  # type: ignore[import-not-found]
        from comparison import load_or_build_comparison_tables  # type: ignore[import-not-found]
        from session_loader import (  # type: ignore[import-not-found]
            SessionLoadError,
            available_seasons,
            extract_session_metadata,
            load_race_session,
        )


def _methods_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "method": "clean_lap_definition",
                "value": "exclude in/out, SC/VSC, red-flag + boundary, invalid lap_time",
            },
            {
                "method": "pit_detection",
                "value": "PitInTime/PitOutTime with missing-marker rules",
            },
            {
                "method": "stint_segmentation",
                "value": "pit boundaries, compound change, red-flag tyre continuity",
            },
            {
                "method": "pit_loss_method",
                "value": "baseline median from up to 3 clean pre + 3 clean post laps",
            },
            {"method": "pace_method", "value": "median clean lap time per stint"},
            {
                "method": "degradation_method",
                "value": "linear slope and first-last clean delta",
            },
            {
                "method": "comparison_overlap_rule",
                "value": "include window iff min(clean overlap A,B) >= 3",
            },
            {"method": "residual_threshold_s", "value": "10.0"},
            {"method": "sc_vsc_in_pit_aggregate", "value": "included"},
        ]
    )


def _render_session_tab() -> None:
    st.subheader("Session loader (Race only)")

    seasons = available_seasons()
    selected_season = st.selectbox(
        "Season",
        options=seasons,
        index=len(seasons) - 1,
    )
    selected_round = st.number_input(
        "Round",
        min_value=1,
        max_value=30,
        value=1,
        step=1,
    )

    if st.button("Load race session", type="primary"):
        with st.spinner("Loading FastF1 race session..."):
            try:
                session = load_race_session(
                    season=int(selected_season),
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

    selected_drivers = st.multiselect(
        "Select exactly 2 drivers",
        options=raw_drivers,
        default=raw_drivers[:2],
        max_selections=2,
    )

    if st.button("Build canonical laps table", type="primary"):
        try:
            laps_df, loaded_from_cache, cache_path = load_or_build_canonical_laps(
                loaded_session,
                selected_drivers,
                cache_dir="cache",
            )
            classified = classify_clean_laps(laps_df)
            filtered_laps, dropped_drivers = drop_drivers_with_telemetry_gaps(classified)
            pits = detect_pits(filtered_laps)
            stints = build_stints(filtered_laps, pits)
            pit_loss_table = compute_pit_loss_per_stop(
                filtered_laps,
                pits,
                include_sc_vsc_in_aggregate=True,
            )
            stints_with_metrics = compute_stint_metrics(filtered_laps, stints)
            comparison_windows, comparison_summary, comparison_from_cache, windows_path, summary_path = (
                load_or_build_comparison_tables(
                    filtered_laps,
                    pits,
                    stints_with_metrics,
                    selected_drivers=selected_drivers,
                    cache_dir="cache",
                )
            )
        except CanonicalLapsError as exc:
            st.error(str(exc))
            st.session_state.pop("canonical_laps", None)
        else:
            st.session_state["canonical_laps"] = filtered_laps
            st.session_state["laps_cache_path"] = str(cache_path)
            st.session_state["laps_from_cache"] = loaded_from_cache
            st.session_state["dropped_drivers"] = dropped_drivers
            st.session_state["selected_drivers"] = selected_drivers
            st.session_state["pits_table"] = pits
            st.session_state["stints_table"] = stints_with_metrics
            st.session_state["pit_loss_table"] = pit_loss_table
            st.session_state["comparison_windows"] = comparison_windows
            st.session_state["comparison_summary"] = comparison_summary
            st.session_state["comparison_from_cache"] = comparison_from_cache
            st.session_state["comparison_windows_cache_path"] = str(windows_path)
            st.session_state["comparison_summary_cache_path"] = str(summary_path)
            st.session_state["methods_table"] = _methods_table()
            source_label = "cache" if loaded_from_cache else "session data"
            st.success(f"Canonical laps loaded from {source_label}.")

    canonical_laps = st.session_state.get("canonical_laps")
    if canonical_laps is None:
        st.info("Select two drivers and build the canonical laps table.")
        return

    cache_path = st.session_state.get("laps_cache_path", "")
    from_cache = st.session_state.get("laps_from_cache", False)
    dropped_drivers = st.session_state.get("dropped_drivers", [])
    st.caption(f"Cache file: {cache_path} | cache hit: {from_cache}")
    if dropped_drivers:
        st.warning(f"Dropped drivers due to telemetry gaps >10%: {', '.join(dropped_drivers)}")
    st.dataframe(canonical_laps, use_container_width=True)


def _render_stints_pits_tab() -> None:
    st.subheader("Stints & pits tables")
    pits = st.session_state.get("pits_table")
    stints = st.session_state.get("stints_table")
    pit_loss = st.session_state.get("pit_loss_table")
    if pits is None or stints is None or pit_loss is None:
        st.info("Build canonical laps first to compute pits and stints.")
        return

    st.write("Pits")
    st.dataframe(pits, use_container_width=True)
    st.write("Stints")
    st.dataframe(stints, use_container_width=True)
    st.write("Pit loss per stop")
    st.dataframe(pit_loss, use_container_width=True)


def _render_comparison_tab() -> None:
    st.subheader("Comparison summary")
    windows = st.session_state.get("comparison_windows")
    summary = st.session_state.get("comparison_summary")
    if windows is None or summary is None:
        st.info("Build canonical laps first to compute comparison tables.")
        return

    windows_cache_path = st.session_state.get("comparison_windows_cache_path", "")
    summary_cache_path = st.session_state.get("comparison_summary_cache_path", "")
    from_cache = st.session_state.get("comparison_from_cache", False)
    st.caption(
        "Comparison cache: "
        f"hit={from_cache} | windows={windows_cache_path} | summary={summary_cache_path}"
    )

    warnings_value = str(summary.iloc[0].get("warnings", "")).strip()
    if warnings_value:
        st.warning(warnings_value)

    excluded = windows[windows["included"] == False]  # noqa: E712
    if not excluded.empty:
        reasons = ", ".join(sorted({str(value) for value in excluded["exclude_reason"].dropna().tolist()}))
        if reasons:
            st.warning(f"Excluded windows: {reasons}")

    st.write("Comparison summary table")
    st.dataframe(summary, use_container_width=True)
    st.write("Comparison windows table")
    st.dataframe(windows, use_container_width=True)


def _render_plots_tab() -> None:
    st.subheader("Plots")
    laps = st.session_state.get("canonical_laps")
    pit_loss = st.session_state.get("pit_loss_table")
    stints = st.session_state.get("stints_table")
    selected_drivers = st.session_state.get("selected_drivers")
    if laps is None or pit_loss is None or stints is None or not selected_drivers:
        st.info("Build canonical laps first to render plots.")
        return

    laps_df: pd.DataFrame = laps
    driver_a = str(selected_drivers[0])
    driver_b = str(selected_drivers[1])

    if "gap" in laps_df.columns and laps_df["gap"].notna().any():
        gap_fig = px.line(
            laps_df,
            x="lap_number",
            y="gap",
            color="driver",
            title="Gap over laps",
        )
        st.plotly_chart(gap_fig, use_container_width=True)

    delta_source = laps_df[laps_df["driver"].isin([driver_a, driver_b])][["lap_number", "driver", "lap_time"]]
    if not delta_source.empty:
        pivot = delta_source.pivot_table(index="lap_number", columns="driver", values="lap_time", aggfunc="first")
        if driver_a in pivot.columns and driver_b in pivot.columns:
            pivot = pivot[[driver_a, driver_b]].dropna()
            if not pivot.empty:
                pivot["cumulative_delta_s"] = pivot[driver_a].cumsum() - pivot[driver_b].cumsum()
                delta_fig = px.line(
                    pivot.reset_index(),
                    x="lap_number",
                    y="cumulative_delta_s",
                    title=f"Cumulative lap-time delta ({driver_a} - {driver_b})",
                )
                st.plotly_chart(delta_fig, use_container_width=True)

    pit_loss_df: pd.DataFrame = pit_loss
    pit_loss_plot = pit_loss_df[pit_loss_df["pit_loss_s"].notna()]
    if not pit_loss_plot.empty:
        pit_loss_fig = px.bar(
            pit_loss_plot,
            x="stop_index",
            y="pit_loss_s",
            color="driver",
            barmode="group",
            title="Pit loss per stop",
        )
        st.plotly_chart(pit_loss_fig, use_container_width=True)

    stints_df: pd.DataFrame = stints
    pace_plot = stints_df[stints_df["pace_median_s"].notna()]
    if not pace_plot.empty:
        pace_fig = px.bar(
            pace_plot,
            x="stint_id",
            y="pace_median_s",
            color="driver",
            barmode="group",
            title="Stint pace per stint",
        )
        st.plotly_chart(pace_fig, use_container_width=True)


def _render_methods_tab() -> None:
    st.subheader("Methods")
    methods = st.session_state.get("methods_table")
    if methods is None:
        st.info("Build canonical laps first to view methods.")
        return
    st.dataframe(methods, use_container_width=True)


def _render_debug_tab() -> None:
    st.subheader("Debug panels")
    show_debug = st.checkbox("Show intermediate tables", value=False)
    if not show_debug:
        st.info("Enable the toggle to inspect intermediate tables.")
        return

    laps = st.session_state.get("canonical_laps")
    windows = st.session_state.get("comparison_windows")
    if laps is None or windows is None:
        st.info("Build canonical laps first.")
        return

    laps_df: pd.DataFrame = laps
    st.write("Laps (canonical)")
    st.dataframe(laps_df, use_container_width=True)

    st.write("Clean/excluded annotations")
    annotation_cols = [
        "driver",
        "lap_number",
        "is_clean",
        "is_excluded",
        "is_in_lap",
        "is_out_lap",
        "is_sc_vsc_lap",
        "is_red_flag_lap",
        "exclude_red_flag_pre",
        "exclude_red_flag_post",
        "has_invalid_lap_time",
    ]
    available_cols = [col for col in annotation_cols if col in laps_df.columns]
    if available_cols:
        st.dataframe(laps_df[available_cols], use_container_width=True)

    st.write("Comparison windows")
    st.dataframe(windows, use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="F1 Post-Race Analyzer", layout="wide")
    st.title("F1 Post-Race Analyzer — MVP")

    tabs = st.tabs(
        [
            "1) Session",
            "2) Drivers",
            "3) Stints & Pits",
            "4) Comparison Summary",
            "5) Plots",
            "6) Methods",
            "7) Debug",
        ]
    )

    with tabs[0]:
        _render_session_tab()

    with tabs[1]:
        _render_driver_tab()

    with tabs[2]:
        _render_stints_pits_tab()

    with tabs[3]:
        _render_comparison_tab()

    with tabs[4]:
        _render_plots_tab()

    with tabs[5]:
        _render_methods_tab()

    with tabs[6]:
        _render_debug_tab()


if __name__ == "__main__":
    main()
