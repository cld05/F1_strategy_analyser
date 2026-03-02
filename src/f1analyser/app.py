from __future__ import annotations

import streamlit as st

try:
    from f1analyser.laps import (
        CanonicalLapsError,
        classify_clean_laps,
        drop_drivers_with_telemetry_gaps,
        load_or_build_canonical_laps,
    )
    from f1analyser.pits_stints import build_stints, detect_pits
    from f1analyser.comparison import load_or_build_comparison_tables
    from f1analyser.session_loader import (
        SessionLoadError,
        available_seasons,
        extract_session_metadata,
        load_race_session,
    )
except ModuleNotFoundError:
    from laps import (
        CanonicalLapsError,
        classify_clean_laps,
        drop_drivers_with_telemetry_gaps,
        load_or_build_canonical_laps,
    )
    from pits_stints import build_stints, detect_pits
    from comparison import load_or_build_comparison_tables
    from session_loader import (
        SessionLoadError,
        available_seasons,
        extract_session_metadata,
        load_race_session,
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
            comparison_windows, comparison_summary, comparison_from_cache, windows_path, summary_path = (
                load_or_build_comparison_tables(
                    filtered_laps,
                    pits,
                    stints,
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
            st.session_state["stints_table"] = stints
            st.session_state["comparison_windows"] = comparison_windows
            st.session_state["comparison_summary"] = comparison_summary
            st.session_state["comparison_from_cache"] = comparison_from_cache
            st.session_state["comparison_windows_cache_path"] = str(windows_path)
            st.session_state["comparison_summary_cache_path"] = str(summary_path)
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
    if pits is None or stints is None:
        st.info("Build canonical laps first to compute pits and stints.")
        return

    st.write("Pits")
    st.dataframe(pits, use_container_width=True)
    st.write("Stints")
    st.dataframe(stints, use_container_width=True)


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
        st.subheader("Plots")
        st.info("MVP scaffold (Plotly).")

    with tabs[5]:
        st.subheader("Methods")
        st.info("MVP scaffold.")

    with tabs[6]:
        st.subheader("Debug panels")
        st.info("MVP scaffold.")


if __name__ == "__main__":
    main()
