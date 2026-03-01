from __future__ import annotations

import streamlit as st

from laps import (
    CanonicalLapsError,
    classify_clean_laps,
    drop_drivers_with_telemetry_gaps,
    load_or_build_canonical_laps,
)
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
        except CanonicalLapsError as exc:
            st.error(str(exc))
            st.session_state.pop("canonical_laps", None)
        else:
            st.session_state["canonical_laps"] = filtered_laps
            st.session_state["laps_cache_path"] = str(cache_path)
            st.session_state["laps_from_cache"] = loaded_from_cache
            st.session_state["dropped_drivers"] = dropped_drivers
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
        st.subheader("Stints & pits tables")
        st.info("MVP scaffold.")

    with tabs[3]:
        st.subheader("Comparison summary")
        st.info("MVP scaffold.")

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
