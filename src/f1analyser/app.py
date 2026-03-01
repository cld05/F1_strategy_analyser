from __future__ import annotations

import streamlit as st

from f1analyser.session_loader import (
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
        st.subheader("Driver selection")
        st.info("MVP scaffold.")

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
