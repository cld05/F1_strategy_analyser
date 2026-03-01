import streamlit as st

st.set_page_config(page_title="F1 Post-Race Analyzer", layout="wide")
st.title("F1 Post-Race Analyzer — MVP")

tabs = st.tabs([
    "1) Session",
    "2) Drivers",
    "3) Stints & Pits",
    "4) Comparison Summary",
    "5) Plots",
    "6) Methods",
    "7) Debug",
])

with tabs[0]:
    st.subheader("Session loader (Race only)")
    st.info("MVP scaffold. Loader will be implemented in Milestone 2.")

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