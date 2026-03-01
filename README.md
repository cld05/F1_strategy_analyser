# F1 Post-Race Analyzer (Streamlit MVP)

Streamlit app to load an F1 Race session (FastF1), compute deterministic strategy metrics (stints, pits, pit loss, pace, degradation), compare two drivers up to a common lap cutoff, visualize results (Plotly), and export CSV + single-page PDF.

## Scope (MVP)
- Session: **Race only**
- Seasons: **2020 → current**
- Drivers: **exactly 2**
- DNFs: compute up to `L_common = min(completed_laps_A, completed_laps_B)`; block if `L_common < 1`
- Canonical tables: `laps`, `stints`, `pits`, `methods`, `comparison_windows`, `comparison_summary`, `exports`
- Cache: processed tables persisted as **Parquet** under `./cache/`
- Exports: **CSV (raw + derived)** and **single-page PDF (derived only)**
- Requirements and formulas are defined in `REQUIREMENTS.md` (authoritative)

## Repository layout