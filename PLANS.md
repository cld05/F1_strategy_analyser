# PLANS.md — Implementation milestones (Streamlit MVP v2)

## Milestone 1 — Repo scaffold + environment + app shell
- Create `src/` layout, package skeleton, pyproject.toml with dependencies
- Add `AGENTS.md`, `REQUIREMENTS.md`, `IMPLEMENT.md`, `README.md`
- Streamlit app with tabs scaffold
- Minimal pytest smoke test (imports package)
- mypy strict configured and passing

## Milestone 2 — Session loader (Race only) + bounded execution
- Implement season/round selector (2020–present) and Race session load via FastF1
- Implement timeout (120s) and retry logic (max 2), with warnings logged
- Display session metadata in UI
- Add mocked tests for loading flow without network reliance

## Milestone 3 — Canonical driver laps extraction + cache-first persistence
- Build canonical per-driver laps dataframe from FastF1 `session.laps`
- Persist canonical tables to `./cache/` as Parquet
- Load cache-first for previously processed session/driver combinations
- Add tests validating schema, non-empty outputs, and required columns

## Milestone 4 — Replace old analytical model with direct FastF1-driven model
- Remove deprecated custom pit/stint reconstruction logic
- Use FastF1 laps fields directly for stint/compound/pit-related visualization inputs
- Define canonical filtering rules for:
  - pit laps removal
  - optional SC/VSC removal
  - missing lap-time removal
- Add warnings and debug visibility for dropped laps
- Remove obsolete comparison-window and residual logic from codebase

## Milestone 5 — Driver selection + lap-level comparison data prep
- Enforce exactly two selected drivers
- Build aligned race-lap comparison tables for selected drivers
- Compute:
  - lap time per driver
  - cumulative race time delta between selected drivers
  - per-lap delta where both laps are valid after filtering
- Define handling for unmatched laps, DNFs, and missing data
- Add tests for alignment and filtering invariants

## Milestone 6 — Lap time trend plots with polynomial fitting
- Implement lap-time-vs-lap plot for each selected driver, side by side
- Show compounds and stints on the plot
- Remove laps where pit stops occurred
- Add checkbox to optionally exclude SC/VSC laps
- Add selectable polynomial degree for fit curve
- Add tests ensuring plot input tables and fit inputs are consistent

## Milestone 7 — Delta plots
- Implement cumulative delta plot on shared graph for both selected drivers
- Implement per-lap delta plot between selected drivers
- Exclude laps where either driver pitted
- Respect optional SC/VSC filtering
- Add tests for delta sign convention and filtered lap alignment

## Milestone 8 — Track layout comparison by lap
- Plot track layout for selected race using FastF1 telemetry/position data
- Let user select one lap per driver
- Compare the two selected laps sector-by-sector or segment-by-segment along track distance
- Color code segments by which driver is faster
- Add tests for telemetry resampling and segment comparison stability

## Milestone 9 — Telemetry comparison panel
- Let user choose one lap for each selected driver
- Build stacked subplots for:
  - throttle vs distance
  - brake vs distance
  - speed vs distance
- Mark corners/turn numbers on x-axis using circuit information when available
- Add telemetry alignment and interpolation logic
- Add tests for distance-normalized telemetry merge

## Milestone 10 — UI integration + debug panels + exports
- Render all comparison tables and plots in Streamlit tabs
- Add debug toggle to expose intermediate filtered tables
- Export filtered lap tables and telemetry comparison tables to CSV
- Export single-page PDF summary with key visualizations
- Persist per-run JSON logs to `./run_logs/`
- Add tests validating export creation and run-log structure