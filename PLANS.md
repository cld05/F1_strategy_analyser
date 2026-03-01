# PLANS.md — Implementation milestones (Streamlit MVP)

## Milestone 1 — Repo scaffold + environment + app shell
- Create `src/` layout, package skeleton, pyproject.toml with dependencies
- Add AGENTS.md, REQUIREMENTS.md, IMPLEMENT.md, README.md
- Streamlit app with tabs scaffold (empty tables/placeholder)
- Minimal pytest smoke test (imports package)
- mypy strict configured and passing (may require minimal type hints)

## Milestone 2 — Session loader (Race only) + bounded execution
- Implement season/round selector (2020–present) and Race session load via FastF1
- Implement timeout (120s) and retry logic (max 2), with warnings logged
- Display session metadata in UI
- Add basic load tests with mocked FastF1 calls (no network reliance)

## Milestone 3 — Canonical `laps` table + Parquet persistence
- Build canonical `laps` dataframe for selected drivers with required columns
- Persist to `./cache/` as Parquet; load from cache-first
- Add tests validating schema, types, non-empty for fixtures

## Milestone 4 — Clean-lap classifier + telemetry gap drop rule
- Implement TrackStatus mapping, unknown status warning
- Implement excluded-lap categorization and red-flag boundary rule
- Implement telemetry gap drop (>10% missing lap_time)
- Add tests for invariants: `n_clean + n_excluded = n_total`

## Milestone 5 — Pit detection + stints + red-flag tyre continuity rule
- Detect pits using PitInTime/PitOutTime, implement missing marker rules + warnings
- Implement 2-lap window warnings
- Implement stint segmentation rules (pit boundaries, compound changes, red flag tyre rule)
- Add tests: pit counts/stint counts for fixtures

## Milestone 6 — Metrics: pit loss, pace, degradation
- Implement pit loss fixed method + SC/VSC include toggle
- Implement stint pace (median), degradation slope + delta-first-last
- Add tests ensuring computability rules and consistent outputs

## Milestone 7 — Comparison windows + delta decomposition + residual checks
- Implement stint-pair overlap windows, inclusion rule (>=3 clean overlap)
- Implement observed_finish_delta at common lap cutoff
- Implement decomposition and residual; unreconciled warnings
- Add fixture tests against residual bounds

## Milestone 8 — UI integration + plots + debug panels
- Render all tables in tabs
- Plotly plots: gap/delta over laps, pit loss bars, stint pace per stint
- Debug toggle to show intermediate tables
- Ensure UI uses canonical tables only (no hidden recompute)

## Milestone 9 — Exports + run logs
- Implement per-run JSON logs in `./run_logs/`
- Implement CSV export (raw laps + derived tables for selected drivers only)
- Implement single-page PDF export (derived tables only)
- Add tests validating export files created and contain required fields

