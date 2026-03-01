# REQUIREMENTS.md — F1 Post-Race Analyzer (Streamlit MVP)

## 0. Purpose
Build a Streamlit app that loads a Formula 1 Race session (FastF1), computes canonical tables (laps, stints, pits, methods, comparison windows, comparison summary, exports), visualizes the results with Plotly, and exports CSV + single-page PDF. The MVP supports exactly two drivers and years 2020–present.

## 1. Scope (MVP)
- Session types: **Race only**
- Seasons: **2020 to current year**
- Drivers: **exactly 2 drivers** selected for comparison
- DNFs: comparisons are computed up to common lap cutoff; block if common laps < 1
- Outputs: **tables + plots**
- Export: **CSV + single-page PDF**
- No manual per-lap user exclusions in MVP

## 2. Tech stack (MVP)
- Python 3.11+
- Streamlit
- FastF1
- pandas
- plotly
- pytest (local only)
- mypy strict
- Parquet for cached processed tables

## 3. Load and caching behavior
### 3.1 Load constraints
- Load operation hard timeout: **120 seconds**
- Retry behavior: max **2 retries**; each retry logged as a warning
- UI responsiveness: show loading state within **≤ 0.5 s**

### 3.2 Cache strategy
- Cache location: **local disk under project app folder** `./cache/`
- Persist processed canonical tables as **Parquet**
- Cache invalidation: **never invalidate** (MVP)

### 3.3 Authoritative source
- When FastF1 sources disagree, use **session results** as authoritative.

## 4. Canonical tables (single source of truth)
All canonical tables are produced for the **selected drivers** and persisted per run.

### 4.1 `laps` (raw, per driver per lap)
Required columns (minimum set):
- `season`, `round`, `session_type`
- `driver` (3-letter code), `driver_id` (if available)
- `lap_number`
- `lap_time` (seconds float or pandas Timedelta, but consistent)
- `compound`
- `tyre_life`
- `track_status` (string code from FastF1 TrackStatus)
- `position` (on track, if available)
- `gap` (to leader or to reference, define consistently)
- `pit_in_time`, `pit_out_time`
- `sector1`, `sector2`, `sector3`

### 4.2 `pits` (one row per pit event)
Columns:
- `driver`, `stop_index`
- `pit_in_lap`, `pit_out_lap`
- `laps_stationary = pit_out_lap - pit_in_lap`
- `is_drive_through` (bool)
- `has_time_penalty_served` (bool)
- `warnings` (string list or joined string)

### 4.3 `stints` (one row per stint)
Columns:
- `driver`, `stint_id`
- `compound`
- `start_lap`, `end_lap`
- `lap_count`
- `n_laps_total`, `n_laps_clean`, `n_laps_excluded`
- excluded breakdown counts by category (see §5)
- `pace_median_s` (nullable)
- `deg_slope_s_per_lap` (nullable)
- `deg_delta_first_last_s` (nullable)
- `warnings`

### 4.4 `methods` (definitions + parameters used in a run)
Must include:
- clean lap definition and excluded categories
- red flag boundary exclusion rule
- pit stop detection rule
- tyre change / red flag continuity rule
- pit loss method and parameters
- pace and degradation methods
- driver comparison and delta decomposition formulas
- thresholds (telemetry gap drop, overlap minimum, residual threshold)
- toggles (SC/VSC include toggle state)

### 4.5 `comparison_windows` (aligned overlap windows per stint pair)
Columns:
- `window_id`
- `driver_a`, `driver_b`
- `stint_a_id`, `stint_b_id`
- `lap_start`, `lap_end`
- `n_overlap_total`
- `n_clean_a`, `n_clean_b`
- `included` (bool)
- `exclude_reason` (nullable)
- `pace_a_window_s` (median clean lap time within window, nullable)
- `pace_b_window_s` (nullable)
- `window_delta_s` ( (pace_a - pace_b) * n_laps, nullable )

### 4.6 `comparison_summary` (single row per run)
Columns:
- `driver_a`, `driver_b`
- `L_common`
- `observed_finish_delta_s`
- `pit_delta_sum_s`
- `stint_delta_sum_s`
- `residual_s`
- `residual_ok` (bool)
- `residual_threshold_s` (10.0)
- `warnings`

### 4.7 `exports` (metadata)
Columns:
- `run_id`
- `timestamp_iso`
- `season`, `round`, `session_type`
- `driver_a`, `driver_b`
- `csv_path`, `pdf_path`
- `warnings_count`

## 5. Deterministic lap classification (clean vs excluded)
### 5.1 TrackStatus mapping
TrackStatus is a string code. Interpret:
- `1` = track clear (green)
- `2` = yellow (sectors unknown)
- `4` = Safety Car
- `5` = Red Flag
- `6` = Virtual Safety Car deployed
- `7` = Virtual Safety Car ending
- Any unexpected code (e.g., `3`) is treated as **green** but must emit a warning:
  - `warning: unknown track status code {code}; treated as green`

### 5.2 Excluded lap categories (used for pace + degradation)
A lap is **excluded** if any of the following is true:
1. **In-lap** (lap with pit entry)
2. **Out-lap** (lap with pit exit)
3. **SC/VSC lap**: TrackStatus in `{4,6,7}`
4. **Red flag lap**: TrackStatus == `5`, plus boundary exclusion rule (below)
5. **Missing/invalid lap time**: `lap_time` missing/NaN/invalid

Clean lap = lap not excluded by any category above.

### 5.3 Red flag boundary exclusion rule (pace/degradation only)
Exclude from pace/degradation:
- All laps where `TrackStatus == 5`
- The **last lap immediately before** the status changed to `5` (the lap directly preceding the first `5` in that red-flag segment)
- The **first lap immediately after** the status returns to `1`

Additionally:
- Use all lap times (including excluded) for reconciliation calculations (comparison residual), except where lap_time is missing.

### 5.4 Telemetry gap definition and drop rule
- Telemetry gap = a lap row with **missing/invalid `lap_time`**
- If telemetry gaps exceed **10%** of a driver’s total laps, drop that driver from analysis and log warning:
  - `warning: telemetry gaps exceed 10% for driver {driver}; dropping from analysis`

## 6. Pit stop detection policy
### 6.1 Detection source
- Use FastF1 pit markers: `PitInTime` and `PitOutTime`.

### 6.2 In-lap/out-lap derivation when markers are missing
- If in-lap marker missing but out-lap exists: set `pit_in_lap = pit_out_lap - 1` and log warning.
- If out-lap marker missing but in-lap exists:
  - If no lap_time recorded after `pit_in_lap`, consider possible DNF in pit and warn:
    - `warning: possible DNF after in-pit at lap {pit_in_lap}`
  - If lap_time exists for lap > `pit_in_lap + 2`, assume `pit_out_lap = pit_in_lap + 1` and warn:
    - `warning: missing out-lap; assuming pit_out_lap={pit_in_lap+1} based on subsequent lap times`

### 6.3 2-lap pit windows
- Accept 2-lap windows as valid but emit warning.

### 6.4 Drive-through and time penalties
- Drive-through penalties: **excluded from pit loss** computations (no service allowed).
- Time penalties:
  - If served at a pit stop **before** the last stint: include as pit loss.
  - If given/served during the **last stint**: exclude from pit loss.

## 7. Stint segmentation policy
### 7.1 Stint boundaries (MVP)
- Each pit stop defines a new stint.
- Compound change without pit defines a new stint.
- Red flag boundary:
  - Split into a new stint only if tyres were changed (see §7.2 tyre-change detection).

### 7.2 Tyre-change detection across red flag boundary
A stint is **continuous** across a red flag boundary iff the following holds:

#### Primary (TyreLife continuity)
Let:
- `(L_pre, T_pre)` = lap number and tyre life of the last valid lap before the red flag
- `(L_post, T_post)` = lap number and tyre life of the first valid lap after the restart

Condition for continuity:
- `T_post - T_pre == L_post - L_pre`

If `T_post < T_pre`, force **new stint** (tyre reset), even if compound is unchanged and no pit stop is recorded.

#### Fallback when TyreLife missing (NaNs)
If TyreLife missing for either pre or post lap, apply:
1. If compound changed ⇒ **new stint**
2. If a pit/tire-change event detected during red flag interval ⇒ **new stint**
3. Else (same compound + no pit event) ⇒ **continuous**, but warn:
   - `warning: TyreLife missing across red flag; assuming continuous tyres for driver {driver}`

## 8. Metric definitions (fixed for MVP)
### 8.1 Pit loss per stop (fixed method)
For each stop:
- `W_pre`: up to 3 **clean** laps immediately before `pit_in_lap` (excluding in-lap)
- `W_post`: up to 3 **clean** laps immediately after `pit_out_lap` (excluding out-lap)
- `baseline = median(W_pre ∪ W_post)`; require `n_total_used >= 3`
- `pit_loss_s = (inlap_time_s - baseline) + (outlap_time_s - baseline)`

Rules:
- If stop at first lap or last lap so baseline cannot be computed from **both** sides ⇒ not computable + warning.
- If in-lap or out-lap missing/invalid ⇒ not computable + warning.
- SC/VSC-affected stops:
  - included in aggregates **by default**
  - user toggle allowed to exclude; toggle is reflected in `methods`

Required per-stop outputs:
- `baseline_s`, `inlap_time_s`, `outlap_time_s`, `pit_loss_s`
- `n_pre`, `n_post`, `n_total_used`
- `is_sc_vsc_affected` (bool)
- `is_computable` (bool) + `reason_not_computable` (nullable)

### 8.2 Stint pace and degradation
- Pace: median of clean lap times in the stint.
- Degradation (1): linear regression slope (s/lap) over clean laps.
- Degradation (2): delta-first-last over clean laps.

Threshold:
- Minimum clean laps for pace/degradation: **>= 3**, else metric is null with reason.

## 9. Driver comparison and delta decomposition
### 9.1 Common lap cutoff and DNF handling
- Let `L_common = min(completed_laps_A, completed_laps_B)`.
- If `L_common < 1`, block comparison:
  - `driver with less than one lap, no comparison possible`
- Comparisons are computed up to `L_common` even if one driver DNFs.

### 9.2 Overlap windows (alignment)
- Alignment unit: **per stint pair**
- Overlap window: intersection by lap number
- Inclusion rule: include only if `min(n_clean_overlap_A, n_clean_overlap_B) >= 3`
- Non-overlap contribution: **0**, with explicit note

### 9.3 Observed delta and decomposition
- `observed_finish_delta_s = sum(lap_time_A[1..L_common]) - sum(lap_time_B[1..L_common])`
- `pit_delta_sum_s = Σ pit_loss_A - Σ pit_loss_B` (using computable pit losses; SC/VSC included by default unless toggle excludes)
- For each included window:
  - `pace_A(window)` = median clean lap time for A within window
  - `pace_B(window)` = median clean lap time for B within window
  - `window_delta_s = (pace_A - pace_B) * n_laps(window)`
- `stint_delta_sum_s = Σ window_delta_s`
- `residual_s = observed_finish_delta_s - (pit_delta_sum_s + stint_delta_sum_s)`

Residual rule:
- Residual threshold: **10.0 s**
- If `abs(residual_s) > 10.0`, flag “unreconciled” and list causes in warnings.

## 10. Streamlit UI (single page with tabs, MVP)
Tabs (minimum):
1. Session loader (season/round, load)
2. Driver selector (exactly two drivers)
3. Stints + pits tables
4. Comparison summary table
5. Plots (Plotly): gap/delta over laps, pit loss bars, stint pace per stint
6. Methods page (render `methods` table + definitions)
7. Debug panels toggle: when enabled, show intermediate tables (`laps`, clean/excluded annotations, windows)

## 11. Exports (MVP)
### 11.1 CSV export
- Scope: **selected drivers only**
- Include:
  - raw `laps`
  - derived `stints`, `pits`, `comparison_windows`, `comparison_summary`, `methods`, `exports`
- Naming:
  - `season-round-race-driverA-driverB-timestamp.csv`

### 11.2 PDF export (single page)
- Include **derived tables only** (no raw laps)
- Naming:
  - `season-round-race-driverA-driverB-timestamp.pdf`

## 12. Logging
- Persist a JSON run log per analysis run: `./run_logs/<run_id>.json`
- Must include inputs, warnings, method parameters, driver selection, timestamps, file paths.

## 13. Tests (local pytest only)
### 13.1 Fixtures (pinned sessions)
Use 2025 Race sessions:
- Round 10: Spain
- Round 11: Austria
- Round 12: Britain

### 13.2 Baseline expectations (stored in JSON under `tests/fixtures/`)
Fixture,Driver,Expected Pit Count,Expected Stint Count,Residual Bound
- Rd10 Spain, VER, 2, 3, < 5.0s
- Rd10 Spain, NOR, 2, 3, < 5.0s
- Rd11 Austria, VER, 3, 4, < 8.0s
- Rd11 Austria, NOR, 2, 3, < 5.0s
- Rd12 Britain, VER, 2, 3, < 12.0s
- Rd12 Britain, NOR, 3, 4, < 12.0s

### 13.3 Test assertions
For each fixture (driver pair VER vs NOR):
- pit count matches expected exactly for each driver
- stint count matches expected exactly for each driver
- `abs(residual_s) < bound` (bound per fixture table)
- core invariants:
  - stints cover all completed laps with 0 overlap per driver
  - `n_clean + n_excluded = n_total` per stint