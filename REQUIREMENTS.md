# REQUIREMENTS.md — F1 Race Analyzer (Streamlit MVP v2)

## 0. Purpose
Build a Streamlit app that loads a Formula 1 Race session using FastF1, lets the user select exactly two drivers, extracts lap-level data directly from FastF1 `session.laps`, and visualizes relevant race and telemetry comparisons. The MVP is focused on plotting and visual comparison, not on reconstructing strategy metrics or decomposing finish deltas.

## 1. Scope (MVP)
- Session types: **Race only**
- Seasons: **2020 to current year**
- Drivers: **exactly 2 drivers**
- Primary goal: **visual comparison of selected drivers**
- Outputs: **plots + supporting tables**
- Export: **CSV + single-page PDF**
- No custom pit/stint inference in MVP
- No strategy decomposition, no residual reconciliation, no custom pit-loss model in MVP

## 2. Tech stack (MVP)
- Python 3.11+
- Streamlit
- FastF1
- pandas
- numpy
- plotly
- scipy or numpy polynomial fit utilities
- pytest
- mypy strict
- Parquet for cached processed tables

## 3. Load and caching behavior
### 3.1 Load constraints
- Load operation hard timeout: **120 seconds**
- Retry behavior: **max 2 retries**
- Each retry logged as a warning
- UI loading state visible within **≤ 0.5 s**

### 3.2 Cache strategy
- Cache location: `./cache/`
- Persist processed canonical tables as Parquet
- Cache processed session-level and driver-level lap tables
- Cache invalidation: **never invalidate** in MVP

### 3.3 Authoritative source
- FastF1 `session.laps` is authoritative for lap-level race data
- FastF1 telemetry APIs are authoritative for speed/throttle/brake and track layout data
- FastF1 lap fields are authoritative for compounds, tyre life, stint, pit-in, pit-out, and related annotations where available

## 4. Canonical tables (single source of truth)

## 4.1 `laps`
One row per driver per lap for selected drivers.

Required columns:
- `season`
- `round`
- `session_type`
- `event_name`
- `driver`
- `driver_number` (if available)
- `team` (if available)
- `lap_number`
- `lap_time_s`
- `compound`
- `tyre_life`
- `stint`
- `track_status`
- `position`
- `pit_in_time`
- `pit_out_time`
- `is_pit_in_lap`
- `is_pit_out_lap`
- `is_pit_lap`
- `is_sc_vsc_lap`
- `is_valid_lap_time`
- `deleted_reason` or `filter_reason` (nullable for filtered variants)

Notes:
- `is_pit_lap = is_pit_in_lap OR is_pit_out_lap`
- `is_sc_vsc_lap` must reflect TrackStatus codes used by FastF1
- `lap_time_s` must be numeric float in seconds for downstream plotting

## 4.2 `laps_filtered`
Derived filtered table used for plotting. Same base columns as `laps` plus:
- `include_for_lap_time_plot`
- `include_for_delta_plot`
- `include_for_fit`
- `filter_reason_list`

This table must make filtering explicit instead of silently dropping rows.

## 4.3 `delta_laps`
Aligned lap comparison table between the two selected drivers.

Required columns:
- `lap_number`
- `driver_a`
- `driver_b`
- `lap_time_a_s`
- `lap_time_b_s`
- `lap_delta_s`
- `cum_time_a_s`
- `cum_time_b_s`
- `cum_delta_s`
- `valid_for_delta`
- `exclude_reason`

Definitions:
- `lap_delta_s = lap_time_a_s - lap_time_b_s`
- `cum_delta_s = cum_time_a_s - cum_time_b_s`

## 4.4 `telemetry_compare`
Distance-aligned telemetry table for one selected lap from each driver.

Required columns:
- `driver_a`
- `driver_b`
- `lap_a`
- `lap_b`
- `distance_m`
- `speed_a`
- `speed_b`
- `throttle_a`
- `throttle_b`
- `brake_a`
- `brake_b`
- `delta_speed`
- `faster_driver_segment` (nullable)

## 4.5 `methods`
Definitions + parameters used in the run.

Must include:
- lap filtering rules
- pit-lap removal rule
- SC/VSC exclusion toggle state
- polynomial fit method and selected degree
- delta sign convention
- telemetry alignment method
- track segment comparison method
- corner-marker source and fallback behavior
- thresholds or interpolation settings used

## 4.6 `exports`
Columns:
- `run_id`
- `timestamp_iso`
- `season`
- `round`
- `session_type`
- `driver_a`
- `driver_b`
- `csv_path`
- `pdf_path`
- `warnings_count`

## 5. Filtering rules

### 5.1 Baseline validity
A lap is invalid for analytical plotting if:
- `lap_time` is missing
- `lap_time` is non-numeric after conversion
- lap record is otherwise corrupted or incomplete for required plot inputs

### 5.2 Pit-lap removal
For lap-time trend plots and per-lap delta plots:
- remove laps where the driver has:
  - pit entry lap
  - pit exit lap
- if either driver pitted on a lap, that lap must be excluded from the direct per-lap delta plot

### 5.3 SC/VSC removal
User must be able to toggle exclusion of SC/VSC laps.
- Default behavior: include SC/VSC laps unless user ticks exclusion box
- If excluded:
  - exclude laps whose TrackStatus corresponds to SC or VSC conditions
- This toggle must affect:
  - lap time trend plots
  - polynomial fitting
  - per-lap delta plot
- The toggle state must be recorded in `methods`

### 5.4 Stints and compounds
- Do not reconstruct stints manually
- Use FastF1-provided lap fields directly for stint and compound visualization
- If a stint field is missing for a lap, the lap remains usable unless that missing value blocks the requested visualization
- Missing stint/compound information must trigger a warning in the UI/debug panel

## 6. Driver comparison plots

## 6.1 Shared cumulative delta plot
For the selected race and selected drivers:
- plot cumulative delta in seconds between the two drivers on one graph
- x-axis: lap number
- y-axis: cumulative delta in seconds
- sign convention must be explicit in `methods`

Definition:
- `cum_delta_s = cumulative_sum(lap_time_a_s) - cumulative_sum(lap_time_b_s)`

Handling:
- allow plotting only on laps where both drivers have valid lap times
- if one driver retires, plot only until common valid lap horizon

## 6.2 Lap time trend plots with polynomial fit
Show one subplot per driver, side by side.

Requirements:
- x-axis: lap number
- y-axis: lap time in seconds
- show scatter points for laps
- show polynomial fit curve
- polynomial degree must be user selectable
- compounds must be visually encoded
- stints must be visually indicated
- pit laps must be removed
- optional SC/VSC removal via checkbox
- fit must use only filtered valid laps

The plot must support the visual style of:
- raw lap-time scatter
- smooth fitted trend
- compound encoding
- stint context

## 6.3 Per-lap delta plot
Plot lap-by-lap delta between selected drivers.

Requirements:
- one shared graph
- x-axis: lap number
- y-axis: lap delta in seconds
- exclude laps where either driver pitted
- respect optional SC/VSC exclusion
- only compute on laps where both drivers have valid comparable data

## 7. Track layout comparison

## 7.1 Track layout
Plot the track layout for the selected race using FastF1 telemetry/position data.

## 7.2 Selected-lap sector/segment comparison
For the two selected drivers:
- user selects one lap for driver A
- user selects one lap for driver B
- compare the two laps along the track

Requirements:
- color track segments according to which driver is faster
- use one color per driver
- comparison basis may be segment time or local speed-derived advantage after distance alignment
- method must be deterministic and recorded in `methods`

Notes:
- if official sector boundaries are not sufficient for localized comparison, use distance-segment comparison
- if sector-only comparison is implemented first, structure the code so finer segmentation can replace it later without breaking the UI

## 8. Telemetry comparison plot

For the two selected drivers:
- user selects one lap for each driver
- compare telemetry along track distance

Required stacked subplots:
1. throttle vs distance
2. brake vs distance
3. speed vs distance

Requirements:
- common x-axis: distance
- mark corners/turn numbers on x-axis when available
- align telemetry by distance, not timestamp
- handle different sampling densities robustly
- interpolation/resampling method must be recorded in `methods`

## 9. Streamlit UI (single page with tabs, MVP)

Minimum tabs:
1. Session loader
   - season
   - round
   - load session
   - session metadata

2. Driver selection
   - exactly two drivers

3. Race delta
   - cumulative delta plot
   - per-lap delta plot
   - filtering controls

4. Lap time trends
   - side-by-side lap time plots
   - polynomial degree selector
   - SC/VSC exclusion checkbox

5. Track comparison
   - track layout
   - selected lap per driver
   - faster-driver segment map

6. Telemetry comparison
   - lap selector per driver
   - throttle/brake/speed stacked plots

7. Methods + debug
   - rendered `methods`
   - intermediate tables
   - filtering diagnostics
   - warnings

## 10. Exports (MVP)

### 10.1 CSV export
Include:
- `laps`
- `laps_filtered`
- `delta_laps`
- `telemetry_compare` when available
- `methods`
- `exports`

Naming:
- `season-round-race-driverA-driverB-timestamp.csv`

### 10.2 PDF export
Single-page summary including:
- cumulative delta plot
- lap time trend plots
- per-lap delta plot
- telemetry comparison snapshot or track comparison snapshot
- methods summary

Naming:
- `season-round-race-driverA-driverB-timestamp.pdf`

## 11. Logging
Persist JSON run log per analysis run in:
- `./run_logs/<run_id>.json`

Must include:
- inputs
- selected session
- selected drivers
- lap selections for telemetry comparison
- toggle states
- polynomial degree
- warnings
- export file paths
- timestamps

## 12. Tests (local pytest only)

### 12.1 Testing philosophy
This MVP is primarily a visual analysis tool.
Tests are intended to validate:
- correctness of canonical data preparation
- correctness of filtering and alignment
- robustness of plot-input generation
- resilience to missing or partial FastF1 fields
- export and logging behavior

Tests are not intended to validate racing conclusions or visual interpretation quality.

### 12.2 Fixtures
Use pinned Race sessions where feasible, or mocked/stubbed FastF1 responses for unit tests.

Recommended real-session fixtures:
- 2025 Spain
- 2025 Austria
- 2025 Britain

Unit tests must not rely on live network calls.

### 12.3 Required test categories

#### A. Canonical schema tests
Verify:
- `laps` is non-empty for valid driver/session inputs
- required `laps` columns exist
- `laps_filtered` contains inclusion flags and reasons
- `delta_laps` contains required alignment fields
- `telemetry_compare` contains required distance-aligned fields when telemetry is available

#### B. Pit and filtering tests
Verify:
- `is_pit_in_lap`, `is_pit_out_lap`, and `is_pit_lap` are populated consistently
- lap trend inputs exclude pit laps
- per-lap delta excludes laps where either driver pitted
- SC/VSC exclusion toggle changes inclusion behavior correctly
- missing lap times are marked invalid and excluded where required

#### C. Delta and alignment tests
Verify:
- cumulative delta uses the documented sign convention
- per-lap delta is computed only when both drivers have comparable filtered laps
- comparison stops at the last common valid comparable lap horizon
- unmatched laps are excluded with reason where applicable

#### D. Polynomial fit tests
Verify:
- fit uses only filtered valid laps
- fit fails gracefully when data is insufficient for the selected degree
- fit output is generated for valid inputs
- degree selection is validated

#### E. Telemetry tests
Verify:
- telemetry alignment is distance-based
- telemetry comparison output is non-empty for valid lap selections
- different telemetry sampling densities are handled correctly
- missing corner metadata does not crash telemetry plotting inputs

#### F. Plot robustness tests
Verify:
- plot builders run successfully on valid prepared inputs
- plot builders return a fallback state or warning on empty/invalid input
- plot builders do not perform hidden recomputation of canonical tables

#### G. Export and logging tests
Verify:
- CSV export is created successfully
- PDF export is created successfully
- JSON run log is created successfully
- export metadata includes session, drivers, settings, and file paths

### 12.4 Minimum acceptance rule
A milestone is not complete unless:
- relevant unit tests for the milestone exist
- all tests pass
- `mypy src` passes