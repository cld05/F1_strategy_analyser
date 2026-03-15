# IMPLEMENT.md — Build instructions for Codex (MVP v2)

## 1. Objective
Rewrite the app from the current Milestone 3 state to match the new MVP specification.

The new MVP is no longer a strategy-decomposition analyzer. It is a race-comparison visualizer for two selected drivers using FastF1 lap and telemetry data directly.

## 2. High-level rewrite decision
Treat the old implementation of:
- custom pit detection
- custom stint reconstruction
- pit loss calculation
- stint pace/degradation aggregation
- comparison windows
- residual decomposition

as deprecated and remove them from the active app flow.

Keep only:
- project scaffold
- session loading
- cache logic
- canonical lap extraction foundation

## 3. Core product behavior
For a selected Race session:
- user selects exactly two drivers
- app extracts lap data for both drivers from FastF1
- app plots:
  1. cumulative delta between the two drivers
  2. side-by-side lap time plots with polynomial fit
  3. lap-by-lap delta plot
  4. track layout comparison for one selected lap per driver
  5. telemetry comparison for one selected lap per driver using stacked throttle/brake/speed plots

## 4. Data model to implement

Implement these canonical tables only:
- `laps`
- `laps_filtered`
- `delta_laps`
- `telemetry_compare`
- `methods`
- `exports`

Do not rebuild `pits`, `stints`, `comparison_windows`, or `comparison_summary` as separate derived strategy tables in this MVP.

If useful internally, helper tables may exist, but they must not become the central analytical model.

## 5. FastF1 usage rules

### 5.1 Laps source
Use `session.laps` as the authoritative source for:
- lap number
- lap time
- compound
- tyre life
- stint
- pit in / pit out
- track status
- driver identity

### 5.2 Telemetry source
Use FastF1 lap telemetry methods for:
- speed
- throttle
- brake
- X/Y position or equivalent track coordinates
- distance-aligned telemetry comparison

### 5.3 No manual stint/pit reconstruction
Do not implement custom logic to infer stints or pit events unless required only to create simple boolean flags such as:
- `is_pit_in_lap`
- `is_pit_out_lap`
- `is_pit_lap`

Those flags must be derived from FastF1 lap fields only.

## 6. Module structure

Use this modular structure:

- `src/f1_analyzer/data_loader.py`
  - session loading
  - cache-first session/lap retrieval
  - season/round/session metadata helpers

- `src/f1_analyzer/laps.py`
  - canonical `laps` table extraction
  - lap-time conversion to seconds
  - pit-lap flags
  - SC/VSC flags
  - basic validity flags

- `src/f1_analyzer/filters.py`
  - build `laps_filtered`
  - apply pit-lap exclusion
  - optional SC/VSC exclusion
  - filtering diagnostics and reason columns

- `src/f1_analyzer/delta.py`
  - build aligned `delta_laps`
  - cumulative delta
  - per-lap delta
  - common comparable lap horizon

- `src/f1_analyzer/fitting.py`
  - polynomial fit helpers
  - selectable polynomial degree
  - robust handling for low data count
  - fit-input validation

- `src/f1_analyzer/telemetry.py`
  - lap telemetry retrieval
  - distance resampling/interpolation
  - telemetry alignment for two laps
  - segment-wise faster-driver classification
  - track layout extraction

- `src/f1_analyzer/plots.py`
  - plotly plot builders for:
    - cumulative delta
    - lap time trends side by side
    - per-lap delta
    - track layout faster-driver comparison
    - stacked telemetry plots

- `src/f1_analyzer/exports.py`
  - CSV export
  - PDF export
  - export metadata table

- `src/f1_analyzer/methods.py`
  - build `methods` table from active settings and rules

- `src/f1_analyzer/app.py`
  - Streamlit UI only
  - no hidden recomputation inside plotting code
  - all plots use canonical prepared tables

## 7. Filtering and comparison rules to implement

### 7.1 Pit laps
For lap-time trend plots:
- remove pit entry laps
- remove pit exit laps

For per-lap delta plots:
- exclude any lap where either driver has a pit lap

### 7.2 SC/VSC
Provide a checkbox:
- unchecked = include SC/VSC laps
- checked = exclude SC/VSC laps

This must affect:
- lap time trend plots
- polynomial fit inputs
- per-lap delta plot

### 7.3 Missing lap times
If lap time is missing:
- mark lap invalid
- exclude from any metric or plot requiring lap time
- expose reason in debug table

### 7.4 Common comparison horizon
For direct lap comparison between two drivers:
- compare only laps where both drivers have a valid comparable lap record after filtering
- if one driver retires, stop comparison at the last common valid comparable lap

## 8. Plot requirements

### 8.1 Cumulative delta plot
Single shared graph:
- x = lap number
- y = cumulative delta in seconds
- make sign convention explicit in subtitle or methods

### 8.2 Lap time trend plots
Two plots side by side, one per driver:
- scatter of lap times
- polynomial fit curve per stint
- polynomial degree selectable in UI
- compounds visually encoded
- stint context visually encoded
- use filtered data only
- compute and surface concise stint-level fit insights

### 8.3 Per-lap delta plot
Single shared graph:
- x = lap number
- y = lap delta in seconds
- exclude laps where either driver pitted
- respect SC/VSC toggle

### 8.4 Track layout faster-driver plot
- plot circuit path
- color Sector 1, Sector 2, and Sector 3 track sections according to the driver faster in each official sector
- one color assigned to each driver
- use official sector times as the comparison basis

### 8.5 Telemetry stacked plots
Three vertically stacked plots:
- throttle vs distance
- brake vs distance
- speed vs distance

Requirements:
- common x-axis = distance
- overlay both drivers
- annotate corners/turn numbers when circuit data is available
- otherwise degrade gracefully without failing the plot

## 9. UI behavior

Implement tabs:

### Tab 1 — Session
- season selector
- round selector
- load button
- session metadata

### Tab 2 — Drivers
- exactly two drivers selected from loaded session driver list

### Tab 3 — Delta
- cumulative delta plot
- per-lap delta plot
- controls:
  - SC/VSC exclusion checkbox

### Tab 4 — Lap trends
- side-by-side lap time plots
- controls:
  - polynomial degree selector
  - SC/VSC exclusion checkbox

### Tab 5 — Track compare
- lap selector for driver A
- lap selector for driver B
- track layout faster-driver plot

### Tab 6 — Telemetry
- lap selector for driver A
- lap selector for driver B
- stacked telemetry plot

### Tab 7 — Methods + Debug
- render `methods`
- show filtered tables
- show dropped-lap reasons
- warnings panel

## 10. Defensive coding requirements
- Never crash if:
  - lap telemetry is unavailable
  - corner metadata is missing
  - stint field is partially missing
  - track status contains unexpected codes
- Emit clear warnings in UI/debug table
- Plot builders must return a meaningful fallback state when inputs are insufficient
- Polynomial fit must fail gracefully when not enough valid points exist for chosen degree

## 11. Tests to write or update

Replace obsolete tests based on:
- custom pit/stint counts
- residual thresholds
- comparison windows
- strategy decomposition

Add tests for:
- canonical `laps` extraction schema
- pit-lap flagging
- SC/VSC flagging
- filtered lap inclusion/exclusion logic
- delta table alignment
- cumulative delta sign convention
- per-lap delta exclusion of pit laps
- polynomial fit input validity
- telemetry resampling and merge output
- export file creation

## 12. Migration instructions from the reverted Milestone 3 state

Starting from the reverted codebase:
1. keep the existing loader and cache logic if stable
2. remove old analytical modules from the active import graph
3. replace app tabs and controls to match the new MVP
4. implement canonical tables in the new shape
5. implement plots in this order:
   - cumulative delta
   - lap time trends
   - per-lap delta
   - telemetry stacked plot
   - track layout faster-driver map
6. update tests
7. update README and screenshots only after plots work

## 13. Delivery priority
Implement in this order:
1. canonical lap extraction
2. filtering
3. delta plots
4. lap trend plots with polynomial fitting
5. telemetry alignment and stacked plots
6. track layout faster-driver comparison
7. exports
8. debug panels and polish
