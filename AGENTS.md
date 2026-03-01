# AGENTS.md — F1 Post-Race Analyzer (Streamlit MVP)

## Prime directive
Implement exactly what is written in REQUIREMENTS.md and only what the current milestone in PLANS.md asks for.

## Working rules (non-negotiable)
1. One milestone per task. No extra features, no refactors unless required to complete the milestone.
2. Do not reinterpret formulas. If something is unclear, surface it in REQUIREMENTS.md as a precise rule and add/adjust tests.
3. Tests must not depend on network. Use mocks/stubs for FastF1 calls in unit tests.
4. After every change set: run `pytest` and `mypy src` and fix failures before finishing.
5. Keep diffs small and local to relevant modules.

## Commands (must use)
- Install: `pip install -e ".[dev]"`
- Run app: `python -m streamlit run src/f1analyser/app.py`
- Tests: `pytest`
- Type check: `mypy src`
- Quick import sanity: `python -c "import f1analyser; print(f1analyser.__file__)"`

## Repo conventions
- Source code: `src/f1analyser/`
- Compute code only in:
  - `pipeline/` (build canonical tables)
  - `metrics/` (pit loss, pace, degradation, compare)
  - `io/` (cache parquet, exports, run logs)
- Streamlit UI stays thin: reads canonical tables, renders tables/plots, triggers pipeline.

## Persistence paths
- Cache: `./cache/` (Parquet)
- Exports: `./exports/` (CSV, PDF)
- Run logs: `./run_logs/<run_id>.json`

## Definition of done (per milestone)
- App runs without import errors
- `pytest` passes
- `mypy src` passes (strict)
- Milestone deliverable matches PLANS.md and REQUIREMENTS.md