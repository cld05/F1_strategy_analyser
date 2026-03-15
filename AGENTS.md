# AGENTS.md — F1 Race Analyzer (Streamlit MVP v2)

## Prime directive
Implement exactly what is written in `REQUIREMENTS.md` and only what the current milestone in `PLANS.md` asks for.

Do not invent features.
Do not change product behavior implicitly.
Do not widen scope to "improve architecture" unless explicitly required by the milestone.

---

## Execution model: multi-agent parallel delivery

The codebase must be developed using a **coordinated multi-agent model**.

### Agent hierarchy
There are five agent types:

1. **Orchestrator**
   - reads `PLANS.md`, `REQUIREMENTS.md`, and `IMPLEMENT.md`
   - decomposes milestone work into parallelizable sub-tasks
   - assigns one owner per sub-task
   - defines file ownership before implementation starts
   - prevents overlapping edits across agents
   - decides execution order when dependencies exist
   - freezes contracts before parallel implementation begins
   - must produce a concrete milestone execution plan before any coding starts

2. **Feature agents**
   - implement isolated modules or features
   - must only modify files in their assigned ownership area
   - must not silently change shared contracts

3. **Integration agent**
   - merges outputs from feature agents
   - is the only agent allowed to make broad cross-module edits
   - updates import wiring, app wiring, and shared configuration after feature work lands

4. **Validation agent**
   - runs `pytest` and `mypy src`
   - checks contract consistency across modules
   - checks that implementation matches milestone scope
   - blocks completion if regressions or scope creep exist

5. **Documentation agent**
   - updates `README.md`, usage notes, and screenshots only when explicitly requested by milestone
   - must not modify compute logic unless specifically assigned

---

## Required orchestrator output per milestone

Before implementation starts for any milestone, the Orchestrator must produce a short execution plan containing:

1. milestone number and objective
2. frozen contracts required for the milestone
3. sequential prerequisite tasks
4. parallel tasks
5. file ownership assignment
6. integration owner
7. validation checklist
8. explicit list of files that must not be edited in that milestone

Use this template for each assigned sub-task:

### Agent task contract
- Role:
- Milestone:
- Objective:
- Owned files:
- Inputs consumed:
- Outputs produced:
- Forbidden edits:
- Tests required:
- Completion criteria:

---

## Parallelization policy

### Allowed parallelization
Parallel work is allowed only when tasks have:
- clear input/output contracts
- disjoint file ownership
- no ambiguity in definitions

Examples of valid parallel splits:
- `laps.py` vs `filters.py`
- `delta.py` vs `fitting.py`
- `telemetry.py` vs `exports.py`
- plot builders in `plots.py` after canonical data contracts are frozen
- tests for a module in parallel with module implementation only if the contract is already explicit in `REQUIREMENTS.md`

### Forbidden parallelization
Do not parallelize:
- multiple agents editing the same source file
- product-definition work and implementation work at the same time
- UI wiring before underlying canonical tables are stable
- tests built against assumptions not already frozen in requirements
- broad repo-wide refactors during milestone delivery

---

## File ownership model

At the start of each milestone, the Orchestrator must assign **exclusive ownership** of files or modules.

### Ownership rule
Only one implementation agent may edit a given file in a task cycle, except:
- the Integration agent
- the Validation agent when adding minimal test fixes
- the Documentation agent for docs-only files

### Shared files
The following are shared but controlled:

- `src/f1analyzer/app.py`
  - owned by Integration agent
  - feature agents must not directly edit unless explicitly assigned

- `src/f1analyzer/plots.py`
  - may be split by plot-builder function only if the Orchestrator assigns exact ownership ranges
  - otherwise owned by a single feature agent

- `REQUIREMENTS.md`, `PLANS.md`, `IMPLEMENT.md`, `AGENTS.md`
  - owned by Orchestrator
  - other agents must not edit unless explicitly instructed

---

## Contract-first rule

Before parallel implementation begins, the Orchestrator must freeze:

1. canonical table schemas
2. naming conventions
3. filtering semantics
4. delta sign convention
5. telemetry alignment method
6. export payload structure

No feature agent may redefine these locally.

If a contract is missing or ambiguous:
- stop implementation of that sub-task
- update the relevant markdown spec first
- then continue

---

## Required module boundaries

Use this module ownership model unless the milestone explicitly requires a different split.

### Data/loading domain
- `src/f1analyzer/data_loader.py`
- ownership: Data agent

Responsibilities:
- session loading
- retries/timeouts
- cache-first retrieval
- session metadata helpers

### Laps/domain tables
- `src/f1analyzer/laps.py`
- ownership: Laps agent

Responsibilities:
- canonical `laps` extraction
- lap-time conversion
- pit flags
- SC/VSC flags
- validity flags

### Filtering domain
- `src/f1analyzer/filters.py`
- ownership: Filtering agent

Responsibilities:
- `laps_filtered`
- inclusion/exclusion flags
- filter reason columns
- filter diagnostics

### Delta domain
- `src/f1analyzer/delta.py`
- ownership: Delta agent

Responsibilities:
- `delta_laps`
- cumulative delta
- per-lap delta
- common comparable lap horizon

### Fitting domain
- `src/f1analyzer/fitting.py`
- ownership: Fitting agent

Responsibilities:
- polynomial fit helpers
- degree validation
- graceful fallback for insufficient data

### Telemetry domain
- `src/f1analyzer/telemetry.py`
- ownership: Telemetry agent

Responsibilities:
- lap telemetry retrieval
- distance normalization
- telemetry alignment
- faster-driver segment classification
- track layout helpers

### Plot domain
- `src/f1analyzer/plots.py`
- ownership: Plot agent

Responsibilities:
- plot builders only
- no business logic
- no hidden recomputation
- consume canonical prepared tables only

### Export domain
- `src/f1analyzer/exports.py`
- ownership: Export agent

Responsibilities:
- CSV export
- PDF export
- export metadata

### Methods domain
- `src/f1analyzer/methods.py`
- ownership: Methods agent

Responsibilities:
- methods table
- active settings rendering
- recorded assumptions and toggles

### App integration
- `src/f1analyzer/app.py`
- ownership: Integration agent

Responsibilities:
- Streamlit tabs
- controls
- wiring canonical tables into plots
- debug panels
- warnings rendering

### Tests
- `tests/`
- ownership: Validation agent, unless milestone explicitly assigns module-local tests to feature agents

---

## Milestone decomposition protocol

For each milestone, the Orchestrator must first classify sub-tasks as one of:

- **sequential prerequisite**
- **parallel implementation**
- **integration**
- **validation**

### Required order
1. freeze contracts
2. assign ownership
3. implement prerequisite modules
4. implement parallel modules
5. integrate
6. validate
7. only then mark milestone done

### Example
For a milestone involving delta plots + lap trend plots:

Sequential prerequisite:
- freeze `laps_filtered`
- freeze `delta_laps`

Parallel implementation:
- Delta agent builds `delta.py`
- Fitting agent builds `fitting.py`
- Plot agent builds plot builders consuming frozen tables

Integration:
- Integration agent wires controls and tabs in `app.py`

Validation:
- Validation agent runs tests and type checking

---

## Quality gates

### Non-negotiable checks after every change set
- `pytest`
- `mypy src`

### Additional required quality checks
The Validation agent must also verify:
- no violation of file ownership occurred
- no duplicate business logic exists across modules
- plot code does not recompute canonical tables
- filtering rules match `REQUIREMENTS.md`
- no deprecated strategy-decomposition code remains in active flow
- new code is local and minimal, not a hidden refactor sweep

### Failure handling
If checks fail:
- fix the failure before further feature work
- do not stack more changes on top of a broken baseline

---

## Deprecated code handling

When a milestone replaces previous analytical logic:

- deprecated modules may remain temporarily in the repository only if they are not imported by the active app flow
- feature agents must not extend deprecated logic
- integration agent must remove deprecated imports from active execution paths
- validation agent must fail the milestone if obsolete strategy-decomposition code still drives current outputs

---

## Commands (must use)
- Install: `pip install -e ".[dev]"`
- Run app: `python -m streamlit run src/f1analyzer/app.py`
- Tests: `pytest`
- Type check: `mypy src`
- Quick import sanity: `python -c "import f1analyzer; print(f1analyzer.__file__)"`

---

## Repo conventions

### Source code
- `src/f1analyzer/`

### Module responsibilities
- business/data prep logic only in domain modules:
  - `data_loader.py`
  - `laps.py`
  - `filters.py`
  - `delta.py`
  - `fitting.py`
  - `telemetry.py`
  - `methods.py`
  - `exports.py`

- plot construction only in:
  - `plots.py`

- UI orchestration only in:
  - `app.py`

### Rule
The Streamlit layer must stay thin.
It may:
- read canonical prepared tables
- call plot builders
- render warnings/debug outputs

It must not:
- perform hidden filtering
- recompute delta tables
- contain fitting logic
- contain telemetry alignment logic

---

## Persistence paths
- Cache: `./cache/` (Parquet)
- Exports: `./exports/` (CSV, PDF)
- Run logs: `./run_logs/<run_id>.json`

---

## Branching and diff discipline

### Diff rule
Keep diffs small and local to the assigned ownership area.

### Merge rule
Before integration:
- each feature agent must provide a concise change summary:
  - files changed
  - contracts consumed
  - contracts produced
  - known limitations
  - tests added or updated

The Integration agent must reject changes that:
- modify unowned files without permission
- silently alter schemas
- embed duplicate logic already owned elsewhere

---

## Definition of done (per milestone)
A milestone is done only if all are true:
- app runs without import errors
- `pytest` passes
- `mypy src` passes
- deliverable matches current milestone in `PLANS.md`
- implementation matches frozen behavior in `REQUIREMENTS.md`
- integration completed without ownership conflicts
- deprecated code for replaced logic is not in active execution path

---

## Definition of not done
A milestone is not done if any of the following is true:
- feature works visually but tests fail
- tests pass but mypy fails
- app works but logic is implemented in the wrong layer
- modules disagree on schema or naming
- multiple agents edited the same file without explicit integration ownership
- milestone includes extra features not requested
- old deprecated analytical logic still drives app behavior