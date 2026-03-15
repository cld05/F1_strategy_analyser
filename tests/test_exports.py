from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from f1analyser.exports import export_analysis_artifacts


def _frame(payload: dict[str, list[object]]) -> pd.DataFrame:
    return pd.DataFrame(payload)


def test_export_analysis_artifacts_creates_files_and_run_log(tmp_path: Path) -> None:
    export_dir = tmp_path / "exports"
    run_log_dir = tmp_path / "run_logs"

    artifacts = export_analysis_artifacts(
        season=2025,
        round_number=10,
        session_type="Race",
        driver_a="VER",
        driver_b="NOR",
        laps=_frame({"driver": ["VER"], "lap_number": [1]}),
        laps_filtered=_frame({"driver": ["VER"], "lap_number": [1], "include_for_lap_time_plot": [True]}),
        delta_laps=_frame(
            {
                "lap_number": [1],
                "valid_for_delta": [True],
                "cum_delta_s": [0.5],
                "lap_delta_s": [0.5],
            }
        ),
        telemetry_compare=_frame({"distance_m": [0.0, 5.0], "delta_speed": [1.0, -1.0]}),
        methods=_frame({"method": ["delta sign convention"], "value": ["driver_a - driver_b"]}),
        warnings=["Dropped 1 laps from filtered views."],
        polynomial_degree=2,
        exclude_sc_vsc=False,
        telemetry_lap_a=10,
        telemetry_lap_b=12,
        export_dir=export_dir,
        run_log_dir=run_log_dir,
        lap_trend_rows=_frame({"driver": ["VER"], "lap_time_s": [80.0]}),
    )

    assert artifacts.csv_path.exists()
    assert artifacts.pdf_path.exists()
    assert artifacts.run_log_path.exists()
    assert list(artifacts.exports_row.columns) == [
        "run_id",
        "timestamp_iso",
        "season",
        "round",
        "session_type",
        "driver_a",
        "driver_b",
        "csv_path",
        "pdf_path",
        "warnings_count",
    ]

    csv_rows = pd.read_csv(artifacts.csv_path)
    assert set(csv_rows["table_name"].tolist()) == {"laps", "laps_filtered", "delta_laps", "telemetry_compare", "methods"}

    run_log = json.loads(artifacts.run_log_path.read_text(encoding="utf-8"))
    assert run_log["inputs"]["selected_drivers"] == ["VER", "NOR"]
    assert run_log["inputs"]["lap_selections_for_telemetry_comparison"] == [10, 12]
    assert run_log["export_file_paths"]["csv_path"] == str(artifacts.csv_path)
