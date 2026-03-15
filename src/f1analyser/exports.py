from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd  # type: ignore[import-untyped]
from reportlab.lib import colors  # type: ignore[import-untyped]
from reportlab.lib.pagesizes import letter  # type: ignore[import-untyped]
from reportlab.pdfgen import canvas  # type: ignore[import-untyped]


@dataclass(frozen=True)
class ExportArtifacts:
    exports_row: pd.DataFrame
    csv_path: Path
    pdf_path: Path
    run_log_path: Path


def _timestamp_slug(timestamp: datetime) -> str:
    return timestamp.strftime("%Y%m%dT%H%M%SZ")


def _ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _export_csv_bundle(
    export_path: Path,
    *,
    laps: pd.DataFrame,
    laps_filtered: pd.DataFrame,
    delta_laps: pd.DataFrame,
    telemetry_compare: pd.DataFrame | None,
    methods: pd.DataFrame,
) -> None:
    rows: list[dict[str, str]] = []
    tables: list[tuple[str, pd.DataFrame | None]] = [
        ("laps", laps),
        ("laps_filtered", laps_filtered),
        ("delta_laps", delta_laps),
        ("telemetry_compare", telemetry_compare),
        ("methods", methods),
    ]
    for table_name, table in tables:
        if table is None:
            continue
        for record in table.to_dict(orient="records"):
            rows.append({"table_name": table_name, "row_json": json.dumps(record, default=str)})
    pd.DataFrame(rows).to_csv(export_path, index=False)


def _draw_series_box(
    pdf: canvas.Canvas,
    *,
    x: float,
    y: float,
    width: float,
    height: float,
    values: list[float],
    color: colors.Color,
    title: str,
) -> None:
    pdf.setStrokeColor(colors.black)
    pdf.rect(x, y, width, height, stroke=1, fill=0)
    pdf.setFont("Helvetica", 8)
    pdf.drawString(x + 4, y + height - 10, title)
    if len(values) < 2:
        return
    min_value = min(values)
    max_value = max(values)
    span = max(max_value - min_value, 1e-6)
    pdf.setStrokeColor(color)
    for index in range(1, len(values)):
        x1 = x + 8 + (width - 16) * (index - 1) / (len(values) - 1)
        x2 = x + 8 + (width - 16) * index / (len(values) - 1)
        y1 = y + 12 + (height - 28) * (values[index - 1] - min_value) / span
        y2 = y + 12 + (height - 28) * (values[index] - min_value) / span
        pdf.line(x1, y1, x2, y2)


def _export_pdf_summary(
    pdf_path: Path,
    *,
    title: str,
    delta_laps: pd.DataFrame,
    lap_trend_rows: pd.DataFrame | None,
    telemetry_compare: pd.DataFrame | None,
    methods: pd.DataFrame,
) -> None:
    pdf = canvas.Canvas(str(pdf_path), pagesize=letter)
    _page_width, page_height = letter
    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(36, page_height - 36, title)

    valid_delta = delta_laps[delta_laps["valid_for_delta"]].copy() if "valid_for_delta" in delta_laps.columns else pd.DataFrame()
    _draw_series_box(
        pdf,
        x=36,
        y=page_height - 220,
        width=250,
        height=120,
        values=valid_delta["cum_delta_s"].dropna().astype("float64").tolist(),
        color=colors.darkblue,
        title="Cumulative delta",
    )
    _draw_series_box(
        pdf,
        x=306,
        y=page_height - 220,
        width=250,
        height=120,
        values=valid_delta["lap_delta_s"].dropna().astype("float64").tolist(),
        color=colors.darkred,
        title="Per-lap delta",
    )

    if lap_trend_rows is not None and not lap_trend_rows.empty:
        drivers = lap_trend_rows["driver"].dropna().astype("string").unique().tolist()
        for offset, driver in enumerate(drivers[:2]):
            values = lap_trend_rows[lap_trend_rows["driver"] == driver]["lap_time_s"].dropna().astype("float64").tolist()
            _draw_series_box(
                pdf,
                x=36 + offset * 270,
                y=page_height - 370,
                width=250,
                height=110,
                values=values,
                color=colors.darkgreen if offset == 0 else colors.purple,
                title=f"Lap trends {driver}",
            )

    if telemetry_compare is not None and not telemetry_compare.empty:
        _draw_series_box(
            pdf,
            x=36,
            y=page_height - 510,
            width=520,
            height=110,
            values=telemetry_compare["delta_speed"].dropna().astype("float64").tolist(),
            color=colors.orange,
            title="Telemetry snapshot: delta speed",
        )

    pdf.setFont("Helvetica", 8)
    pdf.drawString(36, 86, "Methods summary")
    text_object = pdf.beginText(36, 74)
    text_object.setFont("Helvetica", 7)
    for row in methods.itertuples(index=False):
        text_object.textLine(f"{row.method}: {row.value}")
    pdf.drawText(text_object)
    pdf.showPage()
    pdf.save()


def _write_run_log(run_log_path: Path, payload: dict[str, Any]) -> None:
    run_log_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def export_analysis_artifacts(
    *,
    season: int,
    round_number: int,
    session_type: str,
    driver_a: str,
    driver_b: str,
    laps: pd.DataFrame,
    laps_filtered: pd.DataFrame,
    delta_laps: pd.DataFrame,
    telemetry_compare: pd.DataFrame | None,
    methods: pd.DataFrame,
    warnings: list[str],
    polynomial_degree: int,
    exclude_sc_vsc: bool,
    telemetry_lap_a: int | None,
    telemetry_lap_b: int | None,
    export_dir: Path | str = Path("exports"),
    run_log_dir: Path | str = Path("run_logs"),
    lap_trend_rows: pd.DataFrame | None = None,
) -> ExportArtifacts:
    timestamp = datetime.now(timezone.utc)
    run_id = uuid4().hex[:12]
    slug = _timestamp_slug(timestamp)
    base_name = f"{season}-{round_number}-{session_type.lower()}-{driver_a}-{driver_b}-{slug}"
    export_dir_path = Path(export_dir)
    run_log_dir_path = Path(run_log_dir)
    _ensure_directory(export_dir_path)
    _ensure_directory(run_log_dir_path)

    csv_path = export_dir_path / f"{base_name}.csv"
    pdf_path = export_dir_path / f"{base_name}.pdf"
    run_log_path = run_log_dir_path / f"{run_id}.json"

    _export_csv_bundle(
        csv_path,
        laps=laps,
        laps_filtered=laps_filtered,
        delta_laps=delta_laps,
        telemetry_compare=telemetry_compare,
        methods=methods,
    )
    _export_pdf_summary(
        pdf_path,
        title=f"{session_type} Summary: {driver_a} vs {driver_b}",
        delta_laps=delta_laps,
        lap_trend_rows=lap_trend_rows,
        telemetry_compare=telemetry_compare,
        methods=methods,
    )

    exports_row = pd.DataFrame(
        {
            "run_id": [run_id],
            "timestamp_iso": [timestamp.isoformat()],
            "season": [season],
            "round": [round_number],
            "session_type": [session_type],
            "driver_a": [driver_a],
            "driver_b": [driver_b],
            "csv_path": [str(csv_path)],
            "pdf_path": [str(pdf_path)],
            "warnings_count": [len(warnings)],
        }
    )

    run_log_payload: dict[str, Any] = {
        "run_id": run_id,
        "timestamp_iso": timestamp.isoformat(),
        "inputs": {
            "season": season,
            "round": round_number,
            "session_type": session_type,
            "selected_drivers": [driver_a, driver_b],
            "lap_selections_for_telemetry_comparison": [telemetry_lap_a, telemetry_lap_b],
            "toggle_states": {"exclude_sc_vsc": exclude_sc_vsc},
            "polynomial_degree": polynomial_degree,
        },
        "warnings": warnings,
        "export_file_paths": {"csv_path": str(csv_path), "pdf_path": str(pdf_path)},
        "selected_session": {"season": season, "round": round_number, "session_type": session_type},
    }
    _write_run_log(run_log_path, run_log_payload)
    return ExportArtifacts(exports_row=exports_row, csv_path=csv_path, pdf_path=pdf_path, run_log_path=run_log_path)
