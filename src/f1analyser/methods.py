from __future__ import annotations

import pandas as pd  # type: ignore[import-untyped]


def build_methods_table(
    *,
    exclude_sc_vsc: bool,
    polynomial_degree: int,
    telemetry_distance_step_m: float = 5.0,
    track_distance_step_m: float = 10.0,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "method": [
                "lap filtering rules",
                "pit-lap removal rule",
                "SC/VSC exclusion toggle state",
                "polynomial fit method and selected degree",
                "delta sign convention",
                "telemetry alignment method",
                "track segment comparison method",
                "corner-marker source and fallback behavior",
                "thresholds or interpolation settings used",
            ],
            "value": [
                "missing lap_time removed; explicit filter_reason_list retained",
                "pit entry and pit exit laps excluded from trend/delta analysis",
                str(exclude_sc_vsc),
                f"numpy polyfit degree={polynomial_degree}",
                "driver_a - driver_b",
                f"distance interpolation on fixed {telemetry_distance_step_m:.1f}m grid",
                "official sector-time comparison projected onto three track sectors",
                "corner markers unavailable; plots degrade gracefully",
                (
                    f"telemetry_step_m={telemetry_distance_step_m:.1f}; "
                    f"track_step_m={track_distance_step_m:.1f}"
                ),
            ],
        }
    )
