from __future__ import annotations

from f1analyser.methods import build_methods_table


def test_build_methods_table_contains_required_method_rows() -> None:
    methods = build_methods_table(exclude_sc_vsc=True, polynomial_degree=3)

    assert methods["method"].tolist() == [
        "lap filtering rules",
        "pit-lap removal rule",
        "SC/VSC exclusion toggle state",
        "polynomial fit method and selected degree",
        "delta sign convention",
        "telemetry alignment method",
        "track segment comparison method",
        "corner-marker source and fallback behavior",
        "thresholds or interpolation settings used",
    ]
    assert methods["value"].iloc[2] == "True"
    assert methods["value"].iloc[3] == "numpy polyfit degree=3"
