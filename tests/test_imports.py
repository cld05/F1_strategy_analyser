def test_import_package() -> None:
    import src.f1analyser as f1analyser

    assert f1analyser.__version__ == "0.1.0"
