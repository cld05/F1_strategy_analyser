from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace

import pytest

from src.f1analyser import session_loader
from src.f1analyser.session_loader import SessionLoadError


@dataclass
class DummyEvent:
    Year: int = 2024
    RoundNumber: int = 5
    EventName: str = "Spanish Grand Prix"
    EventDate: datetime = datetime(2024, 6, 23)
    Location: str = "Barcelona"


class DummySession:
    def __init__(self) -> None:
        self.name = "Race"
        self.event = DummyEvent()
        self.loaded = False

    def load(self) -> None:
        self.loaded = True


def test_load_race_session_success(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_session = DummySession()

    monkeypatch.setattr(
        session_loader,
        "_get_fastf1_module",
        lambda: SimpleNamespace(get_session=lambda *_: dummy_session),
    )

    session = session_loader.load_race_session(2024, 5, timeout_seconds=1.0, max_retries=2)

    assert session is dummy_session
    assert dummy_session.loaded is True


def test_load_race_session_retries_and_logs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    dummy_session = DummySession()
    get_session_calls = {"count": 0}

    def fake_get_session(*_: object) -> DummySession:
        get_session_calls["count"] += 1
        return dummy_session

    load_calls = {"count": 0}

    def fake_load_with_timeout(*_: object, **__: object) -> None:
        load_calls["count"] += 1
        if load_calls["count"] < 3:
            raise RuntimeError("transient")

    monkeypatch.setattr(
        session_loader,
        "_get_fastf1_module",
        lambda: SimpleNamespace(get_session=fake_get_session),
    )
    monkeypatch.setattr(session_loader, "_load_with_timeout", fake_load_with_timeout)

    caplog.set_level(logging.WARNING)
    session = session_loader.load_race_session(2024, 7, timeout_seconds=1.0, max_retries=2)

    assert session is dummy_session
    assert get_session_calls["count"] == 3
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 2


def test_load_race_session_raises_after_exhausted_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dummy_session = DummySession()

    monkeypatch.setattr(
        session_loader,
        "_get_fastf1_module",
        lambda: SimpleNamespace(get_session=lambda *_: dummy_session),
    )
    monkeypatch.setattr(
        session_loader,
        "_load_with_timeout",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(SessionLoadError):
        session_loader.load_race_session(2024, 2, timeout_seconds=1.0, max_retries=2)


def test_load_with_timeout_raises_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeFuture:
        def result(self, timeout: float) -> None:
            raise session_loader.FuturesTimeoutError()

        def cancel(self) -> None:
            return None

    class FakeExecutor:
        def __init__(self, max_workers: int) -> None:
            self.max_workers = max_workers

        def submit(self, *_: object, **__: object) -> FakeFuture:
            return FakeFuture()

        def shutdown(self, **__: object) -> None:
            return None

    monkeypatch.setattr(session_loader, "ThreadPoolExecutor", FakeExecutor)

    with pytest.raises(TimeoutError):
        session_loader._load_with_timeout(DummySession(), timeout_seconds=0.001)


def test_extract_session_metadata() -> None:
    session = DummySession()

    metadata = session_loader.extract_session_metadata(session)

    assert metadata.season == 2024
    assert metadata.round_number == 5
    assert metadata.event_name == "Spanish Grand Prix"
    assert metadata.session_name == "Race"
    assert metadata.event_date == "2024-06-23"
    assert metadata.circuit_name == "Barcelona"
