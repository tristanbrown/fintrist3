"""Unit tests for Tiingo data readers and stock price helpers."""
from __future__ import annotations

from typing import Any, Iterable, List

import pandas as pd
import pytest

from fintrist3.datareaders.tiingo import TiingoDailyReader, TiingoIEXHistoricalReader, TiingoRequestError
from fintrist3.settings import Config
from fintrist3.stockmarket import calendar
from fintrist3.stockmarket.prices import Stock


class _FakeResponse:
    def __init__(self, payload: Any, status: int = 200) -> None:
        self._payload = payload
        self.status_code = status
        self.text = "" if isinstance(payload, list) else str(payload)

    def json(self) -> Any:
        return self._payload


class _FakeSession:
    def __init__(self, responses: Iterable[Any]) -> None:
        self._responses: List[Any] = list(responses)
        self.calls: List[dict[str, Any]] = []

    def get(self, url: str, params: dict[str, str], headers: dict[str, str], timeout: int) -> _FakeResponse:
        if not self._responses:
            raise AssertionError("No more responses configured for FakeSession.")
        spec = self._responses.pop(0)
        if isinstance(spec, dict):
            status = spec.get("status", 200)
            payload = spec["json"]
        else:
            status = 200
            payload = spec
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        return _FakeResponse(payload, status=status)


def _payload(date: str, **fields: float) -> dict[str, Any]:
    return {"date": date, **fields}


def test_daily_reader_defaults_to_individual_requests() -> None:
    session = _FakeSession(
        [
            [
                _payload("2020-01-01", close=1.0),
                _payload("2020-01-02", close=2.0),
            ],
            [
                _payload("2020-01-01", close=5.0),
            ],
        ]
    )
    reader = TiingoDailyReader(["AAPL", "MSFT"], api_key="token", start="2020-01-01", end="2020-01-05", session=session)
    df = reader.read()

    assert df.index.names == ["symbol", "date"]
    assert df.loc[("AAPL", pd.Timestamp("2020-01-02")), "close"] == 2.0
    assert df.loc[("MSFT", pd.Timestamp("2020-01-01")), "close"] == 5.0

    assert len(session.calls) == 2
    first_call = session.calls[0]
    second_call = session.calls[1]

    assert first_call["url"].endswith("tiingo/daily/AAPL/prices")
    assert first_call["params"]["startDate"] == "2020-01-01"
    assert "tickers" not in first_call["params"]

    assert second_call["url"].endswith("tiingo/daily/MSFT/prices")
    assert second_call["params"]["startDate"] == "2020-01-01"


def test_daily_reader_uses_batch_when_dates_are_not_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        TiingoDailyReader,
        "params",
        property(lambda self: {"format": "json"}),
        raising=False,
    )
    session = _FakeSession([
        [
            {
                "ticker": "AAPL",
                "priceData": [
                    _payload("2020-01-01", close=1.0),
                ],
            },
            {
                "ticker": "MSFT",
                "priceData": [
                    _payload("2020-01-02", close=2.0),
                ],
            },
        ]
    ])
    reader = TiingoDailyReader(["AAPL", "MSFT"], api_key="token", session=session)

    df = reader.read()

    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"].endswith("tiingo/daily/prices")
    assert call["params"]["tickers"] == "AAPL,MSFT"

    assert df.loc[("AAPL", pd.Timestamp("2020-01-01")), "close"] == 1.0
    assert df.loc[("MSFT", pd.Timestamp("2020-01-02")), "close"] == 2.0


def test_daily_reader_raises_if_batch_missing_symbol(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        TiingoDailyReader,
        "params",
        property(lambda self: {"format": "json"}),
        raising=False,
    )
    session = _FakeSession([
        [
            {
                "ticker": "AAPL",
                "priceData": [
                    _payload("2020-01-01", close=1.0),
                ],
            },
        ]
    ])
    reader = TiingoDailyReader(["AAPL", "MSFT"], api_key="token", session=session)

    with pytest.raises(TiingoRequestError):
        reader.read()


def test_daily_reader_raises_on_http_error() -> None:
    session = _FakeSession([
        {"status": 404, "json": {"detail": "Not Found"}},
    ])

    reader = TiingoDailyReader("AAPL", api_key="token", session=session)

    with pytest.raises(TiingoRequestError):
        reader.read()


def test_intraday_reader_includes_frequency() -> None:
    session = _FakeSession([
        [
            _payload("2020-01-01T14:30:00Z", close=1.0, volume=10),
        ]
    ])
    reader = TiingoIEXHistoricalReader("AAPL", api_key="token", freq="15min", session=session)
    df = reader.read()

    assert df.loc[("AAPL", pd.Timestamp("2020-01-01T14:30:00Z")), "volume"] == 10
    call = session.calls[0]
    assert call["params"]["resampleFreq"] == "15min"


def test_daily_reader_rejects_missing_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    monkeypatch.setattr(Config, "APIKEY_TIINGO", None, raising=False)
    with pytest.raises(ValueError):
        TiingoDailyReader("AAPL", api_key=None)


def test_stock_pull_daily_uses_tiingo_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_df = pd.DataFrame({"close": [1.0]}, index=pd.to_datetime(["2020-01-01"]))
    calls: list[dict[str, Any]] = []

    class DummyReader:
        def __init__(self, symbols: Any, start: Any = None, end: Any = None, *, api_key: str | None = None, freq: str | None = None, session: Any = None, timeout: int = 30) -> None:
            calls.append({"symbols": symbols, "start": start, "end": end, "api_key": api_key, "freq": freq})

        def read(self) -> pd.DataFrame:
            return dummy_df

    monkeypatch.setattr(Config, "APIKEY_TIINGO", "token", raising=False)
    monkeypatch.setattr("fintrist3.stockmarket.prices.TiingoDailyReader", DummyReader)

    stock = Stock("AAPL")
    result = stock.pull_daily(source="Tiingo")

    assert result.equals(dummy_df)
    assert calls == [{"symbols": "AAPL", "start": None, "end": None, "api_key": "token", "freq": None}]


def test_stock_pull_intraday_uses_batch_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_index = pd.MultiIndex.from_tuples(
        [
            ("AAPL", pd.Timestamp("2020-01-01T14:30:00Z")),
            ("MSFT", pd.Timestamp("2020-01-01T14:30:00Z")),
        ],
        names=["symbol", "date"],
    )
    dummy_df = pd.DataFrame({"close": [1.0, 2.0]}, index=dummy_index)
    calls: list[dict[str, Any]] = []

    class DummyReader:
        def __init__(self, symbols: Any, start: Any = None, end: Any = None, *, api_key: str | None = None, freq: str | None = None, session: Any = None, timeout: int = 30) -> None:
            calls.append({"symbols": symbols, "start": start, "end": end, "api_key": api_key, "freq": freq})

        def read(self) -> pd.DataFrame:
            return dummy_df

    open_ts = pd.Timestamp("2020-01-01 09:30", tz="America/New_York")
    close_ts = pd.Timestamp("2020-01-01 16:00", tz="America/New_York")
    monkeypatch.setattr(calendar, "latest_market_day", lambda day: pd.Series([open_ts, close_ts]))
    monkeypatch.setattr(Config, "APIKEY_TIINGO", "token", raising=False)
    monkeypatch.setattr("fintrist3.stockmarket.prices.TiingoIEXHistoricalReader", DummyReader)

    stock = Stock(["AAPL", "MSFT"], freq="15min")
    result = stock.pull_intraday(day=pd.Timestamp("2020-01-02"), freq="15min")

    assert result.equals(dummy_df)
    assert calls == [
        {
            "symbols": ["AAPL", "MSFT"],
            "start": None,
            "end": pd.Timestamp("2020-01-02"),
            "api_key": "token",
            "freq": "15min",
        }
    ]
