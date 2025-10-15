"""Unit tests for stock price helpers."""
from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from fintrist3.settings import Config
from fintrist3.stockmarket import calendar
from fintrist3.stockmarket.prices import Stock


def test_stock_pull_daily_uses_tiingo_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_df = pd.DataFrame({"close": [1.0]}, index=pd.to_datetime(["2020-01-01"]))
    calls: list[dict[str, Any]] = []

    class DummyReader:
        def __init__(
            self,
            symbols: Any,
            start: Any = None,
            end: Any = None,
            *,
            api_key: str | None = None,
            freq: str | None = None,
            session: Any = None,
            timeout: int = 30,
        ) -> None:
            calls.append({"symbols": symbols, "start": start, "end": end, "api_key": api_key, "freq": freq})

        def read(self) -> pd.DataFrame:
            return dummy_df

    monkeypatch.setattr(Config, "APIKEY_TIINGO", "token", raising=False)
    monkeypatch.setattr("fintrist3.stockmarket.prices.TiingoDailyReader", DummyReader)

    stock = Stock("AAPL")
    result = stock.pull_daily()

    assert result.equals(dummy_df)
    assert calls == [{"symbols": "AAPL", "start": None, "end": None, "api_key": "token", "freq": None}]


def test_stock_get_data_delegates_to_pull_daily(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_df = pd.DataFrame({"close": [3.0]}, index=pd.to_datetime(["2020-01-03"]))

    def fake_pull_daily(self: Stock) -> pd.DataFrame:
        return dummy_df

    monkeypatch.setattr(Stock, "pull_daily", fake_pull_daily)

    stock = Stock("AAPL")
    result = stock.get_data()

    assert result.equals(dummy_df)


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
        def __init__(
            self,
            symbols: Any,
            start: Any = None,
            end: Any = None,
            *,
            api_key: str | None = None,
            freq: str | None = None,
            session: Any = None,
            timeout: int = 30,
        ) -> None:
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


def test_stock_pull_intraday_returns_single_symbol_slice(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_latest_market_day(day: Any) -> pd.Series:
        captured["calendar_day"] = day
        return pd.Series(
            [
                pd.Timestamp("2020-01-02T14:30:00Z"),
                pd.Timestamp("2020-01-02T21:00:00Z"),
            ],
            index=["market_open", "market_close"],
        )

    monkeypatch.setattr(calendar, "latest_market_day", fake_latest_market_day)

    index = pd.MultiIndex.from_tuples(
        [
            ("AAPL", pd.Timestamp("2020-01-02T15:00:00Z")),
            ("MSFT", pd.Timestamp("2020-01-02T16:00:00Z")),
        ],
        names=["symbol", "date"],
    )
    dummy_df = pd.DataFrame({"close": [1.0, 2.0]}, index=index)

    class DummyReader:
        def __init__(
            self,
            symbols: Any,
            start: Any = None,
            end: Any = None,
            *,
            api_key: str | None = None,
            freq: str | None = None,
            session: Any = None,
            timeout: int = 30,
        ) -> None:
            captured.update({"symbols": symbols, "end": end, "api_key": api_key, "freq": freq})

        def read(self) -> pd.DataFrame:
            return dummy_df

    monkeypatch.setattr(Config, "APIKEY_TIINGO", "token", raising=False)
    monkeypatch.setattr("fintrist3.stockmarket.prices.TiingoIEXHistoricalReader", DummyReader)

    stock = Stock("AAPL", freq="5min")
    result = stock.pull_intraday(day=pd.Timestamp("2020-01-02"), freq="5min")

    expected = dummy_df.loc["AAPL"]
    pd.testing.assert_frame_equal(result, expected)
    assert result.index.name == "date"
    assert captured == {
        "symbols": "AAPL",
        "end": pd.Timestamp("2020-01-02"),
        "api_key": "token",
        "freq": "5min",
        "calendar_day": pd.Timestamp("2020-01-02"),
    }
