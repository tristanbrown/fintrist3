"""Tests for stock price helpers that use the Tiingo data readers."""
from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from fintrist3.settings import Config
from fintrist3.stockmarket import prices
from fintrist3.stockmarket.prices import Stock


def _multiindex_frame(data: Dict[str, float], date: str) -> pd.DataFrame:
    index = pd.MultiIndex.from_product(
        [list(data.keys()), pd.to_datetime([date])],
        names=["symbol", "date"],
    )
    return pd.DataFrame({"close": list(data.values())}, index=index)


def test_pull_daily_uses_tiingo_reader(monkeypatch: Any) -> None:
    monkeypatch.setattr(Config, "APIKEY_TIINGO", "token")
    captured: dict[str, Any] = {}

    class FakeReader:
        def __init__(self, symbols: Any, **kwargs: Any) -> None:
            captured["symbols"] = symbols
            captured["kwargs"] = kwargs

        def read(self) -> pd.DataFrame:
            return _multiindex_frame({"AAPL": 1.0, "MSFT": 2.0}, "2020-01-02")

    monkeypatch.setattr(prices, "TiingoDailyReader", FakeReader)

    stock = Stock(["AAPL", "MSFT"])
    df = stock.pull_daily()

    assert df.loc[("MSFT", pd.Timestamp("2020-01-02")), "close"] == 2.0
    assert list(captured["symbols"]) == ["AAPL", "MSFT"]
    assert captured["kwargs"]["api_key"] == "token"


def test_get_data_delegates_to_daily(monkeypatch: Any) -> None:
    monkeypatch.setattr(Config, "APIKEY_TIINGO", "token")

    def fake_pull_daily(self: Stock) -> pd.DataFrame:
        return _multiindex_frame({"AAPL": 3.0}, "2020-01-03")

    monkeypatch.setattr(Stock, "pull_daily", fake_pull_daily)

    stock = Stock("AAPL")
    df = stock.get_data()

    assert df.loc[("AAPL", pd.Timestamp("2020-01-03")), "close"] == 3.0


def test_pull_intraday_uses_tiingo_reader(monkeypatch: Any) -> None:
    monkeypatch.setattr(Config, "APIKEY_TIINGO", "token")
    open_ts = pd.Timestamp("2020-01-03 09:30", tz="UTC")
    close_ts = pd.Timestamp("2020-01-03 16:00", tz="UTC")
    latest = pd.Series({"market_open": open_ts, "market_close": close_ts})
    monkeypatch.setattr(prices.calendar, "latest_market_day", lambda day: latest)
    captured: dict[str, Any] = {}

    class FakeIEXReader:
        def __init__(self, symbols: Any, **kwargs: Any) -> None:
            captured["symbols"] = symbols
            captured["kwargs"] = kwargs

        def read(self) -> pd.DataFrame:
            index = pd.MultiIndex.from_product(
                [["AAPL"], pd.to_datetime(["2020-01-03T10:00:00Z"])],
                names=["symbol", "date"],
            )
            return pd.DataFrame({"close": [4.0]}, index=index)

    monkeypatch.setattr(prices, "TiingoIEXHistoricalReader", FakeIEXReader)

    stock = Stock("AAPL")
    df = stock.pull_intraday(day=pd.Timestamp("2020-01-03"), freq="15min", tz="UTC")

    assert df.index.name == "date"
    assert df.loc[pd.Timestamp("2020-01-03T10:00:00Z"), "close"] == 4.0
    assert captured["kwargs"]["freq"] == "15min"
    assert captured["kwargs"]["api_key"] == "token"
    assert captured["kwargs"]["end"] == pd.Timestamp("2020-01-03")
