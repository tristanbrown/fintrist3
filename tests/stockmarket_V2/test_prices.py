"""Tests for stock price helpers relying on Tiingo readers."""
from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from fintrist3.stockmarket import prices


@pytest.fixture(autouse=True)
def _set_config_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prices.Config, "APIKEY_TIINGO", "token")


def test_stock_pull_daily_uses_tiingo_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _FakeDailyReader:
        def __init__(self, symbols: Any, api_key: str) -> None:
            captured["symbols"] = symbols
            captured["api_key"] = api_key

        def read(self) -> pd.DataFrame:
            idx = pd.MultiIndex.from_tuples(
                [("AAPL", pd.Timestamp("2020-01-02"))],
                names=["symbol", "date"],
            )
            return pd.DataFrame({"close": [2.0]}, index=idx)

    monkeypatch.setattr(prices, "TiingoDailyReader", _FakeDailyReader)

    stock = prices.Stock(["AAPL", "MSFT"])
    df = stock.pull_daily()

    assert captured == {"symbols": ["AAPL", "MSFT"], "api_key": "token"}
    assert df.loc[("AAPL", pd.Timestamp("2020-01-02")), "close"] == 2.0


def test_stock_pull_intraday_uses_tiingo_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    open_time = pd.Timestamp("2024-03-04 09:30", tz="UTC")
    close_time = pd.Timestamp("2024-03-04 16:00", tz="UTC")
    monkeypatch.setattr(
        prices.calendar,
        "latest_market_day",
        lambda *_args, **_kwargs: pd.Series(
            [open_time, close_time], index=["market_open", "market_close"]
        ),
    )

    class _FakeIntradayReader:
        def __init__(self, symbols: Any, api_key: str, *, end: Any, freq: str) -> None:
            captured.update({"symbols": symbols, "api_key": api_key, "end": end, "freq": freq})

        def read(self) -> pd.DataFrame:
            idx = pd.MultiIndex.from_tuples(
                [("AAPL", pd.Timestamp("2024-03-04 10:00"))],
                names=["symbol", "date"],
            )
            return pd.DataFrame({"close": [5.0]}, index=idx)

    monkeypatch.setattr(prices, "TiingoIEXHistoricalReader", _FakeIntradayReader)

    stock = prices.Stock("AAPL")
    df = stock.pull_intraday(day=pd.Timestamp("2024-03-04"), freq="15min")

    assert captured == {
        "symbols": "AAPL",
        "api_key": "token",
        "end": pd.Timestamp("2024-03-04"),
        "freq": "15min",
    }
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.loc[pd.Timestamp("2024-03-04 10:00"), "close"] == 5.0
