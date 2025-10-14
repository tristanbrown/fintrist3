"""Tests for stockmarket.prices module."""
from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from fintrist3.stockmarket import prices


@pytest.fixture(autouse=True)
def _tiingo_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(prices.Config, "APIKEY_TIINGO", "token", raising=False)


def test_pull_daily_uses_tiingo_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    fake_df = pd.DataFrame({"close": [1.0, 2.0]}, index=pd.date_range("2020-01-01", periods=2))

    class FakeReader:
        def __init__(
            self,
            symbols: Any,
            start: Any = None,
            end: Any = None,
            *,
            api_key: str,
            freq: str | None = None,
            session: Any = None,
            timeout: int = 30,
        ) -> None:
            captured["symbols"] = symbols
            captured["api_key"] = api_key
            captured["start"] = start
            captured["end"] = end
            captured["session"] = session

        def read(self) -> pd.DataFrame:
            return fake_df

    monkeypatch.setattr(prices, "TiingoDailyReader", FakeReader)

    stock = prices.Stock(["AAPL", "MSFT"])
    result = stock.pull_daily(source="Tiingo")

    assert result is fake_df
    assert captured["symbols"] == ["AAPL", "MSFT"]
    assert captured["api_key"] == "token"


def test_pull_intraday_returns_symbol_slice(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_latest_market_day(day: Any) -> pd.Series:
        captured["calendar_day"] = day
        return pd.Series([
            pd.Timestamp("2020-01-02T13:30:00Z"),
            pd.Timestamp("2020-01-02T20:00:00Z"),
        ])

    monkeypatch.setattr(prices.calendar, "latest_market_day", fake_latest_market_day)

    multi_index = pd.MultiIndex.from_tuples(
        [
            ("AAPL", pd.Timestamp("2020-01-02T14:30:00Z")),
            ("MSFT", pd.Timestamp("2020-01-02T15:30:00Z")),
        ],
        names=["symbol", "date"],
    )
    fake_df = pd.DataFrame({"close": [1.0, 2.0]}, index=multi_index)

    class FakeReader:
        def __init__(
            self,
            symbols: Any,
            start: Any = None,
            end: Any = None,
            *,
            api_key: str,
            freq: str | None = None,
            session: Any = None,
            timeout: int = 30,
        ) -> None:
            captured["symbols"] = symbols
            captured["api_key"] = api_key
            captured["end"] = end
            captured["freq"] = freq

        def read(self) -> pd.DataFrame:
            return fake_df

    monkeypatch.setattr(prices, "TiingoIEXHistoricalReader", FakeReader)

    stock = prices.Stock("AAPL")
    result = stock.pull_intraday(day=pd.Timestamp("2020-01-02"), freq="5min")

    expected = fake_df.loc["AAPL"]
    pd.testing.assert_frame_equal(result, expected)

    assert captured["symbols"] == "AAPL"
    assert captured["api_key"] == "token"
    assert captured["end"] == pd.Timestamp("2020-01-02")
    assert captured["freq"] == "5min"
    assert captured["calendar_day"] == pd.Timestamp("2020-01-02")
