import pandas as pd
import pytest
import requests

from fintrist3.settings import Config
from fintrist3.stockmarket.prices import Stock, TiingoIEXPriceVolume
from fintrist3.stockmarket.tiingo import TiingoDailyReader, get_data_tiingo


class MockResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")


@pytest.fixture
def tiingo_env(monkeypatch):
    monkeypatch.setattr(Config, "APIKEY_TIINGO", "test-key")
    yield


def test_tiingo_daily_reader_parses_response(monkeypatch, tiingo_env):
    payload = [
        {
            "date": "2024-01-02T00:00:00.000Z",
            "open": 100,
            "high": 110,
            "low": 90,
            "close": 105,
            "volume": 5000,
            "adjClose": 104.5,
            "adjHigh": 109.5,
            "adjLow": 89.5,
            "adjOpen": 99.5,
            "adjVolume": 5100,
            "divCash": 0,
            "splitFactor": 1,
        }
    ]

    def fake_get(url, params=None, headers=None, timeout=None):
        assert url == "https://api.tiingo.com/tiingo/daily/AAPL/prices"
        assert headers["Authorization"] == "Token test-key"
        assert params["startDate"] == "2024-01-01"
        assert params["endDate"] == "2024-01-03"
        return MockResponse(payload)

    monkeypatch.setattr("fintrist3.stockmarket.tiingo.requests.get", fake_get)

    reader = TiingoDailyReader("AAPL", start="2024-01-01", end="2024-01-03", api_key="test-key")
    df = reader.read()

    assert list(df.index.names) == ["symbol", "date"]
    record = df.loc[("AAPL", pd.Timestamp("2024-01-02T00:00:00Z"))]
    assert record["close"] == 105
    assert record["adjClose"] == 104.5


def test_get_data_tiingo_wrapper(monkeypatch, tiingo_env):
    payload = [
        {
            "date": "2024-02-01T00:00:00.000Z",
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 10.5,
            "volume": 100,
        }
    ]

    def fake_get(url, params=None, headers=None, timeout=None):
        return MockResponse(payload)

    monkeypatch.setattr("fintrist3.stockmarket.tiingo.requests.get", fake_get)
    df = get_data_tiingo("MSFT", api_key="test-key", start="2024-02-01", end="2024-02-02")

    assert ("MSFT", pd.Timestamp("2024-02-01T00:00:00Z")) in df.index
    assert df.loc[("MSFT", slice(None)), "close"].iloc[0] == 10.5


def test_intraday_price_volume_reads(monkeypatch, tiingo_env):
    payload = [
        {
            "date": "2024-03-01T14:30:00.000Z",
            "open": 200,
            "high": 202,
            "low": 199,
            "close": 201,
            "volume": 1000,
        }
    ]

    def fake_get(url, params=None, headers=None, timeout=None):
        assert params["resampleFreq"] == "5min"
        assert "columns" in params
        return MockResponse(payload)

    monkeypatch.setattr("fintrist3.stockmarket.tiingo.requests.get", fake_get)

    reader = TiingoIEXPriceVolume("AAPL", api_key="test-key", start="2024-03-01", end="2024-03-02")
    df = reader.read()

    idx = ("AAPL", pd.Timestamp("2024-03-01T14:30:00Z"))
    assert idx in df.index
    assert df.loc[idx, "volume"] == 1000


def test_stock_pull_daily_uses_tiingo(monkeypatch, tiingo_env):
    payload = [
        {
            "date": "2024-04-01T00:00:00.000Z",
            "open": 50,
            "high": 55,
            "low": 49,
            "close": 54,
            "volume": 3000,
        }
    ]

    def fake_get(url, params=None, headers=None, timeout=None):
        return MockResponse(payload)

    monkeypatch.setattr("fintrist3.stockmarket.tiingo.requests.get", fake_get)

    stock = Stock("AAPL")
    df = stock.pull_daily(source="Tiingo")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert pd.Timestamp("2024-04-01").date() in df.index
