"""Tests for the Tiingo data reader implementation."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from fintrist3.stockmarket.tiingo import (
    TiingoIEXPriceVolume,
    TiingoRequestError,
    get_data_tiingo,
)


class FakeResponse:
    def __init__(self, json_data, status_code: int = 200, text: str | None = None) -> None:
        self._json = json_data
        self.status_code = status_code
        self.text = text or ""

    def json(self):
        return self._json


class FakeSession:
    """Minimal stand-in for :class:`requests.Session` used in tests."""

    def __init__(self, responses: list[dict]):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def get(self, url, params=None, headers=None, timeout=None):
        if not self._responses:
            raise AssertionError("No fake responses left to return")
        response_kwargs = self._responses.pop(0)
        self.calls.append({
            "url": url,
            "params": params or {},
            "headers": headers or {},
            "timeout": timeout,
        })
        return FakeResponse(**response_kwargs)


def test_get_data_tiingo_single_symbol():
    """The reader returns a MultiIndex dataframe even for one symbol."""

    responses = [{
        "json_data": [
            {
                "date": "2024-01-02T00:00:00.000Z",
                "open": 100.0,
                "high": 110.0,
                "low": 95.0,
                "close": 105.0,
                "volume": 1_000,
            },
        ],
    }]
    session = FakeSession(responses)

    frame = get_data_tiingo(
        "AAPL", api_key="abc123", start="2024-01-01", end="2024-01-10", session=session
    )

    assert frame.index.names == ["symbol", "date"]
    assert frame.loc[("AAPL", pd.Timestamp("2024-01-02")), "close"] == 105.0

    call = session.calls[0]
    assert call["url"].endswith("/tiingo/daily/AAPL/prices")
    assert call["params"] == {
        "format": "json",
        "startDate": "2024-01-01",
        "endDate": "2024-01-10",
    }
    assert call["headers"] == {"Authorization": "Token abc123"}


def test_get_data_tiingo_multiple_symbols():
    responses = [
        {"json_data": [{"date": "2024-01-01T00:00:00.000Z", "close": 50.0}]},
        {"json_data": [{"date": "2024-01-01T00:00:00.000Z", "close": 60.0}]},
    ]
    session = FakeSession(responses)

    frame = get_data_tiingo(["MSFT", "GOOG"], session=session)

    expected_index = pd.MultiIndex.from_product(
        [["MSFT", "GOOG"], [pd.Timestamp("2024-01-01")]], names=["symbol", "date"]
    )
    pd.testing.assert_index_equal(frame.index, expected_index)


def test_get_data_tiingo_error_response():
    responses = [{"json_data": {"message": "boom"}, "status_code": 500}]
    session = FakeSession(responses)

    with pytest.raises(TiingoRequestError) as excinfo:
        get_data_tiingo("AAPL", session=session)

    assert "boom" in str(excinfo.value)


def test_tiingo_iex_price_volume_params_and_read():
    responses = [{
        "json_data": [
            {
                "date": "2024-01-02T14:30:00+00:00",
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
                "volume": 123,
            },
        ],
    }]
    session = FakeSession(responses)

    reader = TiingoIEXPriceVolume(
        "AAPL", api_key="token", freq="5min", start=datetime(2024, 1, 2), session=session
    )
    frame = reader.read()

    assert frame.index.names == ["symbol", "date"]
    timestamp = pd.Timestamp("2024-01-02T14:30:00+00:00")
    assert frame.loc[("AAPL", timestamp), "volume"] == 123

    call = session.calls[0]
    assert call["params"]["columns"] == "open,high,low,close,volume"
    assert call["params"]["resampleFreq"] == "5min"
    assert call["params"]["startDate"] == "2024-01-02"
    assert call["headers"] == {"Authorization": "Token token"}

