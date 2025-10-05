"""Tests for the internal Tiingo data readers."""

from __future__ import annotations

from unittest.mock import Mock

import pandas as pd
import pytest

from fintrist3.stockmarket.tiingo import TiingoDailyReader


def _mock_response(payload):
    response = Mock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def test_daily_reader_single_symbol_formats_dataframe():
    session = Mock()
    session.get.return_value = _mock_response(
        [
            {
                "date": "2024-01-02T00:00:00.000Z",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.5,
                "volume": 1000000,
            },
            {
                "date": "2024-01-03T00:00:00.000Z",
                "open": 101.5,
                "high": 103.0,
                "low": 100.5,
                "close": 102.0,
                "volume": 900000,
            },
        ]
    )

    reader = TiingoDailyReader(
        "AAPL",
        api_key="token",
        start="2024-01-01",
        end="2024-01-31",
        session=session,
    )

    frame = reader.read()

    assert list(frame.columns) == ["open", "high", "low", "close", "volume"]
    assert frame.index.tolist() == [pd.Timestamp("2024-01-02").date(), pd.Timestamp("2024-01-03").date()]
    assert frame.index.name == "date"
    assert frame.loc[pd.Timestamp("2024-01-02").date(), "close"] == 101.5

    session.get.assert_called_once()
    call_args, call_kwargs = session.get.call_args
    assert call_args[0] == "https://api.tiingo.com/tiingo/daily/AAPL/prices"
    assert call_kwargs["params"] == {
        "startDate": "2024-01-01",
        "endDate": "2024-01-31",
        "format": "json",
        "token": "token",
    }


def test_daily_reader_multiple_symbols_uses_bulk_endpoint():
    session = Mock()
    session.get.return_value = _mock_response(
        [
            {
                "ticker": "AAPL",
                "date": "2024-01-02T00:00:00.000Z",
                "close": 150.25,
            },
            {
                "ticker": "MSFT",
                "date": "2024-01-02T00:00:00.000Z",
                "close": 320.5,
            },
        ]
    )

    reader = TiingoDailyReader(
        ["AAPL", "MSFT"],
        api_key="token",
        session=session,
    )

    frame = reader.read()

    assert isinstance(frame.index, pd.MultiIndex)
    assert frame.index.names == ["symbol", "date"]
    assert frame.loc[("AAPL", pd.Timestamp("2024-01-02").date()), "close"] == 150.25

    session.get.assert_called_once()
    call_args, call_kwargs = session.get.call_args
    assert call_args[0] == "https://api.tiingo.com/tiingo/daily/prices"
    params = call_kwargs["params"]
    assert params["format"] == "json"
    assert params["tickers"] == "AAPL,MSFT"
    assert params["token"] == "token"
    assert "startDate" not in params
    assert "endDate" not in params


def test_daily_reader_requires_api_key():
    with pytest.raises(ValueError):
        TiingoDailyReader("AAPL", api_key="")
