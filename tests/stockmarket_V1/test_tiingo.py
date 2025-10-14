"""Unit tests for the lightweight Tiingo data readers."""
from __future__ import annotations

from typing import Any, Iterable, List

import pandas as pd
import pytest

from fintrist3.datareaders.tiingo import (
    TiingoDailyReader,
    TiingoIEXHistoricalReader,
    TiingoRequestError,
)


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


def test_daily_reader_requests_each_symbol_when_date_range_present() -> None:
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
    reader = TiingoDailyReader(
        ["AAPL", "MSFT"],
        api_key="token",
        start="2020-01-01",
        end="2020-01-05",
        session=session,
    )

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
    assert second_call["params"]["endDate"] == "2020-01-05"


class _BatchDailyReader(TiingoDailyReader):
    """Remove date filters so that the batch code path is exercised."""

    @property
    def params(self) -> dict[str, str]:  # type: ignore[override]
        params = super().params
        params.pop("startDate", None)
        params.pop("endDate", None)
        return params


def test_daily_reader_uses_batch_endpoint_when_available() -> None:
    session = _FakeSession(
        [
            [
                {"ticker": "AAPL", "priceData": [_payload("2020-01-02", close=2.0)]},
                {
                    "ticker": "MSFT",
                    "priceData": [
                        _payload("2020-01-01", close=5.0),
                        _payload("2020-01-03", close=7.0),
                    ],
                },
            ]
        ]
    )

    reader = _BatchDailyReader(["AAPL", "MSFT"], api_key="token", session=session)

    df = reader.read()

    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"] == reader.batch_endpoint
    assert call["params"]["tickers"] == "AAPL,MSFT"

    assert df.loc[("MSFT", pd.Timestamp("2020-01-03")), "close"] == 7.0
    assert df.loc[("AAPL", pd.Timestamp("2020-01-02")), "close"] == 2.0


def test_batch_reader_raises_when_symbol_missing_from_payload() -> None:
    session = _FakeSession(
        [
            [
                {"ticker": "AAPL", "priceData": []},
            ]
        ]
    )

    reader = _BatchDailyReader(["AAPL", "MSFT"], api_key="token", session=session)

    with pytest.raises(TiingoRequestError, match="missing data for symbol 'MSFT'"):
        reader.read()


def test_intraday_reader_includes_frequency() -> None:
    session = _FakeSession(
        [
            [
                _payload("2020-01-01T14:30:00Z", close=1.0, volume=10),
            ]
        ]
    )
    reader = TiingoIEXHistoricalReader("AAPL", api_key="token", freq="15min", session=session)
    df = reader.read()

    assert df.loc[("AAPL", pd.Timestamp("2020-01-01T14:30:00Z")), "volume"] == 10
    call = session.calls[0]
    assert call["params"]["resampleFreq"] == "15min"


def test_daily_reader_rejects_missing_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TIINGO_API_KEY", raising=False)
    with pytest.raises(ValueError):
        TiingoDailyReader("AAPL", api_key=None)
