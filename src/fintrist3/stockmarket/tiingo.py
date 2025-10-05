"""Utilities for working with the Tiingo market data API."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import List, MutableMapping, Optional, Sequence, Tuple

import pandas as pd
import requests


TIINGO_BASE_URL = "https://api.tiingo.com"


class TiingoAPIError(RuntimeError):
    """Raised when Tiingo returns an unexpected response."""


def _coerce_symbols(symbols: Sequence[str] | str) -> Tuple[List[str], bool]:
    """Return a normalised list of ticker symbols and a flag for single symbol.

    Parameters
    ----------
    symbols:
        Either a single ticker string or a sequence of ticker strings.
    """

    if isinstance(symbols, str):
        cleaned = symbols.strip()
        if not cleaned:
            raise ValueError("Ticker symbol must not be empty.")
        return [cleaned], True

    try:
        iter(symbols)
    except TypeError as exc:  # pragma: no cover - defensive branch
        raise TypeError("Symbols must be a string or an iterable of strings.") from exc

    cleaned_list = [symbol.strip() for symbol in symbols if symbol and symbol.strip()]
    if not cleaned_list:
        raise ValueError("At least one ticker symbol is required.")

    return cleaned_list, len(cleaned_list) == 1


def _coerce_date(value: Optional[dt.date | dt.datetime | str]) -> Optional[str]:
    """Convert the date-like input into the format expected by Tiingo."""

    if value is None:
        return None

    timestamp = pd.Timestamp(value)
    return timestamp.strftime("%Y-%m-%d")


def _coerce_timestamp(value: Optional[dt.date | dt.datetime | str]) -> pd.Timestamp:
    """Return a timezone-aware UTC timestamp."""

    if value is None:
        return pd.Timestamp.utcnow().tz_localize("UTC")

    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


@dataclass
class _TiingoSession:
    """Wrap a requests session to ease mocking in tests."""

    session: requests.Session
    timeout: float

    def get(self, url: str, params: MutableMapping[str, str]) -> requests.Response:
        return self.session.get(url, params=params, timeout=self.timeout)


class _TiingoBaseReader:
    """Common functionality shared by Tiingo data readers."""

    endpoint: str

    def __init__(
        self,
        api_key: str,
        session: Optional[requests.Session] = None,
        timeout: float = 10.0,
    ) -> None:
        if not api_key:
            raise ValueError("A Tiingo API key is required to fetch data.")

        self.api_key = api_key
        self._session = _TiingoSession(session or requests.Session(), timeout)

    def _request(self, url: str, params: MutableMapping[str, str]) -> list[dict]:
        params = {key: value for key, value in params.items() if value is not None}
        params["token"] = self.api_key

        response = self._session.get(url, params=params)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:  # pragma: no cover - network failure path
            raise TiingoAPIError("Error fetching data from Tiingo") from exc

        payload = response.json()
        if not isinstance(payload, list):
            raise TiingoAPIError("Unexpected Tiingo response payload.")
        return payload


class TiingoDailyReader(_TiingoBaseReader):
    """Fetch end-of-day price data from Tiingo."""

    endpoint = f"{TIINGO_BASE_URL}/tiingo/daily"

    def __init__(
        self,
        symbols: Sequence[str] | str,
        api_key: str,
        start: Optional[dt.date | dt.datetime | str] = None,
        end: Optional[dt.date | dt.datetime | str] = None,
        session: Optional[requests.Session] = None,
        timeout: float = 10.0,
    ) -> None:
        super().__init__(api_key=api_key, session=session, timeout=timeout)
        self.symbols, self._single_symbol = _coerce_symbols(symbols)
        self.start = _coerce_date(start)
        self.end = _coerce_date(end)

    def read(self) -> pd.DataFrame:
        params: MutableMapping[str, str] = {
            "startDate": self.start,
            "endDate": self.end,
            "format": "json",
        }

        if self._single_symbol:
            url = f"{self.endpoint}/{self.symbols[0]}/prices"
        else:
            url = f"{self.endpoint}/prices"
            params["tickers"] = ",".join(self.symbols)

        payload = self._request(url, params)
        return self._format_payload(payload)

    def _format_payload(self, payload: list[dict]) -> pd.DataFrame:
        if not payload:
            return pd.DataFrame()

        frame = pd.DataFrame(payload)

        if "ticker" in frame.columns:
            frame = frame.rename(columns={"ticker": "symbol"})

        if "symbol" not in frame.columns and self._single_symbol:
            frame["symbol"] = self.symbols[0]

        frame["date"] = pd.to_datetime(frame["date"], utc=True).dt.date

        numeric_columns = [
            column
            for column in frame.columns
            if column not in {"symbol", "date"}
        ]
        for column in numeric_columns:
            try:
                frame[column] = pd.to_numeric(frame[column])
            except (TypeError, ValueError):
                # Mixed-type columns (e.g. strings) are left unchanged.
                continue

        if self._single_symbol:
            frame = frame.drop(columns=["symbol"], errors="ignore")
            frame = frame.set_index("date")
            frame.index.name = "date"
            return frame.sort_index()

        frame = frame.set_index(["symbol", "date"]).sort_index()
        frame.index.names = ["symbol", "date"]
        return frame


class TiingoIEXHistoricalReader(_TiingoBaseReader):
    """Fetch intraday data from Tiingo's IEX endpoint."""

    endpoint = f"{TIINGO_BASE_URL}/iex"

    def __init__(
        self,
        symbols: Sequence[str] | str,
        api_key: str,
        start: Optional[dt.date | dt.datetime | str] = None,
        end: Optional[dt.date | dt.datetime | str] = None,
        freq: str = "1min",
        session: Optional[requests.Session] = None,
        timeout: float = 10.0,
    ) -> None:
        super().__init__(api_key=api_key, session=session, timeout=timeout)
        self.symbols, self._single_symbol = _coerce_symbols(symbols)
        self.end = _coerce_timestamp(end)
        self.start = _coerce_timestamp(start) if start else self.end.normalize()
        self.freq = freq

    @property
    def params(self) -> MutableMapping[str, str]:
        return {
            "startDate": self.start.strftime("%Y-%m-%d"),
            "endDate": self.end.strftime("%Y-%m-%d"),
            "resampleFreq": self.freq,
            "format": "json",
        }

    def read(self) -> pd.DataFrame:
        frames = {}
        for symbol in self.symbols:
            url = f"{self.endpoint}/{symbol}/prices"
            payload = self._request(url, dict(self.params))
            frames[symbol] = self._format_payload(payload)

        if not frames:
            return pd.DataFrame()

        if self._single_symbol:
            return frames[self.symbols[0]]

        concatenated = pd.concat(frames, names=["symbol"])
        concatenated.index.set_names(["symbol", "timestamp"], inplace=True)
        return concatenated

    def _format_payload(self, payload: list[dict]) -> pd.DataFrame:
        frame = pd.DataFrame(payload)
        if frame.empty:
            return frame

        rename_map = {"date": "timestamp"}
        frame = frame.rename(columns={k: v for k, v in rename_map.items() if k in frame})
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame.set_index("timestamp").sort_index()

        numeric_columns = [column for column in frame.columns if column != "symbol"]
        for column in numeric_columns:
            try:
                frame[column] = pd.to_numeric(frame[column])
            except (TypeError, ValueError):
                continue
        frame.index.name = "timestamp"
        return frame


__all__ = [
    "TiingoAPIError",
    "TiingoDailyReader",
    "TiingoIEXHistoricalReader",
]

