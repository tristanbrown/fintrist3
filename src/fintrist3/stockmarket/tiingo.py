"""Minimal Tiingo data reader implementations.

These classes replicate the subset of :mod:`pandas_datareader` functionality that
Fintrist relies on without depending on :mod:`distutils`, which was removed from
Python 3.12.  The behaviour is intentionally similar to the upstream
implementations so existing code can keep working after the upgrade.
"""
from __future__ import annotations

from datetime import timedelta
import os
from typing import Iterable, List, Sequence

import pandas as pd
import requests


class TiingoError(RuntimeError):
    """Raised when Tiingo returns an invalid or unsuccessful response."""


def _normalise_symbols(symbols: str | Iterable[str]) -> List[str]:
    if isinstance(symbols, str):
        if not symbols:
            raise ValueError("symbols must be a non-empty string")
        return [symbols]
    if isinstance(symbols, Iterable):
        values: List[str] = [str(sym) for sym in symbols if str(sym)]
        if not values:
            raise ValueError("symbols must contain at least one symbol")
        return values
    raise TypeError("symbols must be a string or iterable of strings")


def _coerce_timestamp(value, default: pd.Timestamp) -> pd.Timestamp:
    if value is None:
        return default
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        raise ValueError("timestamp value could not be parsed")
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _empty_index() -> pd.MultiIndex:
    return pd.MultiIndex(levels=[[], []], codes=[[], []], names=["symbol", "date"])


class _TiingoBaseReader:
    """Common functionality shared by the reader implementations."""

    url_template: str = ""
    date_columns: Sequence[str] = ("date", "timestamp", "datetime")
    timeout: float = 30.0

    def __init__(
        self,
        symbols: str | Iterable[str],
        *,
        start=None,
        end=None,
        freq: str | None = None,
        api_key: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.symbols = _normalise_symbols(symbols)
        end_default = self._default_end()
        self.end = _coerce_timestamp(end, end_default)
        start_default = self._default_start(self.end)
        self.start = _coerce_timestamp(start, start_default)
        if self.start > self.end:
            raise ValueError("start must be before or equal to end")
        self.freq = freq
        if api_key is None:
            api_key = os.getenv("TIINGO_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The tiingo API key must be provided either through the api_key parameter "
                "or the TIINGO_API_KEY environment variable."
            )
        self.api_key = api_key
        self._session = session

    # ------------------------------------------------------------------
    # Configuration hooks for subclasses
    def _default_end(self) -> pd.Timestamp:
        return pd.Timestamp.utcnow().normalize()

    def _default_start(self, end: pd.Timestamp) -> pd.Timestamp:
        return end - timedelta(days=30)

    def _expected_columns(self) -> Sequence[str]:
        return ()

    # ------------------------------------------------------------------
    @property
    def params(self) -> dict[str, str]:
        params: dict[str, str] = {"format": "json"}
        if self.start is not None:
            params["startDate"] = self.start.strftime("%Y-%m-%d")
        if self.end is not None:
            params["endDate"] = self.end.strftime("%Y-%m-%d")
        if self.freq:
            params["resampleFreq"] = self.freq
        return params

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Token {self.api_key}",
        }

    def _request(self, url: str, params: dict[str, str]):
        getter = self._session.get if self._session is not None else requests.get
        try:
            response = getter(url, params=params, headers=self._headers(), timeout=self.timeout)
        except requests.RequestException as exc:  # pragma: no cover - defensive programming
            raise TiingoError("Error communicating with Tiingo") from exc
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:  # pragma: no cover - defensive programming
            raise TiingoError(f"Tiingo request failed with status {response.status_code}") from exc
        try:
            return response.json()
        except ValueError as exc:  # pragma: no cover - defensive programming
            raise TiingoError("Tiingo response was not valid JSON") from exc

    def _url(self, symbol: str) -> str:
        if not self.url_template:
            raise NotImplementedError("url_template must be defined on subclasses")
        return self.url_template.format(ticker=symbol)

    def _extract_date_column(self, frame: pd.DataFrame) -> str:
        for column in self.date_columns:
            if column in frame.columns:
                return column
        raise TiingoError("Tiingo response did not include a recognised date column")

    def _format_frame(self, payload, symbol: str) -> pd.DataFrame:
        df = pd.DataFrame(payload)
        if df.empty:
            return pd.DataFrame(columns=self._expected_columns(), index=_empty_index())
        if "symbol" not in df.columns:
            ticker_column = "ticker" if "ticker" in df.columns else None
            if ticker_column:
                df["symbol"] = df[ticker_column]
            else:
                df["symbol"] = symbol
        df["symbol"] = df["symbol"].fillna(symbol)
        date_column = self._extract_date_column(df)
        df["date"] = pd.to_datetime(df[date_column], utc=True)
        df = df.set_index(["symbol", "date"]).sort_index()
        return df

    def read(self) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        for symbol in self.symbols:
            payload = self._request(self._url(symbol), self.params)
            frames.append(self._format_frame(payload, symbol))
        if not frames:
            return pd.DataFrame(columns=self._expected_columns(), index=_empty_index())
        return pd.concat(frames)


class TiingoDailyReader(_TiingoBaseReader):
    """Fetch historical daily pricing data from Tiingo."""

    url_template = "https://api.tiingo.com/tiingo/daily/{ticker}/prices"

    def _default_start(self, end: pd.Timestamp) -> pd.Timestamp:
        return end - timedelta(days=365 * 5)

    def _expected_columns(self) -> Sequence[str]:
        return (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "adjOpen",
            "adjHigh",
            "adjLow",
            "adjClose",
            "adjVolume",
            "divCash",
            "splitFactor",
        )


class TiingoIEXHistoricalReader(_TiingoBaseReader):
    """Fetch intraday IEX price data from Tiingo."""

    url_template = "https://api.tiingo.com/iex/{ticker}/prices"

    def __init__(
        self,
        symbols: str | Iterable[str],
        *,
        start=None,
        end=None,
        freq: str | None = "5min",
        api_key: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        super().__init__(symbols, start=start, end=end, freq=freq, api_key=api_key, session=session)

    def _default_start(self, end: pd.Timestamp) -> pd.Timestamp:
        return end - timedelta(days=7)

    def _expected_columns(self) -> Sequence[str]:
        return ("open", "high", "low", "close", "volume")


def get_data_tiingo(*args, **kwargs) -> pd.DataFrame:
    """Compatibility helper mirroring :func:`pandas_datareader.data.get_data_tiingo`."""

    reader = TiingoDailyReader(*args, **kwargs)
    return reader.read()
