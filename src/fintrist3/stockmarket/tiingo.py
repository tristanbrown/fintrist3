"""Utilities for working with Tiingo's REST API.

The functions and classes in this module provide a small subset of the
``pandas_datareader`` Tiingo functionality that ``fintrist3`` relies on.  They
allow us to fetch daily price data as well as IEX intraday price data without
depending on ``pandas_datareader`` at runtime, which makes the behaviour easier
to test by injecting a fake HTTP session.

Only the features that are currently used by ``fintrist3`` are implemented.
The goal is to keep the public surface minimal and predictable rather than to
mirror the entire upstream API.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd
import requests


class TiingoRequestError(RuntimeError):
    """Raised when Tiingo returns a non-successful HTTP status code."""

    def __init__(self, status_code: int, message: str, url: str) -> None:
        super().__init__(f"Tiingo request failed ({status_code}): {message} [{url}]")
        self.status_code = status_code
        self.message = message
        self.url = url


def _to_timestamp(value: date | datetime | str | None) -> pd.Timestamp | None:
    """Coerce *value* to :class:`pandas.Timestamp` when possible."""

    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value
    return pd.Timestamp(value)


def _format_date(value: date | datetime | str | None) -> str | None:
    """Return a ``YYYY-MM-DD`` formatted string or ``None``."""

    timestamp = _to_timestamp(value)
    if timestamp is None:
        return None
    return timestamp.date().isoformat()


def _concat_frames(frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine symbol-indexed frames into a single MultiIndex frame."""

    if not frames:
        return pd.DataFrame()
    concatenated = pd.concat(frames, names=["symbol", "date"])
    # The concatenated index will have ``date`` as the innermost level.  Ensure
    # the name is preserved even when the individual frames were empty.
    if concatenated.index.nlevels == 2:
        concatenated.index = concatenated.index.set_names(["symbol", "date"])
    return concatenated


@dataclass
class _BaseTiingoReader:
    """Common functionality for Tiingo readers.

    Parameters
    ----------
    symbols:
        The ticker or tickers to download.  A string is interpreted as a single
        ticker; iterables are converted to a list.
    api_key:
        Optional Tiingo API token.  When provided it is attached to the
        ``Authorization`` header of every request.
    session:
        HTTP session used to execute requests.  Supplying a custom session makes
        unit testing straightforward, as callers can provide a fake session that
        returns deterministic responses.
    """

    symbols: str | Iterable[str]
    api_key: str | None = None
    session: requests.Session | None = None

    def __post_init__(self) -> None:  # pragma: no cover - tiny wrapper
        if isinstance(self.symbols, str):
            self._symbols = [self.symbols]
        else:
            self._symbols = list(self.symbols)
        if not self._symbols:
            msg = "At least one symbol must be provided"
            raise ValueError(msg)
        self.session = self.session or requests.Session()

    # ``params`` is intentionally left untyped here so that subclasses can
    # expose the parameters they support without mypy complaining about the
    # specific keys being absent.  The return value is expected to be a mapping
    # that can be passed directly to :meth:`requests.Session.get`.
    @property
    def params(self) -> MutableMapping[str, str]:  # pragma: no cover - overridden
        return {}

    @property
    def headers(self) -> Mapping[str, str]:
        if self.api_key:
            return {"Authorization": f"Token {self.api_key}"}
        return {}

    def _request(self, url: str, params: Mapping[str, str]) -> list[dict]:
        response = self.session.get(url, params=params, headers=self.headers, timeout=30)
        if response.status_code != requests.codes.ok:  # type: ignore[attr-defined]
            try:
                message = response.json().get("message", "Unknown error")
            except ValueError:  # JSON decoding failed
                message = response.text
            raise TiingoRequestError(response.status_code, message, url)
        return response.json()

    def read(self) -> pd.DataFrame:  # pragma: no cover - invoked by subclasses
        raise NotImplementedError


class TiingoDailyReader(_BaseTiingoReader):
    """Download daily price data from Tiingo."""

    base_url = "https://api.tiingo.com/tiingo/daily/{symbol}/prices"

    def __init__(
        self,
        symbols: str | Iterable[str],
        api_key: str | None = None,
        start: date | datetime | str | None = None,
        end: date | datetime | str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.start = _format_date(start)
        self.end = _format_date(end)
        super().__init__(symbols=symbols, api_key=api_key, session=session)

    def _read_one(self, symbol: str) -> pd.DataFrame:
        params: dict[str, str] = {"format": "json"}
        if self.start:
            params["startDate"] = self.start
        if self.end:
            params["endDate"] = self.end
        url = self.base_url.format(symbol=symbol)
        records = self._request(url, params)
        frame = self._records_to_frame(records)
        frame.index.name = "date"
        return frame

    def _records_to_frame(self, records: list[dict]) -> pd.DataFrame:
        if not records:
            empty_index = pd.DatetimeIndex([], name="date")
            return pd.DataFrame(index=empty_index)
        frame = pd.DataFrame.from_records(records)
        frame["date"] = pd.to_datetime(frame["date"], utc=True)
        frame = frame.set_index("date")
        frame.index = frame.index.tz_convert(None)
        return frame.sort_index()

    def read(self) -> pd.DataFrame:
        frames = {symbol: self._read_one(symbol) for symbol in self._symbols}
        return _concat_frames(frames)


class TiingoIEXHistoricalReader(_BaseTiingoReader):
    """Read intraday price data from Tiingo's IEX endpoint."""

    base_url = "https://api.tiingo.com/iex/{symbol}/prices"

    def __init__(
        self,
        symbols: str | Iterable[str],
        api_key: str | None = None,
        start: date | datetime | str | None = None,
        end: date | datetime | str | None = None,
        freq: str = "1min",
        session: requests.Session | None = None,
    ) -> None:
        self.start = _format_date(start)
        self.end = _format_date(end)
        self.freq = freq
        super().__init__(symbols=symbols, api_key=api_key, session=session)

    @property
    def params(self) -> MutableMapping[str, str]:
        params: MutableMapping[str, str] = {
            "format": "json",
            "resampleFreq": self.freq,
        }
        if self.start:
            params["startDate"] = self.start
        if self.end:
            params["endDate"] = self.end
        return params

    def _read_one(self, symbol: str) -> pd.DataFrame:
        url = self.base_url.format(symbol=symbol)
        records = self._request(url, self.params)
        return self._records_to_frame(records)

    def _records_to_frame(self, records: list[dict]) -> pd.DataFrame:
        if not records:
            empty_index = pd.DatetimeIndex([], name="date")
            return pd.DataFrame(index=empty_index)
        frame = pd.DataFrame.from_records(records)
        # Tiingo's IEX endpoint returns timestamps under the ``date`` field.
        if "date" in frame:
            frame["date"] = pd.to_datetime(frame["date"], utc=True)
            frame = frame.set_index("date")
        return frame.sort_index()

    def read(self) -> pd.DataFrame:
        frames = {symbol: self._read_one(symbol) for symbol in self._symbols}
        return _concat_frames(frames)


class TiingoIEXPriceVolume(TiingoIEXHistoricalReader):
    """Adds volume information to intraday price data."""

    @property
    def params(self) -> MutableMapping[str, str]:
        params = super().params
        params["columns"] = "open,high,low,close,volume"
        return params


def get_data_tiingo(
    symbols: str | Iterable[str],
    api_key: str | None = None,
    start: date | datetime | str | None = None,
    end: date | datetime | str | None = None,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Return daily price data for *symbols* using Tiingo.

    The return value mirrors what :func:`pandas_datareader.get_data_tiingo`
    provides: a :class:`pandas.DataFrame` indexed by symbol and date.
    """

    reader = TiingoDailyReader(
        symbols=symbols, api_key=api_key, start=start, end=end, session=session
    )
    return reader.read()

