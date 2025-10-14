"""Lightweight Tiingo data readers used by the project."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Iterable, List, Mapping, Sequence, cast

import pandas as pd
import requests


class TiingoRequestError(RuntimeError):
    """Raised when Tiingo returns an error response."""


@dataclass(frozen=True)
class _Call:
    url: str
    params: dict[str, str]


class _BaseTiingoReader:
    """Shared functionality for Tiingo readers used in tests."""

    endpoint_template: str
    batch_endpoint: str | None = None

    def __init__(
        self,
        symbols: str | Sequence[str],
        start: pd.Timestamp | str | None = None,
        end: pd.Timestamp | str | None = None,
        *,
        api_key: str | None = None,
        freq: str | None = None,
        session: requests.Session | None = None,
        timeout: int = 30,
    ) -> None:
        self._single_symbol = isinstance(symbols, str)
        self.symbols: List[str] = [symbols] if self._single_symbol else list(symbols)
        if not self.symbols:
            raise ValueError("At least one symbol must be supplied.")

        self.session = session or requests.Session()
        self.timeout = timeout
        self.api_key = self._resolve_api_key(api_key)

        self.end = self._coerce_timestamp(end) if end is not None else self._default_end()
        self.start = self._coerce_timestamp(start) if start is not None else self._default_start()
        if self.start > self.end:
            raise ValueError("start must be before end")

        self.freq = freq or self._default_freq()

    # ------------------------------------------------------------------
    # Configuration helpers
    def _default_end(self) -> pd.Timestamp:
        return pd.Timestamp.utcnow().normalize()

    def _default_start(self) -> pd.Timestamp:
        return self.end - pd.DateOffset(years=5)

    def _default_freq(self) -> str | None:  # pragma: no cover - overridden where needed
        return None

    @staticmethod
    def _coerce_timestamp(value: pd.Timestamp | str) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)
        else:
            ts = ts.tz_localize(None)
        return ts

    @staticmethod
    def _resolve_api_key(api_key: str | None) -> str:
        key = api_key or os.getenv("TIINGO_API_KEY")
        if not key:
            raise ValueError(
                "The Tiingo API key must be provided either through the api_key argument "
                "or the TIINGO_API_KEY environment variable."
            )
        return key

    # ------------------------------------------------------------------
    # Request building
    def _build_call(self, symbol: str) -> _Call:
        url = self.endpoint_template.format(ticker=symbol)
        params = dict(self.params)
        return _Call(url, params)

    def _build_batch_call(self, symbols: Sequence[str]) -> _Call:
        params = dict(self.params)
        params["tickers"] = ",".join(symbols)
        endpoint = cast(str, self.batch_endpoint)
        return _Call(endpoint, params)

    def _should_use_batch(self) -> bool:
        if (len(self.symbols) <= 1) or (not self.batch_endpoint):
            return False
        params = self.params
        return not {"startDate", "endDate"} & params.keys()

    @property
    def params(self) -> dict[str, str]:  # pragma: no cover - overridden in subclasses
        return {
            "startDate": self.start.strftime("%Y-%m-%d"),
            "endDate": self.end.strftime("%Y-%m-%d"),
            "format": "json",
        }

    # ------------------------------------------------------------------
    # Public API
    def read(self) -> pd.DataFrame:
        frames = []
        if self._should_use_batch():
            payloads = self._request_batch(self.symbols)
            for symbol in self.symbols:
                if symbol not in payloads:
                    raise TiingoRequestError(
                        f"Tiingo batch response missing data for symbol '{symbol}'"
                    )
                frames.append(self._format_payload(symbol, payloads[symbol]))
        else:
            for symbol in self.symbols:
                payload = self._request_symbol(symbol)
                frames.append(self._format_payload(symbol, payload))

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, keys=[sym for sym in self.symbols], names=["symbol"])
        result.index.set_names(["symbol", "date"], inplace=True)
        return result.sort_index()

    # ------------------------------------------------------------------
    # HTTP helpers
    def _perform_request(self, call: _Call, symbol: str | None = None) -> list[dict[str, object]]:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {self.api_key}",
        }
        response = self.session.get(
            call.url,
            params=call.params,
            headers=headers,
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            target = f" for '{symbol}'" if symbol is not None else ""
            raise TiingoRequestError(
                f"Tiingo request{target} failed with {response.status_code}: {response.text}"
            )
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive guard
            raise TiingoRequestError("Tiingo response was not valid JSON") from exc
        if not isinstance(payload, list):
            raise TiingoRequestError("Tiingo response did not contain price records")
        return payload

    def _request_symbol(self, symbol: str) -> list[dict[str, object]]:
        call = self._build_call(symbol)
        payload = self._perform_request(call, symbol)
        return payload

    def _request_batch(self, symbols: Sequence[str]) -> Mapping[str, list[dict[str, object]]]:
        call = self._build_batch_call(symbols)
        payload = self._perform_request(call)
        grouped: dict[str, list[dict[str, object]]] = {}
        for entry in payload:
            if not isinstance(entry, dict):
                raise TiingoRequestError("Tiingo batch response contained an invalid record")
            ticker = entry.get("ticker") or entry.get("symbol")
            if not isinstance(ticker, str):
                raise TiingoRequestError("Tiingo batch response missing ticker identifier")
            price_data = entry.get("priceData") or entry.get("data") or entry.get("prices")
            if price_data is None:
                if "date" in entry:
                    price_data = [entry]
                else:
                    price_data = []
            elif not isinstance(price_data, list):
                raise TiingoRequestError("Tiingo batch response price data was not a list")
            grouped[ticker] = price_data
        return grouped

    # ------------------------------------------------------------------
    # Data parsing
    def _format_payload(self, symbol: str, payload: Iterable[dict[str, object]]) -> pd.DataFrame:
        frame = pd.DataFrame(payload)
        if frame.empty:
            frame["date"] = pd.Series(dtype="datetime64[ns]")
        if "date" not in frame.columns:
            raise TiingoRequestError("Tiingo response missing 'date' field")
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame.set_index("date").sort_index()
        frame.index.name = "date"
        return frame


class TiingoDailyReader(_BaseTiingoReader):
    """Retrieve historical daily pricing data from Tiingo."""

    endpoint_template = "https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    batch_endpoint = "https://api.tiingo.com/tiingo/daily/prices"


class TiingoIEXHistoricalReader(_BaseTiingoReader):
    """Retrieve intraday pricing data from Tiingo's IEX endpoint."""

    endpoint_template = "https://api.tiingo.com/iex/{ticker}/prices"
    batch_endpoint = "https://api.tiingo.com/iex/prices"

    def _default_freq(self) -> str | None:
        return "5min"

    @property
    def params(self) -> dict[str, str]:  # type: ignore[override]
        params = super().params
        params["resampleFreq"] = self.freq
        params["columns"] = "open,high,low,close,volume"
        return params
