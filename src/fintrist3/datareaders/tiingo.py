"""Direct Tiingo data reader utilities for fintrist3.

Fetches daily and intraday OHLCV data directly from Tiingo's REST API, without
relying on pandas-datareader.
"""

from __future__ import annotations

from typing import Iterable, Optional, Union, List, Dict, Any

import pandas as pd
import requests

from fintrist3.settings import Config


SymbolLike = Union[str, Iterable[str]]


class TiingoDataReader:
    """HTTP client + helpers to read data from Tiingo's API.

    - Uses `Config.APIKEY_TIINGO` by default.
    - Normalizes DataFrame indexes for single vs multi-symbol responses.
    """

    BASE_URL = "https://api.tiingo.com"

    def __init__(self, api_key: Optional[str] = None, session: Optional[requests.Session] = None) -> None:
        token = api_key or Config.APIKEY_TIINGO
        if not token:
            raise ValueError("Missing Tiingo API key. Set APIKEY_TIINGO in environment or pass api_key.")
        self.api_key = token
        self.session = session or requests.Session()

    # --------------- Public API ---------------
    def daily(
        self,
        symbols: SymbolLike,
        start: Union[str, pd.Timestamp] = "1900-01-01",
        end: Optional[Union[str, pd.Timestamp]] = None,
    ) -> pd.DataFrame:
        """Fetch daily OHLCV for one or more symbols.

        - Index: `date` for single symbol; MultiIndex `[symbol, date]` for multiple.
        - Includes any extra fields returned by Tiingo (e.g., adjClose, divCash, splitFactor).
        """
        sym_list = _ensure_list(symbols)
        frames: List[pd.DataFrame] = []
        for sym in sym_list:
            json_rows = self._get(
                f"/tiingo/daily/{sym}/prices",
                params={
                    "startDate": _coerce_date(start),
                    **({"endDate": _coerce_date(end)} if end is not None else {}),
                },
            )
            df = _df_from_daily_json(json_rows)
            frames.append(df)

        if len(frames) == 1 and isinstance(symbols, str):
            return frames[0]

        out = pd.concat(frames, keys=sym_list, names=["symbol", "date"])
        return out.sort_index()

    def intraday(
        self,
        symbols: SymbolLike,
        *,
        day: Optional[Union[str, pd.Timestamp]] = None,
        freq: str = "5min",
    ) -> pd.DataFrame:
        """Fetch intraday OHLCV from Tiingo/IEX for one or more symbols.

        - Index: timestamp for single symbol; MultiIndex `[symbol, timestamp]` for multiple.
        - `day`: If provided, used for both startDate and endDate (UTC date).
        """
        sym_list = _ensure_list(symbols)
        params: Dict[str, Any] = {
            "resampleFreq": freq,
            "columns": "open,high,low,close,volume",
        }
        if day is not None:
            d = _coerce_date(day)
            params.update({"startDate": d, "endDate": d})

        frames: List[pd.DataFrame] = []
        for sym in sym_list:
            json_rows = self._get(f"/iex/{sym}/prices", params=params)
            df = _df_from_intraday_json(json_rows)
            frames.append(df)

        if len(frames) == 1 and isinstance(symbols, str):
            return frames[0]

        out = pd.concat(frames, keys=sym_list, names=["symbol", "timestamp"])
        return out.sort_index()

    # --------------- HTTP helpers ---------------
    def _get(self, path: str, *, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.BASE_URL}{path}"
        p = dict(params or {})
        p["token"] = self.api_key
        resp = self.session.get(url, params=p, timeout=30)
        if resp.status_code >= 400:
            try:
                detail = resp.json()
            except Exception:
                detail = resp.text
            raise RuntimeError(f"Tiingo API error {resp.status_code} for {url}: {detail}")
        return resp.json()


# Convenience functions -----------------------------------------------------

def daily(
    symbols: SymbolLike,
    start: Union[str, pd.Timestamp] = "1900-01-01",
    end: Optional[Union[str, pd.Timestamp]] = None,
    *,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """Module-level helper to fetch daily OHLCV data directly from Tiingo."""
    return TiingoDataReader(api_key=api_key).daily(symbols, start=start, end=end)


def intraday(
    symbols: SymbolLike,
    *,
    day: Optional[Union[str, pd.Timestamp]] = None,
    freq: str = "5min",
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """Module-level helper to fetch intraday OHLCV data from Tiingo/IEX directly."""
    return TiingoDataReader(api_key=api_key).intraday(symbols, day=day, freq=freq)


# ---------------- Internal utilities ----------------

def _ensure_list(symbols: SymbolLike) -> List[str]:
    if isinstance(symbols, str):
        return [symbols]
    return list(symbols)


def _coerce_date(val: Union[str, pd.Timestamp]) -> str:
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    # Allow YYYY-MM-DD strings
    return str(val)


def _df_from_daily_json(rows: Any) -> pd.DataFrame:
    df = pd.DataFrame.from_records(rows)
    if df.empty:
        # Ensure a consistent index/columns even when no data
        df.index = pd.Index([], name="date")
        return df
    # Tiingo daily returns 'date' as ISO string, normalize to date index
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.set_index("date").sort_index()
    return df


def _df_from_intraday_json(rows: Any) -> pd.DataFrame:
    df = pd.DataFrame.from_records(rows)
    if df.empty:
        df.index = pd.Index([], name="timestamp")
        return df
    # Tiingo IEX intraday payload commonly uses 'date' field for timestamp
    ts_field = "date" if "date" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if ts_field is None:
        raise ValueError("Unexpected intraday payload: missing 'date' or 'timestamp' field")
    df["timestamp"] = pd.to_datetime(df[ts_field], utc=True)
    df = df.drop(columns=[c for c in ["date", "timestamp"] if c in df.columns and c != "timestamp"])
    df = df.set_index("timestamp").sort_index()
    return df
