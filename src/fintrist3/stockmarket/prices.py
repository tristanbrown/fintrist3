"""Stock market prices."""
from __future__ import annotations

from typing import Any

import pandas as pd

from fintrist3.settings import Config

from . import calendar
from .tiingo import TiingoDailyReader, TiingoIEXHistoricalReader


class Stock:
    """Pull stock price data and return it without persistence.

    freq: "daily", or intraday resolutions like "5min", "1hour".
    """

    def __init__(self, symbol: Any, freq: str = "daily") -> None:
        self.symbol = symbol
        self.freq = freq

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"Stock: {self.symbol}, {self.freq}"

    def get_data(self) -> pd.DataFrame:
        if self.freq == "daily":
            return self.pull_daily()
        return self.pull_intraday(freq=self.freq)

    def pull_daily(self, source: str | None = None, mock: pd.DataFrame | None = None) -> pd.DataFrame:
        """Get a stock quote history."""

        if mock is not None:
            source = "mock"
        elif not source:
            source = "Tiingo"

        if source == "AV":
            try:  # Lazy import so environments without pandas-datareader can still run tests
                import pandas_datareader as pdr  # type: ignore
            except Exception as exc:  # pragma: no cover - exercised only when dependency missing
                msg = "pandas-datareader is required for AlphaVantage requests."
                raise RuntimeError(msg) from exc

            data = pdr.get_data_alphavantage(  # type: ignore[attr-defined]
                self.symbol, api_key=Config.APIKEY_AV, start="1900"
            )
            data.index = pd.to_datetime(data.index)
        elif source == "Tiingo":
            tiingo = TiingoDailyReader(self.symbol, api_key=Config.APIKEY_TIINGO)
            data = tiingo.read()

        elif source == "mock":
            data = mock
        else:  # pragma: no cover - defensive guard
            raise ValueError(f"Unsupported source '{source}'.")

        return data

    def pull_intraday(
        self,
        day: pd.Timestamp | None = None,
        freq: str = "5min",
        tz: str | None = None,
        source: str | None = None,
        mock: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Get intraday stock data."""

        latest_day = calendar.latest_market_day(day)
        open_time = latest_day.iloc[0].isoformat()
        close_time = latest_day.iloc[1].isoformat()
        if tz is None:
            tz = Config.TZ

        if mock is not None:
            dfs = mock
        elif source == "Alpaca":
            from alpaca_management.connect import trade_api  # type: ignore

            data = trade_api.get_barset(
                self.symbol, timeframe="minute", start=open_time, end=close_time, limit=1000
            )
            missing = [symbol for symbol, records in data.items() if not records]
            if missing:
                raise ValueError(f"No intraday data found for symbol(s) {', '.join(missing)}.")
            dfs = {symbol: format_stockrecords(records, tz) for symbol, records in data.items()}
        else:
            tiingo = TiingoIEXHistoricalReader(self.symbol, api_key=Config.APIKEY_TIINGO, end=day, freq=freq)
            dfs = tiingo.read()

        if isinstance(self.symbol, str):
            dfs = dfs.loc[self.symbol]

        return dfs


def format_stockrecords(records: Any, tz: str) -> pd.DataFrame:
    """Reformat stock tick records as a dataframe."""

    df = pd.DataFrame.from_records(records.__dict__["_raw"])  # type: ignore[attr-defined]
    df = df.rename(
        {
            "o": "open",
            "c": "close",
            "l": "low",
            "h": "high",
            "v": "volume",
            "t": "timestamp",
        },
        axis=1,
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert(tz)
    df = df.set_index("timestamp")
    return df

