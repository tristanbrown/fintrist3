"""Stock market prices."""
import pandas as pd
import pandas_datareader as pdr

from pandas_datareader.tiingo import TiingoIEXHistoricalReader
from fintrist3.settings import Config
from . import calendar

class Stock:
    """Pull stock price data and return it without persistence.

    freq: "daily", or intraday resolutions like "5min", "1hour".
    """

    def __init__(self, symbol, freq="daily"):
        self.symbol = symbol
        self.freq = freq

    def __repr__(self):
        return f"Stock: {self.symbol}, {self.freq}"

    def get_data(self):
        if self.freq == "daily":
            return self.pull_daily()
        else:
            return self.pull_intraday(freq=self.freq)

    def pull_daily(self, source=None, mock=None):
        """Get a stock quote history.

        ::parents:: mock
        ::params:: symbol, source
        ::alerts:: source: AV, source: Tiingo, ex-dividend, split, reverse split
        """
        ## Get the data from whichever source
        if mock is not None:
            source = 'mock'
        elif not source:
            source = 'Tiingo'
        if source == 'AV':
            data = pdr.get_data_alphavantage(self.symbol, api_key=Config.APIKEY_AV, start='1900')
            data.index = pd.to_datetime(data.index)
        elif source == 'Tiingo':
            data = pdr.get_data_tiingo(self.symbol, api_key=Config.APIKEY_TIINGO, start='1900')

            # Multiple stock symbols are possible
            data = data.reset_index().set_index('date')
            data.index = data.index.date
            data.index.name = 'date'
            data = data.set_index('symbol', append=True)
            data = data.reorder_levels(['symbol', 'date'])
            if isinstance(self.symbol, str):  ## Single symbol only
                data = data.droplevel('symbol')
        elif source == 'mock':
            data = mock

        return data

    def pull_intraday(self, day=None, freq='5min', tz=None, source=None, mock=None):
        """Get intraday stock data.

        ::parents:: mock
        ::params:: symbols, day, tz, source
        ::alerts:: source: Alpaca, source: mock
        """
        ## Pick the day
        latest_day = calendar.latest_market_day(day)
        open_time = latest_day[0].isoformat()
        close_time = latest_day[1].isoformat()
        if tz is None:
            tz = Config.TZ

        ## Get the data
        if mock is not None:
            dfs = mock
        elif source == 'Alpaca':
            from alpaca_management.connect import trade_api  # type: ignore
            data = trade_api.get_barset(
                self.symbol, timeframe='minute', start=open_time, end=close_time, limit=1000)
            missing = [symbol for symbol, records in data.items() if not records]
            if missing:
                raise ValueError(f"No intraday data found for symbol(s) {', '.join(missing)}.")
            dfs = {symbol: format_stockrecords(records, tz) for symbol, records in data.items()}
        else:
            tiingo = TiingoIEXPriceVolume(self.symbol, api_key=Config.APIKEY_TIINGO, end=day, freq=freq)
            dfs = tiingo.read()

        if isinstance(self.symbol, str):
            dfs = dfs.loc[self.symbol]

        return dfs

class TiingoIEXPriceVolume(TiingoIEXHistoricalReader):
    """Adds volume to the Tiingo/IEX intraday pricing data."""

    @property
    def params(self):
        """Parameters to use in API calls"""
        return {
            "startDate": self.start.strftime("%Y-%m-%d"),
            "endDate": self.end.strftime("%Y-%m-%d"),
            "resampleFreq": self.freq,
            "format": "json",
            "columns": "open,high,low,close,volume",
        }

def format_stockrecords(records, tz):
    """Reformat stock tick records as a dataframe."""
    df = pd.DataFrame.from_records(records.__dict__['_raw'])
    df = df.rename({
        'o': 'open', 'c': 'close',
        'l': 'low', 'h': 'high',
        'v': 'volume', 't': 'timestamp'}, axis=1
    )
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True).dt.tz_convert(tz)
    df = df.set_index('timestamp')
    return df
