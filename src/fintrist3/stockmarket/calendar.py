"""Functions keeping track of the market open/close times."""
import pandas as pd
import arrow
import pandas_market_calendars as mcal

from fintrist2 import Config

def market_schedule(start, end, tz=None):
    if tz is None:
        tz = Config.TZ
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date=start.datetime, end_date=end.datetime)
    try:
        for col in schedule.columns:
            schedule[col] = schedule[col].dt.tz_convert(tz)
    except AttributeError:
        pass
    return schedule, nyse

def market_open(now=None):
    """Is the market open?"""
    nyse = mcal.get_calendar('NYSE')
    if now is None:
        now = arrow.now('America/New_York')
    schedule = nyse.schedule(start_date=now.shift(days=-7).datetime, end_date=now.datetime)
    return nyse.open_at_time(schedule, now.datetime)  # Market currently open

def latest_market_day(now=None):
    """Get the hours of the most recent time when the market was open."""
    tz = 'America/New_York'
    if now is None:
        now = arrow.now(tz)
    schedule, nyse = market_schedule(now.shift(days=-7), now, tz)
    last_day = schedule.iloc[-1]
    if now.datetime < last_day['market_open']:
        return schedule.iloc[-2]
    else:
        return last_day

def market_current(timestamp):
    """Check if the market has or hasn't progressed since the last timestamp."""
    now = arrow.now(Config.TZ)
    schedule, nyse = market_schedule(now.shift(days=-7), now)
    open_times = schedule[
        (schedule['market_close'] > timestamp.datetime)& \
        (schedule['market_open'] < now.datetime)]
    return open_times.empty
