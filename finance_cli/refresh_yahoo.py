"""Yahoo Finance monthly source fetching."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

import pandas as pd

from .errors import RefreshError

DEFAULT_SYMBOL = "500.PA"
EARLIEST_REQUEST_DATE = "2010-01-01"
FULL_HISTORY_PERIOD1 = 0
EARLIEST_REAL_MONTH = pd.Timestamp("2010-06-01")
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}
YAHOO_CHART_URL_TEMPLATE = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    "?interval=1mo&period1={period1}&period2={period2}"
    "&includePrePost=false&events=div%2Csplits"
)


def build_chart_url(symbol: str, period1: int, period2: int) -> str:
    return YAHOO_CHART_URL_TEMPLATE.format(symbol=symbol, period1=period1, period2=period2)


def fetch_yahoo_monthly_source(
    symbol: str = DEFAULT_SYMBOL,
    *,
    period1: int | None = None,
    earliest_month: pd.Timestamp | None = EARLIEST_REAL_MONTH,
) -> pd.DataFrame:
    effective_period1 = (
        int(pd.Timestamp(EARLIEST_REQUEST_DATE).timestamp()) if period1 is None else period1
    )
    period2 = int(time.time())
    url = build_chart_url(symbol, effective_period1, period2)
    last_error: Exception | None = None

    for attempt in range(3):
        request = urllib.request.Request(url, headers=REQUEST_HEADERS)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.load(response)
            break
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt == 2:
                raise RefreshError(f"Failed to fetch live data for {symbol}: {exc}") from exc
            time.sleep(1 + attempt)
    else:
        raise RefreshError(f"Failed to fetch live data for {symbol}: {last_error}")

    result = payload.get("chart", {}).get("result")
    if not result:
        error = payload.get("chart", {}).get("error")
        raise RefreshError(f"Yahoo Finance returned no chart data for {symbol}: {error}")

    chart = result[0]
    timestamps = chart.get("timestamp", [])
    quote = chart.get("indicators", {}).get("quote", [])
    if not timestamps or not quote:
        raise RefreshError(f"Yahoo Finance returned incomplete quote data for {symbol}.")

    exchange_timezone = chart.get("meta", {}).get("exchangeTimezoneName") or "UTC"

    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(timestamps, unit="s", utc=True)
            .tz_convert(exchange_timezone)
            .tz_localize(None)
        }
    )
    for column, values in quote[0].items():
        frame[column] = values

    monthly = frame.copy()
    monthly["date"] = monthly["date"].dt.to_period("M").dt.to_timestamp()
    monthly = monthly[["date", "open", "high", "low", "close", "volume"]]
    monthly = monthly.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    if earliest_month is not None:
        monthly = monthly[monthly["date"] >= earliest_month].copy()

    if monthly.empty:
        raise RefreshError(f"Yahoo Finance returned no monthly rows for {symbol}.")

    monthly[["open", "high", "low", "close"]] = monthly[
        ["open", "high", "low", "close"]
    ].astype(float).round(2)
    monthly["volume"] = monthly["volume"].fillna(0).round(0).astype(int)
    return monthly


def fetch_full_history_monthly_source(symbol: str) -> pd.DataFrame:
    return fetch_yahoo_monthly_source(
        symbol,
        period1=FULL_HISTORY_PERIOD1,
        earliest_month=None,
    )


def fetch_monthly_source(symbol: str = DEFAULT_SYMBOL) -> pd.DataFrame:
    return fetch_yahoo_monthly_source(symbol)


__all__ = [
    "DEFAULT_SYMBOL",
    "build_chart_url",
    "fetch_full_history_monthly_source",
    "fetch_monthly_source",
    "fetch_yahoo_monthly_source",
]
