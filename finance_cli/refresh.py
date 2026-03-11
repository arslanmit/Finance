"""CSV refresh support for generated Yahoo monthly data."""

from __future__ import annotations

import json
import shutil
import time
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd

from .errors import RefreshError
from .models import RefreshSummary, ResolvedSource
from .sources import ensure_symbol_column

DEFAULT_SYMBOL = "500.PA"
SUPPORTED_REFRESH_PROVIDER = "yahoo"
EARLIEST_REQUEST_DATE = "2010-01-01"
FULL_HISTORY_PERIOD1 = 0
EARLIEST_REAL_MONTH = pd.Timestamp("2010-06-01")
OVERLAP_MONTHS = ("2024-08-01", "2024-10-01")
NOVEMBER_OPEN_MONTH = pd.Timestamp("2024-11-01")
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


def validate_refreshable_source(source: ResolvedSource) -> None:
    dataset = source.dataset
    if dataset is None:
        raise RefreshError("Live refresh is only available for generated datasets, not custom files.")
    if dataset.refresh is None:
        raise RefreshError(f"Dataset '{dataset.id}' does not support live refresh.")
    if dataset.refresh.provider != SUPPORTED_REFRESH_PROVIDER:
        raise RefreshError(
            f"Dataset '{dataset.id}' uses an unsupported refresh provider: {dataset.refresh.provider}"
        )
    if source.input_path.suffix.lower() != ".csv":
        raise RefreshError("Live refresh currently supports only .csv datasets.")


def refresh_selected_source(source: ResolvedSource) -> RefreshSummary:
    validate_refreshable_source(source)
    dataset = source.dataset
    if dataset is None or dataset.refresh is None:
        raise RefreshError("Live refresh is not configured for the selected source.")

    return refresh_yahoo_monthly_csv(
        source.input_path,
        symbol=dataset.refresh.symbol,
        strict_validation=False,
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


def load_existing_csv_data(data_path: Path) -> pd.DataFrame:
    dataframe = pd.read_csv(data_path)
    dataframe.columns = [str(column).strip().lower() for column in dataframe.columns]
    if "symbol" in dataframe.columns:
        dataframe = dataframe.drop(columns=["symbol"])
    dataframe["date"] = pd.to_datetime(dataframe["date"]).dt.normalize()
    return dataframe


def validate_source_contiguity(source: pd.DataFrame) -> None:
    if source["date"].duplicated().any():
        raise RefreshError("Live monthly source contains duplicate dates.")

    expected = pd.date_range(source["date"].min(), source["date"].max(), freq="MS")
    missing = expected.difference(source["date"])
    if not missing.empty:
        preview = ", ".join(day.date().isoformat() for day in missing[:5])
        raise RefreshError(f"Live monthly source is missing dates: {preview}.")


def validate_overlap(existing: pd.DataFrame, source: pd.DataFrame) -> None:
    merged = existing.merge(source, on="date", suffixes=("_existing", "_source"))
    if merged.empty:
        raise RefreshError("Could not compare the current dataset to the live source.")

    completed = merged[
        merged["date"].between(pd.Timestamp(OVERLAP_MONTHS[0]), pd.Timestamp(OVERLAP_MONTHS[1]))
    ].sort_values("date")
    if len(completed) != 3:
        raise RefreshError(
            "Expected three completed overlap months (2024-08 through 2024-10) for validation."
        )

    for column in ("open", "high", "low", "close"):
        delta = (completed[f"{column}_existing"] - completed[f"{column}_source"]).abs().max()
        if float(delta) > 0.02:
            raise RefreshError(
                f"Live source validation failed for {column}; max delta was {float(delta):.4f}."
            )

    november = merged[merged["date"] == NOVEMBER_OPEN_MONTH]
    if november.empty:
        raise RefreshError("Expected an overlap row for 2024-11-01.")

    open_delta = abs(
        float(november.iloc[0]["open_existing"]) - float(november.iloc[0]["open_source"])
    )
    if open_delta > 0.02:
        raise RefreshError(
            f"Live source validation failed for 2024-11 open; delta was {open_delta:.4f}."
        )


def create_backup(data_path: Path) -> Path:
    backup_dir = Path("tmp/refresh_backups")
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_name = f"{data_path.stem}.backup.{time.strftime('%Y%m%d-%H%M%S')}.csv"
    backup_path = backup_dir / backup_name
    shutil.copy2(data_path, backup_path)
    return backup_path


def write_source_csv(data_path: Path, source: pd.DataFrame, symbol: str) -> None:
    descending = source.sort_values("date", ascending=False).reset_index(drop=True)
    output = ensure_symbol_column(descending, symbol)
    output.to_csv(data_path, index=False, date_format="%Y-%m-%d")


def refresh_yahoo_monthly_csv(
    data_path: Path,
    symbol: str = DEFAULT_SYMBOL,
    strict_validation: bool = True,
) -> RefreshSummary:
    if not data_path.exists():
        raise RefreshError(f"Refresh target does not exist: {data_path}")

    existing = load_existing_csv_data(data_path)
    source = fetch_monthly_source(symbol) if strict_validation else fetch_full_history_monthly_source(symbol)
    validate_source_contiguity(source)
    if strict_validation:
        validate_overlap(existing, source)
    backup_path = create_backup(data_path)
    write_source_csv(data_path, source, symbol)

    return RefreshSummary(
        symbol=symbol,
        row_count=len(source),
        min_date=source["date"].min().date().isoformat(),
        max_date=source["date"].max().date().isoformat(),
        backup_path=str(backup_path),
    )


def refresh_generated_dataset(
    data_path: Path,
    symbol: str = DEFAULT_SYMBOL,
) -> RefreshSummary:
    return refresh_yahoo_monthly_csv(
        data_path,
        symbol=symbol,
        strict_validation=False,
    )
