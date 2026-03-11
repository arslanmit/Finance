# Finance CLI

Finance CLI is a CSV-first command-line tool for screening ETF, stock, and similar market datasets with a monthly moving-average rule.

It supports three practical workflows:

- run analysis on a generated dataset discovered from `data/generated`
- run analysis on your own CSV file
- create and refresh symbol-backed monthly datasets from Yahoo Finance

## What The Project Does

The project is built around a simple market-screening idea:

- load monthly price history
- compare the current monthly `open` to its rolling average over a chosen number of months
- compute two gap ratios that show how far the current `open` sits above or below that average
- mark each row with a binary condition: `1` when the moving average is above the current `open`, otherwise `0`

This is best understood as a lightweight trend-following or time-series-momentum style screen, not as a complete investment system.

## Installation

```bash
python3 -m pip install -r requirements.txt
```

If your system Python is externally managed and the command above fails:

```bash
python3 -m pip install --user --break-system-packages -r requirements.txt
```

## Quick Start

Open the guided wizard:

```bash
python3 dynamic_range_average.py
```

The wizard lets you:

- create a new dataset from a Yahoo symbol
- run a custom CSV file
- choose an existing generated dataset from `data/generated`

Run a generated dataset directly:

```bash
python3 dynamic_range_average.py run --dataset 500_pa --months 6
```

Run your own CSV file:

```bash
python3 dynamic_range_average.py run --file data/data_to_pyhton.csv --months 12
```

Refresh a generated symbol-backed dataset before running analysis:

```bash
python3 dynamic_range_average.py run --dataset nvda --months 6 --refresh
```

## Dataset Workflows

### Generated Datasets

Named datasets are discovered only from:

```text
data/generated/*.csv
```

Dataset ids come directly from file names:

- `data/generated/500_pa.csv` -> `500_pa`
- `data/generated/nvda.csv` -> `nvda`
- `data/generated/spy.csv` -> `spy`

Files outside `data/generated` are never auto-discovered, but they still work with `run --file`.

List generated datasets:

```bash
python3 dynamic_range_average.py datasets list
```

### Create A Dataset From Yahoo Finance

Create a new generated dataset from a Yahoo Finance symbol:

```bash
python3 dynamic_range_average.py datasets create --symbol SPY
```

Another example:

```bash
python3 dynamic_range_average.py datasets create --symbol 500.PA
```

What this does:

- downloads full available monthly OHLCV history for the symbol from Yahoo Finance
- writes `data/generated/<symbol_slug>.csv`
- makes the dataset immediately discoverable
- stores the original symbol in the first `symbol` column
- enables future live refresh for that dataset

Examples:

- `SPY` -> `data/generated/spy.csv`
- `500.PA` -> `data/generated/500_pa.csv`

### Add Or Remove A Generated Dataset

Copy an existing CSV into generated storage using its source filename:

```bash
python3 dynamic_range_average.py datasets add --path data/data_to_pyhton.csv
```

If the imported file should support live refresh, provide the Yahoo symbol:

```bash
python3 dynamic_range_average.py datasets add --path data/my_sp500.csv --refresh-symbol 500.PA
```

Remove a generated dataset:

```bash
python3 dynamic_range_average.py datasets remove --id spy
```

### Refresh Generated Datasets

Refresh one generated symbol-backed dataset:

```bash
python3 dynamic_range_average.py datasets refresh --id 500_pa
```

Refresh every generated symbol-backed dataset:

```bash
python3 dynamic_range_average.py datasets refresh --all
```

Refresh works only for generated datasets that have a non-empty `symbol` column. Refresh backups are written to:

```text
tmp/refresh_backups/<input_stem>.backup.<timestamp>.csv
```

## Financial Logic And Methodology

### Input Model

The project is designed around monthly market data, especially for symbol-created and refreshed datasets.

- symbol-created and live-refreshed generated datasets are monthly Yahoo Finance OHLCV CSVs with:
  `symbol`, `date`, `open`, `high`, `low`, `close`, `volume`
- imported generated datasets and direct custom CSV files must contain at least `date` and `open`
- extra source columns are allowed and will flow through to processed output
- the CLI does not enforce monthly frequency for custom files, but the methodology in this README is intended for monthly datasets

Before analysis, the app:

- parses `date`
- converts `open` to numeric
- validates that the required fields are present
- sorts rows in ascending date order

### Signal Definition

For a chosen `--months` window, the app computes:

- `Moving_Average` = rolling mean of monthly `open`
- `condition = 1` when `Moving_Average > open`, else `0`
- `moving_average_minus_open_over_open = (Moving_Average - open) / open`
- `open_minus_moving_average_over_moving_average = (open - Moving_Average) / Moving_Average`
- `moving_average_window_months` = the entered window size for traceability

Financially, this is a simple screen for whether the current monthly open is below or above its recent average level.

- a positive `moving_average_minus_open_over_open` means the current open is below its moving average
- a positive `open_minus_moving_average_over_moving_average` means the current open is above its moving average
- `condition = 1` lines up with the first case, where the moving average sits above the current open

### Why This Can Make Sense

This methodology is a simplified version of ideas often associated with:

- technical analysis
- moving-average trading rules
- trend-following
- time-series momentum

The intuition is straightforward:

- moving averages smooth noisy price series
- comparing a current price level to its recent average is a common heuristic for detecting trend or regime direction
- using monthly data reduces some short-term noise compared with daily data, but it also reacts more slowly

This repo does not claim that the rule is optimal. It exposes the signal clearly so you can inspect the data and build on it.

### How To Use These Methods In Practice

The project gives you one core method with different practical uses depending on the moving-average window you choose.

#### 1. Shorter Window For Faster Regime Detection

Use a smaller `--months` value when you want a more reactive screen:

```bash
python3 dynamic_range_average.py run --dataset nvda --months 3
```

Typical use:

- faster detection of recent price dislocations versus the recent average
- more sensitivity to short-term market changes
- more noise and more frequent signal flips

#### 2. Medium Window For A Balanced Screen

Use a mid-range window when you want a practical default:

```bash
python3 dynamic_range_average.py run --dataset spy --months 6
```

Typical use:

- balances responsiveness and smoothing
- useful as a general screening window for monthly datasets
- often the easiest starting point for comparing assets with the same logic

#### 3. Longer Window For Slower Trend Context

Use a larger window when you want a slower-moving view of regime direction:

```bash
python3 dynamic_range_average.py run --dataset 500_pa --months 12
```

Typical use:

- emphasizes longer-term trend context
- reduces sensitivity to short-term noise
- reacts later to regime changes

#### 4. Use Refresh Before Applying The Method

If the dataset is symbol-backed, refresh first so the screen uses the latest available monthly data:

```bash
python3 dynamic_range_average.py run --dataset aapl --months 6 --refresh
```

Typical use:

- operational workflow for recurring monthly screening
- useful when generated datasets are part of a repeatable research process

#### 5. Apply The Same Method To Your Own CSV

You can use the same moving-average methodology on an external CSV as long as it includes `date` and `open`:

```bash
python3 dynamic_range_average.py run --file data/data_to_pyhton.csv --months 12
```

Typical use:

- applying the method to internal research exports
- comparing non-Yahoo datasets with the same signal logic
- extending the screen with extra custom columns that remain in the processed CSV

### How To Read The Signal

The most useful interpretation is usually:

- `condition = 1`: the moving average is above the current `open`
- positive `moving_average_minus_open_over_open`: the current `open` is below its recent average
- larger positive values in that primary gap column: the current `open` is further below its recent average

In practical screening terms, that means the project is most naturally used to find rows where price is trading below its recent average level, then inspect the size of that gap.

## Output Columns And Interpretation

By default the app writes:

```text
output/<input_stem>_processed.csv
```

Examples:

- `data/generated/500_pa.csv` -> `output/500_pa_processed.csv`
- `data/generated/spy.csv` -> `output/spy_processed.csv`

You can override the output path with `--output`, but the file must still end in `.csv`.

### Terminal Output

The terminal output shows all analyzed rows, not only `condition == 1`.

It uses a dynamic column order:

- key fields first when present: `symbol`, the two gap columns, `date`, `open`
- then the remaining source columns in their original CSV order
- then analysis-derived columns such as `moving_average_window_months`, `Moving_Average`, and `condition`

### Processed CSV Output

Processed CSV files follow the same dynamic ordering as the terminal output.

That means the processed file preserves:

- your original source columns
- any extra columns from custom CSV inputs
- the derived analysis fields added by the app

## Limits And Caveats

This project is intentionally narrow. It is not:

- a discounted cash-flow or fundamental valuation model
- a complete trading system with entries, exits, position sizing, slippage, taxes, fees, or risk controls
- a portfolio-construction engine
- a cross-sectional ranking model across many assets
- a benchmark-comparison or performance-attribution framework
- a statistical backtest engine with Sharpe ratio, drawdown, turnover, or transaction-cost analysis

Methodological caveats:

- the signal is driven only by the `open` series
- monthly frequency can miss faster regime changes
- live refresh updates source data, but does not validate investment performance
- Yahoo Finance data quality and symbol coverage are external dependencies

Use the project as a transparent screening and inspection tool, not as proof of investability.

## References Appendix

- Brock, Lakonishok, and LeBaron (1992), *Simple Technical Trading Rules and the Stochastic Properties of Stock Returns*  
  [JSTOR](https://www.jstor.org/stable/2328994)  
  Relevance: classic evidence paper on simple technical trading rules, including moving-average-style rules.

- Moskowitz, Ooi, and Pedersen (2012), *Time Series Momentum*  
  [Yale-hosted PDF](https://fairmodel.econ.yale.edu/ec439/jpde.pdf)  
  Relevance: connects price-versus-history logic to time-series momentum intuition across assets.

- Han, Yang, Zhou, and Zhu (2019), *Theoretical and practical motivations for the use of the moving average rule in the stock market*  
  [Oxford Academic PDF](https://academic.oup.com/imaman/article-pdf/31/1/117/34157139/dpz006.pdf)  
  Relevance: directly discusses why moving-average rules are used and how they can be interpreted.

- Lo, Mamaysky, and Wang (2000), *Foundations of Technical Analysis*  
  [NBER](https://www.nber.org/papers/w7613)  
  Relevance: broader background on systematic technical-analysis framing.

## Contributor Notes

Useful paths:

- entrypoint: `dynamic_range_average.py`
- CLI implementation: `finance_cli/`
- generated datasets: `data/generated/`
- processed outputs: `output/`
- refresh backups: `tmp/refresh_backups/`

Run tests:

```bash
python3 -m pytest
```

Helpful CLI help commands:

```bash
python3 dynamic_range_average.py --help
python3 dynamic_range_average.py run --help
python3 dynamic_range_average.py datasets --help
```
