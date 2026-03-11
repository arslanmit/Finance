# Finance CLI

Simple command-line tool for analyzing ETF, stock, and similar market datasets with a moving average rule.

The app supports three main workflows:

- run analysis on a registered dataset
- run analysis on your own CSV file
- create a brand-new CSV dataset from a Yahoo Finance symbol such as `SPY`, `VOO`, `AAPL`, or `500.PA`

## Install

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

The wizard can:

- use an existing registered dataset
- use a custom CSV file
- create a new dataset from a Yahoo symbol

## Common Commands

Run a registered dataset:

```bash
python3 dynamic_range_average.py run --dataset default --months 6
```

Run your own CSV file:

```bash
python3 dynamic_range_average.py run --file data/LU1681048804.csv --months 12
```

Run a registered dataset and refresh it first:

```bash
python3 dynamic_range_average.py run --dataset default --months 6 --refresh
```

List registered datasets:

```bash
python3 dynamic_range_average.py datasets list
```

Refresh every registered live dataset:

```bash
python3 dynamic_range_average.py datasets refresh --all
```

## Create A Dataset From A Symbol

Create and register a new dataset from Yahoo Finance:

```bash
python3 dynamic_range_average.py datasets create --symbol SPY
```

Another example:

```bash
python3 dynamic_range_average.py datasets create --symbol AAPL
```

What this does:

- downloads full available monthly OHLCV history for the symbol
- creates a CSV file in `data/generated/`
- registers the dataset in `datasets.json`
- enables future refresh with the same Yahoo symbol
- writes the symbol itself as the first column in the generated CSV

Generated files use this pattern:

```text
data/generated/<symbol_slug>.csv
```

Examples:

- `SPY` -> `data/generated/spy.csv`
- `500.PA` -> `data/generated/500_pa.csv`

After creation, you can run the new dataset immediately:

```bash
python3 dynamic_range_average.py run --dataset spy --months 6
```

## Dataset Registry

The registered dataset list is stored in [datasets.json](/Users/Development/Finance/datasets.json).

List datasets:

```bash
python3 dynamic_range_average.py datasets list
```

Add an existing CSV file manually:

```bash
python3 dynamic_range_average.py datasets add --id amundi_copy --label "Amundi Copy" --path data/LU1681048804.csv
```

Add an existing refreshable CSV:

```bash
python3 dynamic_range_average.py datasets add --id live_sp500 --label "Live S&P 500" --path data/sp500_raw_data.csv --refresh-symbol 500.PA
```

Remove a dataset:

```bash
python3 dynamic_range_average.py datasets remove --id amundi_copy
```

Refresh one live dataset:

```bash
python3 dynamic_range_average.py datasets refresh --id default
```

Refresh all live datasets:

```bash
python3 dynamic_range_average.py datasets refresh --all
```

## How Analysis Works

The analysis logic is intentionally simple:

- requires `date` and `open`
- sorts rows by ascending date
- computes `Moving_Average` from `open`
- computes `condition = (Moving_Average > open)`
- prints rows where `condition == 1`
- saves the full processed dataset with derived columns

`--months` is the moving average window size. Examples:

- short window: `--months 3`
- medium window: `--months 6`
- long window: `--months 12`

## Supported Input Files

Supported input format:

- `.csv`

Notes:

- Excel files are no longer supported
- symbol-created and refreshed datasets are always stored as `.csv`
- symbol-backed CSV files keep `symbol` as the first column

## Output

By default the app writes:

```text
output/<input_stem>_processed.csv
```

Examples:

- `data/LU1681048804.csv` -> `output/LU1681048804_processed.csv`
- `data/generated/spy.csv` -> `output/spy_processed.csv`

You can override this with `--output`, but the output format must still be `.csv`.

Refresh backups are written to:

```text
tmp/refresh_backups/<input_stem>.backup.<timestamp>.csv
```

## Tests

Run the automated test suite:

```bash
python3 -m pytest
```
