# Finance CLI

Simple command-line tool for analyzing ETF, stock, and similar market datasets with a moving average rule.

The app supports three main workflows:

- run analysis on a generated dataset discovered from `data/generated`
- run analysis on your own CSV file
- create a brand-new generated CSV dataset from a Yahoo Finance symbol such as `SPY`, `VOO`, `AAPL`, or `500.PA`

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

- use an existing generated dataset discovered from `data/generated`
- use a custom CSV file
- create a new dataset from a Yahoo symbol

## Common Commands

Run a generated dataset:

```bash
python3 dynamic_range_average.py run --dataset 500_pa --months 6
```

Run your own CSV file:

```bash
python3 dynamic_range_average.py run --file data/data_to_pyhton.csv --months 12
```

Run a generated refreshable dataset and refresh it first:

```bash
python3 dynamic_range_average.py run --dataset 500_pa --months 6 --refresh
```

List generated datasets:

```bash
python3 dynamic_range_average.py datasets list
```

Refresh every generated symbol-backed dataset:

```bash
python3 dynamic_range_average.py datasets refresh --all
```

## Generated Dataset Folder

Named datasets are discovered from this folder only:

- `data/generated/*.csv`

Dataset ids come directly from file names. For example:

- `data/generated/500_pa.csv` -> `500_pa`
- `data/generated/nvda.csv` -> `nvda`
- `data/generated/spy.csv` -> `spy`

Files outside `data/generated` are never auto-discovered, but they can still be used with `run --file`.

## Create A Dataset From A Symbol

Create a new generated dataset from Yahoo Finance:

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
- makes the dataset discoverable automatically
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

## Add Or Remove Generated Datasets

Copy an existing CSV into generated storage using its filename:

```bash
python3 dynamic_range_average.py datasets add --path data/data_to_pyhton.csv
```

If the file should be refreshable, add the symbol while importing:

```bash
python3 dynamic_range_average.py datasets add --path data/my_sp500.csv --refresh-symbol 500.PA
```

Remove a generated dataset:

```bash
python3 dynamic_range_average.py datasets remove --id spy
```

Refresh one generated dataset:

```bash
python3 dynamic_range_average.py datasets refresh --id 500_pa
```

Refresh all generated symbol-backed datasets:

```bash
python3 dynamic_range_average.py datasets refresh --all
```

## How Analysis Works

The analysis logic is intentionally simple:

- requires `date` and `open`
- sorts rows by ascending date
- computes `Moving_Average` from `open`
- stores the entered window size in `moving_average_window_months`
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

- Excel files are not supported
- symbol-created and refreshed datasets are always stored as `.csv`
- symbol-backed CSV files keep `symbol` as the first column
- terminal output for symbol-backed datasets also shows `symbol` in the first column

## Output

By default the app writes:

```text
output/<input_stem>_processed.csv
```

Examples:

- `data/generated/500_pa.csv` -> `output/500_pa_processed.csv`
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
