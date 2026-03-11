# Finance CLI

Simple command-line tool for analyzing ETF, stock, and similar market datasets with a moving average rule.

The app supports three main workflows:

- run analysis on a registered dataset
- run analysis on your own CSV or Excel file
- create a brand-new dataset from a Yahoo Finance symbol such as `SPY`, `VOO`, `AAPL`, or `500.PA`

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
- use a custom file
- create a new dataset from a Yahoo symbol

## Common Commands

Run a registered dataset:

```bash
python3 dynamic_range_average.py run --dataset default --months 6
```

Run your own file:

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
- creates a workbook in `data/generated/`
- registers the dataset in `datasets.json`
- enables future refresh with the same Yahoo symbol
- writes the symbol itself as the first column in the generated workbook

Generated files use this pattern:

```text
data/generated/<symbol_slug>.xlsx
```

Examples:

- `SPY` -> `data/generated/spy.xlsx`
- `500.PA` -> `data/generated/500_pa.xlsx`

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

Add an existing file manually:

```bash
python3 dynamic_range_average.py datasets add --id amundi_copy --label "Amundi Copy" --path data/LU1681048804.xlsx
```

Add an existing refreshable workbook:

```bash
python3 dynamic_range_average.py datasets add --id live_sp500 --label "Live S&P 500" --path data/sp500_raw_data.xlsx --sheet Sheet1 --refresh-symbol 500.PA
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

Supported input formats:

- `.csv`
- `.xlsx`
- `.xls`

Notes:

- custom Excel files with one sheet are handled automatically
- custom Excel files with multiple sheets require sheet selection in the wizard or `--sheet` in command mode
- symbol-created datasets are always stored as `.xlsx`

## Output

By default the app writes:

```text
output/<input_stem>_processed.<input_extension>
```

Examples:

- `data/LU1681048804.csv` -> `output/LU1681048804_processed.csv`
- `data/generated/spy.xlsx` -> `output/spy_processed.xlsx`

You can override this with `--output`.

## Tests

Run the automated test suite:

```bash
python3 -m pytest
```
