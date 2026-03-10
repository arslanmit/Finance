# Finance Data Analysis Tools

This project now uses a wizard-first CLI with explicit commands for repeatable runs and dataset management.

## Install

```bash
python3 -m pip install -r requirements.txt
```

If your system Python is externally managed and the command above fails, use:

```bash
python3 -m pip install --user --break-system-packages -r requirements.txt
```

## Main Usage

Start the guided wizard:

```bash
python3 dynamic_range_average.py
```

Run a registered dataset directly:

```bash
python3 dynamic_range_average.py run --dataset default --months 6
```

Run your own file:

```bash
python3 dynamic_range_average.py run --file data/LU1681048804.csv --months 12
```

Refresh a live dataset before analysis:

```bash
python3 dynamic_range_average.py run --dataset default --months 6 --refresh
```

Create a new dataset from a Yahoo Finance symbol:

```bash
python3 dynamic_range_average.py datasets create --symbol SPY
```

Generated symbol datasets are saved under `data/generated/`.

## Dataset Commands

List registered datasets:

```bash
python3 dynamic_range_average.py datasets list
```

Add a dataset:

```bash
python3 dynamic_range_average.py datasets add --id amundi_copy --label "Amundi Copy" --path data/LU1681048804.xlsx
```

Add a refreshable workbook:

```bash
python3 dynamic_range_average.py datasets add --id live_sp500 --label "Live S&P 500" --path data/sp500_raw_data.xlsx --sheet Sheet1 --refresh-symbol 500.PA
```

Create and register a new stock or ETF dataset automatically:

```bash
python3 dynamic_range_average.py datasets create --symbol AAPL
```

Remove a dataset:

```bash
python3 dynamic_range_average.py datasets remove --id amundi_copy
```

The registry lives in [datasets.json](/Users/Development/Finance/datasets.json).

## Wizard Behavior

The no-argument wizard asks for:

- dataset or custom file
- or create a new dataset from a Yahoo symbol
- Excel sheet if a custom workbook has multiple sheets
- `months`
- refresh choice for refreshable registered datasets
- output path override

## Output

The CLI:

- prints the rows where `Moving_Average > open`
- saves the full processed dataset to `output/<input_stem>_processed.<input_extension>` by default
- keeps the original rows plus `Moving_Average` and `condition`

## Tests

Run the automated test suite with:

```bash
python3 -m pytest
```
