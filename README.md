# Finance Data Analysis Tools

This repository contains a small CLI and notebook for financial time-series analysis. The main workflow calculates a moving average over a dataset, prints the rows where the moving average is above the open price, and saves the processed data to disk.

## Contents

- `dynamic_range_average.py` - CLI for moving-average analysis over Excel and CSV files
- `Dynamic_Range_Average.ipynb` - Jupyter notebook for interactive exploration
- `data/` - Sample datasets used by the CLI and notebook

## Requirements

- Python 3.8+
- `pandas`
- `openpyxl` for `.xlsx` support
- `xlrd` for `.xls` input support
- `xlwt` for `.xls` output support
- `jupyter` if you want to use the notebook

## Installation

```bash
python3 -m pip install -r requirements.txt
```

If your system Python is externally managed and the command above fails, use:

```bash
python3 -m pip install --user --break-system-packages -r requirements.txt
```

If you want to use the notebook as well, install Jupyter separately:

```bash
python3 -m pip install jupyter
```

## CLI Usage

The CLI accepts `.csv`, `.xlsx`, and `.xls` inputs.

```bash
python3 dynamic_range_average.py --months <N>
```

This reads the default sample input at `data/sp500_raw_data.xlsx`, prints the filtered rows, and writes the full processed dataset to `output/sp500_raw_data_processed.xlsx`. Replace `<N>` with the moving average window you want to analyze.

### Months Examples

Short-term example:

```bash
python3 dynamic_range_average.py --months 3
```

Medium-term example:

```bash
python3 dynamic_range_average.py --months 6
```

Long-term example:

```bash
python3 dynamic_range_average.py --months 12
```

Use a different input file:

```bash
python3 dynamic_range_average.py --months 6 --input data/LU1681048804.csv
```

Choose a specific Excel sheet:

```bash
python3 dynamic_range_average.py --months 12 --input data/sp500_raw_data.xlsx --sheet Sheet1
```

Write to a custom output file:

```bash
python3 dynamic_range_average.py --months 24 --output output/custom_results.csv
```

Refresh the default workbook from live `500.PA` monthly data before running the analysis:

```bash
python3 dynamic_range_average.py --months 6 --refresh
```

If `--months` is omitted in an interactive terminal, the script prompts for it. In non-interactive runs, `--months` is required.

`--refresh` is only supported for the default workbook at `data/sp500_raw_data.xlsx`. If the live refresh fails, the CLI exits with an error instead of silently falling back to stale local data.

## Input Expectations

Input data must contain at least these columns:

- `date` or `Date`
- `open`

Additional columns are allowed and are preserved in the saved output. The CLI normalizes column names, parses dates, sorts rows by date, and validates the requested moving-average window against the number of rows in the dataset.

## Output

Each run produces:

- Console output showing rows where `Moving_Average > open`
- When `--refresh` is used, a refresh summary showing the live symbol, date range, row count, and backup path
- A saved dataset containing all original rows plus:
  - `Moving_Average`
  - `condition`

By default, the output path is `output/<input_stem>_processed.<input_extension>`.

## Notebook Usage

Start Jupyter and open `Dynamic_Range_Average.ipynb`:

```bash
jupyter notebook
```

## License

This project is for educational and analytical purposes.
