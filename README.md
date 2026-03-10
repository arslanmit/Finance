# Finance Data Analysis Tools

This project provides a simple CLI for moving-average analysis on finance datasets. You can choose a ready-made dataset alias, point to your own file, or just run the script and follow the prompts.

## Install

```bash
python3 -m pip install -r requirements.txt
```

If your system Python is externally managed and the command above fails, use:

```bash
python3 -m pip install --user --break-system-packages -r requirements.txt
```

## Main Usage

Start with the guided flow:

```bash
python3 dynamic_range_average.py
```

Run a configured dataset directly:

```bash
python3 dynamic_range_average.py --dataset default --months 6
```

Run your own file:

```bash
python3 dynamic_range_average.py --file /path/to/data.xlsx --months 12
```

Refresh the default live dataset before analysis:

```bash
python3 dynamic_range_average.py --dataset default --months 6 --refresh
```

## List Available Datasets

```bash
python3 dynamic_range_average.py --list-datasets
```

Current datasets are defined in [datasets.json](/Users/Development/Finance/datasets.json).

## Changing The Dataset

Use one of the configured dataset aliases:

- `default`
- `amundi_csv`
- `amundi_xlsx`
- `spy_history`
- `raw_recent`

Or use your own file:

```bash
python3 dynamic_range_average.py --file data/LU1681048804.csv --months 6
```

If you use your own Excel file and it has more than one sheet, the script asks you to choose a sheet in interactive mode.

## Adding A New Dataset

Open [datasets.json](/Users/Development/Finance/datasets.json) and add a new entry with:

- `id`
- `label`
- `path`
- `sheet`
- `refresh`

Example:

```json
{
  "id": "my_dataset",
  "label": "My Excel file",
  "path": "data/my_file.xlsx",
  "sheet": "Sheet1",
  "refresh": null
}
```

After saving the file, your new alias will appear in:

```bash
python3 dynamic_range_average.py --list-datasets
```

## Output

The script:

- prints the matching rows where `Moving_Average > open`
- saves the processed dataset to `output/<input_stem>_processed.<input_extension>`
- keeps the full dataset plus the derived `Moving_Average` and `condition` columns

## Notebook

The notebook is still available if you want interactive exploration:

```bash
jupyter notebook
```
