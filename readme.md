# Finance Data Analysis Tools

This repository contains Python tools and Jupyter notebooks for financial data analysis, with a focus on time series analysis of market data such as the S&P 500. The primary functionality includes dynamic range average calculations and technical analysis.

## 📊 Contents

- `dynamic_range_average.py` - Python script for calculating moving averages and analyzing market conditions
- `Dynamic_Range_Average.ipynb` - Jupyter notebook with interactive analysis and visualization
- `data/` - Directory containing sample financial datasets

## 🚀 Features

- Calculate moving averages for any specified time period
- Analyze market conditions based on price and moving average relationships
- Process Excel files with financial time series data
- Generate filtered results showing specific market conditions

## ⚙️ Requirements

- Python 3.8+
- pandas
- openpyxl (for Excel file support)
- Jupyter Notebook (for the interactive notebook)

## 🛠 Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/arslanmit/Finance.git
   cd Finance
   ```

2. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```
   (If you don't have a requirements.txt, install packages individually: `pip install pandas openpyxl jupyter`)

## 📋 Usage

### Using the Python Script

```bash
python dynamic_range_average.py
```

When prompted, enter the number of months for the moving average calculation.

### Using the Jupyter Notebook

1. Start Jupyter Notebook:
   ```bash
   jupyter notebook
   ```
2. Open `Dynamic_Range_Average.ipynb`
3. Follow the instructions in the notebook

## 📂 Data Format

Input Excel/CSV files should contain at least these columns:
- `date`: Date of the observation
- `open`: Opening price
- Other price columns (high, low, close) are optional

## 📊 Example

```python
# Example of calculating a 10-month moving average
dynamic_range = 10  # Number of months for the moving average

# The script will:
# 1. Calculate the moving average for the specified period
# 2. Identify periods where the price is below the moving average
# 3. Output the filtered results
```

## 📝 Output

The tool generates:
- Console output with filtered results
- Option to save processed data to a new Excel file
- Visualizations in the Jupyter notebook

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is for educational and analytical purposes.
