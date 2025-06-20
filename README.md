Finance Repository
This repository contains financial data analysis tools and notebooks. The primary tool currently available is the Dynamic Range Average calculation for financial time series, such as S&P 500 data.

Contents
Dynamic_Range_Average.ipynb
Jupyter notebook to calculate moving averages and analyze conditions based on financial data (e.g., S&P 500).
Dynamic Range Average Notebook
This notebook allows users to:

Upload or load a financial dataset (Excel, with columns like date, open, etc.).
Set a dynamic range (number of months) for calculating moving averages.
Filter and analyze periods where the moving average is greater than the opening price.
(Optional) Save the processed data to a new Excel file.
Requirements
Python 3
pandas
openpyxl (for Excel file support)
Usage
Open the notebook in Jupyter or Google Colab.
Upload your Excel file (default path: /content/sample_data/sp500_raw_data.xlsx, sheet name: Sheet1).
Specify the dynamic range (number of months) when prompted.
The notebook performs the calculation and displays filtered results.
Example
Python
dynamic_range = int(input('geriye donuk kac ayin ortalmasi goremek istiyorsun?'))
Output
The notebook prints a filtered DataFrame where the moving average is greater than the opening price.

License
This repository is for educational and analytical purposes.

