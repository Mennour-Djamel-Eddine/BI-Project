# Northwind BI Project

This project implements a complete BI solution based on the Northwind database. It implements a **Hybrid Architecture** retrieving data from both **SQL Server** and **Microsoft Access**, with a comprehensive ETL process in Python and an analytical dashboard using Streamlit.

## Project Structure

- `data/`: Contains raw data (CSV/Access) and cleaned data.
- `scripts/`: Python scripts for ETL and Visualization.
    - `etl.py`: Main ETL script. Orchestrates connections to SQL Server and Access.
    - `extract.py`: Handles hybrid extraction logic (splitting tables between sources).
    - `transform.py`: Cleans and normalizes data.
    - `dashboard.py`: Streamlit dashboard.
- `scriptNorthwind.txt`: SQL script to initialize the SQL Server database.

## Setup

1. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
   *Note: On Windows, ensure you have the correct ODBC drivers installed for SQL Server and Access (`ACE.OLEDB`).*

2. **Data Sources:**
   - **SQL Server:** Run `scriptNorthwind.txt` to populate your SQL Server instance.
   - **Access:** Place your `Northwind.accdb` in `data/raw/` (or specify path).

## Running the Project

### 1. Run ETL Process (Hybrid Mode)
To extract from both databases and merge them:

```bash
python scripts/etl.py --sql-host localhost --sql-user sa --sql-pass yourPassword --access "data/raw/Northwind 2012.accdb"
```

*CSV Fallback Mode (for testing without DBs):*
```bash
python scripts/etl.py --csv-fallback data/raw
```

### 2. Launch Dashboard
To view the analytical dashboard:

```bash
streamlit run scripts/dashboard.py
```

## Hybrid Extraction Logic
The system is configured (in `scripts/extract.py`) to split tables as follows:
- **SQL Server:** Orders, OrderDetails, Products, Categories
- **Access:** Customers, Employees, Shippers, Suppliers
*You can modify `source_map` in `scripts/extract.py` to change this distribution.*
