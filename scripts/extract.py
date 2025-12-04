# --- FILE: extract.py
"""
extract.py
Functions to extract tables from the database or CSV fallback.
"""
from connect_db import datasource_from_csv
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def extract_table_from_conn(conn, table_name: str, query: str = None):
    """Extract a table using a DB connection (pyodbc or sqlalchemy engine).
    If query is provided, it will use it; otherwise it selects * from table_name.
    Returns a pandas.DataFrame.
    """
    sql = query or f"SELECT * FROM {table_name}"
    try:
        # If conn is a SQLAlchemy engine
        if hasattr(conn, "execute") and not hasattr(conn, "cursor"):
            df = pd.read_sql(sql, conn)
        else:
            # pyodbc connection has .cursor(); use pandas read_sql_query
            df = pd.read_sql_query(sql, conn)
        logger.info("Extracted %s: %d rows", table_name, len(df))
        return df
    except Exception as e:
        logger.exception("Failed to extract table %s: %s", table_name, e)
        raise


def extract_all_tables(conn=None, csv_fallback_dir: str = "data/raw"):
    """Extract core Northwind tables. If conn is None, use CSV fallback.
    Returns a dict of DataFrames: orders, order_details, products, customers, employees
    """
    tables = ["Orders", "OrderDetails", "Products", "Customers", "Employees", "Categories"]
    result = {}

    if conn is None:
        # CSV fallback
        csvs = datasource_from_csv(csv_fallback_dir)  # imported from connect_db locally
        # try case-insensitive matching
        for t in tables:
            key = t.lower()
            match = None
            for k in csvs.keys():
                if k.lower().startswith(key):
                    match = k
                    break
            if match:
                result[t] = csvs[match]
                logger.info("Fallback: loaded %s from %s", t, match)
            else:
                logger.warning("Fallback: CSV for %s not found", t)
        return result

    # Use DB connection
    for t in tables:
        result[t] = extract_table_from_conn(conn, t)
    return result
