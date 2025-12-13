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



def extract_all_tables(sql_conn=None, access_conn=None, csv_fallback_dir: str = "data/raw"):
    """
    Extract tables based on a hybrid strategy (SQL Server + Access).
    If connections are missing, attempts to use CSV fallback.
    """
    # Define Source Mapping: Which table comes from where?
    # Adjust this mapping as per specific project requirements.
    source_map = {
        "Orders": "sql",
        "OrderDetails": "sql",
        "Products": "sql",
        "Categories": "sql",
        "Customers": "access",
        "Employees": "access",
        "Shippers": "access",
        "Suppliers": "access",
    }
    
    results = {}
    
    # Check if we are in fallback mode (no DB connections)
    if not sql_conn and not access_conn:
        logger.warning("No database connections provided. switch to CSV fallback.")
        csvs = datasource_from_csv(csv_fallback_dir)
        for t in source_map.keys():
            # Loose matching for CSVs
            key = t.lower()
            match = None
            for k in csvs.keys():
                if k.lower().startswith(key.replace(" ", "")): # Handle OrderDetails vs order_details
                    match = k
                    break
            if match:
                results[t] = csvs[match]
                logger.info("Loaded %s from CSV %s", t, match)
            else:
                logger.warning("CSV for %s not found", t)
        return results

    # Hybrid Extraction
    for table_name, source_type in source_map.items():
        try:
            df = None
            if source_type == "sql" and sql_conn:
                # SQL Server often expects exact casing or brackets depending on settings
                # But generic "SELECT * FROM Table" usually works
                query = f"SELECT * FROM [{table_name}]" if " " in table_name else f"SELECT * FROM {table_name}"
                if table_name == "OrderDetails": query = "SELECT * FROM [Order Details]" # Common SQL Server naming
                
                logger.info("Extracting %s from SQL Server...", table_name)
                df = extract_table_from_conn(sql_conn, table_name, query)
                
            elif source_type == "access" and access_conn:
                logger.info("Extracting %s from Access...", table_name)
                df = extract_table_from_conn(access_conn, table_name)
            
            if df is not None:
                results[table_name] = df
            else:
                logger.warning("Skipped %s (Source: %s, Conn Available: %s)", 
                               table_name, source_type, 
                               "Yes" if (source_type=="sql" and sql_conn) or (source_type=="access" and access_conn) else "No")
                
        except Exception as e:
            logger.error("Error extracting %s from %s: %s", table_name, source_type, e)

    return results

