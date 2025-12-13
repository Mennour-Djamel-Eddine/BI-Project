# --- FILE: etl.py
"""
etl.py
Top-level script to run the ETL end-to-end.
Usage examples:
    python etl.py --access db/Northwind.accdb
    python etl.py --csv-fallback data/raw
"""

import argparse
import logging
from pathlib import Path

from connect_db import connect_access, datasource_from_csv, connect_sql_server
from extract import extract_all_tables
from transform import build_sales_table, save_clean

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def run_etl(access_db: str = None, csv_fallback: str = None, sql_server: dict = None):
    sql_conn = None
    access_conn = None

    # Try connecting to SQL Server if params provided
    if sql_server:
        logger.info('Connecting to SQL Server...')
        try:
            sql_conn = connect_sql_server(**sql_server)
        except Exception as e:
            logger.error("SQL Server connection failed: %s", e)

    # Try connecting to Access if path provided
    if access_db:
        logger.info('Connecting to Access DB: %s', access_db)
        try:
            access_conn = connect_access(access_db)
        except Exception as e:
            logger.error("Access connection failed: %s", e)

    if not sql_conn and not access_conn:
        logger.info('No active DB connections. Using CSV fallback at %s', csv_fallback or 'data/raw')

    # Extract with hybrid support
    tables = extract_all_tables(sql_conn=sql_conn, access_conn=access_conn, csv_fallback_dir=csv_fallback or 'data/raw')

    if not tables:
        logger.error("No tables extracted. Exiting.")
        return

    try:
        sales_clean = build_sales_table(tables)
        
        # Save raw copies (optional) - skipped for brevity in hybrid mode
        
        # Save the cleaned sales
        save_clean(sales_clean, out_path='data/clean/sales_clean.csv')
        logger.info('ETL finished successfully')
    except Exception as e:
        logger.error("Transformation failed: %s", e)
        raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Northwind ETL')
    parser.add_argument('--access', help='Path to Access DB (.accdb or .mdb)')
    parser.add_argument('--csv-fallback', help='Path to CSV fallback folder', default='data/raw')
    parser.add_argument('--sql-host', help='SQL Server host')
    parser.add_argument('--sql-user', help='SQL Server user')
    parser.add_argument('--sql-pass', help='SQL Server password')
    parser.add_argument('--sql-db', help='SQL Server database', default='Northwind')
    args = parser.parse_args()

    sql_params = None
    if args.sql_host: # minimalist check, user might use windows auth
        sql_params = dict(
            host=args.sql_host, 
            database=args.sql_db,
            user=args.sql_user if args.sql_user else '',
            password=args.sql_pass if args.sql_pass else ''
        )

    run_etl(access_db=args.access, csv_fallback=args.csv_fallback, sql_server=sql_params)

