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
    conn = None
    tables = None

    if access_db:
        logger.info('Using Access DB: %s', access_db)
        conn = connect_access(access_db)
    elif sql_server:
        logger.info('Using SQL Server connection')
        conn = connect_sql_server(**sql_server)
    else:
        logger.info('No DB provided, using CSV fallback at %s', csv_fallback or 'data/raw')

    tables = extract_all_tables(conn if conn is not None else None, csv_fallback_dir=csv_fallback or 'data/raw')

    sales_clean = build_sales_table(tables)

    # Save raw copies (optional)
    out_raw_dir = Path('data/raw')
    out_raw_dir.mkdir(parents=True, exist_ok=True)

    # Save the cleaned sales
    save_clean(sales_clean, out_path='data/clean/sales_clean.csv')
    logger.info('ETL finished successfully')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Northwind ETL')
    parser.add_argument('--access', help='Path to Access DB (.accdb or .mdb)')
    parser.add_argument('--csv-fallback', help='Path to CSV fallback folder', default='data/raw')
    parser.add_argument('--sql-host', help='SQL Server host')
    parser.add_argument('--sql-user', help='SQL Server user')
    parser.add_argument('--sql-pass', help='SQL Server password')
    parser.add_argument('--sql-db', help='SQL Server database')
    args = parser.parse_args()

    sql_params = None
    if args.sql_host and args.sql_user and args.sql_pass and args.sql_db:
        sql_params = dict(user=args.sql_user, password=args.sql_pass, host=args.sql_host, database=args.sql_db)

    run_etl(access_db=args.access, csv_fallback=args.csv_fallback, sql_server=sql_params)
