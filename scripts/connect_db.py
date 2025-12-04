# --- FILE: connect_db.py
"""
connect_db.py
Provides database connection helpers.
Supports:
 - Microsoft Access via pyodbc (Windows / ODBC driver)
 - SQL Server via sqlalchemy
 - Fallback: read from CSV files in /data/raw when DB is not available

Configure by setting environment variables or passing arguments.
"""
import os
import logging
from pathlib import Path

try:
    import pyodbc
except Exception:
    pyodbc = None

try:
    from sqlalchemy import create_engine
except Exception:
    create_engine = None

logger = logging.getLogger(__name__)


def connect_access(db_path: str):
    """Return a pyodbc connection to an Access .accdb/.mdb file.

    db_path: path to .accdb or .mdb file
    Requires an ODBC driver for Access installed on the system.
    """
    if pyodbc is None:
        raise ImportError("pyodbc is required for Access connections. Install it with pip.")

    db_path = str(Path(db_path).expanduser())
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Access DB not found: {db_path}")

    driver = os.environ.get("ACCESS_ODBC_DRIVER", "{Microsoft Access Driver (*.mdb, *.accdb)}")
    conn_str = f"Driver={driver};DBQ={db_path};"
    logger.info("Connecting to Access DB: %s", db_path)
    return pyodbc.connect(conn_str)


def connect_sql_server(user: str, password: str, host: str, database: str, driver=None):
    """Return a SQLAlchemy engine for Microsoft SQL Server.
    Example connection string for pyodbc: mssql+pyodbc://user:pwd@host/DB?driver=Driver
    """
    if create_engine is None:
        raise ImportError("sqlalchemy is required for SQL Server connections. Install it with pip.")

    driver = driver or "ODBC Driver 17 for SQL Server"
    conn_url = f"mssql+pyodbc://{user}:{password}@{host}/{database}?driver={driver.replace(' ', '+')}"
    logger.info("Creating SQL Server engine to %s@%s/%s", user, host, database)
    return create_engine(conn_url)


def datasource_from_csv(data_dir: str = "data/raw"):
    """Utility fallback: if DB isn't available, read CSVs from data/raw.
    Returns a dict of DataFrame readers keyed by table name.
    """
    import pandas as pd

    data_dir = Path(data_dir)
    tables = {}
    for fname in data_dir.glob("*.csv"):
        key = fname.stem
        try:
            tables[key] = pd.read_csv(fname)
            logger.info("Loaded CSV fallback: %s", fname)
        except Exception as e:
            logger.warning("Failed to read %s: %s", fname, e)
    return tables
