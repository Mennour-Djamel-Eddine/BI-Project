import logging
import pandas as pd
from pathlib import Path

# Try importing pyodbc
try:
    import pyodbc
except ImportError:
    pyodbc = None

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Manages connections to both SQL Server and Access databases.
    """
    def __init__(self):
        self.sql_conn = None
        self.access_conn = None

    def connect_sql(self, host, user, password, database, driver='{SQL Server}'):
        """Connect to SQL Server."""
        if not pyodbc:
            logger.error("pyodbc not installed. Cannot connect to SQL Server.")
            return None
        
        try:
            # Construct connection string
            # If using Windows Authentication, Trusted_Connection=yes; user/pass might be ignored
            if not user and not password:
                 conn_str = (
                    f"Driver={driver};"
                    f"Server={host};"
                    f"Database={database};"
                    f"Trusted_Connection=yes;"
                )
            else:
                conn_str = (
                    f"Driver={driver};"
                    f"Server={host};"
                    f"Database={database};"
                    f"UID={user};"
                    f"PWD={password};"
                )

            self.sql_conn = pyodbc.connect(conn_str)
            logger.info("Connected to SQL Server at %s", host)
            return self.sql_conn
        except Exception as e:
            logger.error("Failed to connect to SQL Server: %s", e)
            raise

    def connect_access(self, db_path, driver='{Microsoft Access Driver (*.mdb, *.accdb)}'):
        """Connect to Access Database."""
        if not pyodbc:
            logger.error("pyodbc not installed. Cannot connect to Access.")
            return None

        try:
            # Need raw string for path
            conn_str = fr"Driver={driver};DBQ={db_path};"
            self.access_conn = pyodbc.connect(conn_str)
            logger.info("Connected to Access DB at %s", db_path)
            return self.access_conn
        except Exception as e:
            logger.error("Failed to connect to Access DB: %s", e)
            raise

    def close_all(self):
        if self.sql_conn: self.sql_conn.close()
        if self.access_conn: self.access_conn.close()

def get_db_manager():
    return DatabaseManager()

def connect_access(db_path):
    mgr = DatabaseManager()
    return mgr.connect_access(db_path)

def connect_sql_server(host, user, password, database):
    mgr = DatabaseManager()
    return mgr.connect_sql(host, user, password, database)



def datasource_from_csv(directory: str):
    """
    Load all CSVs from a directory into a dictionary of DataFrames.
    Keys are the filenames without extension (e.g. 'orders').
    """
    d = Path(directory)
    results = {}
    if not d.exists():
        logger.warning("CSV directory %s does not exist", directory)
        return results

    logger.info("Loading CSVs from %s", d)
    for f in d.glob('*.csv'):
        try:
            # Try reading with default options
            df = pd.read_csv(f)
            results[f.stem] = df
            logger.info("Loaded %s: %d rows", f.name, len(df))
        except Exception as e:
            logger.error("Failed to read %s: %s", f.name, e)
    
    return results