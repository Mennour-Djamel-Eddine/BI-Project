import pyodbc

class DatabaseManager:
    def __init__(self):
        self.sql_conn = None
        self.access_conn = None

    def connect_sql(self):
        """Connect to SQL Server Northwind"""
        self.sql_conn = pyodbc.connect(
            "Driver={SQL Server};"
            "Server=localhost\\SQLEXPRESS;"
            "Database=Northwind;"
            "Trusted_Connection=yes;"
        )
        print("Connected to SQL Server")

    def connect_access(self):
        """Connect to Access Northwind"""
        self.access_conn = pyodbc.connect(
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
            r"DBQ=..\\data\\raw\\Northwind 2012.accdb;"
        )
        print("Connected to Access")

    def query_sql(self, query):
        cursor = self.sql_conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def query_access(self, query):
        cursor = self.access_conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()


db = DatabaseManager()
db.connect_access()