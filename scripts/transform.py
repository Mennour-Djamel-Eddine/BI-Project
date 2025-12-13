# --- FILE: transform.py
"""
transform.py
Data cleaning and transformation routines for Northwind -> sales_clean
"""
from typing import Dict
import pandas as pd
import numpy as np
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names from camelCase (CSV) to PascalCase (Script expectation)."""
    mapper = {
        'orderID': 'OrderID',
        'customerID': 'CustomerID',
        'employeeID': 'EmployeeID',
        'productID': 'ProductID',
        'categoryID': 'CategoryID',
        'orderDate': 'OrderDate',
        'shippedDate': 'ShippedDate',
        'unitsInStock': 'UnitsInStock',
        'unitPrice': 'UnitPrice',
        'quantity': 'Quantity',
        'discount': 'Discount',
        'companyName': 'CompanyName',
        'categoryName': 'CategoryName',
        'productName': 'ProductName',
        'supplierID': 'SupplierID',
        'contactName': 'ContactName',
        'contactTitle': 'ContactTitle',
        'shipCountry': 'ShipCountry',
        # Add others as needed
    }
    # Only rename if column exists
    return df.rename(columns=mapper)

def clean_orders(df_orders: pd.DataFrame) -> pd.DataFrame:
    # Copy to avoid side-effects
    df = df_orders.copy()

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]
    
    # Fix casing if needed
    df = normalize_cols(df)

    # Convert dates
    for col in [c for c in df.columns if 'date' in c.lower()]:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        except Exception:
            logger.debug("Couldn't convert %s to datetime", col)

    # Drop rows without OrderID
    if 'OrderID' in df.columns:
        df = df[df['OrderID'].notna()]

    return df


def clean_order_details(df_od: pd.DataFrame) -> pd.DataFrame:
    df = df_od.copy()
    df.columns = [c.strip() for c in df.columns]
    df = normalize_cols(df)

    # Ensure numeric types
    for col in ['UnitPrice', 'Quantity', 'Discount']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Fill missing quantities with 1 (assumption)
    if 'Quantity' in df.columns:
        df['Quantity'] = df['Quantity'].fillna(1).astype(int)

    return df


def build_sales_table(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Required keys: Orders, OrderDetails, Products, Customers, Employees, Categories(optional)
    orders = tables.get('Orders')
    od = tables.get('OrderDetails')
    products = tables.get('Products')
    customers = tables.get('Customers')
    employees = tables.get('Employees')
    categories = tables.get('Categories', None)

    # Normalize optional tables too
    if products is not None: products = normalize_cols(products)
    if customers is not None: customers = normalize_cols(customers)
    if employees is not None: employees = normalize_cols(employees)
    if categories is not None: categories = normalize_cols(categories)

    if orders is None or od is None:
        raise ValueError('Orders and OrderDetails are required')

    orders = clean_orders(orders)
    od = clean_order_details(od)

    # Merge Orders + OrderDetails
    # In Northwind, OrderDetails may have OrderID, ProductID, UnitPrice, Quantity
    sales = od.merge(orders, how='left', left_on='OrderID', right_on='OrderID', suffixes=('_od','_ord'))

    # Merge Products
    if products is not None:
        sales = sales.merge(products, how='left', left_on='ProductID', right_on='ProductID', suffixes=('','_prod'))

    # Merge Categories
    if categories is not None and 'CategoryID' in sales.columns:
        sales = sales.merge(categories, how='left', left_on='CategoryID', right_on='CategoryID', suffixes=('','_cat'))

    # Merge Customers
    if customers is not None and 'CustomerID' in sales.columns:
        sales = sales.merge(customers, how='left', left_on='CustomerID', right_on='CustomerID', suffixes=('','_cust'))

    # Compute TotalPrice
    if 'UnitPrice' in sales.columns and 'Quantity' in sales.columns:
        # Note: if Discount exists, maybe use it? Northwind discount is usually 0.0 to 1.0 (percent)
        # Net = UnitPrice * Quantity * (1 - Discount)
        
        up = pd.to_numeric(sales['UnitPrice'], errors='coerce')
        qty = pd.to_numeric(sales['Quantity'], errors='coerce')
        
        if 'Discount' in sales.columns:
            disc = pd.to_numeric(sales['Discount'], errors='coerce').fillna(0)
            sales['TotalPrice'] = up * qty * (1 - disc)
        else:
            sales['TotalPrice'] = up * qty
    else:
        sales['TotalPrice'] = np.nan

    # Extract Year/Month
    # Try OrderDate or 'OrderDate' columns
    date_col = None
    for cand in ['OrderDate', 'OrderDate_ord', 'OrderDate_od']:
        if cand in sales.columns:
            date_col = cand
            break
    if date_col:
        sales[date_col] = pd.to_datetime(sales[date_col], errors='coerce')
        sales['Year'] = sales[date_col].dt.year
        sales['Month'] = sales[date_col].dt.month

    # Select a sensible subset of columns for the clean dataset
    # Added ShipCountry
    keep_cols = [c for c in ['OrderID','ProductID','ProductName','CategoryID','CategoryName','CustomerID','CompanyName','EmployeeID','OrderDate','ShippedDate','UnitPrice','Quantity','Discount','TotalPrice','Year','Month','ShipCountry'] if c in sales.columns]
    sales_clean = sales[keep_cols].copy()

    # Final cleaning: drop duplicates, drop rows without TotalPrice
    sales_clean = sales_clean.drop_duplicates()
    sales_clean = sales_clean[sales_clean['TotalPrice'].notna()]

    logger.info('Built sales_clean with %d rows', len(sales_clean))
    return sales_clean


def save_clean(df: pd.DataFrame, out_path: str = 'data/clean/sales_clean.csv'):
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)
    logger.info('Saved cleaned sales to %s', p)
