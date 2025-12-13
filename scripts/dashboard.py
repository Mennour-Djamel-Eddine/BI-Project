import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set page title and layout
st.set_page_config(page_title="Northwind BI Dashboard", layout="wide")

@st.cache_data
def load_data():
    """Load cleaned sales data from CSV."""
    df = pd.read_csv('data/clean/sales_clean.csv')
    df['OrderDate'] = pd.to_datetime(df['OrderDate'])
    return df

st.title("Northwind Analytical Dashboard")
st.markdown("### Key Business Indicators & Trends")

try:
    df = load_data()
except FileNotFoundError:
    st.error("Data file not found. Please run the ETL script first: `python scripts/etl.py --csv-fallback data/raw`")
    st.stop()

# --- Sidebar Filters ---
st.sidebar.header("Filters")

# Year Filter
all_years = sorted(df['Year'].dropna().unique().astype(int))
selected_year = st.sidebar.selectbox("Select Year", ["All"] + list(all_years))

if selected_year != "All":
    df_filtered = df[df['Year'] == selected_year]
else:
    df_filtered = df

# --- KPIs ---
total_sales = df_filtered['TotalPrice'].sum()
total_orders = df_filtered['OrderID'].nunique()
total_customers = df_filtered['CustomerID'].nunique()
avg_order_value = total_sales / total_orders if total_orders > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue", f"${total_sales:,.2f}")
col2.metric("Total Orders", total_orders)
col3.metric("Total Customers", total_customers)
col4.metric("Avg Order Value", f"${avg_order_value:,.2f}")

st.markdown("---")

# --- Visualizations ---

col_left, col_right = st.columns(2)

# 1. Sales Over Time (Monthly)
with col_left:
    st.subheader("Sales Trend (Monthly)")
    if selected_year == "All":
        # Group by Year-Month
        monthly_sales = df_filtered.groupby(['Year', 'Month'])['TotalPrice'].sum().reset_index()
        monthly_sales['Date'] = pd.to_datetime(monthly_sales[['Year', 'Month']].assign(DAY=1))
        # Sort by date
        monthly_sales = monthly_sales.sort_values('Date')
        
        st.line_chart(monthly_sales.set_index('Date')['TotalPrice'])
    else:
        # Just month numbers
        monthly_sales = df_filtered.groupby('Month')['TotalPrice'].sum().reset_index()
        st.bar_chart(monthly_sales.set_index('Month')['TotalPrice'])

# 2. Sales by Category
with col_right:
    st.subheader("Sales by Category")
    cat_sales = df_filtered.groupby('CategoryName')['TotalPrice'].sum().sort_values(ascending=True)
    st.bar_chart(cat_sales)

col3, col4 = st.columns(2)

# 3. Top Products
with col3:
    st.subheader("Top 10 Products by Revenue")
    top_products = df_filtered.groupby('ProductName')['TotalPrice'].sum().nlargest(10).sort_values()
    
    # Use matplotlib for horizontal bar which looks nice
    fig, ax = plt.subplots(figsize=(8, 6))
    top_products.plot(kind='barh', ax=ax, color='#4CAF50')
    ax.set_xlabel("Revenue")
    ax.set_ylabel("")
    st.pyplot(fig)

# 4. Sales by Country
with col4:
    st.subheader("Sales by Country")
    if 'ShipCountry' in df_filtered.columns:
        country_sales = df_filtered.groupby('ShipCountry')['TotalPrice'].sum().sort_values(ascending=False).head(10)
        st.bar_chart(country_sales)
    else:
        st.info("ShipCountry data not available.")

# --- Raw Data View ---
with st.expander("View Raw Data"):
    st.dataframe(df_filtered.head(100))

