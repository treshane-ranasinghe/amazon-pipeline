import pandas as pd
from sqlalchemy import create_engine

# ================================
# Database Configuration
# ================================
DB_CONFIG = {
    "user": "treshaneranasinghe",
    "password": "Dahami224466",
    "host": "localhost",
    "port": "5432",
    "database": "amazon"
}

# Create SQLAlchemy engine
def get_engine():
    db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(db_url)

# ================================
# Step 1: Extract
# ================================
def extract_csv(file_path):
    return pd.read_csv(file_path)

# ================================
# Step 2: Transform
# ================================

def clean_sales_data(df):
    df = df[[
        "index", "Order ID", "Date", "SKU", "ASIN", "Qty", "Amount", "B2B", "currency", "Sales Channel ", "fulfilled-by"
    ]].copy()
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    df["Sales Channel "] = df["Sales Channel "].str.strip()
    return df.dropna(subset=["Order ID", "SKU", "Date"])

def clean_product_catalog(df):
    df = df[[
        "index", "SKU", "Style", "Category", "Size", "ASIN"
    ]].drop_duplicates().copy()
    return df.dropna(subset=["SKU", "ASIN"])

def clean_fulfilment_data(df):
    df = df[[
        "index", "Order ID", "Date", "Status", "Fulfilment", "Courier Status", "ship-service-level",
        "ship-city", "ship-state", "ship-postal-code", "ship-country"
    ]].copy()
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    return df.dropna(subset=["Order ID", "Date", "Status"])

# ================================
# Step 3: Load to PostgreSQL
# ================================

def load_to_postgresql(df, table_name, if_exists="replace"):
    engine = get_engine()
    df.to_sql(table_name, engine, index=False, if_exists=if_exists, method="multi")
    print(f"✅ Loaded to PostgreSQL table: {table_name}")

# ================================
# Main ETL Pipeline
# ================================

def run_etl_pipeline():
    # --- Extract ---
    sales_raw = extract_csv("sales_data.csv")
    catalog_raw = extract_csv("product_catalog.csv")
    fulfilment_raw = extract_csv("fulfilment_data.csv")

    # --- Transform ---
    sales_clean = clean_sales_data(sales_raw)
    catalog_clean = clean_product_catalog(catalog_raw)
    fulfilment_clean = clean_fulfilment_data(fulfilment_raw)

    # --- Load ---
    load_to_postgresql(sales_clean, "sales_data")
    load_to_postgresql(catalog_clean, "product_catalog")
    load_to_postgresql(fulfilment_clean, "fulfilment_data")

    print("\n🎉 ETL Pipeline completed successfully and data is in PostgreSQL!")

# Run the pipeline
if __name__ == "__main__":
    run_etl_pipeline()
