import pandas as pd # for reading csv files and transforming data
from sqlalchemy import create_engine # for connecting to postgresql


# Database Configurations

DB_CONFIG = {
    "user": "treshaneranasinghe",
    "password": "Dahami224466",
    "host": "localhost",
    "port": "5432",
    "database": "amazon"
}

# Create SQLAlchemy connection url
def get_engine():
    db_url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(db_url)


#Extract

def extract_csv(file_path):
    return pd.read_csv(file_path)


#Transform


def standardize_column_names(df):
    df.columns = (
        df.columns.str.strip() # Removes any leading/trailing spaces in column name
        .str.lower()   # Converts all column names to lowercase
        .str.replace(" ", "_") # Replaces spaces with underscores
        .str.replace("-", "_") # Replaces dashes (-) with underscores
    )
    return df

# cleaningo of the sales data

def clean_sales_data(df):
    df = standardize_column_names(df)

    df = df[[
        "order_id", "date", "sku", "asin", "qty", "amount", "b2b", "currency",
        "sales_channel", "fulfilled_by"
    ]].copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["sales_channel"] = df["sales_channel"].str.strip()

    # Convert b2b to string before filtering
    df["b2b"] = df["b2b"].astype(str).str.upper()
    df = df[df["b2b"] != "YES"]

    # Derived metrics
    df["total_revenue"] = df["qty"] * df["amount"]
    df["is_high_value"] = df["total_revenue"] > 5000

    return df.dropna(subset=["order_id", "sku", "date"])


# cleaning of product data

def clean_product_catalog(df):
    df = standardize_column_names(df)

    df = df[[
        "sku", "style", "category", "size", "asin"
    ]].drop_duplicates().copy()

    df = df[df["category"].notna()]  # Business rule remove rows with missing category
    return df.dropna(subset=["sku", "asin"])

# cleaning of fulfilment data

def clean_fulfilment_data(df):
    df = standardize_column_names(df)

    df = df[[
        "order_id", "date", "status", "fulfilment", "courier_status",
        "ship_service_level", "ship_city", "ship_state", "ship_postal_code", "ship_country"
    ]].copy()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Business rule: map status values to consistent format
    df["status"] = df["status"].str.lower().replace({
        "shipped": "Shipped",
        "delivered": "Delivered",
        "in transit": "In Transit"
    })

    return df.dropna(subset=["order_id", "date", "status"])

# Joins product info to each sales record using the sku
def enrich_sales_with_catalog(sales_df, catalog_df):
    return pd.merge(sales_df, catalog_df, on="sku", how="left")


#sends each data frame to the relavant postgre table

def load_to_postgresql(df, table_name, if_exists="replace"):
    engine = get_engine()
    df.to_sql(table_name, engine, index=False, if_exists=if_exists, method="multi")
    print(f" Loaded to PostgreSQL table: {table_name}")


# Main ETL Pipeline

def run_etl_pipeline():

    # Absolute file paths
    sales_path = "/Users/treshaneranasinghe/Documents/amazon-pipeline/sales_data.csv"
    catalog_path = "/Users/treshaneranasinghe/Documents/amazon-pipeline/product_catalog.csv"
    fulfilment_path = "/Users/treshaneranasinghe/Documents/amazon-pipeline/fulfilment_data.csv"

    # Extract using absolute paths
    sales_raw = extract_csv(sales_path)
    catalog_raw = extract_csv(catalog_path)
    fulfilment_raw = extract_csv(fulfilment_path)

    # Transform
    sales_clean = clean_sales_data(sales_raw)
    catalog_clean = clean_product_catalog(catalog_raw)
    fulfilment_clean = clean_fulfilment_data(fulfilment_raw)

    # Business Rule - enrich sales with catalog data
    sales_enriched = enrich_sales_with_catalog(sales_clean, catalog_clean)

    # Load
    load_to_postgresql(sales_enriched, "sales_data")
    load_to_postgresql(catalog_clean, "product_catalog")
    load_to_postgresql(fulfilment_clean, "fulfilment_data")

    print("\nETL Pipeline completed successfully and data is in PostgreSQL!")


# Run the pipeline
if __name__ == "__main__":
    run_etl_pipeline()
