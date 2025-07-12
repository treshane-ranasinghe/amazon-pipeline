"""

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL



DATABASE_CONFIG = {
    "drivername": "postgresql",
    "username": "treshaneranasinghe",
    "password": "Dahami224466",
    "host": "localhost",
    "port": "5432",
    "database": "amazon"
}

PRODUCT_CATALOG_FILE = "product_catalog.csv"


def extract_csv(filepath):
    return pd.read_csv(filepath)


def clean_product_catalog(df):
    if "index" in df.columns:
        df = df.drop(columns=["index"])  # Drop 'index' if present

    df = df.drop_duplicates()
    df["SKU"] = df["SKU"].str.strip()
    df["Style"] = df["Style"].str.strip()
    df["Category"] = df["Category"].str.strip()
    df["Size"] = df["Size"].astype(str).str.strip()
    return df.dropna(subset=["SKU", "ASIN"])


def load_to_postgresql(df, table_name, if_exists="replace"):
    
    url = URL.create(**DATABASE_CONFIG)
    engine = create_engine(url)
    with engine.begin() as connection:
        df.to_sql(table_name, con=connection, index=False, if_exists=if_exists)
        print(f"✔ Loaded '{table_name}' into PostgreSQL.")


def run_product_catalog_etl():
    # Extract
    catalog_df = extract_csv(PRODUCT_CATALOG_FILE)

    # Transform
    catalog_df_clean = clean_product_catalog(catalog_df)

    # Fix column names to lowercase to match PostgreSQL table
    catalog_df_clean.columns = [col.lower() for col in catalog_df_clean.columns]

    # Load
    load_to_postgresql(catalog_df_clean, "product_catalog", if_exists="append")

    print("🎉 Product catalog ETL completed successfully!")


if __name__ == "__main__":
    run_product_catalog_etl()


"""

