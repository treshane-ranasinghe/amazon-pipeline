"""

import pandas as pd




# Load the full dataset
input_file = "Amazon Sale Report.csv"  # make sure this file is in the same folder
df = pd.read_csv(input_file)



# --- 1. Sales Data (Transactional) ---
sales_data = df[[
    "index", "Order ID", "Date", "SKU", "ASIN", "Qty", "Amount", "B2B", "currency", "Sales Channel ", "fulfilled-by"
]]
# Optional: Clean up date format
sales_data["Date"] = pd.to_datetime(sales_data["Date"], errors='coerce')

# --- 2. Product Catalog (Reference data) ---
product_catalog = df[[
    "index", "SKU", "Style", "Category", "Size", "ASIN"
]].drop_duplicates()

# --- 3. Fulfilment Data (Logistics/Delivery) ---
fulfilment_data = df[[
    "index", "Order ID", "Date", "Status", "Fulfilment", "Courier Status", "ship-service-level",
    "ship-city", "ship-state", "ship-postal-code", "ship-country"
]]
# Convert Date
fulfilment_data["Date"] = pd.to_datetime(fulfilment_data["Date"], errors='coerce')


sales_data.to_csv("sales_data.csv", index=False)
product_catalog.to_csv("product_catalog.csv", index=False)
fulfilment_data.to_csv("fulfilment_data.csv", index=False)

print(" ETL Complete! Data split into:")
print("   - sales_data.csv")
print("   - product_catalog.csv")
print("   - fulfilment_data.csv")

"""