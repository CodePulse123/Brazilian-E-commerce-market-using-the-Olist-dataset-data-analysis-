import pandas as pd

# 1. LOAD DATA
orders = pd.read_csv('olist_orders_dataset.csv')
items = pd.read_csv('olist_order_items_dataset.csv')
products = pd.read_csv('olist_products_dataset.csv')
customers = pd.read_csv('olist_customers_dataset.csv')
translation = pd.read_csv('product_category_name_translation.csv')

# 2. DATA CLEANING
# Convert all date columns to datetime objects
date_cols = ['order_purchase_timestamp', 'order_delivered_customer_date', 'order_estimated_delivery_date']
for col in date_cols:
    orders[col] = pd.to_datetime(orders[col])

# Translate product categories to English
products = pd.merge(products, translation, on='product_category_name', how='left')
products['category'] = products['product_category_name_english'].fillna('other')
products = products[['product_id', 'category']] # Keep only what we need

# 3. AGGREGATION (Crucial for Tableau)
# We group by order_id so that 1 row = 1 order, preventing double-counting revenue
items_clean = items.groupby('order_id').agg({
    'price': 'sum',
    'freight_value': 'sum',
    'product_id': 'first' # Identify the primary product in the order
}).reset_index()

# 4. FEATURE ENGINEERING
# Calculate total order value
items_clean['total_order_value'] = items_clean['price'] + items_clean['freight_value']

# Calculate delivery lead time (Actual vs Estimated)
orders['delivery_days_diff'] = (orders['order_delivered_customer_date'] - orders['order_estimated_delivery_date']).dt.days

# 5. MASTER JOIN
# Merging orders with their aggregated items, products, and customer locations
master = pd.merge(orders, items_clean, on='order_id', how='inner')
master = pd.merge(master, products, on='product_id', how='left')
master = pd.merge(master, customers[['customer_id', 'customer_city', 'customer_state']], on='customer_id', how='left')

# 6. FINAL FILTERING
# Keep only delivered orders for accurate sales reporting
master = master[master['order_status'] == 'delivered'].copy()

# 7. EXPORT
master.to_csv('brazil_ecommerce_master.csv', index=False)
print("Cleaning complete. File 'brazil_ecommerce_master.csv' is ready for Tableau!")