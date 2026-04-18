"""
Load dimension tables from CSV files into PostgreSQL.
Dimensions are reference data that changes slowly.
"""

import pandas as pd
import psycopg


DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'ecommerce',
    'user': 'pipeline',
    'password': 'pipeline123'
}


def get_connection():
    """Create database connection."""
    return psycopg.connect(**DB_CONFIG)


def load_customers(conn, filepath):
    """Load customers dimension."""
    print("Loading customers...")
    
    df = pd.read_csv(filepath)
    df = df.drop_duplicates(subset=['customer_id'])
    
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE fact_orders, dim_customers CASCADE")
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO dim_customers 
            (customer_id, customer_unique_id, customer_zip_code, customer_city, customer_state)
            VALUES (%s, %s, %s, %s, %s)
        """, (row.customer_id, row.customer_unique_id, str(row.customer_zip_code_prefix),
              row.customer_city, row.customer_state))
    
    conn.commit()
    print(f"  Loaded {len(df)} customers")


def load_products(conn, filepath):
    """Load products dimension."""
    print("Loading products...")
    
    df = pd.read_csv(filepath)
    df = df.drop_duplicates(subset=['product_id'])
    df = df.fillna({'product_category_name': 'unknown', 'product_weight_g': 0,
                    'product_length_cm': 0, 'product_height_cm': 0, 'product_width_cm': 0})
    
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE fact_order_items, dim_products CASCADE")
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO dim_products
            (product_id, product_category, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (row.product_id, row.product_category_name, int(row.product_weight_g),
              int(row.product_length_cm), int(row.product_height_cm), int(row.product_width_cm)))
    
    conn.commit()
    print(f"  Loaded {len(df)} products")


def load_sellers(conn, filepath):
    """Load sellers dimension."""
    print("Loading sellers...")
    
    df = pd.read_csv(filepath)
    df = df.drop_duplicates(subset=['seller_id'])
    
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE dim_sellers CASCADE")
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO dim_sellers
            (seller_id, seller_zip_code, seller_city, seller_state)
            VALUES (%s, %s, %s, %s)
        """, (row.seller_id, str(row.seller_zip_code_prefix), row.seller_city, row.seller_state))
    
    conn.commit()
    print(f"  Loaded {len(df)} sellers")


def main():
    print("Loading dimension tables...\n")
    
    conn = get_connection()
    
    try:
        load_customers(conn, 'data/raw/olist_customers_dataset.csv')
        load_products(conn, 'data/raw/olist_products_dataset.csv')
        load_sellers(conn, 'data/raw/olist_sellers_dataset.csv')
        print("\nAll dimensions loaded!")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
