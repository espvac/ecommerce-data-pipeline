"""
Load fact tables from CSV files into PostgreSQL.
Facts are events/transactions that happen over time.
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
    return psycopg.connect(**DB_CONFIG)


def load_orders(conn, filepath):
    """Load orders fact table."""
    print("Loading orders...")
    
    df = pd.read_csv(filepath)
    
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE fact_orders CASCADE")
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO fact_orders 
            (order_id, customer_id, order_status, order_purchase_timestamp,
             order_approved_at, order_delivered_carrier_date, 
             order_delivered_customer_date, order_estimated_delivery_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row.order_id,
            row.customer_id,
            row.order_status,
            row.order_purchase_timestamp if pd.notna(row.order_purchase_timestamp) else None,
            row.order_approved_at if pd.notna(row.order_approved_at) else None,
            row.order_delivered_carrier_date if pd.notna(row.order_delivered_carrier_date) else None,
            row.order_delivered_customer_date if pd.notna(row.order_delivered_customer_date) else None,
            row.order_estimated_delivery_date if pd.notna(row.order_estimated_delivery_date) else None
        ))
    
    conn.commit()
    print(f"  Loaded {len(df)} orders")


def load_order_items(conn, filepath):
    """Load order items fact table."""
    print("Loading order items...")
    
    df = pd.read_csv(filepath)
    
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE fact_order_items")
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO fact_order_items
            (order_id, order_item_id, product_id, seller_id, price, freight_value)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row.order_id,
            row.order_item_id,
            row.product_id,
            row.seller_id,
            row.price,
            row.freight_value
        ))
    
    conn.commit()
    print(f"  Loaded {len(df)} order items")


def main():
    print("Loading fact tables...\n")
    
    conn = get_connection()
    
    try:
        load_orders(conn, 'data/raw/olist_orders_dataset.csv')
        load_order_items(conn, 'data/raw/olist_order_items_dataset.csv')
        print("\nAll facts loaded!")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
