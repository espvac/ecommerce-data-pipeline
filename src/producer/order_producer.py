"""
Order Producer: Reads orders from CSV and sends them to Kafka.
Simulates real-time order stream by replaying historical data.
"""

import json
import time
from kafka import KafkaProducer
import pandas as pd


def create_producer():
    """Create and return a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )


def load_orders(filepath):
    """Load orders from CSV file."""
    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} orders")
    print(f"Date range: {df['order_purchase_timestamp'].min()} to {df['order_purchase_timestamp'].max()}")
    return df


def send_orders(producer, orders_df, topic='orders', delay=0.1):
    """
    Send orders to Kafka topic.
    
    Args:
        producer: KafkaProducer instance
        orders_df: DataFrame containing orders
        topic: Kafka topic name
        delay: Seconds between messages (simulates real-time)
    """
    print(f"\nSending orders to topic '{topic}'...")
    
    for index, row in orders_df.iterrows():
        # Convert row to dictionary
        order = row.to_dict()
        
        # Send to Kafka
        producer.send(topic, value=order)
        
        # Progress update every 100 orders
        if (index + 1) % 100 == 0:
            print(f"Sent {index + 1} orders...")
        
        # Small delay to simulate real-time stream
        time.sleep(delay)
    
    # Ensure all messages are sent
    producer.flush()
    print(f"\nDone! Sent {len(orders_df)} orders to Kafka.")


def main():
    # Configuration
    csv_path = 'data/raw/olist_orders_dataset.csv'
    
    # Load data
    orders = load_orders(csv_path)
    
    # Show sample
    print("\nSample order:")
    print(orders.iloc[0].to_dict())
    
    # Create producer and send
    producer = create_producer()
    
    # Send first 50 orders as a test (remove limit for full dataset)
    send_orders(producer, orders.head(50), delay=0.05)


if __name__ == '__main__':
    main()
