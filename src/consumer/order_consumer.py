"""
Order Consumer: Reads orders from Kafka and processes them.
Simple example to understand consumer concepts before moving to Spark.
"""

import json
from kafka import KafkaConsumer


def create_consumer(topic='orders'):
    """Create and return a Kafka consumer instance."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',  # Start from beginning if no offset saved
        group_id='order-processor'     # Consumer group name
    )


def process_order(order):
    """
    Process a single order. This is where your business logic goes.
    For now, we just print summary info.
    """
    order_id = order.get('order_id', 'unknown')[:8]  # First 8 chars
    status = order.get('order_status', 'unknown')
    timestamp = order.get('order_purchase_timestamp', 'unknown')
    
    print(f"Order {order_id}... | Status: {status:12} | Time: {timestamp}")


def main():
    print("Starting consumer... waiting for orders.\n")
    print("-" * 70)
    
    consumer = create_consumer()
    orders_processed = 0
    
    try:
        for message in consumer:
            order = message.value
            process_order(order)
            orders_processed += 1
            
    except KeyboardInterrupt:
        print(f"\n\nStopped. Processed {orders_processed} orders.")
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
