"""
Spark Streaming Consumer: Reads orders from Kafka and processes them.
This is production-grade streaming like you'd see in real data engineering.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


# Define the schema of our order data
ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])


def create_spark_session():
    """Create Spark session with Kafka support."""
    return (SparkSession
            .builder
            .appName("OrderStreamProcessor")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .getOrCreate())


def main():
    print("Starting Spark Streaming consumer...")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce noise
    
    # Read from Kafka
    raw_stream = (spark
                  .readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .option("subscribe", "orders")
                  .option("startingOffsets", "earliest")
                  .load())
    
    # Parse JSON from Kafka value
    orders_stream = (raw_stream
                     .selectExpr("CAST(value AS STRING) as json_str")
                     .select(from_json(col("json_str"), ORDER_SCHEMA).alias("order"))
                     .select("order.*"))
    
    # Write to console (for testing)
    query = (orders_stream
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .start())
    
    print("\nWaiting for orders... (Ctrl+C to stop)\n")
    query.awaitTermination()


if __name__ == '__main__':
    main()
