"""
Streaming job that reads from Kafka instead of files.
Demonstrates Kafka integration with Spark Structured Streaming.
"""

import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    min as _min,
    max as _max,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

from src.common.schema import CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA
from src.batch.io_utils import load_config, get_absolute_path, read_dimension_table
from src.stream.stream_transformations import (
    clean_streaming_data,
    derive_streaming_columns,
    enrich_with_dimensions_streaming,
    aggregate_windowed_revenue,
    aggregate_product_totals,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent


def create_kafka_spark_session(config):
    """Create SparkSession with Kafka support."""
    spark_config = config["spark"]

    spark = (
        SparkSession.builder.appName(f"{spark_config['app_name']} - Kafka Streaming")
        .master(spark_config["master"])
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "false")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(spark_config["log_level"])

    return spark

ORDERS_KAFKA_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("order_timestamp", StringType(), nullable=True),  # String from JSON
        StructField("quantity", IntegerType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("status", StringType(), nullable=True),
    ]
)


class KafkaStreamingMetrics:
    """Helper for tracking Kafka streaming metrics."""

    def __init__(self, query_name):
        self.query_name = query_name
        self.batch_count = 0

    def log_batch(self, batch_df, batch_id):
        """Log metrics for current micro-batch."""
        self.batch_count += 1
        row_count = batch_df.count()

        print(f"\n[{self.query_name}] Batch #{batch_id}")
        print(f"  Total batches processed: {self.batch_count}")
        print(f"  Rows in this batch: {row_count}")

        if row_count > 0 and "order_timestamp" in batch_df.columns:
            stats = batch_df.agg(
                _min("order_timestamp").alias("min_time"),
                _max("order_timestamp").alias("max_time"),
            ).collect()[0]

            print(f"  Event time range: {stats['min_time']} to {stats['max_time']}")


def main():
    """Main Kafka streaming pipeline."""

    print("=" * 70)
    print("KAFKA STREAMING ETL JOB STARTED")
    print(f"Timestamp: {datetime.now()}")
    print("=" * 70)

    start_time = time.time()

    # Load config
    print("\n[1/6] Loading configuration...")
    config = load_config()
    kafka_config = config.get("kafka", {})
    stream_config = config["stream"]

    # Create Spark session with Kafka support
    print("[2/6] Creating Spark session with Kafka support...")
    spark = create_kafka_spark_session(config)
    print(f"Spark version: {spark.version}")

    # Load dimension tables
    print("\n[3/6] Loading dimension tables...")
    customers_path = PROJECT_ROOT / config["dimensions"]["customers_path"]
    products_path = PROJECT_ROOT / config["dimensions"]["products_path"]

    customers_df = read_dimension_table(spark, customers_path, CUSTOMERS_SCHEMA)
    products_df = read_dimension_table(spark, products_path, PRODUCTS_SCHEMA)
    print(f"  ✓ Customers: {customers_df.count()} records")
    print(f"  ✓ Products: {products_df.count()} records")

    # Read from Kafka
    print("\n[4/6] Setting up Kafka streaming source...")

    bootstrap_servers = kafka_config.get("bootstrap_servers", "localhost:9092")
    topic = kafka_config.get("topic", "ecommerce-orders")

    print(f"  Bootstrap servers: {bootstrap_servers}")
    print(f"  Topic: {topic}")
    print(f"  Starting offset: earliest")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # Read from beginning
        .option("failOnDataLoss", "false")
        .load()
    )

    print("  ✓ Kafka source configured")

    # Parse JSON from Kafka value
    print("\n[5/6] Parsing JSON and applying transformations...")

    orders_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), ORDERS_KAFKA_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("order_timestamp", col("order_timestamp").cast(TimestampType()))
    )

    print("  ✓ JSON parsed from Kafka messages")

    # Apply same transformations as file-based streaming
    watermark_delay = stream_config.get("watermark_delay", "30 minutes")

    cleaned_df = clean_streaming_data(orders_df, watermark_delay)
    print(f"  ✓ Data cleaning applied (watermark: {watermark_delay})")

    enriched_df = derive_streaming_columns(cleaned_df)
    print("  ✓ Derived columns added")

    enriched_df = enrich_with_dimensions_streaming(
        enriched_df, customers_df, products_df
    )
    print("  ✓ Joined with dimensions")

    # Aggregations
    print("\n[6/6] Setting up aggregations...")
    window_duration = stream_config.get("window_duration", "10 minutes")

    windowed_revenue = aggregate_windowed_revenue(enriched_df, window_duration)
    product_totals = aggregate_product_totals(enriched_df)

    print(f"  ✓ Windowed aggregation: {window_duration} windows")
    print("  ✓ Product totals aggregation")

    # Output sinks
    print("\n[7/7] Starting streaming queries...")

    output_path = get_absolute_path(stream_config["output_path"])
    checkpoint_path = get_absolute_path(stream_config["checkpoint_path"])

    # Console sink with metrics
    console_metrics = KafkaStreamingMetrics("Kafka-Console")

    def console_handler(batch_df, batch_id):
        console_metrics.log_batch(batch_df, batch_id)
        if batch_df.count() > 0:
            batch_df.show(20, truncate=False)

    console_query = (
        windowed_revenue.writeStream.outputMode("update")
        .trigger(processingTime="5 seconds")
        .foreachBatch(console_handler)
        .start()
    )

    print("  ✓ Console sink started")

    # File sink
    file_metrics = KafkaStreamingMetrics("Kafka-File")

    def file_handler(batch_df, batch_id):
        file_metrics.log_batch(batch_df, batch_id)
        if batch_df.count() > 0:
            batch_df.write.mode("append").partitionBy("product_id").parquet(
                f"{output_path}/kafka_windowed_revenue/"
            )

    file_query = (
        windowed_revenue.writeStream.outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", f"{checkpoint_path}/kafka_windowed_revenue/")
        .foreachBatch(file_handler)
        .start()
    )

    print("  ✓ File sink started")

    # Product totals to console
    totals_metrics = KafkaStreamingMetrics("Kafka-ProductTotals")

    def totals_handler(batch_df, batch_id):
        totals_metrics.log_batch(batch_df, batch_id)
        if batch_df.count() > 0:
            print("\n Top 10 Products by Revenue:")
            batch_df.orderBy(col("total_revenue").desc()).show(10, truncate=False)

    totals_query = (
        product_totals.writeStream.outputMode("complete")
        .trigger(processingTime="10 seconds")
        .foreachBatch(totals_handler)
        .start()
    )

    print("  ✓ Product totals sink started")

    print("\n" + "=" * 70)
    print("KAFKA STREAMING QUERIES RUNNING")
    print("=" * 70)
    print("Waiting for Kafka messages...")
    print("\nPress Ctrl+C to stop")
    print("=" * 70)

    try:
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        print("\n\n  Received interrupt. Stopping queries...")

    finally:
        # Stop all queries
        for query in spark.streams.active:
            print(f"Stopping query: {query.name or query.id}")
            query.stop()

        end_time = time.time()
        duration = end_time - start_time

        print("\n" + "=" * 70)
        print("KAFKA STREAMING JOB COMPLETED")
        print("=" * 70)
        print(f"Total runtime: {duration:.2f} seconds")
        print("=" * 70)

        spark.stop()


if __name__ == "__main__":
    main()
