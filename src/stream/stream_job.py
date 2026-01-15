"""
Main streaming job for e-commerce orders processing.

Reads streaming data from files and processes in near-real-time.
"""

import time
from datetime import datetime
from pathlib import Path

from src.common.schema import ORDERS_SCHEMA, CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA
from src.batch.io_utils import load_config, get_absolute_path, read_dimension_table
from src.stream.stream_transformations import (
    clean_streaming_data,
    derive_streaming_columns,
    enrich_with_dimensions_streaming,
    aggregate_windowed_revenue,
    aggregate_product_totals,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as _min, max as _max, count as _count


def create_streaming_spark_session(config):
    """Create SparkSession configured for streaming."""
    spark_config = config["spark"]

    spark = (
        SparkSession.builder.appName(f"{spark_config['app_name']} - Streaming")
        .master(spark_config["master"])
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(spark_config["log_level"])

    return spark


class StreamingMetricsListener:
    """Helper class to track streaming metrics."""

    def __init__(self, query_name):
        self.query_name = query_name
        self.batch_count = 0

    def log_batch_metrics(self, batch_df, batch_id):
        """Log metrics for current micro-batch."""
        self.batch_count += 1

        row_count = batch_df.count()

        if "order_timestamp" in batch_df.columns:
            stats = batch_df.agg(
                _min("order_timestamp").alias("min_time"),
                _max("order_timestamp").alias("max_time"),
            ).collect()[0]

            min_time = stats["min_time"]
            max_time = stats["max_time"]

            print(
                f"\n[{self.query_name}] Batch #{batch_id} (Total batches: {self.batch_count})"
            )
            print(f"  Rows processed: {row_count}")
            print(f"  Event time range: {min_time} to {max_time}")
        else:
            print(f"\n[{self.query_name}] Batch #{batch_id}")
            print(f"  Rows processed: {row_count}")


def main():
    """Main streaming processing pipeline."""

    print("=" * 70)
    print("STREAMING ETL JOB STARTED")
    print(f"Timestamp: {datetime.now()}")
    print("=" * 70)

    start_time = time.time()

    # Load configuration
    print("\n[1/6] Loading configuration...")
    config = load_config()
    stream_config = config["stream"]

    # Create Spark session
    print("[1/6] Creating Spark session for streaming...")
    spark = create_streaming_spark_session(config)
    print(f"Spark version: {spark.version}")

    # Read dimension tables (batch DataFrames for stream-to-batch join)
    print("\n[2/6] Loading dimension tables...")
    project_root = Path(__file__).parent.parent.parent
    customers_path = project_root / config["dimensions"]["customers_path"]
    products_path = project_root / config["dimensions"]["products_path"]

    customers_df = read_dimension_table(spark, customers_path, CUSTOMERS_SCHEMA)
    products_df = read_dimension_table(spark, products_path, PRODUCTS_SCHEMA)
    print(f"  ✓ Customers: {customers_df.count()} records")
    print(f"  ✓ Products: {products_df.count()} records")

    # Read streaming data
    print("\n[3/6] Setting up streaming source...")
    input_path = get_absolute_path(stream_config["input_path"])
    max_files = stream_config.get("max_files_per_trigger", 2)

    print(f"  Input path: {input_path}")
    print(f"  Max files per trigger: {max_files}")
    print(f"  Schema: Predefined ORDERS_SCHEMA")

    streaming_df = (
        spark.readStream.format("csv")
        .option("header", "true")
        .schema(ORDERS_SCHEMA)
        .option("maxFilesPerTrigger", max_files)
        .load(input_path)
    )

    print("  ✓ Streaming source configured")

    # Apply transformations
    print("\n[4/6] Applying transformations...")

    watermark_delay = stream_config.get("watermark_delay", "30 minutes")

    # Clean data (includes watermarking for deduplication)
    cleaned_df = clean_streaming_data(streaming_df, watermark_delay)
    print(f"  ✓ Data cleaning applied (watermark: {watermark_delay})")

    # Derive columns
    enriched_df = derive_streaming_columns(cleaned_df)
    print("  ✓ Derived columns added")

    # Join with dimension tables (stream-to-batch join)
    enriched_df = enrich_with_dimensions_streaming(
        enriched_df, customers_df, products_df
    )
    print("  ✓ Joined with customer dimension (customer_name, customer_segment, etc.)")
    print("  ✓ Joined with product dimension (product_name, category, brand, etc.)")

    # Windowed aggregations
    print("\n[5/6] Setting up aggregations...")
    window_duration = stream_config.get("window_duration", "10 minutes")

    windowed_revenue = aggregate_windowed_revenue(enriched_df, window_duration)
    print(
        f"  ✓ Windowed aggregation configured ({window_duration} windows, watermark: {watermark_delay})"
    )

    product_totals = aggregate_product_totals(enriched_df)
    print("  ✓ Product totals aggregation configured")

    # Set up output sinks
    print("\n[6/6] Starting streaming queries...")

    output_path = get_absolute_path(stream_config["output_path"])
    checkpoint_path = get_absolute_path(stream_config["checkpoint_path"])

    # Metrics listener for console output
    console_listener = StreamingMetricsListener("Console-WindowedRevenue")

    # Sink 1: Console output for debugging (windowed revenue)
    # Using foreachBatch for metrics logging
    def console_batch_handler(batch_df, batch_id):
        console_listener.log_batch_metrics(batch_df, batch_id)
        batch_df.show(20, truncate=False)

    console_query = (
        windowed_revenue.writeStream.outputMode("update")
        .trigger(processingTime="5 seconds")
        .foreachBatch(console_batch_handler)
        .start()
    )

    print("  ✓ Console sink started (windowed revenue)")

    # Sink 2: File output (Parquet) - windowed revenue
    # Using foreachBatch to log metrics AND write files with partitioning
    file_listener = StreamingMetricsListener("File-WindowedRevenue")

    def file_batch_handler(batch_df, batch_id):
        file_listener.log_batch_metrics(batch_df, batch_id)
        if batch_df.count() > 0:
            batch_df.write.mode("append").partitionBy("product_id").parquet(
                f"{output_path}/windowed_revenue/"
            )

    file_query = (
        windowed_revenue.writeStream.outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", f"{checkpoint_path}/windowed_revenue/")
        .foreachBatch(file_batch_handler)
        .start()
    )

    print("  ✓ File sink started (windowed revenue)")

    # Sink 3: Memory sink for product totals (update mode works with memory)
    # File sinks don't support "complete" mode, so we use memory for running totals
    totals_listener = StreamingMetricsListener("Memory-ProductTotals")

    def totals_batch_handler(batch_df, batch_id):
        totals_listener.log_batch_metrics(batch_df, batch_id)

        if batch_df.count() > 0:
            print("\n  Top 10 Products by Revenue:")
            batch_df.orderBy(col("total_revenue").desc()).show(10, truncate=False)

    totals_query = (
        product_totals.writeStream.outputMode("complete")
        .trigger(processingTime="10 seconds")
        .foreachBatch(totals_batch_handler)
        .start()
    )

    print("  ✓ Memory sink started (product totals)")

    # Also write product totals to CSV periodically
    # This uses foreachBatch to write snapshots
    def write_totals_snapshot(batch_df, batch_id):
        """Write current totals snapshot to CSV."""
        if batch_df.count() > 0:
            snapshot_path = (
                f"{output_path}/product_totals_snapshots/batch_{batch_id:04d}"
            )
            batch_df.orderBy(col("total_revenue").desc()).coalesce(1).write.mode(
                "overwrite"
            ).option("header", "true").csv(snapshot_path)
            print(f"  📊 Snapshot written: batch_{batch_id:04d}.csv")

    totals_csv_query = (
        product_totals.writeStream.outputMode("complete")
        .trigger(processingTime="15 seconds")
        .foreachBatch(write_totals_snapshot)
        .start()
    )

    print("  ✓ CSV snapshot sink started (product totals)")

    print("\n" + "=" * 70)
    print("STREAMING QUERIES ARE RUNNING")
    print("=" * 70)
    print("Waiting for data to arrive...")
    print("Press Ctrl+C to stop")
    print("=" * 70)

    try:
        spark.streams.awaitAnyTermination(timeout=300)

    except KeyboardInterrupt:
        print("\n\nReceived interrupt signal. Stopping queries...")

    finally:

        print("\n" + "=" * 70)
        print("STREAMING JOB STATISTICS")
        print("=" * 70)

        # Stop all active queries
        for query in spark.streams.active:
            print(f"Stopping query: {query.name or query.id}")
            query.stop()

        end_time = time.time()
        duration = end_time - start_time

        print("\n" + "=" * 70)
        print("STREAMING ETL JOB COMPLETED")
        print("=" * 70)
        print(f"Total runtime: {duration:.2f} seconds")
        print("=" * 70)

        spark.stop()


if __name__ == "__main__":
    main()
