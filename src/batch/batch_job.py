"""
Main batch ETL job for e-commerce orders processing.

Reads historical orders data, applies cleaning and transformations,
and produces analytical tables.
"""
import time
from datetime import datetime
from pathlib import Path

from src.common.schema import ORDERS_SCHEMA, CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA
from src.batch.io_utils import (
    load_config,
    create_spark_session,
    read_batch_data,
    read_dimension_table,
    write_parquet,
    write_quarantine,
    write_cancelled_orders,
    get_absolute_path
)
from src.batch.transformations import (
    filter_missing_order_id,
    filter_invalid_values,
    normalize_status,
    remove_duplicates,
    filter_cancelled_orders,
    derive_columns,
    enrich_with_dimensions,
    aggregate_daily_revenue,
    aggregate_product_revenue,
    aggregate_hourly_revenue,
    get_top_products
)

PROJECT_ROOT = Path(__file__).parent.parent.parent

def log_metrics(stage, df, message=""):
    """Helper function to log DataFrame metrics."""
    count = df.count()
    print(f"[{stage}] {message} Count: {count}")
    return count


def main():
    """Main batch processing pipeline."""

    print("=" * 70)
    print("BATCH ETL JOB STARTED")
    print(f"Timestamp: {datetime.now()}")
    print("=" * 70)

    start_time = time.time()

    print("Loading configuration...")
    config = load_config()

    print("Creating Spark session...")
    spark = create_spark_session(config)
    print(f"Spark version: {spark.version}")

    print("Reading input data...")
    raw_df = read_batch_data(spark, config, ORDERS_SCHEMA)
    input_count = log_metrics("READ", raw_df, "Input records")

    print("Reading dimension tables...")
    customers_path = PROJECT_ROOT / config['dimensions']['customers_path']
    products_path = PROJECT_ROOT / config['dimensions']['products_path']

    customers_df = read_dimension_table(spark, customers_path, CUSTOMERS_SCHEMA)
    products_df = read_dimension_table(spark, products_path, PRODUCTS_SCHEMA)
    print(f"--- Customers: {customers_df.count()} records")
    print(f"--- Products: {products_df.count()} records")

    print("Data quality checks...")
    valid_df, missing_id_df = filter_missing_order_id(raw_df)
    log_metrics("QUALITY", missing_id_df, "Missing order_id")

    if missing_id_df.count() > 0:
        write_quarantine(missing_id_df, config, "missing_order_id")
        print("  ✓ Missing order_id records written to quarantine")

    valid_df, invalid_values_df = filter_invalid_values(valid_df)
    log_metrics("QUALITY", invalid_values_df, "Invalid values (negative qty/price)")

    if invalid_values_df.count() > 0:
        write_quarantine(invalid_values_df, config, "invalid_values")
        print("  ✓ Invalid values written to quarantine")

    valid_df = normalize_status(valid_df)
    print("Status values normalized")

    before_dedup =valid_df.count()
    valid_df = remove_duplicates(valid_df)
    after_dedup = valid_df.count()
    duplicates_removed = after_dedup - before_dedup
    print(f"Duplicates removed: {duplicates_removed}")

    print("Separating cancelled orders...")
    active_df, cancelled_df = filter_cancelled_orders(valid_df)
    log_metrics("CANCELLED", cancelled_df, "Cancelled orders")

    if cancelled_df.count() > 0:
        cancelled_df = derive_columns(cancelled_df)
        write_cancelled_orders(cancelled_df, config)
        print("  ✓ Cancelled orders written to separate directory")

    print("Enriching with dimension tables and deriving columns...")
    enriched_df = derive_columns(active_df)
    enriched_df = enrich_with_dimensions(enriched_df, customers_df, products_df)

    log_metrics("ENRICHED", enriched_df, "Valid active orders")
    print("  ✓ Derived: order_date, order_day_of_week, hour_of_day, total_amount")
    print("  ✓ Joined with customer dimension (customer_name, customer_segment, etc.)")
    print("  ✓ Joined with product dimension (product_name, category, brand, etc.)")

    print("Calculating business metrics...")

    daily_revenue = aggregate_daily_revenue(enriched_df)
    print(f"  ✓ Daily revenue aggregated ({daily_revenue.count()} days)")

    product_revenue = aggregate_product_revenue(enriched_df)
    print(f"  ✓ Product revenue aggregated ({product_revenue.count()} products)")

    top_products = get_top_products(enriched_df, n=10)
    print("  ✓ Top 10 products by revenue:")
    top_products.show(10, truncate=False)

    hourly_revenue = aggregate_hourly_revenue(enriched_df)
    print(f"  ✓ Hourly revenue aggregated (24 hours)")

    print("Writing results to warehouse...")
    output_path = get_absolute_path(config['batch']['output_path'])

    orders_output = f"{output_path}orders/"
    write_parquet(enriched_df, orders_output, partition_by="order_date")
    print(f"  ✓ Orders written to: {orders_output}/")

    write_parquet(daily_revenue, f"{output_path}/daily_revenue/")
    print(f"  ✓ Daily revenue written")

    write_parquet(product_revenue, f"{output_path}/product_revenue/")
    print(f"  ✓ Product revenue written")

    write_parquet(hourly_revenue, f"{output_path}/hourly_revenue/")
    print(f"  ✓ Hourly revenue written")

    end_time = time.time()
    duration = end_time - start_time

    print("\n" + "=" * 70)
    print("BATCH ETL JOB COMPLETED")
    print("=" * 70)
    print(f"Input records:        {input_count}")
    print(f"Valid output records: {enriched_df.count()}")
    print(f"Cancelled orders:     {cancelled_df.count() if cancelled_df.count() > 0 else 0}")
    print(f"Quarantined records:  {missing_id_df.count() + invalid_values_df.count()}")
    print(f"Duplicates removed:   {duplicates_removed}")
    print(f"Execution time:       {duration:.2f} seconds")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()
