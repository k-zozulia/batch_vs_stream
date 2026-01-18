"""
Data transformations for streaming processing.
Adapted from batch transformations with streaming-specific constraints.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    to_date,
    dayofweek,
    hour,
    sum as _sum,
    count,
    window,
    expr,
)


def clean_streaming_data(
    df: DataFrame, watermark_delay: str = "30 minutes"
) -> DataFrame:
    """
    Apply basic cleaning for streaming data.

    Limitations in streaming vs batch:
    - Cannot write quarantine files during processing (would need separate sink)
    - dropDuplicates requires watermark on event-time column
    - Must filter out bad data instead of separating it

    Args:
        df: Streaming DataFrame
        watermark_delay: Watermark delay for handling late arrivals

    Returns:
        Cleaned DataFrame
    """
    df = df.withWatermark("order_timestamp", watermark_delay)

    df = df.filter(col("order_id").isNotNull())

    df = df.filter((col("quantity") > 0) & (col("price") >= 0))

    df = df.withColumn("status", lower(trim(col("status"))))
    df = df.withColumn(
        "status",
        expr(
            """
            CASE 
                WHEN status IN ('complete', 'done') THEN 'completed'
                WHEN status = 'canceled' THEN 'cancelled'
                ELSE status
            END
        """
        ),
    )

    df = df.dropDuplicatesWithinWatermark(["order_id"])

    df = df.filter(col("status") != "cancelled")

    return df


def derive_streaming_columns(df: DataFrame) -> DataFrame:
    """
    Add derived columns for streaming data.

    Args:
        df: Streaming DataFrame

    Returns:
        DataFrame with derived columns
    """
    df = (
        df.withColumn("order_date", to_date(col("order_timestamp")))
        .withColumn("order_day_of_week", dayofweek(col("order_timestamp")))
        .withColumn("hour_of_day", hour(col("order_timestamp")))
        .withColumn("total_amount", col("quantity") * col("price"))
    )

    return df


def enrich_with_dimensions_streaming(
    stream_df: DataFrame, customers_df: DataFrame, products_df: DataFrame
) -> DataFrame:
    """
    Enrich streaming data with dimension tables.

    Note: This is a stream-to-batch join. Dimension tables are static/slowly changing
    and loaded as batch DataFrames.

    Args:
        stream_df: Streaming DataFrame (orders)
        customers_df: Batch DataFrame (customers dimension)
        products_df: Batch DataFrame (products dimension)

    Returns:
        Enriched streaming DataFrame
    """
    # Join with customers (stream-to-batch join)
    enriched = stream_df.join(customers_df, on="customer_id", how="left")

    # Join with products (stream-to-batch join)
    enriched = enriched.join(products_df, on="product_id", how="left")

    return enriched


def aggregate_windowed_revenue(
    df: DataFrame, window_duration: str = "10 minutes"
) -> DataFrame:
    """
    Calculate revenue per time window and product.

    Args:
        df: Streaming DataFrame with order_timestamp (must have watermark!)
        window_duration: Window duration (e.g., "10 minutes")

    Returns:
        Aggregated DataFrame with windowed metrics
    """
    return (
        df.groupBy(window(col("order_timestamp"), window_duration), col("product_id"))
        .agg(
            count("order_id").alias("orders_count"),
            _sum("total_amount").alias("total_revenue"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product_id"),
            col("orders_count"),
            col("total_revenue"),
        )
    )


def aggregate_product_totals(df: DataFrame) -> DataFrame:
    """
    Calculate running totals per product (stateful aggregation).

    Note: This creates a running total that cannot use append mode with file sinks.
    Must use 'update' mode or memory/console sinks.

    Args:
        df: Streaming DataFrame (must have watermark!)

    Returns:
        Aggregated DataFrame with product totals
    """
    return df.groupBy("product_id").agg(
        count("order_id").alias("total_orders"),
        _sum("total_amount").alias("total_revenue"),
    )
