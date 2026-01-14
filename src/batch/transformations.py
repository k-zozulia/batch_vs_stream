"""
Data transformations for batch processing.
Pure functions that operate on DataFrames.
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
    desc,
    when,
)


def filter_missing_order_id(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Separate records with missing order_id.

    Returns:
        Tuple of (valid_df, invalid_df)
    """

    valid = df.filter(col("order_id").isNotNull())
    invalid = df.filter(col("order_id").isNull())

    return valid, invalid


def filter_invalid_values(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Separate records with negative quantity or price.

    Returns:
        Tuple of (valid_df, invalid_df)
    """

    valid = df.filter((col("quantity") > 0) & (col("price") >= 0))

    invalid = df.filter((col("quantity") <= 0) | (col("price") < 0))

    return valid, invalid


def normalize_status(df: DataFrame) -> DataFrame:
    """
    Normalize status values to lowercase and trim whitespace.

    Also maps common variations:
    - 'complete', 'done' -> 'completed'
    - 'canceled' -> 'cancelled'
    """

    df = df.withColumn("status", lower(trim(col("status"))))

    df = df.withColumn(
        "status",
        when(col("status").isin(["complete", "done"]), "completed")
        .when(col("status") == "canceled", "cancelled")
        .otherwise(col("status")),
    )

    return df


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Remove duplicate orders based on order_id.
    Keeps the first occurrence.
    """
    return df.dropDuplicates(["order_id"])


def filter_cancelled_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Separate cancelled orders from active ones.

    Returns:
        Tuple of (active_df, cancelled_df)
    """

    active = df.filter(col("status") != "cancelled")
    cancelled = df.filter(col("status") == "cancelled")

    return active, cancelled


def derive_columns(df: DataFrame) -> DataFrame:
    """
    Add derived columns:
    - order_date: date from order_timestamp
    - order_day_of_week: day of week (1=Sunday, 7=Saturday)
    - hour_of_day: hour (0-23)
    - total_amount: quantity * price
    """
    df = (
        df.withColumn("order_date", to_date(col("order_timestamp")))
        .withColumn("order_day_of_week", dayofweek(col("order_timestamp")))
        .withColumn("hour_of_day", hour(col("order_timestamp")))
        .withColumn("total_amount", col("quantity") * col("price"))
    )

    return df


def aggregate_daily_revenue(df: DataFrame) -> DataFrame:
    """
    Calculate daily revenue metrics.

    Returns:
        DataFrame with columns: order_date, total_orders, total_revenue
    """
    return (
        df.groupBy("order_date")
        .agg(
            count("order_id").alias("total_orders"),
            _sum("total_amount").alias("total_revenue"),
        )
        .orderBy("order_date")
    )


def aggregate_product_revenue(df: DataFrame) -> DataFrame:
    """
    Calculate revenue per product.

    Returns:
        DataFrame with columns: product_id, total_orders, total_revenue
    """

    return (
        df.groupBy("product_id")
        .agg(
            count("order_id").alias("total_orders"),
            _sum("total_amount").alias("total_revenue"),
        )
        .orderBy(desc("total_revenue"))
    )


def aggregate_hourly_revenue(df: DataFrame) -> DataFrame:
    """
    Calculate revenue by hour of day (0-23).

    Returns:
        DataFrame with columns: hour_of_day, total_orders, total_revenue
    """
    return (
        df.groupBy("hour_of_day")
        .agg(
            count("order_id").alias("total_orders"),
            _sum("total_amount").alias("total_revenue"),
        )
        .orderBy("hour_of_day")
    )


def get_top_products(df: DataFrame, n: int = 10) -> DataFrame:
    """
    Get top N products by revenue.

    Args:
        df: Orders DataFrame
        n: Number of top products to return

    Returns:
        DataFrame with top N products
    """
    return aggregate_product_revenue(df).limit(n)


def enrich_with_customers(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """
    Enrich orders with customer information.

    Args:
        orders_df: Orders DataFrame
        customers_df: Customers dimension table

    Returns:
        Enriched DataFrame with customer info
    """
    return orders_df.join(customers_df, on="customer_id", how="left")


def enrich_with_products(orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Enrich orders with product information.

    Args:
        orders_df: Orders DataFrame
        products_df: Products dimension table

    Returns:
        Enriched DataFrame with product info
    """
    return orders_df.join(products_df, on="product_id", how="left")


def enrich_with_dimensions(
    orders_df: DataFrame, customers_df: DataFrame, products_df: DataFrame
) -> DataFrame:
    """
    Enrich orders with both customer and product dimensions.

    Args:
        orders_df: Orders DataFrame
        customers_df: Customers dimension table
        products_df: Products dimension table

    Returns:
        Fully enriched DataFrame
    """
    enriched = enrich_with_customers(orders_df, customers_df)
    enriched = enrich_with_products(enriched, products_df)
    return enriched
