"""
IO utilities for batch processing: SparkSession creation and config loading.
"""

import yaml
from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).parent.parent.parent


def load_config():
    """Load configuration from YAML file."""
    config_path = PROJECT_ROOT / "config" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_absolute_path(relative_path):
    """
    Convert relative path from config to absolute path.

    Args:
        relative_path: Path relative to project root

    Returns:
        Absolute path as string
    """
    return str(PROJECT_ROOT / relative_path)


def create_spark_session(config):
    """
    Create and configure SparkSession for batch processing.

    Args:
        config: Configuration dictionary from YAML

    Returns:
        SparkSession: Configured Spark session
    """

    spark_config = config["spark"]

    spark = (
        SparkSession.builder.appName(spark_config["app_name"])
        .master(spark_config["master"])
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(spark_config["log_level"])

    return spark


def read_batch_data(spark, config, schema):
    """
    Read all CSV files from batch input directory.

    Args:
        spark: SparkSession
        config: Configuration dictionary
        schema: StructType schema for orders

    Returns:
        DataFrame: Raw orders data
    """
    input_path = get_absolute_path(config["batch"]["input_path"])

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(f"{input_path}/*.csv")
    )

    return df


def write_parquet(df, output_path, partition_by=None, mode="overwrite"):
    """
    Write DataFrame to Parquet format.

    Args:
        df: DataFrame to write
        output_path: Output directory path
        partition_by: Column(s) to partition by
        mode: Write mode (overwrite, append)
    """

    writer = df.write.mode(mode).format("parquet")

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.save(output_path)


def write_quarantine(df, config, reason):
    """
    Write invalid records to quarantine directory.

    Args:
        df: DataFrame with invalid records
        config: Configuration dictionary
        reason: Reason for quarantine (e.g., 'negative_values', 'missing_id')
    """

    quarantine_path = PROJECT_ROOT / config["batch"]["quarantine_path"]
    output_path = f"{quarantine_path}/{reason}/"

    write_parquet(df, output_path, mode="overwrite")


def write_cancelled_orders(df, config):
    """
    Write cancelled orders to separate directory.

    Args:
        df: DataFrame with cancelled orders
        config: Configuration dictionary
    """

    cancelled_path = PROJECT_ROOT / config["batch"]["cancelled_path"]
    write_parquet(df, str(cancelled_path), partition_by="order_date", mode="overwrite")


def read_dimension_table(spark, path, schema):
    """
    Read dimension table (customers or products).

    Args:
        spark: SparkSession
        path: Path to CSV file
        schema: StructType schema

    Returns:
        DataFrame: Dimension table
    """
    return (
        spark.read.format("csv").option("header", "true").schema(schema).load(str(path))
    )
