"""
Data schema that used for orders dataset amd dimension tables.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("order_timestamp", TimestampType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("status", StringType(), nullable=True),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("customer_name", StringType(), nullable=True),
        StructField("customer_email", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("registration_date", StringType(), nullable=True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("brand", StringType(), nullable=True),
        StructField("list_price", DoubleType(), nullable=True),
    ]
)

VALID_STATUSES = ["completed", "cancelled", "pending", "processing"]

STATUS_VARIANTS = [
    "completed",
    "Completed",
    "COMPLETED",
    "Complete",
    "cancelled",
    "Cancelled",
    "CANCELLED",
    "canceled",
    "pending",
    "Pending",
    "PENDING",
    "processing",
    "Processing",
    "PROCESSING",
    "Done",
    "done",
]
