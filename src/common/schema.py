"""
Схема даних для orders dataset.
Використовується в batch і stream проектах.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("order_timestamp", TimestampType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("status", StringType(), nullable=True)
])

VALID_STATUSES = ["completed", "cancelled", "pending", "processing"]

STATUS_VARIANTS = [
    "completed", "Completed", "COMPLETED", "Complete",
    "cancelled", "Cancelled", "CANCELLED", "canceled",
    "pending", "Pending", "PENDING",
    "processing", "Processing", "PROCESSING", "Done", "done"
]