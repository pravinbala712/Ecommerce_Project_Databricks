from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

orders_schema = StructType([
    StructField("dt", StringType(), True),
    StructField("order_ts", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("item_seq", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price_currency", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount_pct", StringType(), True),
    StructField("tax_amount", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("coupon_code", StringType(), True)
])


@dp.materialized_view(name="ecommerce.bronze.brz_order_items_test")
def brz_order_items_test():
    raw_path = "/Volumes/ecommerce/source_data/raw/order_items/landing/*.csv"

    return (
        spark.read
        .option("header", True)
        .option("delimiter", ",")
        .schema(orders_schema)
        .csv(raw_path)
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("ingest_timestamp", F.current_timestamp())
    )
