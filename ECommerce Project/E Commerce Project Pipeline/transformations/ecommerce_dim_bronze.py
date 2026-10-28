from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

brands_schema=StructType([
    StructField("brand_code", StringType(), False),
    StructField("brand_name", StringType(), True),
    StructField("category_code", StringType(), True)
])

@dp.materialized_view(name="brz_brands_test")
def brz_brands_test():
    path = "/Volumes/ecommerce/source_data/raw/brands/*"
    return (
        spark.read.option("header", True)
        .option("delimiter", ",")
        .schema(brands_schema)
        .csv(path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("ingested_at", F.current_timestamp())
    )


# 2. Category
category_schema = StructType([
    StructField("category_code", StringType(), False),
    StructField("category_name", StringType(), True)
])

@dp.materialized_view(name="brz_category_test")
def brz_category_test():
    path = "/Volumes/ecommerce/source_data/raw/category/*.csv"
    return (
        spark.read.option("header", True)
        .option("delimiter", ",")
        .schema(category_schema)
        .csv(path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# 3. Products
products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),
    StructField("length_cm", StringType(), True),
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("file_name", StringType(), True),
    StructField("ingest_timestamp", TimestampType(), True)
])

@dp.materialized_view(name="brz_products_test")
def brz_products_test():
    path = "/Volumes/ecommerce/source_data/raw/products/*"
    return (
        spark.read.option("header", True)
        .option("delimiter", ",")
        .schema(products_schema)
        .csv(path)
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("ingest_timestamp", F.current_timestamp())
    )


# 4. Customers
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
])

@dp.materialized_view(name="brz_customers_test")
def brz_customers_test():
    path = "/Volumes/ecommerce/source_data/raw/customers/*"
    return (
        spark.read.option("header", True)
        .option("delimiter", ",")
        .schema(customers_schema)
        .csv(path)
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("ingest_timestamp", F.current_timestamp())
    )


# 5. Calendar
date_schema = StructType([
    StructField("date", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("week_of_year", IntegerType(), True)
])

@dp.materialized_view(name="brz_calendar_test")
def brz_calendar_test():
    path = "/Volumes/ecommerce/source_data/raw/date/*.csv"
    return (
        spark.read.option("header", True)
        .option("delimiter", ",")
        .schema(date_schema)
        .csv(path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
