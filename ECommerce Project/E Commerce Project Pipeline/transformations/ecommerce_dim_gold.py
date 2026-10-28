
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# 1. GOLD DIMENSION: PRODUCTS
@dp.materialized_view(name="ecommerce.gold.gld_dim_products_test")
def gld_dim_products_test():
    products = spark.read.table("ecommerce.silver.slv_products_test")
    brands = spark.read.table("ecommerce.silver.slv_brands_test")
    categories = spark.read.table("ecommerce.silver.slv_category_test")

    brands_categories = brands.join(
        categories,
        brands.category_code == categories.category_code,
        "inner"
    ).select(
        brands.brand_name,
        brands.brand_code,
        categories.category_name,
        categories.category_code
    )

    result = products.join(
        brands_categories,
        products.brand_code == brands_categories.brand_code,
        "left"
    ).select(
        products.product_id,
        products.sku,
        products.category_code,
        F.coalesce(
            brands_categories.category_name,
            F.lit("Not Available")
        ).alias("category_name"),
        products.brand_code,
        F.coalesce(
            brands_categories.brand_name,
            F.lit("Not Available")
        ).alias("brand_name"),
        products.color,
        products.size,
        products.material,
        products.weight_grams,
        products.length_cm,
        products.width_cm,
        products.height_cm,
        products.rating_count,
        products.file_name,
        products.ingest_timestamp
    )

    return result


# 2. GOLD DIMENSION: CUSTOMERS
@dp.materialized_view(name="ecommerce.gold.gld_dim_customers_test")
def gld_dim_customers_test():
    customers = spark.read.table("ecommerce.silver.slv_customers_test")

    region_rows = [
        ("India", "MH", "West"),
        ("India", "GJ", "West"),
        ("India", "RJ", "West"),
        ("India", "KA", "South"),
        ("India", "TN", "South"),
        ("India", "TS", "South"),
        ("India", "AP", "South"),
        ("India", "KL", "South"),
        ("India", "UP", "North"),
        ("India", "WB", "North"),
        ("India", "DL", "North"),
        ("Australia", "VIC", "SouthEast"),
        ("Australia", "WA", "West"),
        ("Australia", "NSW", "East"),
        ("Australia", "QLD", "NorthEast"),
        ("United Kingdom", "ENG", "England"),
        ("United Kingdom", "WLS", "Wales"),
        ("United Kingdom", "NIR", "Northern Ireland"),
        ("United Kingdom", "SCT", "Scotland"),
        ("United States", "MA", "NorthEast"),
        ("United States", "FL", "South"),
        ("United States", "NJ", "NorthEast"),
        ("United States", "CA", "West"),
        ("United States", "NY", "NorthEast"),
        ("United States", "TX", "South"),
        ("United Arab Emirates", "AUH", "Abu Dhabi"),
        ("United Arab Emirates", "DU", "Dubai"),
        ("United Arab Emirates", "SHJ", "Sharjah"),
        ("Singapore", "SG", "Singapore"),
        ("Canada", "BC", "West"),
        ("Canada", "AB", "West"),
        ("Canada", "ON", "East"),
        ("Canada", "QC", "East"),
        ("Canada", "NS", "East"),
        ("Canada", "IL", "Other")
    ]

    region_schema = ["country", "state", "region"]
    region_df = spark.createDataFrame(region_rows, region_schema)

    result = customers.join(
        region_df,
        on=["country", "state"],
        how="left"
    )

    return result.fillna({"region": "Other"})


# 3. GOLD DIMENSION: DATE
@dp.materialized_view(name="ecommerce.gold.gld_dim_date_test")
def gld_dim_date_test():
    calendar = spark.read.table("ecommerce.silver.slv_calendar_test")

    result = (
        calendar
        .withColumn(
            "date_id",
            F.date_format(F.col("date"), "yyyyMMdd").cast("int")
        )
        .withColumn(
            "month_name",
            F.date_format(F.col("date"), "MMMM")
        )
        .withColumn(
            "is_weekend",
            F.when(
                F.col("day_name").isin("Saturday", "Sunday"), 1
            ).otherwise(0)
        )
    )

    return result.select(
        "date_id",
        "date",
        "year",
        "month_name",
        "day_name",
        "is_weekend",
        "quarter",
        "week",
        "_ingested_at",
        "_source_file"
    )

    