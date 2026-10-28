from pyspark import pipelines as dp
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import functions as F

# 1. BRANDS
@dp.materialized_view(name="ecommerce.silver.slv_brands_test")
def slv_brands_test():
    df = spark.read.table("ecommerce.bronze.brz_brands_test")

    df = df.withColumn("brand_name", F.trim(F.col("brand_name")))
    df = df.withColumn(
        "brand_code",
        F.regexp_replace(F.col("brand_code"), r"[^A-Za-z0-9]", "")
    )

    anomalies = {
        "GROCERY": "GRCY",
        "BOOKS": "BKS",
        "TOYS": "TOY"
    }

    return df.replace(anomalies, subset=["category_code"])


# 2. CATEGORY
@dp.materialized_view(name="ecommerce.silver.slv_category_test")
def slv_category_test():
    df = spark.read.table("ecommerce.bronze.brz_category_test")

    df = df.dropDuplicates(["category_code"])
    df = df.withColumn(
        "category_code",
        F.upper(F.col("category_code"))
    )

    return df


# 3. PRODUCTS
@dp.materialized_view(name="ecommerce.silver.slv_products_test")
def slv_products_test():
    df = spark.read.table("ecommerce.bronze.brz_products_test")

    df = df.withColumn(
        "weight_grams",
        F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType())
    )

    df = df.withColumn(
        "length_cm",
        F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType())
    )

    df = (
        df.withColumn("category_code", F.upper(F.col("category_code")))
          .withColumn("brand_code", F.upper(F.col("brand_code")))
    )

    df = df.withColumn(
        "material",
        F.when(F.col("material") == "Coton", "Cotton")
         .when(F.col("material") == "Ruber", "Rubber")
         .when(F.col("material") == "Alumium", "Aluminium")
         .otherwise(F.col("material"))
    )

    df = df.withColumn(
        "rating_count",
        F.when(
            F.col("rating_count").isNotNull(),
            F.abs(F.col("rating_count"))
        ).otherwise(F.lit(0))
    )

    return df


# 4. CUSTOMERS
@dp.materialized_view(name="ecommerce.silver.slv_customers_test")
def slv_customers_test():
    df = spark.read.table("ecommerce.bronze.brz_customers_test")

    df = df.dropna(subset=["customer_id"])
    df = df.fillna("Not Available", subset=["phone"])

    return df


# 5. CALENDAR
@dp.materialized_view(name="ecommerce.silver.slv_calendar_test")
def slv_calendar_test():
    df = spark.read.table("ecommerce.bronze.brz_calendar_test")

    df = df.withColumn(
        "date",
        F.to_date(F.col("date"), "dd-MM-yyyy")
    )

    df = df.dropDuplicates(["date"])

    df = df.withColumn("day_name", F.initcap(F.col("day_name")))
    df = df.withColumn(
        "week_of_year",
        F.abs(F.col("week_of_year"))
    )

    df = df.withColumn(
        "quarter",
        F.concat(
            F.lit("Q"),
            F.col("quarter"),
            F.lit("-"),
            F.col("year")
        )
    )

    df = df.withColumn(
        "week_of_year",
        F.concat(
            F.lit("Week"),
            F.col("week_of_year"),
            F.lit("-"),
            F.col("year")
        )
    )

    df = df.withColumnRenamed("week_of_year", "week")

    return df
