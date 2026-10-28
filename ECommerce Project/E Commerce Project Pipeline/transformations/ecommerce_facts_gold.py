from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


@dp.materialized_view(name="ecommerce.gold.gld_fact_order_items_test")
def gld_fact_order_items_test():
    df = spark.read.table(
        "ecommerce.silver.slv_order_items_test"
    )

    df = df.withColumn(
        "gross_amount",
        F.col("unit_price") * F.col("quantity")
    )

    df = df.withColumn(
        "discount_amount",
        F.ceil(
            F.col("gross_amount")
            * (F.col("discount_pct") / 100.0)
        )
    )

    df = df.withColumn(
        "sale_amount",
        F.col("gross_amount")
        - F.col("discount_amount")
        + F.col("tax_amount")
    )

    df = df.withColumn(
        "date_id",
        F.date_format(F.col("dt"), "yyyyMMdd")
         .cast(IntegerType())
    )

    df = df.withColumn(
        "coupon_flag",
        F.when(F.col("coupon_code").isNotNull(), 1)
         .otherwise(0)
    )

    # Currency conversion rates from the original PySpark notebook
    fx_rates = [
        ("INR", 1.00),
        ("AED", 24.18),
        ("AUD", 57.55),
        ("CAD", 62.93),
        ("GBP", 117.98),
        ("SGD", 68.18),
        ("USD", 88.29)
    ]

    rates_df = spark.createDataFrame(
        fx_rates, ["currency", "inr_rate"]
    )

    df = df.join(
        rates_df,
        F.upper(F.trim(F.col("unit_price_currency")))
        == F.col("currency"),
        "left"
    )

    df = df.withColumn(
        "sale_amount_inr",
        F.ceil(F.col("sale_amount") * F.col("inr_rate"))
    )

    return df.select(
        "date_id",
        F.col("dt").alias("transaction_date"),
        F.col("order_ts").alias("transaction_ts"),
        F.col("order_id").alias("transaction_id"),
        "customer_id",
        F.col("item_seq").alias("seq_no"),
        "product_id",
        "channel",
        "coupon_code",
        "coupon_flag",
        "unit_price_currency",
        "quantity",
        "unit_price",
        "gross_amount",
        F.col("discount_pct").alias("discount_percent"),
        "discount_amount",
        "tax_amount",
        F.col("sale_amount").alias("net_amount"),
        F.col("sale_amount_inr").alias("net_amount_inr")
    )
