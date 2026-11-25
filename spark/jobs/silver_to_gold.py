import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _safe_rate(numerator_col, denominator_col):
    """Return (num / denom * 100) rounded to 2 decimals, or 0.0 when denom == 0."""
    return F.round(
        F.when(F.col(denominator_col) > 0,
               F.col(numerator_col) / F.col(denominator_col) * 100.0)
        .otherwise(0.0),
        2,
    )


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def build_daily_metrics(df):
    """Aggregate daily metrics from Silver data.

    Returns one row per event_date with columns:
        event_date, total_events, total_views, total_carts, total_purchases,
        unique_users, unique_sessions, purchase_value
    """
    return (
        df.groupBy("event_date")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "view",     1).otherwise(0)).alias("total_views"),
            F.sum(F.when(F.col("event_type") == "cart",     1).otherwise(0)).alias("total_carts"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("user_session").alias("unique_sessions"),
            F.round(
                F.coalesce(
                    F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))),
                    F.lit(0.0),
                ),
                2,
            ).alias("purchase_value"),
        )
    )


def build_product_performance(df):
    """One row per (event_date, product_id).

    Product attributes (brand, category_id, category_code) are taken from the
    chronologically first non-null value, using (event_time, kafka_partition,
    kafka_offset) as a deterministic tiebreaker.

    Returns columns:
        event_date, product_id, category_id, category_code, brand,
        total_views, total_carts, total_purchases, unique_users, purchase_value
    """
    # Unbounded window covering the whole (event_date, product_id) partition,
    # ordered deterministically so first() is stable across executors.
    w = (
        Window.partitionBy("event_date", "product_id")
        .orderBy("event_time", "kafka_partition", "kafka_offset")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df_attrs = df.select(
        "event_date",
        "product_id",
        F.first("category_id",   ignorenulls=True).over(w).alias("category_id"),
        F.first("category_code", ignorenulls=True).over(w).alias("category_code"),
        F.first("brand",         ignorenulls=True).over(w).alias("brand"),
    ).dropDuplicates(["event_date", "product_id"])

    df_agg = (
        df.groupBy("event_date", "product_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "view",     1).otherwise(0)).alias("total_views"),
            F.sum(F.when(F.col("event_type") == "cart",     1).otherwise(0)).alias("total_carts"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
            F.countDistinct("user_id").alias("unique_users"),
            F.round(
                F.coalesce(
                    F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))),
                    F.lit(0.0),
                ),
                2,
            ).alias("purchase_value"),
        )
    )

    return df_agg.join(df_attrs, on=["event_date", "product_id"], how="inner")


def build_category_performance(df):
    """One row per (event_date, category_id).

    Rows where category_id IS NULL are excluded.
    When category_code is null for some events, the first non-null value
    (chronologically) is used as the representative category_code for the row.

    Returns columns:
        event_date, category_id, category_code,
        total_views, total_carts, total_purchases,
        unique_products, unique_users, purchase_value
    """
    # Exclude only rows with a null category_id
    df_filtered = df.filter(F.col("category_id").isNotNull())

    # Representative category_code per (event_date, category_id):
    # pick the first non-null value chronologically.
    w_cat = (
        Window.partitionBy("event_date", "category_id")
        .orderBy("event_time", "kafka_partition", "kafka_offset")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    df_code = df_filtered.select(
        "event_date",
        "category_id",
        F.first("category_code", ignorenulls=True).over(w_cat).alias("category_code"),
    ).dropDuplicates(["event_date", "category_id"])

    df_agg = (
        df_filtered.groupBy("event_date", "category_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "view",     1).otherwise(0)).alias("total_views"),
            F.sum(F.when(F.col("event_type") == "cart",     1).otherwise(0)).alias("total_carts"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
            F.countDistinct("product_id").alias("unique_products"),
            F.countDistinct("user_id").alias("unique_users"),
            F.round(
                F.coalesce(
                    F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))),
                    F.lit(0.0),
                ),
                2,
            ).alias("purchase_value"),
        )
    )

    return df_agg.join(df_code, on=["event_date", "category_id"], how="inner")


def build_conversion_metrics(df):
    """Session-level funnel conversion rates per day.

    A session is identified by (event_date, user_session).
    Sessions where user_session IS NULL are ignored.

    Chronological funnel enforced with successive joins:
        has_view     : session has at least one view
        has_cart     : session has a cart event AFTER the first view
        has_purchase : session has a purchase event AFTER the first valid cart
                       (which is itself after the first view)

    Returns columns:
        event_date, sessions_with_views, sessions_with_carts,
        sessions_with_purchases, view_to_cart_rate, cart_to_purchase_rate,
        view_to_purchase_rate
    """
    # Ignore null sessions
    df_valid = df.filter(F.col("user_session").isNotNull())

    # Step 1 – per session: earliest view timestamp
    df_first_view = (
        df_valid.filter(F.col("event_type") == "view")
        .groupBy("event_date", "user_session")
        .agg(F.min("event_time").alias("first_view_ts"))
    )

    # All sessions (even those without any view) — needed for the base count
    df_all_sessions = (
        df_valid
        .select("event_date", "user_session")
        .dropDuplicates()
        .join(df_first_view, on=["event_date", "user_session"], how="left")
    )

    # Step 2 – per session: earliest cart that is AFTER the first view
    df_cart_after_view = (
        df_valid.filter(F.col("event_type") == "cart")
        .join(
            df_first_view,
            on=["event_date", "user_session"],
            how="inner",
        )
        .filter(F.col("event_time") > F.col("first_view_ts"))
        .groupBy("event_date", "user_session")
        .agg(F.min("event_time").alias("first_valid_cart_ts"))
    )

    # Step 3 – per session: earliest purchase that is AFTER the first valid cart
    df_purchase_after_cart = (
        df_valid.filter(F.col("event_type") == "purchase")
        .join(
            df_cart_after_view,
            on=["event_date", "user_session"],
            how="inner",
        )
        .filter(F.col("event_time") > F.col("first_valid_cart_ts"))
        .groupBy("event_date", "user_session")
        .agg(F.min("event_time").alias("first_valid_purchase_ts"))
    )

    # Combine everything
    df_combined = (
        df_all_sessions
        .join(df_cart_after_view,    on=["event_date", "user_session"], how="left")
        .join(df_purchase_after_cart, on=["event_date", "user_session"], how="left")
        .withColumn("has_view",     F.col("first_view_ts").isNotNull())
        .withColumn("has_cart",     F.col("first_valid_cart_ts").isNotNull())
        .withColumn("has_purchase", F.col("first_valid_purchase_ts").isNotNull())
    )

    df_agg = (
        df_combined.groupBy("event_date")
        .agg(
            F.sum(F.when(F.col("has_view"),     1).otherwise(0)).cast("long").alias("sessions_with_views"),
            F.sum(F.when(F.col("has_cart"),     1).otherwise(0)).cast("long").alias("sessions_with_carts"),
            F.sum(F.when(F.col("has_purchase"), 1).otherwise(0)).cast("long").alias("sessions_with_purchases"),
        )
    )

    return (
        df_agg
        .withColumn("view_to_cart_rate",
                    _safe_rate("sessions_with_carts",     "sessions_with_views"))
        .withColumn("cart_to_purchase_rate",
                    _safe_rate("sessions_with_purchases", "sessions_with_carts"))
        .withColumn("view_to_purchase_rate",
                    _safe_rate("sessions_with_purchases", "sessions_with_views"))
    )


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def _check_destination_free(spark, path):
    """Raise RuntimeError if the path already exists (HDFS or local FS)."""
    jvm = spark._jvm
    jsc = spark._jsc
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(jsc.hadoopConfiguration())
    if fs.exists(p):
        raise RuntimeError(
            f"Destination already exists, refusing to overwrite: {path}"
        )


def write_gold_tables(df, output_base):
    """Build and write the four Gold tables from a Silver DataFrame.

    Layout under output_base:
        daily_metrics/
        product_performance/
        category_performance/
        conversion_metrics/

    Each table is written as Parquet partitioned by event_date.
    Raises RuntimeError if any destination already exists.
    """
    spark = df.sparkSession

    destinations = {
        "daily_metrics":        f"{output_base}/daily_metrics",
        "product_performance":  f"{output_base}/product_performance",
        "category_performance": f"{output_base}/category_performance",
        "conversion_metrics":   f"{output_base}/conversion_metrics",
    }

    # Pre-flight check: all four destinations must be free before any write
    for name, path in destinations.items():
        _check_destination_free(spark, path)

    build_daily_metrics(df).write \
        .mode("errorifexists").partitionBy("event_date") \
        .parquet(destinations["daily_metrics"])

    build_product_performance(df).write \
        .mode("errorifexists").partitionBy("event_date") \
        .parquet(destinations["product_performance"])

    build_category_performance(df).write \
        .mode("errorifexists").partitionBy("event_date") \
        .parquet(destinations["category_performance"])

    build_conversion_metrics(df).write \
        .mode("errorifexists").partitionBy("event_date") \
        .parquet(destinations["conversion_metrics"])


def main():
    parser = argparse.ArgumentParser(description="Silver to Gold aggregation job")
    parser.add_argument("--input",      required=True,
                        help="Path to Silver Parquet partition(s)")
    parser.add_argument("--output-dir", required=True,
                        help="Base directory for Gold tables")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("SilverToGold")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.input)
    write_gold_tables(df, args.output_dir)
    spark.stop()


if __name__ == "__main__":
    main()
