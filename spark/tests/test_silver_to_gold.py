import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "jobs"),
)

from silver_to_gold import (
    build_category_performance,
    build_conversion_metrics,
    build_daily_metrics,
    build_product_performance,
)


SILVER_SCHEMA = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", LongType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_session", StringType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", LongType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("event_date", DateType(), True),
])


def make_event(
    ts_str,
    event_type,
    product_id=1001,
    category_id="cat1",
    category_code="electronics.audio",
    brand="brandA",
    price=100.0,
    user_id=10,
    user_session="sess-1",
    offset=1,
):
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return (
        dt,
        event_type,
        product_id,
        category_id,
        category_code,
        brand,
        price,
        user_id,
        user_session,
        "evenements_web",
        0,
        offset,
        dt.date(),
    )


class TestSilverToGold(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("TestSilverToGold")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _df(self, rows):
        return self.spark.createDataFrame(rows, schema=SILVER_SCHEMA)

    def test_funnel_complete_view_cart_purchase(self):
        rows = [
            make_event("2026-11-22T10:00:00Z", "view", user_session="s1", offset=1),
            make_event("2026-11-22T10:05:00Z", "cart", user_session="s1", offset=2),
            make_event("2026-11-22T10:10:00Z", "purchase", user_session="s1", offset=3),
        ]
        res = build_conversion_metrics(self._df(rows)).collect()
        self.assertEqual(len(res), 1)
        row = res[0]
        self.assertEqual(row["sessions_with_views"], 1)
        self.assertEqual(row["sessions_with_carts"], 1)
        self.assertEqual(row["sessions_with_purchases"], 1)
        self.assertAlmostEqual(row["view_to_cart_rate"], 100.0)
        self.assertAlmostEqual(row["cart_to_purchase_rate"], 100.0)
        self.assertAlmostEqual(row["view_to_purchase_rate"], 100.0)

    def test_funnel_purchase_view_cart_not_complete(self):
        rows = [
            make_event("2026-11-22T10:00:00Z", "purchase", user_session="s1", offset=1),
            make_event("2026-11-22T10:05:00Z", "view", user_session="s1", offset=2),
            make_event("2026-11-22T10:10:00Z", "cart", user_session="s1", offset=3),
        ]
        res = build_conversion_metrics(self._df(rows)).collect()[0]
        self.assertEqual(res["sessions_with_views"], 1)
        self.assertEqual(res["sessions_with_carts"], 1)
        self.assertEqual(res["sessions_with_purchases"], 0)
        self.assertAlmostEqual(res["cart_to_purchase_rate"], 0.0)
        self.assertAlmostEqual(res["view_to_purchase_rate"], 0.0)

    def test_funnel_view_purchase_cart_partial(self):
        rows = [
            make_event("2026-11-22T10:00:00Z", "view", user_session="s1", offset=1),
            make_event("2026-11-22T10:05:00Z", "purchase", user_session="s1", offset=2),
            make_event("2026-11-22T10:10:00Z", "cart", user_session="s1", offset=3),
        ]
        res = build_conversion_metrics(self._df(rows)).collect()[0]
        self.assertEqual(res["sessions_with_views"], 1)
        self.assertEqual(res["sessions_with_carts"], 1)
        self.assertEqual(res["sessions_with_purchases"], 0)
        self.assertAlmostEqual(res["view_to_cart_rate"], 100.0)
        self.assertAlmostEqual(res["cart_to_purchase_rate"], 0.0)

    def test_two_views_same_session_counted_once(self):
        rows = [
            make_event("2026-11-22T10:00:00Z", "view", user_session="s1", offset=1),
            make_event("2026-11-22T10:02:00Z", "view", user_session="s1", offset=2),
        ]
        df = self._df(rows)
        conv = build_conversion_metrics(df).collect()[0]
        daily = build_daily_metrics(df).collect()[0]
        self.assertEqual(conv["sessions_with_views"], 1)
        self.assertEqual(daily["total_views"], 2)
        self.assertEqual(daily["unique_sessions"], 1)

    def test_two_users_same_product(self):
        rows = [
            make_event("2026-11-22T10:00:00Z", "view", product_id=42, user_id=1, user_session="s1", offset=1),
            make_event("2026-11-22T10:05:00Z", "view", product_id=42, user_id=2, user_session="s2", offset=2),
            make_event("2026-11-22T10:06:00Z", "cart", product_id=42, user_id=2, user_session="s2", offset=3),
        ]
        res = build_product_performance(self._df(rows)).collect()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["unique_users"], 2)
        self.assertEqual(res[0]["total_views"], 2)
        self.assertEqual(res[0]["total_carts"], 1)

    def test_product_multiple_brands_single_row_per_day(self):
        rows = [
            make_event("2026-11-22T09:00:00Z", "view", product_id=99, brand=None, user_id=1, user_session="s1", offset=1),
            make_event("2026-11-22T10:00:00Z", "view", product_id=99, brand="BrandFirst", user_id=2, user_session="s2", offset=2),
            make_event("2026-11-22T11:00:00Z", "purchase", product_id=99, brand="BrandSecond", price=50.0, user_id=2, user_session="s2", offset=3),
        ]
        res = build_product_performance(self._df(rows)).collect()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["brand"], "BrandFirst")
        self.assertEqual(res[0]["total_views"], 2)
        self.assertEqual(res[0]["total_purchases"], 1)
        self.assertAlmostEqual(res[0]["purchase_value"], 50.0)

    def test_no_purchase_zero_value(self):
        rows = [
            make_event("2026-11-22T10:00:00Z", "view", price=99.9, user_session="s1", offset=1),
            make_event("2026-11-22T10:01:00Z", "cart", price=99.9, user_session="s1", offset=2),
        ]
        df = self._df(rows)
        daily = build_daily_metrics(df).collect()[0]
        prod = build_product_performance(df).collect()[0]
        cat = build_category_performance(df).collect()[0]
        self.assertEqual(daily["total_purchases"], 0)
        self.assertAlmostEqual(daily["purchase_value"], 0.0)
        self.assertAlmostEqual(prod["purchase_value"], 0.0)
        self.assertAlmostEqual(cat["purchase_value"], 0.0)

    def test_zero_denominator_rates_equal_zero(self):
        rows = [
            make_event("2026-11-22T10:00:00Z", "cart", user_session="s1", offset=1),
        ]
        conv = build_conversion_metrics(self._df(rows)).collect()[0]
        self.assertEqual(conv["sessions_with_views"], 0)
        self.assertEqual(conv["sessions_with_carts"], 0)
        self.assertEqual(conv["sessions_with_purchases"], 0)
        self.assertAlmostEqual(conv["view_to_cart_rate"], 0.0)
        self.assertAlmostEqual(conv["cart_to_purchase_rate"], 0.0)
        self.assertAlmostEqual(conv["view_to_purchase_rate"], 0.0)

    def test_null_category_excluded_from_category_performance(self):
        rows = [
            make_event("2026-11-22T10:00:00Z", "view", product_id=1, category_id=None, category_code=None, user_session="s1", offset=1),
            make_event("2026-11-22T10:05:00Z", "view", product_id=2, category_id="cat2", category_code="tech", user_session="s2", offset=2),
        ]
        res = build_category_performance(self._df(rows)).collect()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["category_id"], "cat2")
        self.assertEqual(res[0]["unique_products"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
