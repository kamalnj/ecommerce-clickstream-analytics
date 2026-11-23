import sys
import unittest

from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "jobs")
)

from bronze_to_silver import (
    deduplicate_events,
    transform_events,
    validate_events,
)


class TestBronzeToSilver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("TestBronzeToSilver")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def create_events(self):
        data = [
            (
                "2019-10-01 05:30:07 UTC",
                "view", 1001, "cat1", "electronics",
                "brand1", 0.0, 10, "session-1",
                "evenements_web", 0, 100
            ),
            (
                "2026-11-22T12:20:00Z",
                "purchase", 1002, "cat2", "electronics",
                "brand2", 150.0, 20, "session-2",
                "evenements_web", 0, 101
            ),
            (
                "2026-11-22T12:20:01Z",
                "invalid", 1003, "cat3", "electronics",
                "brand3", 50.0, 30, "session-3",
                "evenements_web", 0, 102
            ),
            (
                "2026-11-22T12:20:02Z",
                "cart", 1004, "cat4", "electronics",
                "brand4", -10.0, 40, "session-4",
                "evenements_web", 0, 103
            ),
        ]

        columns = [
            "event_time", "event_type", "product_id",
            "category_id", "category_code", "brand",
            "price", "user_id", "user_session",
            "topic", "partition", "offset"
        ]

        event_rows = []

        for row in data:
            event_rows.append((
                (
                    row[0], row[1], row[2], row[3],
                    row[4], row[5], row[6], row[7], row[8]
                ),
                (row[9], row[10], row[11]),
                None
            ))

        from pyspark.sql import Row

        event_schema = """
            event struct<
                event_time:string,
                event_type:string,
                product_id:long,
                category_id:string,
                category_code:string,
                brand:string,
                price:double,
                user_id:long,
                user_session:string
            >,
            _kafka struct<
                topic:string,
                partition:long,
                offset:long
            >,
            _corrupt_record string
        """

        return self.spark.createDataFrame(
            event_rows,
            schema=event_schema
        )

    def test_deduplicate_kafka_events(self):
        df = self.create_events()

        duplicated = df.unionByName(df.filter(
            df["_kafka.offset"] == 100
        ))

        result = deduplicate_events(duplicated)

        self.assertEqual(result.count(), 4)

    def test_parse_both_date_formats(self):
        df = transform_events(self.create_events())

        self.assertEqual(
            df.filter(df.event_time.isNotNull()).count(),
            4
        )

    def test_zero_price_is_kept(self):
        df = validate_events(
            transform_events(self.create_events())
        )

        zero_price = df.filter(df.price == 0)

        self.assertEqual(zero_price.count(), 1)
        self.assertEqual(
            zero_price.first()["rejection_reason"],
            ""
        )

    def test_invalid_event_is_rejected(self):
        df = validate_events(
            transform_events(self.create_events())
        )

        invalid = df.filter(
            df.rejection_reason.contains("invalid_event_type")
        )

        self.assertEqual(invalid.count(), 1)

    def test_negative_price_is_rejected(self):
        df = validate_events(
            transform_events(self.create_events())
        )

        negative = df.filter(
            df.rejection_reason.contains("negative_price")
        )

        self.assertEqual(negative.count(), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)