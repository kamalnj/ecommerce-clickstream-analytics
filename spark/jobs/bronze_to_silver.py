import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)


def get_schema():
    """
    Définit explicitement la structure des événements JSON.
    Cela évite de laisser Spark deviner les types des colonnes.
    """

    event_schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),
    ])

    kafka_schema = StructType([
        StructField("topic", StringType(), True),
        StructField("partition", LongType(), True),
        StructField("offset", LongType(), True),
    ])

    return StructType([
        StructField("event", event_schema, True),
        StructField("_kafka", kafka_schema, True),
        StructField("_corrupt_record", StringType(), True),
    ])


def read_bronze(spark, input_path):
    """Lit les événements JSONL depuis HDFS Bronze."""

    return (
        spark.read
        .schema(get_schema())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(input_path)
    )


def deduplicate_events(df):
    """
    Déduplique les événements avec topic + partition + offset.
    Les événements sans métadonnées Kafka complètes sont conservés
    pour être isolés dans Rejects.
    """

    df_with_keys = (
        df
        .withColumn("_kafka_topic", F.col("_kafka.topic"))
        .withColumn("_kafka_partition", F.col("_kafka.partition"))
        .withColumn("_kafka_offset", F.col("_kafka.offset"))
    )

    has_kafka_metadata = (
        F.col("_kafka_topic").isNotNull()
        & F.col("_kafka_partition").isNotNull()
        & F.col("_kafka_offset").isNotNull()
    )

    identified = (
        df_with_keys
        .filter(has_kafka_metadata)
        .dropDuplicates([
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
        ])
        .drop(
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
        )
    )

    unidentified = (
        df_with_keys
        .filter(~has_kafka_metadata)
        .drop(
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
        )
    )

    return identified.unionByName(unidentified)

def transform_events(df):
    """Aplatit les données et convertit les dates."""

    event_time_raw = F.col("event.event_time")

    # Format original du dataset :
    # 2019-10-01 05:30:07 UTC
    original_format = F.to_timestamp(
        event_time_raw,
        "yyyy-MM-dd HH:mm:ss 'UTC'"
    )

    # Format ISO UTC des événements de test :
    # 2026-11-22T12:20:00Z
    iso_format = F.to_timestamp(
        event_time_raw,
        "yyyy-MM-dd'T'HH:mm:ssX"
    )

    parsed_event_time = F.coalesce(
        original_format,
        iso_format,
    )

    return df.select(
        F.col("event.event_time").alias("event_time_raw"),
        parsed_event_time.alias("event_time"),
        F.col("event.event_type").alias("event_type"),
        F.col("event.product_id").alias("product_id"),
        F.col("event.category_id").alias("category_id"),
        F.col("event.category_code").alias("category_code"),
        F.col("event.brand").alias("brand"),
        F.col("event.price").alias("price"),
        F.col("event.user_id").alias("user_id"),
        F.col("event.user_session").alias("user_session"),
        F.col("_kafka.topic").alias("kafka_topic"),
        F.col("_kafka.partition").alias("kafka_partition"),
        F.col("_kafka.offset").alias("kafka_offset"),
        F.col("_corrupt_record").alias("corrupt_record"),
    ).withColumn(
        "event_date",
        F.to_date("event_time"),
    )


def validate_events(df):
    """
    Ajoute un motif de rejet pour chaque événement invalide.
    Un événement qui présente plusieurs problèmes conserve
    tous ses motifs de rejet.
    """

    rejection_reasons = F.concat_ws(
        "|",
        F.array_remove(
            F.array(

                F.when(
                    F.col("corrupt_record").isNotNull(),
                    F.lit("invalid_json")
                ).otherwise(F.lit("")),

                F.when(
                    F.col("event_type").isNull()
                    | ~F.col("event_type").isin("view", "cart", "purchase"),
                    F.lit("invalid_event_type")
                ).otherwise(F.lit("")),

                F.when(
                    F.col("product_id").isNull()
                    | (F.col("product_id") <= 0),
                    F.lit("invalid_product_id")
                ).otherwise(F.lit("")),

                F.when(
                    F.col("user_id").isNull()
                    | (F.col("user_id") <= 0),
                    F.lit("invalid_user_id")
                ).otherwise(F.lit("")),

                F.when(
                    F.col("event_time").isNull(),
                    F.lit("invalid_event_time")
                ).otherwise(F.lit("")),

                F.when(
                    F.col("price").isNull(),
                    F.lit("missing_price")
                ).otherwise(F.lit("")),

                F.when(
                    F.col("price") < 0,
                    F.lit("negative_price")
                ).otherwise(F.lit("")),

                F.when(
                    F.col("user_session").isNull()
                    | (F.length(F.trim(F.col("user_session"))) == 0),
                    F.lit("missing_user_session")
                ).otherwise(F.lit("")),

                F.when(
                    F.col("kafka_topic").isNull()
                    | F.col("kafka_partition").isNull()
                    | F.col("kafka_offset").isNull(),
                    F.lit("missing_kafka_metadata")
                ).otherwise(F.lit("")),
            ),
            ""
        )
    )
    return df.withColumn(
        "rejection_reason",
        rejection_reasons,
    )



def write_outputs(df, silver_path, rejects_path):
    """Sépare les événements valides et invalides, puis écrit Parquet."""
    valid_events = df.filter(F.col("rejection_reason") == "")
    rejected_events = df.filter(F.col("rejection_reason") != "")

    silver_events = valid_events.drop(
        "corrupt_record", "rejection_reason", "event_time_raw"
    )
    rejects_events = rejected_events.drop("event_time_raw")

    print("Écriture des événements Silver...", flush=True)
    silver_events.write.mode("errorifexists").parquet(silver_path)
    print("Écriture Silver terminée.", flush=True)

    print("Écriture des événements Rejects...", flush=True)
    rejects_events.write.mode("errorifexists").parquet(rejects_path)
    print("Écriture Rejects terminée.", flush=True)


def main():
    parser = argparse.ArgumentParser(
        description="Traitement des données Bronze vers Silver"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Chemin HDFS des données Bronze"
    )
    parser.add_argument(
        "--silver-output",
        required=True,
        help="Chemin HDFS de sortie Silver"
    )
    parser.add_argument(
        "--rejects-output",
        required=True,
        help="Chemin HDFS des événements rejetés"
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("BronzeToSilver")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Lecture des données Bronze
        print("Lecture des données Bronze...", flush=True)
        bronze_df = read_bronze(spark, args.input)

        # 2. Déduplication Kafka
        print("Déduplication des événements Kafka...", flush=True)
        deduplicated_df = deduplicate_events(bronze_df).coalesce(8)

        # 3. Transformation
        print("Transformation des événements...", flush=True)
        transformed_df = transform_events(deduplicated_df)

        # 4. Validation
        print("Validation des événements...", flush=True)
        validated_df = validate_events(transformed_df).persist()

        # 5. Écriture des résultats finaux
        print("Écriture des résultats Silver et Rejects...", flush=True)

        write_outputs(
            validated_df,
            args.silver_output,
            args.rejects_output
        )

        validated_df.unpersist()

        print("Traitement Bronze vers Silver terminé.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
