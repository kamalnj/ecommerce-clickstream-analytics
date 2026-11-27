from pyspark.sql import SparkSession
from uuid import uuid4

spark = SparkSession.builder.appName(
    "IL02-Hudi-Upsert-Test"
).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

path = "hdfs://namenode:9000/partie3/tests/upsert-" + uuid4().hex[:8]

options = {
    "hoodie.table.name": "test_upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "version",
    "hoodie.datasource.write.keygenerator.class":
        "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    "hoodie.upsert.shuffle.parallelism": "2",
    "hoodie.insert.shuffle.parallelism": "2",
    "hoodie.metadata.enable": "false"
}

def write(df):
    df.write.format("hudi").options(**options).mode("append").save(path)

try:
    initial = spark.createDataFrame(
        [(1, 100.0, 1), (2, 200.0, 1)],
        ["id", "price", "version"]
    )

    write(initial)

    print("AVANT UPSERT")
    avant = spark.read.format("hudi").load(path)
    avant.select("id", "price", "version").orderBy("id").show()

    assert avant.count() == 2

    update = spark.createDataFrame(
        [(1, 150.0, 2), (3, 300.0, 1)],
        ["id", "price", "version"]
    )

    write(update)

    print("APRES UPSERT")
    apres = spark.read.format("hudi").load(path)

    apres.select(
        "id", "price", "version"
    ).orderBy("id").show()

    assert apres.count() == 3
    assert apres.select("id").distinct().count() == 3
    assert apres.filter("id = 1").first()["price"] == 150.0

    print("HISTORIQUE DES COMMITS")

    commits = apres.select("_hoodie_commit_time").distinct()
    commits.show(truncate=False)

    assert commits.count() == 2

    print("[OK] Mise a jour validee")
    print("[OK] Insertion validee")
    print("[OK] Aucun doublon")
    print("[OK] Deux commits Hudi")
    print("Table de test :", path)

finally:
    spark.stop()
