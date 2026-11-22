import json
import logging
import os
import signal
import time
import uuid

from datetime import datetime, timezone

import requests
from confluent_kafka import Consumer, KafkaException, TopicPartition


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "evenements_web")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "hdfs-bronze-consumer")

HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "hadoop")
HDFS_BASE_PATH = "/bronze/evenements_web"

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", "10"))

POLL_TIMEOUT = 0.5

RUNNING = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger("consumer-hdfs")


# --------------------------------------------------
# ARRET PROPRE
# --------------------------------------------------

def stop_consumer(signum, frame):
    global RUNNING
    logger.info("Signal d'arret recu. Preparation de l'arret...")
    RUNNING = False


signal.signal(signal.SIGTERM, stop_consumer)
signal.signal(signal.SIGINT, stop_consumer)


# --------------------------------------------------
# WEBHDFS
# --------------------------------------------------

session = requests.Session()


def create_hdfs_directory(path):
    url = f"{HDFS_URL}/webhdfs/v1{path}"

    response = session.put(
        url,
        params={
            "op": "MKDIRS",
            "user.name": HDFS_USER
        },
        timeout=30
    )

    response.raise_for_status()

    result = response.json()

    if not result.get("boolean", False):
        raise RuntimeError(f"Impossible de creer le dossier HDFS : {path}")


def write_hdfs_file(path, content):
    url = f"{HDFS_URL}/webhdfs/v1{path}"

    # Premiere requete : demander au NameNode de creer le fichier.
    response = session.put(
        url,
        params={
            "op": "CREATE",
            "overwrite": "false",
            "user.name": HDFS_USER
        },
        allow_redirects=False,
        timeout=30
    )

    if response.status_code != 307:
        raise RuntimeError(
            f"Redirection WebHDFS inattendue : "
            f"HTTP {response.status_code} - {response.text}"
        )

    datanode_url = response.headers.get("Location")

    if not datanode_url:
        raise RuntimeError("URL du DataNode absente de la reponse WebHDFS")

    # Deuxieme requete : envoyer les donnees au DataNode.
    response = session.put(
        datanode_url,
        data=content,
        headers={"Content-Type": "application/octet-stream"},
        timeout=(30, 120)
    )

    if response.status_code != 201:
        raise RuntimeError(
            f"Ecriture HDFS echouee : "
            f"HTTP {response.status_code} - {response.text}"
        )


# --------------------------------------------------
# TRANSFORMATION ENVELOPE
# --------------------------------------------------

def prepare_event(message):
    raw_value = message.value()

    try:
        event = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        event = {
            "_raw": raw_value.decode("utf-8", errors="replace"),
            "_parse_error": True
        }

    return {
        "event": event,
        "_kafka": {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset()
        }
    }


# --------------------------------------------------
# ECRITURE PAR LOTS
# --------------------------------------------------

def flush_batch(consumer, batch):
    if not batch:
        return

    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    directory = f"{HDFS_BASE_PATH}/date={ingestion_date}"

    filename = (
        f"events_{datetime.now(timezone.utc).strftime('%H%M%S')}_"
        f"{uuid.uuid4().hex}.jsonl"
    )

    hdfs_path = f"{directory}/{filename}"

    content = "\n".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        for record in batch
    ) + "\n"

    payload = content.encode("utf-8")

    # 1. Ecrire le lot dans HDFS.
    create_hdfs_directory(directory)
    write_hdfs_file(hdfs_path, payload)

    # 2. Calculer les offsets exacts du lot par partition Kafka.
    max_offsets = {}

    for record in batch:
        metadata = record["_kafka"]

        key = (metadata["topic"], metadata["partition"])
        next_offset = metadata["offset"] + 1

        max_offsets[key] = max(
            max_offsets.get(key, 0),
            next_offset
        )

    offsets = [
        TopicPartition(topic, partition, offset)
        for (topic, partition), offset in max_offsets.items()
    ]

    # 3. Confirmer uniquement les offsets de ce lot.
    committed = consumer.commit(
        offsets=offsets,
        asynchronous=False
    )

    if committed is None:
        raise RuntimeError("Le commit Kafka n'a retourne aucun resultat")

    for partition in committed:
        if partition.error:
            raise RuntimeError(
                f"Erreur commit Kafka : {partition.error}"
            )

    logger.info(
        "Lot enregistre : %s evenements | %s | %s octets",
        len(batch),
        hdfs_path,
        len(payload)
    )

# --------------------------------------------------
# CONSOMMATEUR PRINCIPAL
# --------------------------------------------------

def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": False,
        "enable.auto.offset.store": False
    })

    batch = []
    batch_started = time.monotonic()

    try:
        consumer.subscribe([KAFKA_TOPIC])

        logger.info("Consumer demarre")
        logger.info("Kafka : %s", KAFKA_BOOTSTRAP)
        logger.info("Topic : %s", KAFKA_TOPIC)
        logger.info("Consumer group : %s", KAFKA_GROUP_ID)
        logger.info("Destination : %s", HDFS_BASE_PATH)

        while RUNNING:
            message = consumer.poll(POLL_TIMEOUT)

            if message is not None:
                if message.error():
                    raise KafkaException(message.error())

                batch.append(prepare_event(message))


            elapsed = time.monotonic() - batch_started

            if batch and (
                len(batch) >= BATCH_SIZE
                or elapsed >= BATCH_TIMEOUT
                or not RUNNING
            ):
                flush_batch(consumer, batch)

                batch.clear()
                batch_started = time.monotonic()

        # Traiter les derniers evenements avant l'arret.
        if batch:
            flush_batch(consumer, batch)
            batch.clear()

    finally:
        consumer.close()
        session.close()
        logger.info("Consumer arrete")


if __name__ == "__main__":
    main()
