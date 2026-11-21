import json
import pandas as pd
from confluent_kafka import Producer

TOPIC = "evenements_web"
LIMITE = 1000

producer = Producer({"bootstrap.servers": "localhost:9092"})
confirmes = 0
erreurs = 0

def rapport(err, msg):
    """Appelée par Kafka pour chaque message : confirmé ou en erreur."""
    global confirmes, erreurs
    if err is not None:
        erreurs += 1
    else:
        confirmes += 1

envoyes = 0
for chunk in pd.read_csv("data/sample.csv", chunksize=10_000):
    chunk = chunk.astype(object).where(chunk.notna(), None)
    for ligne in chunk.to_dict("records"):
        if envoyes >= LIMITE:
            break
        producer.produce(
            TOPIC,
            key=ligne["user_session"],
            value=json.dumps(ligne),
            on_delivery=rapport,
        )
        producer.poll(0)
        envoyes += 1
    if envoyes >= LIMITE:
        break

producer.flush()
print(f"Envoyés : {envoyes} | Confirmés : {confirmes} | Erreurs : {erreurs}")
