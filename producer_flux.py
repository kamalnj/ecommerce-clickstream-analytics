import json
import time
import pandas as pd
from confluent_kafka import Producer

TOPIC = "evenements_web"
LIMITE = 1000
DEBIT = 100          # événements par seconde

producer = Producer({"bootstrap.servers": "localhost:9092"})
confirmes = 0
erreurs = 0

def rapport(err, msg):
    global confirmes, erreurs
    if err is not None:
        erreurs += 1
    else:
        confirmes += 1

envoyes = 0
debut = time.time()

for chunk in pd.read_csv("data/sample.csv", chunksize=10_000):
    chunk = chunk.astype(object).where(chunk.notna(), None)
    for ligne in chunk.to_dict("records"):
        if envoyes >= LIMITE:
            break

        # Contrôle du débit : le message n doit partir à debut + n/DEBIT
        heure_prevue = debut + envoyes / DEBIT
        attente = heure_prevue - time.time()
        if attente > 0:
            time.sleep(attente)

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
duree = time.time() - debut
print(f"Envoyés : {envoyes} | Confirmés : {confirmes} | Erreurs : {erreurs}")
print(f"Durée : {duree:.1f} s | Débit réel : {envoyes / duree:.0f} événements/s")
