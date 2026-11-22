import argparse
import os
import json
import time
import pandas as pd
from confluent_kafka import Producer

ap = argparse.ArgumentParser()
ap.add_argument("--mode", choices=["fixe", "rejeu"], default="fixe",
                help="fixe : débit constant | rejeu : respecte les vrais horaires")
ap.add_argument("--debit", type=float, default=100, help="événements/s (mode fixe)")
ap.add_argument("--vitesse", type=float, default=600,
                help="facteur d'accélération (mode rejeu)")
ap.add_argument("--limite", type=int, default=1000, help="nombre d'événements")
ap.add_argument("--fichier", default="data/sample.csv")
args = ap.parse_args()

TOPIC = "evenements_web"
producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")})
confirmes = 0
erreurs = 0

def rapport(err, msg):
    global confirmes, erreurs
    if err is not None:
        erreurs += 1
    else:
        confirmes += 1

EPOCH = pd.Timestamp("1970-01-01", tz="UTC")
envoyes = 0
t0 = None              # horaire du tout premier événement (mode rejeu)
debut = time.time()

for chunk in pd.read_csv(args.fichier, chunksize=10_000):
    # Horaires en secondes (calculés AVANT de remplacer les NaN)
    secondes = (pd.to_datetime(chunk["event_time"], utc=True) - EPOCH).dt.total_seconds().tolist()
    chunk = chunk.astype(object).where(chunk.notna(), None)

    for ligne, s in zip(chunk.to_dict("records"), secondes):
        if envoyes >= args.limite:
            break

        # --- Contrôle du rythme ---
        if args.mode == "fixe":
            attente = debut + envoyes / args.debit - time.time()
        else:
            if t0 is None:
                t0 = s
            attente = debut + (s - t0) / args.vitesse - time.time()
        if attente > 0.001:
            time.sleep(attente)

        producer.produce(
            TOPIC,
            key=ligne["user_session"],
            value=json.dumps(ligne),
            on_delivery=rapport,
        )
        producer.poll(0)
        envoyes += 1

        if envoyes % 5000 == 0:
            print(f"{envoyes} envoyés...", flush=True)
    if envoyes >= args.limite:
        break

producer.flush()
duree = time.time() - debut
print(f"Envoyés : {envoyes} | Confirmés : {confirmes} | Erreurs : {erreurs}")
print(f"Durée : {duree:.1f} s | Débit moyen : {envoyes / duree:.0f} événements/s")
