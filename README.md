# Projet Big Data IL-02 - Parcours client e-commerce

## Partie 1 : collecte, Python et Kafka (Kamal Ennaji)

### Lancer Kafka
```bash
docker compose up -d
./reset_topic.sh        # (re)crée le topic evenements_web (3 partitions)
```
Interface web : http://localhost:8080

### Envoyer les événements
```bash
python producer_flux.py --mode fixe  --debit 1000 --limite 20000
python producer_flux.py --mode rejeu --vitesse 600 --limite 200000
```
- `fixe` : débit constant (tests).
- `rejeu` : respecte les écarts réels entre les `event_time`, accélérés par `--vitesse`.

### Contrat de données
Topic `evenements_web` : 3 partitions, clé = `user_session`, valeur = JSON.

| champ | type | remarque |
|---|---|---|
| event_time | string | ex. `2019-10-01 00:00:00 UTC`, date d'origine du clic |
| event_type | string | view, cart, purchase |
| product_id | long | |
| category_id | long | |
| category_code | string | peut être null |
| brand | string | peut être null |
| price | double | |
| user_id | long | |
| user_session | string | |

Adresse du broker : `localhost:9092` depuis l'hôte, `kafka:19092` depuis un conteneur du même réseau Docker.

### Données
Le dossier `data/` n'est pas dans Git. Télécharger le dataset sur Kaggle (REES46, `2019-Oct.csv`),
puis créer l'échantillon : `head -n 2000001 data/2019-Oct.csv > data/sample.csv`.
