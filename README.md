# Projet Big Data IL-02 - Parcours client e-commerce

## Partie 1 : collecte, Python et Kafka (Kamal Ennaji)

### Démarrer la plateforme
```bash
docker compose up -d
```
Démarre Kafka, crée le topic `evenements_web` (3 partitions) et lance l'interface web
http://localhost:8080. Aucune donnée n'est envoyée à ce stade.

### Envoyer les événements (échantillon de 200 000 événements, environ 37 s)
```bash
docker compose run --rm producteur
```
Le producteur rejoue `echantillon/sample_200k.csv` à rythme réel accéléré (vitesse 600).
Résultat attendu : `Envoyés : 200000 | Confirmés : 200000 | Erreurs : 0`.
Ne le lancer qu'une fois par topic : Kafka ajoute les messages à la suite, un second envoi crée des doublons.

### Repartir de zéro
```bash
docker compose down -v
```
Supprime les conteneurs, le topic et tous les messages.

### Autres options du producteur
```bash
docker compose run --rm producteur --fichier /donnees/sample_200k.csv --mode fixe --debit 1000 --limite 20000
```

### Contrat de données
Topic `evenements_web` : 3 partitions, clé = `user_session`, valeur = JSON.

| champ | type | remarque |
|---|---|---|
| event_time | string | ex. `2019-10-01 00:00:00 UTC`, date d'origine du clic |
| event_type | string | view, cart, purchase |
| product_id | long | |
| category_id | long | |
| category_code | string | peut être null (31,8 % sur l'échantillon de 2 M) |
| brand | string | peut être null (14,9 %) |
| price | double | 3 433 valeurs à 0 sur 2 M |
| user_id | long | |
| user_session | string | |

Adresse du broker : `localhost:9092` depuis l'hôte, `kafka:19092` depuis un conteneur du même réseau Docker.

### Données
Le dataset complet (14 Go) n'est pas dans Git et n'est utile que pour le producteur.
Un échantillon de 200 000 événements est fourni dans `echantillon/sample_200k.csv` :
les autres membres n'ont pas besoin de télécharger le dataset Kaggle.
