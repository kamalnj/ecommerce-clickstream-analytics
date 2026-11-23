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

## Partie 2 : Ingestion HDFS Bronze et traitement Spark Silver (Anass El Rhattas)

Cette partie commence après l'exécution de la Partie 1. Kafka, HDFS et les autres services définis dans Docker Compose sont déjà démarrés, et le topic `evenements_web` contient les événements e-commerce.

### 1. Démarrer le consommateur Kafka → HDFS

Activer le profil `consumer` pour démarrer le service responsable de la lecture des événements Kafka et de leur stockage dans HDFS Bronze :

```bash
docker compose --profile consumer up -d consumer-hdfs
```

Consulter les logs du consommateur :

```bash
docker compose logs -f consumer-hdfs
```

### 2. Vérifier les données dans HDFS Bronze

Les événements sont stockés au format JSONL dans :

```text
/bronze/evenements_web/date=YYYY-MM-DD/
```

Afficher les fichiers disponibles :

```bash
docker exec namenode hdfs dfs -ls -R /bronze/evenements_web
```

Attendre que les événements souhaités soient ingérés et que les fichiers soient terminés avant de lancer Spark.

### 3. Construire l'image Docker Spark

À effectuer lors de la première utilisation ou après une modification du Dockerfile ou des dépendances :

```bash
docker compose build spark
```

### 4. Exécuter les tests unitaires

```bash
docker compose run --rm spark python -m unittest discover -s /app/tests -v
```

Résultat attendu :

```text
Ran 5 tests

OK
```

### 5. Exécuter le pipeline Bronze → Silver

```bash
docker compose run --rm spark spark-submit --master "local[2]" /app/jobs/bronze_to_silver.py --input "hdfs://namenode:9000/bronze/evenements_web/*/*.jsonl" --silver-output "hdfs://namenode:9000/silver/evenements_web/date=YYYY-MM-DD" --rejects-output "hdfs://namenode:9000/rejects/evenements_web/date=YYYY-MM-DD"
```

Remplacer `YYYY-MM-DD` par la date de traitement souhaitée. Vérifier que les répertoires de sortie n'existent pas déjà avant de lancer le traitement.

### 6. Vérifier les résultats

Données Silver :

```bash
docker exec namenode hdfs dfs -ls -R /silver/evenements_web
```

Données rejetées :

```bash
docker exec namenode hdfs dfs -ls -R /rejects/evenements_web
```

Les données Silver et Rejects sont enregistrées au format Parquet.
