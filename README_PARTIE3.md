# IL-02 — Partie 3 : Spark et MLlib

Complément proposé pour la branche `dev` du dépôt kamalnj/ecommerce-clickstream-analytics. Aucun fichier existant n'est remplacé. Les scripts s'appuient sur le schéma Silver et les fonctions `build_daily_metrics`, `build_product_performance`, `build_category_performance` de `spark/jobs/silver_to_gold.py`.

## Exécution (PowerShell, depuis la racine du dépôt)

1. Démarrer Docker Desktop. `docker compose up -d` puis `docker compose ps -a`.
2. `docker compose build spark`.
3. Sur une installation neuve seulement, `docker compose run --rm producteur` et `docker compose --profile consumer up -d consumer-hdfs`. Attendre le lag 0 sur les trois partitions avec `docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group hdfs-bronze-consumer --describe`. Ne pas rejouer si le topic contient déjà l'échantillon. Le consommateur confirme les offsets après écriture HDFS.
4. `docker exec namenode hdfs dfs -ls -R /bronze/evenements_web` puis `docker exec namenode hdfs dfs -ls /silver/evenements_web`. Réutiliser une partition Silver complète existante si disponible ; sinon exécuter le job Bronze-Silver du README principal avec un nouveau répertoire de sortie. La date d'ingestion est distincte de la date des clics (2019).
5. Lancer les tests :

```powershell
docker compose run --rm spark python -m unittest discover -s /app/tests -v
```

6. Définir `$silver` d'après la partition HDFS réellement disponible (remplacer la date exemple) et lancer le pipeline :

```powershell
$silver = 'hdfs://namenode:9000/silver/evenements_web/date=YYYY-MM-DD'
$parquet = 'hdfs://namenode:9000/partie3/parquet-v1'
docker compose run --rm spark spark-submit --master 'local[2]' /app/jobs/partie3_analytics.py --input $silver --output-root $parquet
```

7. Vérifier :

```powershell
docker compose run --rm spark spark-submit --master 'local[2]' /app/jobs/partie3_verify.py --output-root $parquet
docker exec namenode hdfs dfs -ls -R /partie3/parquet-v1
```

8. Hudi (premier téléchargement Maven nécessaire, version compatible Spark 3.5.1). Après coordination avec Anass, lancer sur un chemin séparé :

```powershell
$hudi = 'hdfs://namenode:9000/partie3/hudi-v1'
$opts = @('--master','local[2]','--packages','org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0','--conf','spark.serializer=org.apache.spark.serializer.KryoSerializer','--conf','spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog','--conf','spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension','--conf','spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar')
docker compose run --rm spark spark-submit @opts /app/jobs/partie3_analytics.py --input $silver --output-root $hudi --format hudi --skip-ml
docker compose run --rm spark spark-submit @opts /app/jobs/partie3_verify.py --output-root $hudi --format hudi --skip-ml
docker exec namenode hdfs dfs -ls -R /partie3/hudi-v1/silver/events/.hoodie
```

Le modèle est déjà sauvegardé dans la première exécution ; le second traitement ne fait que matérialiser les tables Hudi et les vérifier. Les sorties sont volontairement isolées. Une racine existante provoque une erreur afin de ne pas écraser des résultats ; choisir `v2` pour une nouvelle exécution. Ne pas effacer les volumes du groupe. L'orchestration journalière et le choix du chemin Gold définitif sont à convenir avec Walid et Anass. Cette version calcule un snapshot entier de l'historique fourni, pas un DAG incrémental. Les tables Hudi COW non partitionnées utilisent une clé stable et l'opération upsert ; la preuve d'une mise à jour sur un second commit et l'intégration au chemin officiel restent à faire avec Anass. Référence Hudi : https://hudi.apache.org/docs/0.15.0/quick-start-guide/.

## Schémas et règles

| Table | Grain | Règle |
|---|---|---|
| silver/events | événement métier | 9 champs source identiques = doublon, conserver le premier offset selon un ordre stable ; vides brand/category_code normalisés à null ; prix zéro conservé |
| silver/sessions | user_id + user_session | session complète observée, date du premier clic, durée, volumes, produits distincts, achats |
| gold/daily_metrics | event_date | compte des clics et somme des prix des événements purchase (pas de CA certifié) |
| gold/conversion_metrics | event_date | session-jour, vue puis panier puis achat, horodatages strictement croissants, taux en % ; parcours coupés à minuit et égalités de seconde non comptés |
| gold/product_performance | event_date + product_id | fonctions existantes du groupe, classement par vues ou achats |
| gold/category_performance | event_date + category_id | code inconnu si absent ; catégorie ID nulle exclue comme dans le code initial |
| gold/cohort_metrics | cohort_date + event_date | première date observée par utilisateur, utilisateurs actifs par jour, rétention, jours sans activité à 0 dans la période observée |
| ml/model | pipeline MLlib | VectorAssembler, StandardScaler, LogisticRegression |
| ml/metrics | évaluation | accuracy, précision, rappel, F1 achat, AUC ROC/PR, matrice de confusion, baseline |
| ml/predictions | session éligible du test | prédictions et probabilités |

La déduplication métier complète celle de Kafka mais peut aussi supprimer deux clics réellement distincts aux neuf champs identiques (pas d'identifiant source unique). La cohorte est la première activité *observée*, pas la date d'inscription du client. Un extrait limité à un jour ne permet pas de mesurer la rétention J+1 ; utiliser plusieurs jours pour cela. Aucun jour postérieur à la période observée n'est produit. Le tunnel mesure la progression dans la session, pas le même produit. Il n'assimile pas tous les achats à un tunnel complet.

ML : prédiction après le premier clic view/cart de la session. Variables connues à cet instant : prix, heure, type panier, présence d'une catégorie et d'une marque. Label = achat ultérieur observé ; les sessions commençant par un achat sont exclues. Séparation déterministe par utilisateur (environ 80/20), pondération de classe calculée sur le train uniquement, évaluation sur test non pondéré. Aucune variable de fin de session n'est utilisée pour prédire. Les sessions tronquées peuvent donner des faux négatifs ; pas de validation sur un futur mois avec ce petit échantillon. Les deux classes sont requises dans train et test. La probabilité issue du modèle pondéré n'est pas calibrée.

## Validation et contribution

Les scripts sont des propositions non exécutées sous Docker/Spark dans l'environnement de génération. Compilation Python uniquement vérifiée. Exécuter les tests, vérifier les journaux, sauvegarder les résultats réels et faire valider les chemins avec le groupe avant intégration. Aucun résultat numérique n'a été inventé. Ne pas ajouter les données ou modèles HDFS à Git : seulement les scripts, les tests et la documentation.

## Resultats de validation - 27/11/2026

### Environnement

- Apache Kafka 3.8.0
- Hadoop HDFS 3.5.0
- Apache Spark et PySpark 3.5.1
- Python 3.11.18
- Java 17
- NumPy 1.26.4
- Apache Hudi 0.15.0

### Ingestion et preparation des donnees

| Indicateur | Resultat |
|---|---:|
| Evenements produits | 200000 |
| Evenements confirmes par Kafka | 200000 |
| Erreurs de production | 0 |
| Lag Kafka final | 0 |
| Evenements Silver initiaux | 200000 |
| Evenements rejetes | 0 |
| Doublons metier supprimes | 138 |
| Evenements apres nettoyage | 199862 |
| Consultations | 193355 |
| Ajouts au panier | 2868 |
| Achats | 3639 |
| Sessions reconstruites | 47699 |
| Utilisateurs distincts | 38440 |

### Tunnel de conversion

| Etape | Nombre de sessions |
|---|---:|
| Consultation | 47676 |
| Consultation puis panier | 1786 |
| Consultation puis panier puis achat | 1000 |

- Consultation vers panier : 3.75 %
- Panier vers achat : 55.99 %
- Conversion globale du tunnel : 2.10 %

### Analyse des cohortes

La cohorte du 01/10/2019 contient 38440 utilisateurs.
La retention a J0 est de 100 %.
L'echantillon ne couvre qu'une seule date, ce qui ne permet
pas de mesurer la retention a J+1 et au-dela.

### Machine Learning - Regression logistique

Le modele utilise les informations disponibles au debut
de la session pour predire un achat ulterieur.

| Indicateur | Resultat |
|---|---:|
| Sessions entrainement | 38248 |
| Sessions test | 9410 |
| Accuracy | 45.73 % |
| Precision | 7.79 % |
| Recall | 66.88 % |
| F1-score | 13.95 % |
| AUC-ROC | 0.5745 |
| AUC-PR | 0.0778 |
| Vrais positifs | 414 |
| Vrais negatifs | 3889 |
| Faux positifs | 4902 |
| Faux negatifs | 205 |

Le modele est fonctionnel mais presente un pouvoir
predictif limite. La ponderation des classes favorise
le rappel au prix d'un nombre eleve de faux positifs.

### Tables Silver et Gold

| Table | Nombre de lignes |
|---|---:|
| silver/events | 199862 |
| silver/sessions | 47699 |
| gold/daily_metrics | 1 |
| gold/conversion_metrics | 1 |
| gold/product_performance | 30223 |
| gold/category_performance | 522 |
| gold/cohort_metrics | 1 |

Les sept tables ont ete creees et relues avec succes
aux formats Parquet et Apache Hudi.

### Chemins HDFS valides localement

- Source : /silver/evenements_web/date=2026-11-27
- Parquet : /partie3/parquet-v1
- Hudi : /partie3/hudi-v1
- Modele : /partie3/parquet-v1/ml/model
- Metriques ML : /partie3/parquet-v1/ml/metrics
- Predictions : /partie3/parquet-v1/ml/predictions

### Tests et validation

- Bronze vers Silver : 5/5
- Silver vers Gold : 9/9
- Partie 3 Spark et MLlib : 6/6
- Total : 20/20 tests reussis
- Duree : 29.111 secondes
- Erreurs : 0
- Echecs : 0

Le test Upsert Hudi sur une table independante confirme
la modification, l'insertion, l'absence de doublons
et la creation de deux commits.

Script : spark/jobs/partie3_hudi_upsert_demo.py

### Integration collective

Les chemins de stockage sont a coordonner avec les
modules HDFS/Hudi et Airflow/Superset.

Le traitement complet a ete valide localement.
L'orchestration quotidienne et les mises a jour
incrementales des tables analytiques restent a valider
lors de l'integration collective.
