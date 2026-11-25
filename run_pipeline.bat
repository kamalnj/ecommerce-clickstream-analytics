@echo off
setlocal enabledelayedexpansion

echo ======================================================================
echo    ECOMMERCE CLICKSTREAM ANALYTICS - PIPELINE END-TO-END
echo    (Bronze -^> Silver -^> Gold)
echo ======================================================================
echo.

:: 1. Verifier que Docker est demarre
docker info >nul 2>&1
if errorlevel 1 (
    echo [ERREUR] Docker n'est pas demarre. Lancez Docker Desktop puis recommencez.
    exit /b 1
)

:: 2. Recuperer la date du jour au format UTC (YYYY-MM-DD)
for /f "usebackq tokens=*" %%i in (`powershell -NoProfile -Command "(Get-Date).ToUniversalTime().ToString('yyyy-MM-dd')"`) do set TODAY=%%i
echo [INFO] Date d'ingestion (UTC) : %TODAY%
echo.

:: 3. Demarrage de la plateforme de base (Kafka, Zookeeper, NameNode, DataNode, UI)
echo ======================================================================
echo [1/6] Demarrage des services d'infrastructure (Kafka, HDFS)...
echo ======================================================================
docker compose up -d
if errorlevel 1 (
    echo [ERREUR] Echec lors du lancement de Docker Compose.
    exit /b 1
)

echo En attente de la disponibilite de HDFS et Kafka...
:wait_namenode
docker exec namenode hdfs dfsadmin -report >nul 2>&1
if errorlevel 1 (
    timeout /t 3 /nobreak >nul
    goto wait_namenode
)
echo [OK] HDFS et Kafka sont prets.
echo.

:: 4. Construction de l'image Docker Spark (si premiere execution)
echo ======================================================================
echo [2/6] Construction / verification de l'image Docker Spark...
echo ======================================================================
docker compose build spark
if errorlevel 1 (
    echo [ERREUR] Impossible de construire l'image Spark.
    exit /b 1
)
echo [OK] Image Spark prete.
echo.

:: 5. Envoi des 200 000 evenements dans Kafka via le producteur
echo ======================================================================
echo [3/6] Envoi des 200 000 evenements vers Kafka (topic evenements_web)...
echo ======================================================================
docker compose run --rm producteur
if errorlevel 1 (
    echo [ERREUR] Echec lors de la production des evenements Kafka.
    exit /b 1
)
echo [OK] Evenements envoyes avec succes a Kafka.
echo.

:: 6. Demarrage du consommateur Kafka vers HDFS Bronze
echo ======================================================================
echo [4/6] Demarrage du consommateur Kafka -^> HDFS Bronze...
echo ======================================================================
docker compose --profile consumer up -d consumer-hdfs
if errorlevel 1 (
    echo [ERREUR] Impossible de demarrer le consommateur HDFS.
    exit /b 1
)

echo Ingestion en cours dans HDFS (/bronze/evenements_web/)...
echo En attente que le consommateur ait traite tous les messages Kafka...
:wait_consumer_lag
timeout /t 5 /nobreak >nul
powershell -NoProfile -Command "$res = docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group hdfs-bronze-consumer --describe 2>$null; if ($res -match 'evenements_web') { $lag = ($res | Select-String 'evenements_web' | ForEach-Object { [int]($_ -split '\s+')[5] } | Measure-Object -Sum).Sum; if ($lag -eq 0) { exit 0 } else { exit 1 } } else { exit 1 }"
if errorlevel 1 (
    goto wait_consumer_lag
)
echo [OK] Ingestion Bronze terminee (Lag Kafka = 0).
echo.

:: 7. Execution du pipeline Bronze vers Silver
echo ======================================================================
echo [5/6] Execution du job Spark Bronze -^> Silver...
echo ======================================================================
docker compose run --rm spark spark-submit ^
  --master "local[2]" ^
  /app/jobs/bronze_to_silver.py ^
  --input "hdfs://namenode:9000/bronze/evenements_web/*/*.jsonl" ^
  --silver-output "hdfs://namenode:9000/silver/evenements_web/date=%TODAY%" ^
  --rejects-output "hdfs://namenode:9000/rejects/evenements_web/date=%TODAY%"
if errorlevel 1 (
    echo [ERREUR] Echec du pipeline Bronze vers Silver.
    exit /b 1
)
echo [OK] Traitement Silver termine.
echo.

:: 8. Execution du pipeline Silver vers Gold
echo ======================================================================
echo [6/6] Execution du job Spark Silver -^> Gold...
echo ======================================================================
docker compose run --rm spark spark-submit ^
  --master "local[2]" ^
  /app/jobs/silver_to_gold.py ^
  --input "hdfs://namenode:9000/silver/evenements_web/date=%TODAY%" ^
  --output-dir "hdfs://namenode:9000/gold/evenements_web"
if errorlevel 1 (
    echo [ERREUR] Echec du pipeline Silver vers Gold.
    exit /b 1
)
echo [OK] Traitement Gold termine.
echo.

:: 9. Affichage final des tables HDFS Gold
echo ======================================================================
echo [SUCCES] Pipeline execute avec succes jusqu'a la couche Gold !
echo ======================================================================
echo.
echo Tables Parquet disponibles dans HDFS :
docker exec namenode hdfs dfs -ls -R /gold/evenements_web
echo.
echo Vous pouvez maintenant entamer l'etape suivante sur les donnees Gold.
echo ======================================================================

pause
