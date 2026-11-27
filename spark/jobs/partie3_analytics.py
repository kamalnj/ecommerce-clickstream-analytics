"""IL-02 - Partie 3 : analyses Spark SQL, MLlib et stockage Gold/Hudi."""
import argparse
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from silver_to_gold import (build_daily_metrics, build_product_performance,
                            build_category_performance)

BUSINESS = ['event_time','event_type','product_id','category_id','category_code',
            'brand','price','user_id','user_session']
FEATURES = ['price','hour','is_cart','known_category','known_brand']


def clean_events(df):
    # Silver upstream already validates the types and deduplicates Kafka offsets.
    for c in ('category_code', 'brand'):
        df = df.withColumn(c, F.when(F.length(F.trim(c)) > 0, F.trim(c)))
    df = df.filter(F.col('event_time').isNotNull() & F.col('event_type').isin('view','cart','purchase')
                   & (F.col('price') >= 0) & ~F.isnan('price') & (F.col('price') < float('inf'))
                   & (F.col('user_id') > 0) & (F.col('product_id') > 0)
                   & (F.length(F.trim('user_session')) > 0))
    w = Window.partitionBy(*BUSINESS).orderBy('kafka_topic','kafka_partition','kafka_offset')
    return (df.withColumn('_rank', F.row_number().over(w)).filter('_rank = 1')
            .drop('_rank').withColumn('event_date', F.to_date('event_time')))


def sessions_sql(events):
    events.createOrReplaceTempView('click_events')
    return events.sparkSession.sql('''
        SELECT user_id, user_session, MIN(event_time) AS session_start,
               MAX(event_time) AS session_end, TO_DATE(MIN(event_time)) AS event_date,
               UNIX_TIMESTAMP(MAX(event_time))-UNIX_TIMESTAMP(MIN(event_time)) AS duration_seconds,
               COUNT(*) AS total_events, COUNT(DISTINCT product_id) AS unique_products,
               SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END) AS total_views,
               SUM(CASE WHEN event_type='cart' THEN 1 ELSE 0 END) AS total_carts,
               SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) AS total_purchases,
               MAX(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) AS has_purchase,
               ROUND(SUM(CASE WHEN event_type='purchase' THEN price ELSE 0 END),2) AS purchase_value
        FROM click_events GROUP BY user_id, user_session
    ''')


def conversion_sql(events):
    # Same daily grain and strict timestamp rule as existing Gold, but SQL and user_id included.
    events.createOrReplaceTempView('click_events')
    return events.sparkSession.sql('''
        WITH first_views AS (
          SELECT event_date,user_id,user_session, MIN(event_time) AS view_ts
          FROM click_events WHERE event_type='view' GROUP BY event_date,user_id,user_session
        ), first_carts AS (
          SELECT e.event_date,e.user_id,e.user_session,MIN(e.event_time) AS cart_ts
          FROM click_events e JOIN first_views v USING(event_date,user_id,user_session)
          WHERE e.event_type='cart' AND e.event_time>v.view_ts
          GROUP BY e.event_date,e.user_id,e.user_session
        ), first_purchases AS (
          SELECT e.event_date,e.user_id,e.user_session,MIN(e.event_time) AS purchase_ts
          FROM click_events e JOIN first_carts c USING(event_date,user_id,user_session)
          WHERE e.event_type='purchase' AND e.event_time>c.cart_ts
          GROUP BY e.event_date,e.user_id,e.user_session
        ), counts AS (
          SELECT s.event_date, COUNT(v.view_ts) AS sessions_with_views,
                 COUNT(c.cart_ts) AS sessions_with_carts,
                 COUNT(p.purchase_ts) AS sessions_with_purchases
          FROM (SELECT DISTINCT event_date,user_id,user_session FROM click_events) s
          LEFT JOIN first_views v USING(event_date,user_id,user_session)
          LEFT JOIN first_carts c USING(event_date,user_id,user_session)
          LEFT JOIN first_purchases p USING(event_date,user_id,user_session)
          GROUP BY s.event_date
        ) SELECT *,
          CASE WHEN sessions_with_views>0 THEN ROUND(100.0*sessions_with_carts/sessions_with_views,2) ELSE 0.0 END AS view_to_cart_rate,
          CASE WHEN sessions_with_carts>0 THEN ROUND(100.0*sessions_with_purchases/sessions_with_carts,2) ELSE 0.0 END AS cart_to_purchase_rate,
          CASE WHEN sessions_with_views>0 THEN ROUND(100.0*sessions_with_purchases/sessions_with_views,2) ELSE 0.0 END AS view_to_purchase_rate
        FROM counts
    ''')


def cohorts_sql(events):
    events.createOrReplaceTempView('click_events')
    return events.sparkSession.sql('''
        WITH activity AS (SELECT DISTINCT user_id,event_date FROM click_events),
        first_seen AS (SELECT user_id,MIN(event_date) AS cohort_date FROM activity GROUP BY user_id),
        sizes AS (SELECT cohort_date,COUNT(*) AS cohort_size FROM first_seen GROUP BY cohort_date),
        active AS (SELECT f.cohort_date,a.event_date,COUNT(*) AS active_users
          FROM activity a JOIN first_seen f USING(user_id) GROUP BY f.cohort_date,a.event_date),
        last_day AS (SELECT MAX(event_date) AS last_date FROM activity),
        calendar AS (SELECT cohort_date,cohort_size,
          EXPLODE(SEQUENCE(cohort_date,last_date,INTERVAL 1 DAY)) AS event_date
          FROM sizes CROSS JOIN last_day)
        SELECT c.cohort_date,c.event_date,DATEDIFF(c.event_date,c.cohort_date) AS day_number,
               c.cohort_size,COALESCE(a.active_users,0) AS active_users,
               ROUND(100.0*COALESCE(a.active_users,0)/c.cohort_size,2) AS retention_rate
        FROM calendar c LEFT JOIN active a USING(cohort_date,event_date)
    ''')


def ml_examples(events):
    # Predict after the first observed non-purchase click, not at the end of a session.
    w = Window.partitionBy('user_id','user_session').orderBy('event_time','kafka_partition','kafka_offset')
    ranked = events.withColumn('rn',F.row_number().over(w))
    labels = ranked.groupBy('user_id','user_session').agg(
        F.max(F.when((F.col('rn')>1)&(F.col('event_type')=='purchase'),1.0).otherwise(0.0)).alias('label'))
    return (ranked.filter((F.col('rn')==1)&F.col('event_type').isin('view','cart'))
            .join(labels,['user_id','user_session'])
            .select('user_id','user_session','label','price',F.hour('event_time').cast('double').alias('hour'),
                    (F.col('event_type')=='cart').cast('double').alias('is_cart'),
                    F.col('category_code').isNotNull().cast('double').alias('known_category'),
                    F.col('brand').isNotNull().cast('double').alias('known_brand')))


def split_users(df):
    bucket = F.pmod(F.xxhash64('user_id'),F.lit(10))
    return df.filter(bucket < 8),df.filter(bucket >= 8)


def train_model(events):
    examples = ml_examples(events).cache()
    train,test = split_users(examples)
    train,test = train.cache(),test.cache()
    counts = {float(r['label']):r['count'] for r in train.groupBy('label').count().collect()}
    test_counts = {float(r['label']):r['count'] for r in test.groupBy('label').count().collect()}
    if set(counts) != {0.,1.} or set(test_counts) != {0.,1.}:
        raise ValueError('Les deux classes doivent exister dans train et test. Utiliser un extrait plus grand.')
    n = sum(counts.values())
    train = train.withColumn('weight', F.when(F.col('label')==1., n/(2*counts[1.])).otherwise(n/(2*counts[0.])))
    pipe = Pipeline(stages=[VectorAssembler(inputCols=FEATURES,outputCol='features_raw'),
        StandardScaler(inputCol='features_raw',outputCol='features',withStd=True,withMean=False),
        LogisticRegression(featuresCol='features',labelCol='label',weightCol='weight',maxIter=50,regParam=0.01)])
    model = pipe.fit(train)
    pred = model.transform(test).cache()
    cm = {(float(r['label']),float(r['prediction'])):r['count'] for r in pred.groupBy('label','prediction').count().collect()}
    tn,fp,fn,tp = (cm.get(k,0) for k in [(0.,0.),(0.,1.),(1.,0.),(1.,1.)])
    div = lambda a,b: float(a/b) if b else 0.0
    precision,recall = div(tp,tp+fp),div(tp,tp+fn)
    metrics = {'train_size':int(n),'test_size':int(sum(test_counts.values())),
        'test_positives':int(test_counts[1.]),'tn':tn,'fp':fp,'fn':fn,'tp':tp,
        'accuracy':div(tp+tn,tp+tn+fp+fn),'precision_purchase':precision,'recall_purchase':recall,
        'f1_purchase':div(2*precision*recall,precision+recall),
        'baseline_accuracy':div(test_counts[0.],sum(test_counts.values())),
        'baseline_pr_auc':div(test_counts[1.],sum(test_counts.values())),
        'roc_auc':BinaryClassificationEvaluator(metricName='areaUnderROC').evaluate(pred),
        'pr_auc':BinaryClassificationEvaluator(metricName='areaUnderPR').evaluate(pred)}
    return model,events.sparkSession.createDataFrame([metrics]),pred.select('user_id','user_session','label','prediction',F.col('probability'))


def write_table(df,path,name,keys,fmt):
    if fmt == 'parquet':
        writer = df.write.mode('errorifexists')
        if 'event_date' in df.columns:
            writer = writer.partitionBy('event_date')
        writer.parquet(path)
    else:
        # Non-partitioned COW tables (small demo dataset); deterministic keys for upsert.
        df = df.withColumn('record_key',F.sha2(F.to_json(F.struct(*keys),{'ignoreNullFields':'false'}),256))
        df = df.withColumn('updated_at',F.lit(1).cast('long'))
        (df.write.format('hudi').mode('append')
            .option('hoodie.table.name',name)
            .option('hoodie.datasource.write.table.type','COPY_ON_WRITE')
            .option('hoodie.datasource.write.operation','upsert')
            .option('hoodie.datasource.write.recordkey.field','record_key')
            .option('hoodie.datasource.write.precombine.field','updated_at')
            .option('hoodie.datasource.write.keygenerator.class','org.apache.hudi.keygen.NonpartitionedKeyGenerator')
            .option('hoodie.upsert.shuffle.parallelism','4')
            .option('hoodie.insert.shuffle.parallelism','4')
            .option('hoodie.metadata.enable','false').save(path))
    print(f'[OK] {path}',flush=True)


def check_free(spark,path):
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    if p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p):
        raise RuntimeError(f'Sortie existante, utiliser un nouveau --output-root : {path}')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input',required=True,help='Snapshot Silver Parquet existant')
    p.add_argument('--output-root',required=True)
    p.add_argument('--format',choices=['parquet','hudi'],default='parquet')
    p.add_argument('--skip-ml',action='store_true',help='Ne lancer que les analyses')
    args = p.parse_args()
    root = args.output_root.rstrip('/')
    spark = (SparkSession.builder.appName('IL02-Partie3').config('spark.sql.session.timeZone','UTC')
             .config('spark.sql.shuffle.partitions','8').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        check_free(spark,root)
        raw = spark.read.parquet(args.input)
        raw.printSchema()
        events = clean_events(raw).cache()
        before,after = raw.count(),events.count()
        if not after:
            raise ValueError('Aucun evenement valide')
        print(f'Silver avant={before} ; apres nettoyage metier={after}',flush=True)
        events.groupBy('event_type').count().show()
        sessions = sessions_sql(events).cache()
        daily = build_daily_metrics(events)
        funnel = conversion_sql(events)
        products = build_product_performance(events)
        categories = build_category_performance(events).withColumn('category_code',F.coalesce('category_code',F.lit('inconnue')))
        cohorts = cohorts_sql(events)
        # Prepare ML before writes: fail early if dataset is insufficient.
        ml = None if args.skip_ml else train_model(events)
        tables = [ ('silver','events',events,BUSINESS),
            ('silver','sessions',sessions,['user_id','user_session']),
            ('gold','daily_metrics',daily,['event_date']),('gold','conversion_metrics',funnel,['event_date']),
            ('gold','product_performance',products,['event_date','product_id']),
            ('gold','category_performance',categories,['event_date','category_id']),
            ('gold','cohort_metrics',cohorts,['cohort_date','event_date']) ]
        for zone,name,df,keys in tables:
            write_table(df,f'{root}/{zone}/{name}',f'il02_{name}',keys,args.format)
        print('SESSIONS',flush=True); sessions.show(10,False)
        print('TUNNEL',flush=True); funnel.show(20,False)
        print('COHORTES',flush=True); cohorts.orderBy('cohort_date','day_number').show(20,False)
        print('PRODUITS POPULAIRES',flush=True); products.orderBy(F.desc('total_views')).show(10,False)
        if ml:
            model,metrics,pred = ml
            model.write().save(f'{root}/ml/model')
            metrics.write.mode('errorifexists').parquet(f'{root}/ml/metrics')
            # VectorUDT saved as Parquet for Spark readers, not intended for dashboard.
            pred.write.mode('errorifexists').parquet(f'{root}/ml/predictions')
            metrics.show(vertical=True,truncate=False)
        print('[OK] Partie 3 : traitement termine',flush=True)
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
