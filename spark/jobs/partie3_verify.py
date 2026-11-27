"""Read back the actual results, check key invariants and reload ML model."""
import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel

p=argparse.ArgumentParser()
p.add_argument('--output-root',required=True)
p.add_argument('--format',choices=['parquet','hudi'],default='parquet')
p.add_argument('--skip-ml',action='store_true')
a=p.parse_args()
spark=SparkSession.builder.appName('IL02-Partie3-Verification').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
root=a.output_root.rstrip('/')
def read(zone,name):
    d=spark.read.format(a.format).load(f'{root}/{zone}/{name}')
    print(f'{zone}/{name} : {d.count()} lignes',flush=True)
    return d
try:
    events=read('silver','events'); sessions=read('silver','sessions')
    daily=read('gold','daily_metrics'); funnel=read('gold','conversion_metrics')
    read('gold','product_performance'); read('gold','category_performance'); cohort=read('gold','cohort_metrics')
    assert events.count()==daily.agg(F.sum('total_events')).first()[0]
    assert events.count()==sessions.agg(F.sum('total_events')).first()[0]
    assert funnel.filter('sessions_with_views < sessions_with_carts OR sessions_with_carts < sessions_with_purchases').count()==0
    assert cohort.filter('active_users > cohort_size OR retention_rate < 0 OR retention_rate > 100').count()==0
    assert cohort.filter('day_number = 0 AND active_users != cohort_size').count()==0
    if a.format=='hudi':
        assert '_hoodie_commit_time' in events.columns
        events.select('_hoodie_commit_time').distinct().show()
    if not a.skip_ml:
        PipelineModel.load(f'{root}/ml/model')
        m=spark.read.parquet(f'{root}/ml/metrics').first().asDict()
        assert m['tp']+m['tn']+m['fp']+m['fn']==m['test_size']
        print(m,flush=True)
    print('[OK] Verifications de coherence terminees.',flush=True)
finally:
    spark.stop()
