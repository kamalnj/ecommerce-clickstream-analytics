import sys
import unittest
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'jobs'))
from pyspark.sql import SparkSession, functions as F
from partie3_analytics import clean_events, sessions_sql, conversion_sql, cohorts_sql, ml_examples, split_users

class TestPartie3(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[2]').appName('TestPartie3')
                     .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        def row(day,second,event,user,session,offset):
            return (datetime(2019,10,day,0,0,second),event,1,'10','electronics','brand',10.0,user,session,'evenements_web',0,offset)
        raw = cls.spark.createDataFrame([
            row(1,1,'view',1,'a',0),row(1,1,'view',1,'a',1), # same business event
            row(1,2,'cart',1,'a',2),row(1,3,'purchase',1,'a',3),
            row(2,1,'view',1,'b',4),row(1,1,'purchase',2,'c',5),
            row(1,1,'view',3,'d',6),row(1,2,'purchase',3,'d',7),
            row(1,1,'view',4,'e',8),row(1,1,'cart',4,'e',9),
        ], 'event_time timestamp,event_type string,product_id long,category_id string,category_code string,brand string,price double,user_id long,user_session string,kafka_topic string,kafka_partition long,kafka_offset long')
        cls.events=clean_events(raw).cache()
    @classmethod
    def tearDownClass(cls):
        cls.events.unpersist(); cls.spark.stop()
    def test_business_duplicates(self):
        self.assertEqual(self.events.count(),9)
        self.assertEqual(self.events.filter('kafka_offset=1').count(),0)
    def test_sessions(self):
        a=sessions_sql(self.events).filter("user_session='a'").first()
        self.assertEqual((a.total_events,a.duration_seconds,a.has_purchase),(3,2,1))
    def test_funnel(self):
        r=conversion_sql(self.events).filter("event_date='2019-10-01'").first()
        self.assertEqual((r.sessions_with_views,r.sessions_with_carts,r.sessions_with_purchases),(3,1,1))
    def test_cohort(self):
        r=cohorts_sql(self.events).filter("cohort_date='2019-10-01' AND day_number=1").first()
        self.assertEqual((r.cohort_size,r.active_users),(4,1))
        self.assertAlmostEqual(float(r.retention_rate),25.0)
    def test_ml_target_and_no_future_features(self):
        rows={r.user_session:r for r in ml_examples(self.events).collect()}
        self.assertNotIn('c',rows) # purchase already happened at first event
        self.assertEqual(rows['a'].label,1.0)
        self.assertEqual(rows['a'].is_cart,0.0)
        self.assertEqual(rows['b'].label,0.0)
        self.assertEqual(rows['d'].label,1.0) # purchase without cart still counts for ML
        self.assertNotIn('total_purchases',rows['a'].asDict())
    def test_user_split(self):
        tr,te=split_users(ml_examples(self.events))
        self.assertEqual(tr.select('user_id').intersect(te.select('user_id')).count(),0)

if __name__=='__main__': unittest.main()
