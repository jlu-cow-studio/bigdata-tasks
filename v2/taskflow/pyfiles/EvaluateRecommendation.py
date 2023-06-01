#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("EvaluateRecommendation")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "2")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[2]:


from datetime import datetime

date_string = datetime.today().strftime('%Y-%m-%d')


# In[3]:


from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DoubleType

# 定义schema
schema = StructType([
    StructField("item_category", IntegerType(), True),
    StructField("rec_algo", StringType(), True),
    StructField("precision", DoubleType(), True),
    StructField("recall", DoubleType(), True),
    StructField("f1", DoubleType(), True),
    StructField("date", StringType(), True)
])

# 创建一个空的DataFrame
spark.createDataFrame([],schema).write.mode("overwrite").partitionBy("date").saveAsTable("recommendation_evaluate_result")


# In[6]:


from pyspark.sql.types import StructType, StructField, StringType

rec_algo_list = [
    ('content_based',),
    ('list_based',),
    ('cf_based',)
]

schema = StructType([
    StructField("rec_algo", StringType(), True)
])

event_score_df = spark.createDataFrame(rec_algo_list, schema)
event_score_df.show()
event_score_df.createOrReplaceTempView("item_rec_algo")


# In[7]:


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
category_list = [
    ('cattle_product',),
    ('twitter',)
]

schema = StructType([
    StructField("category", StringType(), True),
])

event_score_df = spark.createDataFrame(category_list, schema)
event_score_df.show()
event_score_df.createOrReplaceTempView("item_category")


# In[11]:


evaluate_record = spark.sql(f'''
with item_category as(
    select
        distinct id as item_id,
        category
    from
        item_ods
    where
        date = '{date_string}'
),today_action as(
    select
        user_id,
        item_id
    from
        event_ods
    where
        timestamp = '{date_string}'
    group by
        user_id,
        item_id
), today_action_with_category(
    select
        a.user_id,
        a.item_id,
        b.category
    from
        today_action a
    left join
        item_category b on a.item_id = b.item_id
),today_feed as(
    select
        user_id,
        item_id,
        rec_algo
    from
        feed_ods
    where
        date = '{date_string}'
), category_rec_algo_cross as(
    select
        rec_algo,
        category
    from
        item_rec_algo,
        item_category
), eval_precision_recall(
    select
        a.category,
        a.rec_algo,
        sum(if(c.item_id is not null and b.item_id is not null,1,0))/sum(if(c.item_id is not null,1,0)) as precision,
        sum(if(c.item_id is not null and b.item_id is not null,1,0))/sum(if(b.item_id is not null,1,0)) as recall
    from
        category_rec_algo_cross a
    left join
        today_action_with_category b on a.category = b.category
    left join
        today_feed c on a.rec_algo = c.rec_algo
    group by
        a.category, a.rec_algo
)
select
    category,
    rec_algo,
    precision,
    recall,
    2*precision*recall / (precision + recall) as f1,
    '{date_string}' as date
from
    eval_precision_recall
''')


# In[12]:


evaluate_record.write.mode("overwrite").partitionBy("date").saveAsTable("recommendation_evaluate_result")


# In[13]:


spark.stop()


# In[ ]:




