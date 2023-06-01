#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("ContentBasedRecToCache")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "1")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[2]:


from datetime import datetime

date_string = datetime.today().strftime('%Y-%m-%d')


# In[3]:


import redis
def cache_cattle_prod(row):
    pool = redis.ConnectionPool(host='cowstudio.wayne-lee.cn',port=3002,password='cowstudio', decode_responses=True)
    redis_cli = redis.Redis(connection_pool=pool)
    user_id, item_list = row
    result = redis_cli.lset(f'content_based_cattle_prod_user_{user_id}',item_list)
    return f'content_based_cattle_prod_user_{user_id}',result

def cache_vet_twitte(row):
    pool = redis.ConnectionPool(host='cowstudio.wayne-lee.cn',port=3002,password='cowstudio', decode_responses=True)
    redis_cli = redis.Redis(connection_pool=pool)
    user_id, item_list = row
    result = redis_cli.lset(f'content_based_vet_twitte_user_{user_id}',item_list)
    return f'content_based_vet_twitte_user_{user_id}',result
        


# In[4]:


cattle_prod_list = spark.sql(f'''
select
    user_id,
    collect_list(item_id) over (partition by user_id order by sim desc) as item_list
from
    cattle_prod_user_item_sim
where
    date = '{date_string}'
''').rdd
cattle_prod_list.map(cache_cattle_prod).collect()


# In[5]:


vet_twitte_list = spark.sql(f'''
select
    user_id,
    collect_list(item_id) over (partition by user_id order by sim desc) as item_list
from
    vet_twitte_user_item_sim
where
    date = '{date_string}'
''').rdd
vet_twitte_list.map(cache_vet_twitte).collect()


# In[6]:


spark.stop()

