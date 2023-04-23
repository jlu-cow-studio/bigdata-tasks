#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("item_inverse_index")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "6")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[8]:


import redis
def send_inverse_index(item):
    pool = redis.ConnectionPool(host='cowstudio.wayne-lee.cn',port=3002,password='cowstudio', decode_responses=True)
    redis_cli = redis.Redis(connection_pool=pool)
    key_word,mp = item
    redis_cli.zadd("item-inverse-index-"+key_word,mp)
    return f"item-inverse-index-{key_word}",mp


# In[6]:


# define map functions 
from datetime import datetime, timedelta

today_string = datetime.today().strftime('%Y-%m-%d')
print(today_string)


# In[9]:


item_inverse_index = spark.sql(f'''
select
    key_word,
    map_from_arrays(collect_list(item_id),collect_list(tfidf)) as mp
from
    item_word_tfidf
where
    date = '{today_string}'
group by
    key_word
''').rdd

print(item_inverse_index.take(1))
item_inverse_index.map(send_inverse_index).collect()


# In[ ]:




