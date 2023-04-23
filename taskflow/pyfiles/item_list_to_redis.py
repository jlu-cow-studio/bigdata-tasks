#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("item_list_to_redis")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "2")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[2]:


import redis
def send_hot_list(item):
    pool = redis.ConnectionPool(host='cowstudio.wayne-lee.cn',port=3002,password='cowstudio', decode_responses=True)
    redis_cli = redis.Redis(connection_pool=pool)
    item_id,category,score,rn,date = item
    redis_cli.zadd("item-hot-list-"+category,{item_id:score})
    return f"item-hot-list-{category}",item_id,score
def send_fresh_list(item):
    pool = redis.ConnectionPool(host='cowstudio.wayne-lee.cn',port=3002,password='cowstudio', decode_responses=True)
    redis_cli = redis.Redis(connection_pool=pool)
    item_id,category,score,rn,date = item
    redis_cli.zadd("item-fresh-list-"+category,{item_id:score})
    return f"item-fresh-list-{category}",item_id,score


# In[3]:


# define map functions 
from datetime import datetime, timedelta

today_string = datetime.today().strftime('%Y-%m-%d')
print(today_string)


# In[4]:


item_hot_list = spark.sql(f'''
select
    *
from
    item_hot_list
where
    date = '{today_string}'
''').rdd

print(item_hot_list.take(1))
item_hot_list.map(send_hot_list).collect()


# In[5]:


item_fresh_list = spark.sql(f'''
select
    *
from
    item_fresh_list
where
    date = '{today_string}'
''').rdd

print(item_fresh_list.take(1))
item_fresh_list.map(send_fresh_list).collect().task(10)


# In[6]:


spark.stop()


# In[ ]:




