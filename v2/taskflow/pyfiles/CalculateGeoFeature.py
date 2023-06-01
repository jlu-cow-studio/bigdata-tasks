#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("CalculateGeoFeature")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "1")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[2]:


from datetime import datetime

date_string = datetime.today().strftime('%Y-%m-%d')


# In[3]:


user_geo_feature = spark.sql(f'''
select
    uid as user_id,
    province,
    city,
    district,
    '{date_string}' as date
from
    user_ods
where
    date = '{date_string}'
''')
user_geo_feature.write.mode("overwrite").partitionBy("date").saveAsTable("user_geo_feature")


# In[4]:


item_geo_feature = spark.sql(f'''
select
    id as item_id,
    province,
    city,
    district,
    '{date_string}' as date
from
    item_ods
where
    date = '{date_string}'
''')
item_geo_feature.write.mode("overwrite").partitionBy("date").saveAsTable("item_geo_feature")


# In[5]:


spark.stop()


# In[ ]:




