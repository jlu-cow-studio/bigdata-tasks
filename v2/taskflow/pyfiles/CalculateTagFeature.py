#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("CalculateTagFeature")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "1")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[2]:


from datetime import datetime

date_string = datetime.today().strftime('%Y-%m-%d')


# In[5]:


user_tag_feature = spark.sql(f'''
with all_tags as(
    select
        distinct tag_id
    from
        tag_ods
    where
        date = '{date_string}'
), all_users as(
    select
        distinct uid as user_id
    from
        user_ods
    where
        date = '{date_string}'
), tag_user_cross as(
    select
        tag_id,
        user_id
    from
        all_tags,
        all_users
)
select
    a.user_id,
    a.tag_id,
    if(b.user_id is null, 0, 1) as has,
    '{date_string}' as date
from
    tag_user_cross a
left join
    user_tag_ods b on a.user_id = b.user_id and a.tag_id = b.tag_id
''')
user_tag_feature.write.mode("overwrite").partitionBy("date").saveAsTable("user_tag_feature")


# In[8]:


item_tag_feature = spark.sql(f'''
with all_tags as(
    select
        distinct tag_id
    from
        tag_ods
    where
        date = '{date_string}'
), all_items as(
    select
        distinct id as item_id
    from
        item_ods
    where
        date = '{date_string}'
), tag_item_cross as(
    select
        tag_id,
        item_id
    from
        all_tags,
        all_items
)
select
    a.item_id,
    a.tag_id,
    if(b.item_id is null, 0, 1) as has,
    '{date_string}' as date
from
    tag_item_cross a
left join
    item_tag_ods b on a.item_id = b.item_id and a.tag_id = b.tag_id and b.date = '{date_string}'
''')
item_tag_feature.write.mode("overwrite").partitionBy("date").saveAsTable("item_tag_feature")


# In[9]:


spark.stop()


# In[ ]:




