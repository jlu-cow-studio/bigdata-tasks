#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("CalculateVetTwitteTextFeature")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "1")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[2]:


from datetime import datetime

date_string = datetime.today().strftime('%Y-%m-%d')


# In[3]:


#注册余弦相似度udf
from pyspark.sql.types import FloatType
import numpy as np

def cosine_similarity(user_vec, item_vec):
    return np.dot(user_vec, item_vec) / (np.linalg.norm(user_vec) * np.linalg.norm(item_vec))

spark.udf.register("cos_sim", cosine_similarity, FloatType())


# In[4]:


w_tag = 3
w_title = 4
w_content = 7 


# In[5]:


from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DoubleType

# 定义schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("sim", DoubleType(), True),
    StructField("date", StringType(), True)
])

# 创建一个空的DataFrame
spark.createDataFrame([],schema).createOrReplaceTempView("vet_twitte_user_item_sim")


# In[7]:


user_item_sim = spark.sql(f'''
with all_users as(
    select
        uid as user_id
    from
        user_ods
    where
        date = '{date_string}'
), all_items as(
    select
        id as item_id
    from
        item_ods
    where
        date = '{date_string}'
    and 
        category = 'twitte'
), user_item_cross as(
    select
        user_id,
        item_id
    from
        all_users,
        all_items
), tag_sim as(
    select
        a.user_id,
        a.item_id,
        sum(b.has * c.has)/sum(1) as tag_sim
    from
        user_item_cross a
    left join
        user_tag_feature b on a.user_id = b.user_id and b.date = '{date_string}'
    left join
        item_tag_feature c on a.item_id = c.item_id and c.date = '{date_string}'
    group by
        a.user_id,a.item_id
), title_user_tfidf_vec as(
    select
        user_id,
        collect_list(val) over (partition by user_id order by key_word) as user_vec
    from
        vet_twitte_title_user_text_feature
    where
        date = '{date_string}'
), title_item_tfidf_vec as(
    select
        item_id,
        collect_list(val) over (partition by item_id order by key_word) as item_vec
    from
        vet_twitte_title_item_text_feature
    where
        date = '{date_string}'
), title_text_sim as(
    select
        a.user_id,
        a.item_id,
        cos_sim(b.user_vec,c.item_vec) as title_sim
    from
        user_item_cross a
    left join
        title_user_tfidf_vec b on a.user_id = b.user_id
    left join
        title_item_tfidf_vec c on a.item_id = c.item_id
), content_user_tfidf_vec as(
    select
        user_id,
        collect_list(val) over (partition by user_id order by key_word) as user_vec
    from
        vet_twitte_content_user_text_feature
    where
        date = '{date_string}'
), content_item_tfidf_vec as(
    select
        item_id,
        collect_list(val) over (partition by item_id order by key_word) as item_vec
    from
        vet_twitte_content_item_text_feature
    where
        date = '{date_string}'
), content_text_sim as(
    select
        a.user_id,
        a.item_id,
        cos_sim(b.user_vec,c.item_vec) as content_sim
    from
        user_item_cross a
    left join
        content_user_tfidf_vec b on a.user_id = b.user_id
    left join
        content_item_tfidf_vec c on a.item_id = c.item_id
)
select
    a.user_id,
    a.item_id,
    (b.tag_sim * {w_tag} + c.title_sim * {w_title} + d.content_sim) / ({w_tag} + {w_title} + {w_content}) as sim,
    '{date_string}' as date
from
    user_item_cross a
left join
    tag_sim b on a.user_id = b.user_id and a.item_id = b.item_id
left join
    title_text_sim c on a.user_id = c.user_id and a.item_id = c.item_id
left join
    content_text_sim d on a.user_id = d.user_id and a.item_id = d.item_id
''')


# In[8]:


user_item_sim.write.mode("overwrite").partitionBy("date").saveAsTable("vet_twitte_user_item_sim")


# In[9]:


spark.stop()


# In[ ]:




