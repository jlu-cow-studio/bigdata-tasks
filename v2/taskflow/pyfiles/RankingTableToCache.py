#!/usr/bin/env python
# coding: utf-8

# In[2]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("RankingTableToCache")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "2")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[3]:


from datetime import datetime

date_string = datetime.today().strftime('%Y-%m-%d')


# In[4]:


from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DoubleType

# 定义schema
schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("rn", IntegerType(), True),
    StructField("date", StringType(), True)
])

# 创建一个空的DataFrame
spark.createDataFrame([],schema).write.mode("overwrite").partitionBy("date").saveAsTable("item_top_n")
spark.createDataFrame([],schema).write.mode("overwrite").partitionBy("date").saveAsTable("item_bottom_m")


# In[5]:


N = 80
spark.sql(f'''
select
    *
from
    item_score_rank
where
    date = '{date_string}'
and
    rn <= {N}
''').write.mode("overwrite").partitionBy("date").saveAsTable("item_top_n")


# In[10]:


M = 200
spark.sql(f'''
with max_rn as(
    select
        category,
        if(max(rn) <= {M},{M},max(rn)) as mx_rn
    from
        item_score_rank
    where
        date = '{date_string}'
    group by 
        category
)
select
    a.*
from
    item_score_rank a
left join
    max_rn b on a.category = b.category
where
    b.mx_rn - a.rn <= {M}
''').write.mode("overwrite").partitionBy("date").saveAsTable("item_bottom_m")


# In[11]:


spark.stop()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




