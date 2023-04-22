#!/usr/bin/env python
# coding: utf-8

# In[1]:


# mysql connection
import mysql.connector

mydb = mysql.connector.connect(
  host="cowstudio.wayne-lee.cn",
  user="cowstudio",
  password="cowstudio_2119",
  database="cowstudio"
)


# In[2]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("item_scores")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "3")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[3]:


# define map functions 
from datetime import datetime, timedelta

today_string = datetime.today().strftime('%Y-%m-%d')
history_string = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
print(today_string)
print(history_string)

time_factor = 0.9


# In[4]:


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
event_score_data = [
    ('view', 1.0),
    ('click', 4.0),
    ('long_view', 7.0),
    ('add_to_favorites', 20.0),
    ('purchase', 30.0),
    ('search_view', 5.0),
    ('search_click', 10.0)
]

schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("score", DoubleType(), True)
])

event_score_df = spark.createDataFrame(event_score_data, schema)
event_score_df.show()
event_score_df.createOrReplaceTempView("event_score")


# In[5]:


item_score_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("date", StringType(), True)
])
spark.createDataFrame([],item_score_schema).createOrReplaceTempView("item_score")


# In[6]:


item_score = spark.sql(f'''
with item_all as(
    select
        distinct id as item_id
    from
        item_ods
), history_item_score as(
    select
        a.item_id,
        if(b.item_id is null, 0, b.score) as score
    from
        item_all a
    left join
        item_score b on a.item_id = b.item_id
), today_item_score as(
    select
        a.item_id,
        sum(if(c.score is null, 0,c.score)) as score
    from
        item_all a
    left join
        event_ods b on a.item_id = b.item_id and b.timestamp = '{today_string}'
    left join
        event_score c on b.event_type = c.event_type
    group by
        a.item_id
    order by
        score desc
)
select
    a.item_id,
    b.score * {time_factor} + c.score as score,
    '{today_string}' as date
from
    item_all a
left join
    history_item_score b on a.item_id = b.item_id
left join
    today_item_score c on a.item_id = c.item_id
order by
    score desc
''')
item_score.show()


# In[7]:


spark.sql("drop table if exists item_score").show()


# In[8]:


item_score.write.mode("overwrite").saveAsTable("item_score")


# In[9]:


spark.sql("show tables").show()


# In[10]:


spark.stop()
mydb.close()


# In[ ]:




