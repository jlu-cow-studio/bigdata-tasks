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
spark = SparkSession.builder     .appName("user_item_action_matrix")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "3")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[7]:


# define map functions 
from datetime import datetime, timedelta

today_string = datetime.today().strftime('%Y-%m-%d')
history_string = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
print(today_string)
print(history_string)

time_factor = 0.9


# In[8]:


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


# In[9]:


user_item_action_matrix_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("date", StringType(), True)
])
spark.createDataFrame([],user_item_action_matrix_schema).createOrReplaceTempView("user_item_action_matrix")


# In[23]:


user_item_action_matrix = spark.sql(f'''
with item_all as(
    select
        distinct id as item_id
    from
        item_ods
), user_all as(
    select
        distinct uid as user_id
    from
        user_ods
),history_score as(
    select
        a.item_id,
        b.user_id,
        if(c.item_id is null, 0, c.score) as score
    from
        item_all a
    left join
        user_all b
    left join
        user_item_action_matrix c on a.item_id = c.item_id and b.user_id = c.user_id and c.date = '{history_string}'   
), today_score as(
    select
        a.item_id,
        b.user_id,
        sum(if(d.score is null, 0,d.score)) as score
    from
        item_all a
    left join 
        user_all b
    left join
        event_ods c on a.item_id = c.item_id and b.user_id = c.user_id and c.timestamp = '{today_string}'
    left join
        event_score d on c.event_type = d.event_type
    group by
        a.item_id,
        b.user_id
    order by
        score desc
)
select
    a.item_id,
    b.user_id,
    c.score * {time_factor} + d.score as score,
    '{today_string}' as date
from
    item_all a
left join
    user_all b
left join
    history_score c on a.item_id = c.item_id and b.user_id = c.user_id
left join
    today_score d on a.item_id = d.item_id and b.user_id = d.user_id
order by
    score desc
''')
user_item_action_matrix.show()


# In[24]:


user_item_action_matrix.write.mode("overwrite").partitionBy("date").saveAsTable("user_item_action_matrix")


# In[25]:


spark.sql("show tables").show()


# In[26]:


spark.stop()
mydb.close()


# In[ ]:




