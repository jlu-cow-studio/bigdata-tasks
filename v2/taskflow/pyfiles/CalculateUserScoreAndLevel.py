#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("CalculateUserScoreAndLevel")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "3")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[2]:


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


# In[4]:


# define map functions 
from datetime import datetime, timedelta

today_string = datetime.today().strftime('%Y-%m-%d')
history_string = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
print(today_string)
print(history_string)

time_factor = 0.9


# In[12]:


user_score = spark.sql(f"""
with all_users as(
    select
        distinct user_id
    from
        user_ods
), history_user_score as(
    select
        a.user_id,
        if(b.user_id is null,0,b.score) as score
    from
        all_users a
    left_join
        user_score_and_rec_level b on a.user_id = b.user_id
), today_user_score as(
    select
        a.user_id,
        sum(if(c.score is null,0,c.score)) as score
    from
        all_users a
    left join
        event_ods b on a.user_id = b.user_id and b.timestamp = '{today_string}'
    left join
        event_score c on b.event_type = c.event_type
    group by
        a.user_id
    order by
        score desc
)
select 
    a.user_id,
    b.score + c.score as score,
    '{today_string}' as date
from
    all_users a
left_join
    history_user_score b on a.user_id = b.user_id
left_join
    today_user_score c on a.user_id = c.user_id
order by
    score desc
""")
user_score.show()


# In[14]:


user_score_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("date", StringType(), True)
])
user_score = spark.createDataFrame([],user_score_schema)
spark.createDataFrame([],user_score_schema).createOrReplaceTempView("user_score_and_rec_level")


# In[17]:


user_score.write.mode("overwrite").partitionBy("date").saveAsTable("user_score_and_rec_level")


# In[16]:


spark.sql("show tables").show()


# In[ ]:


spark.stop()


# In[ ]:




