#!/usr/bin/env python
# coding: utf-8

# In[1]:


# get spark session, 2g mem per executor
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os

# set python env
os.environ['PYSPARK_PYTHON'] = "/opt/conda3/envs/lab2/bin/python"
spark = SparkSession.builder     .appName("CalculateCattleProdActionMatrix")     .master("spark://node01:10077")     .enableHiveSupport()    .config("spark.driver.memory", "2g")     .config("spark.executor.memory", "2g")     .config("spark.cores.max", "1")     .config("spark.sql.shuffle.partitions", "12")     .config("spark.sql.autoBroadcastJoinThreshold", "-1")     .getOrCreate()

sc = spark.sparkContext


# In[2]:


from datetime import datetime

date_string = datetime.today().strftime('%Y-%m-%d')
time_factor = 0.9


# In[3]:


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


# In[13]:


from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DoubleType

# 定义schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("val", DoubleType(), True),
    StructField("date", StringType(), True)
])

# 创建一个空的DataFrame
spark.createDataFrame([],schema).createOrReplaceTempView("cattle_prod_user_action_matrix")


# In[14]:


cattle_prod_action = spark.sql(f'''
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
        category = 'cattle_product'
), user_item_cross as(
    select
        user_id,
        item_id
    from
        all_users,
        all_items
), history_action as(
    select
        a.user_id,
        a.item_id,
        if(b.user_id is null,0,b.val) as val
    from
        user_item_cross a
    left join
        cattle_prod_user_action_matrix b on a.user_id = b.user_id and a.item_id = b.item_id and b.date = '{date_string}'
), today_action as(
    select
        a.item_id,
        a.user_id,
        sum(if(c.score is null,0,c.score)) as val
    from
        user_item_cross a
    left join
        event_ods b on a.user_id = b.user_id and a.item_id = b.item_id and b.timestamp = '{date_string}'
    left join
        event_score c on b.event_type = c.event_type
    group by
        a.item_id, a.user_id
    order by 
        val desc
)
select
    a.user_id,
    a.item_id,
    b.val * {time_factor} + c.val as val,
    '{date_string}' as date
from
    user_item_cross a
left join
    history_action b on a.user_id = b.user_id and a.item_id = b.item_id
left join
    today_action c on a.user_id = c.user_id and a.item_id = c.item_id
''')
cattle_prod_action.show()


# In[ ]:


cattle_prod_action.write.mode("overwrite").partitionBy("date").saveAsTable("cattle_prod_user_action_matrix")


# In[ ]:


spark.stop()

